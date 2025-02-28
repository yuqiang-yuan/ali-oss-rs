use crate::{
    presign::SignedOssRequest,
    presign_common::{build_presign_get_request, PresignGetOptions},
    request::OssRequest,
    util::{self, get_iso8601_date_time_string},
};

use super::Client;

impl Client {
    /// Presign URL for GET request without any additional headers supported, for brower mostly
    pub fn presign_url<S1, S2>(&self, bucket_name: S1, object_key: S2, options: PresignGetOptions) -> String
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let mut request = build_presign_get_request(bucket_name.as_ref(), object_key.as_ref(), &options);

        let date_time_string = request.query.get("x-oss-date").unwrap().clone();
        let date_string = &date_time_string[..8];

        let credential = format!("{}/{}/{}/oss/aliyun_v4_request", self.access_key_id, date_string, self.region);

        request = request.add_query("x-oss-credential", &credential);

        if let Some(s) = &self.sts_token {
            request = request.add_query("x-oss-security-token", s);
        }

        let canonical_request = request.build_canonical_request();

        let canonical_request_hash = util::sha256(canonical_request.as_bytes());

        let string_to_sign = format!(
            "OSS4-HMAC-SHA256\n{}\n{}/{}/oss/aliyun_v4_request\n{}",
            date_time_string,
            date_string,
            self.region,
            hex::encode(&canonical_request_hash)
        );

        let sig = self.calculate_signature(&string_to_sign, date_string);

        request = request.add_query("x-oss-signature", &sig);

        let uri = request.build_request_uri();
        let query_string = request.build_canonical_query_string();

        let domain_name = if request.bucket_name.is_empty() {
            format!("{}://{}{}", self.scheme, self.endpoint, uri)
        } else {
            format!("{}://{}.{}{}", self.scheme, request.bucket_name, self.endpoint, uri)
        };

        if query_string.is_empty() {
            domain_name
        } else {
            format!("{}?{}", domain_name, query_string)
        }
    }

    pub fn presign_raw_request(&self, mut oss_request: OssRequest) -> SignedOssRequest {
        let date_header = "x-oss-date".to_string();
        {
            oss_request.headers_mut().entry(date_header.clone()).or_insert(get_iso8601_date_time_string());
        }

        let date_time_string = oss_request.headers.get(&date_header).unwrap().to_string();
        let date_string = &date_time_string[..8];

        if let Some(s) = &self.sts_token {
            if !oss_request.headers.contains_key("x-oss-security-token") {
                oss_request = oss_request.add_header("x-oss-security-token", s);
            }
        }
        let additional_headers = oss_request.build_additional_headers();
        let string_to_sign = oss_request.build_string_to_sign(&self.region);

        log::debug!("string to sign: \n--------\n{}\n--------", string_to_sign);

        let sig = self.calculate_signature(&string_to_sign, date_string);

        log::debug!("signature: {}", sig);

        let auth_string = format!(
            "OSS4-HMAC-SHA256 Credential={}/{}/{}/oss/aliyun_v4_request,{}Signature={}",
            self.access_key_id,
            date_string,
            self.region,
            if additional_headers.is_empty() {
                "".to_string()
            } else {
                format!("{},", additional_headers)
            },
            sig
        );

        oss_request = oss_request.add_header("authorization", &auth_string);

        let uri = oss_request.build_request_uri();
        let query_string = oss_request.build_canonical_query_string();

        let url = if oss_request.bucket_name.is_empty() {
            format!("{}://{}{}", self.scheme, self.endpoint, uri)
        } else {
            format!("{}://{}.{}{}", self.scheme, oss_request.bucket_name, self.endpoint, uri)
        };

        let url = if query_string.is_empty() { url } else { format!("{}?{}", url, query_string) };

        SignedOssRequest {
            url,
            headers: oss_request.headers,
        }
    }
}
