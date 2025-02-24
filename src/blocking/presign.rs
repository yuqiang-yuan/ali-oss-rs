use crate::{
    presign_common::{build_presign_get_request, PresignGetOptions},
    util,
};

use super::Client;

/// Operations for presign request
pub trait PresignOperations {
    /// Presign URL for GET request without any additional headers supported, for brower mostly
    fn presign_url<S1, S2>(&self, bucket_name: S1, object_key: S2, options: PresignGetOptions) -> String
    where
        S1: AsRef<str>,
        S2: AsRef<str>;
}

impl PresignOperations for Client {
    /// Presign URL for GET request without any additional headers supported, for brower mostly
    fn presign_url<S1, S2>(&self, bucket_name: S1, object_key: S2, options: PresignGetOptions) -> String
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let mut request = build_presign_get_request(bucket_name.as_ref(), object_key.as_ref(), &options);

        let date_time_string = request.query.get("x-oss-date").unwrap().clone();
        let date_string = &date_time_string[..8];

        let credential = format!("{}/{}/{}/oss/aliyun_v4_request", self.access_key_id, date_string, self.region);
        // log::debug!("credential = {}", credential);
        request = request.add_query("x-oss-credential", &credential);

        if let Some(s) = &self.sts_token {
            request = request.add_query("x-oss-security-token", s);
        }

        let canonical_request = request.build_canonical_request();
        // log::debug!("canonical request = \n--------\n{}\n--------", canonical_request);

        let canonical_request_hash = util::sha256(canonical_request.as_bytes());

        let string_to_sign = format!(
            "OSS4-HMAC-SHA256\n{}\n{}/{}/oss/aliyun_v4_request\n{}",
            date_time_string,
            date_string,
            self.region,
            hex::encode(&canonical_request_hash)
        );

        // log::debug!("string to sign = \n--------\n{}\n--------", string_to_sign);

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
}
