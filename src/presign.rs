//! Trait and implementation for pre-signing URL for OSS object

use std::collections::HashMap;

use crate::{
    presign_common::{build_presign_get_request, PresignGetOptions},
    request::OssRequest,
    util::{self, get_iso8601_date_time_string},
    Client,
};

/// All data for sending request to aliyun oss api after signature calculated
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct SignedOssRequest {
    /// The full url
    pub url: String,

    /// The request headers with calculated authorization value.
    pub headers: HashMap<String, String>,
}

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

    /// Presign a raw request, get the url and headers which contain calculated signature.
    /// So you can use the url and headers in other applications, frameworks or languages to complete the request.
    ///
    /// # Examples
    ///
    /// Get the presigned url and headers using `ali-oss-rs` crate.
    ///
    /// ```rust
    /// let client = Client::from_env();
    ///
    /// let object = format!("rust-sdk-test/{}.webp", Uuid::new_v4());
    ///
    /// let request = OssRequest::new()
    ///     .method(RequestMethod::Put)
    ///     .bucket("yuanyq")
    ///     .object(&object)
    ///     .add_header("content-type", "image/webp")
    ///     .add_header("content-length", "36958");
    ///
    /// let SignedOssRequest {url, headers} = client.presign_raw_request(request);
    /// log::debug!("{} {:#?}", url, headers);
    /// ```
    ///
    /// You will get the headers includes calculated authorization string.
    /// Then copy the url and headers to your javascript code which sending HTTP request using Axios:
    ///
    /// ```javascript
    /// const Axios = require("axios");
    /// const fs = require("fs");
    ///
    /// const axios = Axios.create();
    ///
    /// (async function() {
    ///     const filePath = "/home/yuanyq/Pictures/test-8.webp";
    ///     const fileStream = fs.createReadStream(filePath);
    ///
    ///     const response = await axios.request({
    ///         method: "PUT",
    ///         url: "https://yuanyq.oss-cn-beijing.aliyuncs.com/rust-sdk-test/xxxxx.webp",
    ///         headers: {
    ///             "content-type": "image/webp",
    ///             "authorization": "OSS4-HMAC-SHA256 Credential=LTAIxxxxxxpeA/20250228/cn-beijing/oss/aliyun_v4_request,Signature=xxxxx",
    ///             "x-oss-content-sha256": "UNSIGNED-PAYLOAD",
    ///             "x-sdk-client": "ali-oss-rs/0.1.3",
    ///             "x-oss-date": "20250228T074254Z",
    ///             "content-length": "36958",
    ///         },
    ///         data: fileStream
    ///     });
    ///     console.log(response);
    /// })();
    /// ```
    ///
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

#[cfg(all(test, feature = "blocking"))]
mod test_presign {
    use std::{str::FromStr, sync::Once};

    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    use uuid::Uuid;

    use crate::{
        presign::SignedOssRequest,
        presign_common::PresignGetOptionsBuilder,
        request::{OssRequest, RequestMethod},
        util::debug_blocking_request,
        Client,
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[test]
    fn test_presign_get_with_options() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/test-1.webp";

        let options = PresignGetOptionsBuilder::new(3600).process("style/test-img-process").build();

        let url = client.presign_url(bucket, object, options);

        log::debug!("{}", url);

        let response = reqwest::blocking::get(url);
        assert!(response.is_ok());
        assert_eq!(reqwest::StatusCode::OK, response.unwrap().status());
    }

    #[test]
    fn test_presign_raw_request() {
        setup();
        let client = Client::from_env();

        let request = OssRequest::new()
            .method(RequestMethod::Get)
            .bucket("yuanyq")
            .object("rust-sdk-test/20dcd5da-804f-406d-a921-3c3f08de04e3.jpg")
            .add_header("x-oss-expires", "3600")
            .add_query("x-oss-process", "style/test-img-process");

        let SignedOssRequest { url, headers } = client.presign_raw_request(request);
        log::debug!("{} {:#?}", url, headers);

        let mut req_headers = HeaderMap::new();
        headers.into_iter().for_each(|(k, v)| {
            req_headers.append(HeaderName::from_str(k.as_str()).unwrap(), HeaderValue::from_str(v.as_str()).unwrap());
        });

        let http_client = reqwest::blocking::Client::new();
        let req = http_client
            .request(reqwest::Method::GET, reqwest::Url::parse(url.as_str()).unwrap())
            .headers(req_headers)
            .build()
            .unwrap();

        debug_blocking_request(&req);

        let response = http_client.execute(req);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(reqwest::StatusCode::OK, response.status());
    }

    #[test]
    fn test_presign_raw_request_2() {
        setup();
        let client = Client::from_env();

        let object = format!("rust-sdk-test/{}.webp", Uuid::new_v4());

        let request = OssRequest::new()
            .method(RequestMethod::Put)
            .bucket("yuanyq")
            .object(&object)
            .add_header("content-type", "image/webp")
            .add_header("content-length", "36958");

        let SignedOssRequest { url, headers } = client.presign_raw_request(request);
        log::debug!("{} {:#?}", url, headers);
    }
}
