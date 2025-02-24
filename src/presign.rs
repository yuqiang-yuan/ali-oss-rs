//! Trait and implementation for pre-signing URL for OSS object

use crate::{
    presign_common::{build_presign_get_request, PresignGetOptions},
    util, Client,
};

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

#[cfg(test)]
mod test_presign {
    use std::sync::Once;

    use crate::{presign_common::PresignGetOptionsBuilder, Client};

    use super::PresignOperations;

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[test]
    fn test_presign_get() {
        setup();
        let mut client = Client::from_env();
        client.sts_token = Some("CAISxwJ1q6Ft5B2yfSjIr5f9HIzinaYU5aW7YW7hkEgMXOIZivDEgDz2IHhMe3hrAeAdt/8ynm5W5/8YlqRvRoRZAFbZdtc19ZhUqYV7k1p64Z7b16cNrbH4M0rxYkeJ8a2/SuH9S8ynCZXJQlvYlyh17KLnfDG5JTKMOoGIjpgVBbZ+HHPPD1x8CcxROxFppeIDKHLVLozNCBPxhXfKB0ca0WgVy0EHsPjjnZDDsUaG0wClkrNF9r6ceMb0M5NeW75kSMqw0eBMca7M7TVd8RAj9t0t1PQbqGyf7orEWAQLvUvXarDOlcxyJQlof+0gEqsBtv/4mO2pZn4uCH1WofwkHJa2M0y3LOjIqKNPCPy0kQiaW5zPmmUf8xF+B2jkMjleFVYxHi20FRRvRBH+B7IihB+kZCHJtjKuQEOXL4LhtgVe3+TBQQiAlbIagAFqSwN6zpF0i5Waph9q9Cqp19coGGSnf/a+Fqx9gXaznsA+F96x2wAmr8W+vlJB/5YK5csbQEpvdF88D4mKzOXZ5fAEuAqNt2wrh8UK4u1OV3mMMvst//drCIGSSFINz98EviqdRN8YZvUHVbI8usADoQ40wfSPMzIbztyFCW0l3CAA".to_string());

        let bucket = "yuanyq";
        let object = "rust-sdk-test/katex.zip";

        let options = PresignGetOptionsBuilder::new(3600).build();

        let url = client.presign_url(bucket, object, options);

        log::debug!("{}", url);
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
    }
}
