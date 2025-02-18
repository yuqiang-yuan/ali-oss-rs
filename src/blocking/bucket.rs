use crate::{
    bucket_common::{
        build_list_buckets_request, build_list_objects_request, build_put_bucket_request, extract_bucket_location, BucketDetail, BucketStat,
        ListBucketsOptions, ListBucketsResult, ListObjectsOptions, ListObjectsResult, PutBucketConfiguration, PutBucketOptions,
    },
    error::{ClientError, ClientResult},
    request::{RequestBuilder, RequestMethod},
    util::validate_bucket_name,
};

use super::Client;

pub trait BucketOperations {
    fn put_bucket<S: AsRef<str>>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> ClientResult<()>;
    fn list_buckets(&self, options: Option<ListBucketsOptions>) -> ClientResult<ListBucketsResult>;
    fn get_bucket_info<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<BucketDetail>;
    fn get_bucket_location<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<String>;
    fn get_bucket_stat<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<BucketStat>;
    fn list_objects<S: AsRef<str>>(&self, bucket_name: S, options: Option<ListObjectsOptions>) -> ClientResult<ListObjectsResult>;
    fn delete_bucket<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<()>;
}

impl BucketOperations for Client {
    fn put_bucket<S: AsRef<str>>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> ClientResult<()> {
        if !validate_bucket_name(bucket_name.as_ref()) {
            return Err(ClientError::Error(format!(
                "invalid bucket name: {}. please see the official document for more details",
                bucket_name.as_ref()
            )));
        }

        let request_builder = build_put_bucket_request(bucket_name.as_ref(), &config, &options)?;

        self.do_request::<()>(request_builder)?;

        Ok(())
    }

    fn list_buckets(&self, options: Option<ListBucketsOptions>) -> ClientResult<ListBucketsResult> {
        let request_builder = build_list_buckets_request(&options);

        let (_, content) = self.do_request::<String>(request_builder)?;

        ListBucketsResult::from_xml(&content)
    }

    fn get_bucket_info<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<BucketDetail> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("bucketInfo", "");

        let (_, content) = self.do_request::<String>(request_builder)?;

        BucketDetail::from_xml(&content)
    }

    fn get_bucket_location<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<String> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("location", "");

        let (_, content) = self.do_request::<String>(request_builder)?;

        extract_bucket_location(content.as_str())
    }

    fn get_bucket_stat<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<BucketStat> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("stat", "");

        let (_, content) = self.do_request::<String>(request_builder)?;

        BucketStat::from_xml(&content)
    }

    fn list_objects<S: AsRef<str>>(&self, bucket_name: S, options: Option<ListObjectsOptions>) -> ClientResult<ListObjectsResult> {
        let request = build_list_objects_request(bucket_name.as_ref(), &options);

        let (_, content) = self.do_request::<String>(request)?;

        ListObjectsResult::from_xml(&content)
    }

    fn delete_bucket<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<()> {
        let request_builder = RequestBuilder::new().method(RequestMethod::Delete).bucket(bucket_name.as_ref());

        self.do_request::<()>(request_builder)?;

        Ok(())
    }
}

#[cfg(all(test, feature = "blocking"))]
mod test_bucket_blocking {
    use std::sync::Once;

    use crate::{
        blocking::{bucket::BucketOperations, Client},
        bucket_common::ListObjectsOptions,
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[test]
    fn test_list_buckets() {
        setup();

        let client = Client::from_env();
        let result = client.list_buckets(None);
        assert!(result.is_ok());
        let buckets = result.unwrap().buckets;
        assert!(!buckets.is_empty());
    }

    #[test]
    fn test_list_objects_1() {
        setup();

        let client = Client::from_env();

        let response = client.list_objects("mi-dev-public", None);
        assert!(response.is_ok());

        let result = response.unwrap();
        log::debug!("{:?}", result);
    }

    #[test]
    fn test_list_objects_2() {
        setup();

        let client = Client::from_env();

        let options = ListObjectsOptions {
            delimiter: Some("/".to_string()),
            prefix: Some("yuanyu-test/".to_string()),
            fetch_owner: Some(true),
            ..Default::default()
        };

        let response = client.list_objects("mi-dev-public", Some(options));
        assert!(response.is_ok());

        let result = response.unwrap();
        log::debug!("{:?}", result);
    }
}
