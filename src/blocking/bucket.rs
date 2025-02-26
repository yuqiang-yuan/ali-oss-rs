use crate::{
    bucket_common::{
        build_list_buckets_request, build_list_objects_request, build_put_bucket_request, extract_bucket_location, BucketDetail, BucketStat,
        ListBucketsOptions, ListBucketsResult, ListObjectsOptions, ListObjectsResult, PutBucketConfiguration, PutBucketOptions,
    },
    error::Error,
    request::{OssRequest, RequestMethod},
    util::validate_bucket_name,
    Result,
};

use super::Client;

pub trait BucketOperations {
    fn put_bucket<S: AsRef<str>>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> Result<()>;
    fn list_buckets(&self, options: Option<ListBucketsOptions>) -> Result<ListBucketsResult>;
    fn get_bucket_info<S: AsRef<str>>(&self, bucket_name: S) -> Result<BucketDetail>;
    fn get_bucket_location<S: AsRef<str>>(&self, bucket_name: S) -> Result<String>;
    fn get_bucket_stat<S: AsRef<str>>(&self, bucket_name: S) -> Result<BucketStat>;
    fn list_objects<S: AsRef<str>>(&self, bucket_name: S, options: Option<ListObjectsOptions>) -> Result<ListObjectsResult>;
    fn delete_bucket<S: AsRef<str>>(&self, bucket_name: S) -> Result<()>;
}

impl BucketOperations for Client {
    fn put_bucket<S: AsRef<str>>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> Result<()> {
        if !validate_bucket_name(bucket_name.as_ref()) {
            return Err(Error::Other(format!(
                "invalid bucket name: {}. please see the official document for more details",
                bucket_name.as_ref()
            )));
        }

        let request_builder = build_put_bucket_request(bucket_name.as_ref(), &config, &options)?;

        self.do_request::<()>(request_builder)?;

        Ok(())
    }

    fn list_buckets(&self, options: Option<ListBucketsOptions>) -> Result<ListBucketsResult> {
        let request_builder = build_list_buckets_request(&options);

        let (_, content) = self.do_request::<String>(request_builder)?;

        ListBucketsResult::from_xml(&content)
    }

    fn get_bucket_info<S: AsRef<str>>(&self, bucket_name: S) -> Result<BucketDetail> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request_builder = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("bucketInfo", "");

        let (_, content) = self.do_request::<String>(request_builder)?;

        BucketDetail::from_xml(&content)
    }

    fn get_bucket_location<S: AsRef<str>>(&self, bucket_name: S) -> Result<String> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request_builder = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("location", "");

        let (_, content) = self.do_request::<String>(request_builder)?;

        extract_bucket_location(content.as_str())
    }

    fn get_bucket_stat<S: AsRef<str>>(&self, bucket_name: S) -> Result<BucketStat> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request_builder = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("stat", "");

        let (_, content) = self.do_request::<String>(request_builder)?;

        BucketStat::from_xml(&content)
    }

    fn list_objects<S: AsRef<str>>(&self, bucket_name: S, options: Option<ListObjectsOptions>) -> Result<ListObjectsResult> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request = build_list_objects_request(bucket_name, &options)?;

        let (_, content) = self.do_request::<String>(request)?;

        ListObjectsResult::from_xml(&content)
    }

    fn delete_bucket<S: AsRef<str>>(&self, bucket_name: S) -> Result<()> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request_builder = OssRequest::new().method(RequestMethod::Delete).bucket(bucket_name);

        self.do_request::<()>(request_builder)?;

        Ok(())
    }
}

#[cfg(all(test, feature = "blocking"))]
mod test_bucket_blocking {
    use std::sync::Once;

    use crate::{
        blocking::{bucket::BucketOperations, Client},
        bucket_common::{ListBucketsOptions, ListObjectsOptions, ListObjectsOptionsBuilder},
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    fn setup_comp() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::from_filename(".env.comp").unwrap();
        });
    }

    #[test]
    fn test_list_buckets_blocking() {
        setup();

        let client = Client::from_env();
        let result = client.list_buckets(None);
        assert!(result.is_ok());
        let buckets = result.unwrap().buckets;
        assert!(!buckets.is_empty());
    }

    #[test]
    fn test_list_objects_1_blocking() {
        setup();

        let client = Client::from_env();

        let response = client.list_objects("mi-dev-public", None);
        assert!(response.is_ok());

        let result = response.unwrap();
        log::debug!("{:?}", result);
    }

    #[test]
    fn test_list_objects_2_blocking() {
        setup();

        let client = Client::from_env();

        let options = ListObjectsOptions {
            delimiter: Some('/'),
            prefix: Some("yuanyu-test/".to_string()),
            fetch_owner: Some(true),
            ..Default::default()
        };

        let response = client.list_objects("mi-dev-public", Some(options));
        assert!(response.is_ok());

        let result = response.unwrap();
        log::debug!("{:?}", result);
    }

    #[test]
    fn test_list_buckets_with_options_blocking() {
        setup_comp();
        let client = crate::blocking::Client::from_env();

        let options = ListBucketsOptions {
            max_keys: Some(10),
            ..Default::default()
        };

        let response = client.list_buckets(Some(options));
        log::debug!("list buckets, page1: {:#?}", response);

        assert!(response.is_ok());

        let ret = response.unwrap();
        assert_eq!(10, ret.buckets.len());

        assert!(ret.next_marker.is_some());
        assert!(ret.is_truncated);

        let options = ListBucketsOptions {
            max_keys: Some(10),
            marker: ret.next_marker,
            ..Default::default()
        };

        let response = client.list_buckets(Some(options));
        log::debug!("list buckets, page2: {:#?}", response);
        assert!(response.is_ok());

        let ret = response.unwrap();
        assert_eq!(9, ret.buckets.len());
    }

    #[test]
    fn test_list_objects_blocking_1() {
        setup_comp();
        let client = crate::blocking::Client::from_env();

        let options = ListObjectsOptionsBuilder::new().prefix("").delimiter('/').build();

        let response = client.list_objects("mi-dev-public", Some(options));
        assert!(response.is_ok());

        let result = response.unwrap();
        assert!(result.key_count > 0);
        assert_eq!(result.key_count, (result.common_prefixes.len() + result.contents.len()) as u64);
    }
}
