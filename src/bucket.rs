//! Basic bucket operations
use async_trait::async_trait;

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

#[async_trait]
pub trait BucketOperations {
    /// Create a new bucket
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putbucket>
    async fn put_bucket<S>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> Result<()>
    where
        S: AsRef<str> + Send;

    /// List buckets
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listbuckets>
    async fn list_buckets(&self, options: Option<ListBucketsOptions>) -> Result<ListBucketsResult>;

    /// Get bucket information
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketinfo>
    async fn get_bucket_info<S: AsRef<str> + Send>(&self, bucket_name: S) -> Result<BucketDetail>;

    /// Get bucket location
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketlocation>
    async fn get_bucket_location<S>(&self, bucket_name: S) -> Result<String>
    where
        S: AsRef<str> + Send;

    /// Get bucket statistics data
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketstat>
    async fn get_bucket_stat<S>(&self, bucket_name: S) -> Result<BucketStat>
    where
        S: AsRef<str> + Send;

    /// List objects in a bucket (V2)
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listobjectsv2>
    async fn list_objects<S>(&self, bucket_name: S, options: Option<ListObjectsOptions>) -> Result<ListObjectsResult>
    where
        S: AsRef<str> + Send;

    /// Delete a bucket
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletebucket>
    async fn delete_bucket<S>(&self, bucket_name: S) -> Result<()>
    where
        S: AsRef<str> + Send;
}

#[async_trait]
impl BucketOperations for crate::Client {
    /// Create a bucket.
    ///
    /// `bucket_name` constraint:
    ///
    /// - 3 to 63 characters length
    /// - only lower case ascii alphabets, numbers and hyphen (`-`) are allowed
    /// - not starts or ends with hyphen character
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putbucket>
    async fn put_bucket<S: AsRef<str> + Send>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> Result<()> {
        if !validate_bucket_name(bucket_name.as_ref()) {
            return Err(Error::Other(format!(
                "invalid bucket name: {}. please see the official document for more details",
                bucket_name.as_ref()
            )));
        }

        let request_builder = build_put_bucket_request(bucket_name.as_ref(), &config, &options)?;

        self.do_request::<()>(request_builder).await?;

        Ok(())
    }

    /// See official document for more details: <https://help.aliyun.com/zh/oss/developer-reference/listbuckets>
    async fn list_buckets(&self, options: Option<ListBucketsOptions>) -> Result<ListBucketsResult> {
        let request_builder = build_list_buckets_request(&options);

        let (_, content) = self.do_request::<String>(request_builder).await?;

        ListBucketsResult::from_xml(&content)
    }

    /// Delete a bucket. Only non-empty bucket can be deleted
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletebucket>
    async fn delete_bucket<S: AsRef<str> + Send>(&self, bucket_name: S) -> Result<()> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request_builder = OssRequest::new().method(RequestMethod::Delete).bucket(bucket_name);

        self.do_request::<()>(request_builder).await?;

        Ok(())
    }

    /// Get bucket info
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketinfo>
    async fn get_bucket_info<S: AsRef<str> + Send>(&self, bucket_name: S) -> Result<BucketDetail> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }
        let request_builder = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("bucketInfo", "");

        let (_, content) = self.do_request::<String>(request_builder).await?;

        BucketDetail::from_xml(&content)
    }

    /// Get bucket location
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketlocation>
    async fn get_bucket_location<S: AsRef<str> + Send>(&self, bucket_name: S) -> Result<String> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request_builder = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("location", "");

        let (_, content) = self.do_request::<String>(request_builder).await?;

        extract_bucket_location(content.as_str())
    }

    /// Get bucket statistics data
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketstat>
    async fn get_bucket_stat<S: AsRef<str> + Send>(&self, bucket_name: S) -> Result<BucketStat> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request_builder = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("stat", "");

        let (_, content) = self.do_request::<String>(request_builder).await?;

        BucketStat::from_xml(&content)
    }

    /// List objects in a bucket (V2)
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listobjectsv2>
    async fn list_objects<S: AsRef<str> + Send>(&self, bucket_name: S, options: Option<ListObjectsOptions>) -> Result<ListObjectsResult> {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request = build_list_objects_request(bucket_name, &options)?;

        let (_, content) = self.do_request::<String>(request).await?;

        ListObjectsResult::from_xml(&content)
    }
}

#[cfg(test)]
pub mod test_bucket_async {
    use std::sync::Once;

    use crate::{
        bucket::BucketOperations,
        bucket_common::{ListBucketsOptions, ListObjectsOptionsBuilder},
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

    #[tokio::test]
    async fn test_list_buckets_async() {
        setup();
        let client = crate::Client::from_env();

        let response = client.list_buckets(None).await;

        assert!(response.is_ok());

        let result = response.unwrap();
        assert!(!result.buckets.is_empty());

        let bucket = &result.buckets[0];
        assert!(!bucket.name.is_empty());

        log::debug!("{:?}", result);
    }

    #[tokio::test]
    async fn test_list_buckets_with_options_async() {
        setup_comp();
        let client = crate::Client::from_env();

        let options = ListBucketsOptions {
            max_keys: Some(10),
            ..Default::default()
        };

        let response = client.list_buckets(Some(options)).await;
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

        let response = client.list_buckets(Some(options)).await;
        log::debug!("list buckets, page2: {:#?}", response);
        assert!(response.is_ok());

        let ret = response.unwrap();
        assert_eq!(9, ret.buckets.len());
    }

    #[tokio::test]
    async fn test_list_objects_async_1() {
        setup_comp();
        let client = crate::Client::from_env();

        let options = ListObjectsOptionsBuilder::new().prefix("").delimiter('/').build();

        let response = client.list_objects("mi-dev-public", Some(options)).await;
        assert!(response.is_ok());

        let result = response.unwrap();
        assert!(result.key_count > 0);
        assert_eq!(result.key_count, (result.common_prefixes.len() + result.contents.len()) as u64);
    }
}
