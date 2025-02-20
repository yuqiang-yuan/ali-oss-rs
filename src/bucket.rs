//! Basic bucket operations
use async_trait::async_trait;

use crate::{
    bucket_common::{
        build_list_buckets_request, build_list_objects_request, build_put_bucket_request, extract_bucket_location, BucketDetail, BucketStat,
        ListBucketsOptions, ListBucketsResult, ListObjectsOptions, ListObjectsResult, PutBucketConfiguration, PutBucketOptions,
    },
    error::{ClientError, ClientResult},
    request::{RequestBuilder, RequestMethod},
    util::validate_bucket_name,
};

#[async_trait]
pub trait BucketOperations {
    /// Create a new bucket
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putbucket>
    async fn put_bucket<S: AsRef<str> + Send>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> ClientResult<()>;

    /// List buckets
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listbuckets>
    async fn list_buckets(&self, options: Option<ListBucketsOptions>) -> ClientResult<ListBucketsResult>;

    /// Get bucket information
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketinfo>
    async fn get_bucket_info<S: AsRef<str> + Send>(&self, bucket_name: S) -> ClientResult<BucketDetail>;

    /// Get bucket location
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketlocation>
    async fn get_bucket_location<S: AsRef<str> + Send>(&self, bucket_name: S) -> ClientResult<String>;

    /// Get bucket statistics data
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketstat>
    async fn get_bucket_stat<S: AsRef<str> + Send>(&self, bucket_name: S) -> ClientResult<BucketStat>;

    /// List objects in a bucket (V2)
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listobjectsv2>
    async fn list_objects<S: AsRef<str> + Send>(&self, bucket_name: S, options: Option<ListObjectsOptions>) -> ClientResult<ListObjectsResult>;

    /// Delete a bucket
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletebucket>
    async fn delete_bucket<S: AsRef<str> + Send>(&self, bucket_name: S) -> ClientResult<()>;
}

#[async_trait]
impl BucketOperations for crate::Client {
    /// Create a bucket.
    ///
    /// `bucket_name` constraint:
    ///
    /// - 3 to 63 characters length
    /// - only lower case ascii, numbers and hyphen (`-`) are allowed
    /// - not starts or ends with hyphen character
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putbucket>
    async fn put_bucket<S: AsRef<str> + Send>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> ClientResult<()> {
        if !validate_bucket_name(bucket_name.as_ref()) {
            return Err(ClientError::Error(format!(
                "invalid bucket name: {}. please see the official document for more details",
                bucket_name.as_ref()
            )));
        }

        let request_builder = build_put_bucket_request(bucket_name.as_ref(), &config, &options)?;

        self.do_request::<()>(request_builder).await?;

        Ok(())
    }

    /// See official document for more details: <https://help.aliyun.com/zh/oss/developer-reference/listbuckets>
    async fn list_buckets(&self, options: Option<ListBucketsOptions>) -> ClientResult<ListBucketsResult> {
        let request_builder = build_list_buckets_request(&options);

        let (_, content) = self.do_request::<String>(request_builder).await?;

        ListBucketsResult::from_xml(&content)
    }

    /// Delte a bucket. Only non-empty bucket can be deleted
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletebucket>
    async fn delete_bucket<S: AsRef<str> + Send>(&self, bucket_name: S) -> ClientResult<()> {
        let request_builder = RequestBuilder::new().method(RequestMethod::Delete).bucket(bucket_name.as_ref());

        self.do_request::<()>(request_builder).await?;

        Ok(())
    }

    /// Get bucket info
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketinfo>
    async fn get_bucket_info<S: AsRef<str> + Send>(&self, bucket_name: S) -> ClientResult<BucketDetail> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("bucketInfo", "");

        let (_, content) = self.do_request::<String>(request_builder).await?;

        BucketDetail::from_xml(&content)
    }

    /// Get bucket location
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketlocation>
    async fn get_bucket_location<S: AsRef<str> + Send>(&self, bucket_name: S) -> ClientResult<String> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("location", "");

        let (_, content) = self.do_request::<String>(request_builder).await?;

        extract_bucket_location(content.as_str())
    }

    /// Get bucket statistics data
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getbucketstat>
    async fn get_bucket_stat<S: AsRef<str> + Send>(&self, bucket_name: S) -> ClientResult<BucketStat> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("stat", "");

        let (_, content) = self.do_request::<String>(request_builder).await?;

        BucketStat::from_xml(&content)
    }

    /// List objects in a bucket (V2)
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listobjectsv2>
    async fn list_objects<S: AsRef<str> + Send>(&self, bucket_name: S, options: Option<ListObjectsOptions>) -> ClientResult<ListObjectsResult> {
        let request = build_list_objects_request(bucket_name.as_ref(), &options)?;

        let (_, content) = self.do_request::<String>(request).await?;

        ListObjectsResult::from_xml(&content)
    }
}

#[cfg(all(test, not(feature = "blocking")))]
pub mod test_bucket_async {
    use std::sync::Once;

    use crate::bucket::BucketOperations;

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[tokio::test]
    async fn test_list_buckets() {
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
}
