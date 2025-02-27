//! Object symlink module

use async_trait::async_trait;

use crate::symlink_common::{build_get_symlink_request, build_put_symlink_request, GetSymlinkOptions, PutSymlinkOptions, PutSymlinkResult};
use crate::{Client, Result};

#[async_trait]
pub trait ObjectSymlinkOperations {
    /// Put a symlink object.
    ///
    /// `target_object_key` should be a full and valid object key.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putsymlink>
    async fn put_symlink<S1, S2, S3>(
        &self,
        bucket_name: S1,
        symlink_object_key: S2,
        target_object_key: S3,
        options: Option<PutSymlinkOptions>,
    ) -> Result<PutSymlinkResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send;

    /// Get a symlink object. The returned string is the target object key
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getsymlink>
    async fn get_symlink<S1, S2>(&self, bucket_name: S1, symlink_object_key: S2, options: Option<GetSymlinkOptions>) -> Result<String>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;
}

#[async_trait]
impl ObjectSymlinkOperations for Client {
    /// Put a symlink object.
    ///
    /// `target_object_key` should be a full and valid object key.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putsymlink>
    async fn put_symlink<S1, S2, S3>(
        &self,
        bucket_name: S1,
        symlink_object_key: S2,
        target_object_key: S3,
        options: Option<PutSymlinkOptions>,
    ) -> Result<PutSymlinkResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send,
    {
        let request = build_put_symlink_request(bucket_name.as_ref(), symlink_object_key.as_ref(), target_object_key.as_ref(), &options)?;
        let (headers, _) = self.do_request::<()>(request).await?;
        Ok(headers.into())
    }

    /// Get a symlink object. The returned string is the target object key
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getsymlink>
    async fn get_symlink<S1, S2>(&self, bucket_name: S1, symlink_object_key: S2, options: Option<GetSymlinkOptions>) -> Result<String>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let request = build_get_symlink_request(bucket_name.as_ref(), symlink_object_key.as_ref(), &options)?;
        let (headers, _) = self.do_request::<()>(request).await?;
        let object_key = headers.get("x-oss-symlink-target").unwrap_or(&String::new()).to_string();

        Ok(urlencoding::decode(&object_key)?.to_string())
    }
}

#[cfg(test)]
mod test_symlink_async {
    use std::sync::Once;

    use uuid::Uuid;

    use crate::{
        object::ObjectOperations,
        object_common::{PutObjectApiResponse, PutObjectResult},
        symlink::ObjectSymlinkOperations,
        symlink_common::PutSymlinkOptionsBuilder,
        Client,
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[tokio::test]
    async fn test_symlink_async() {
        log::debug!("test symlink");
        setup();

        let client = Client::from_env();
        let bucket_name = "yuanyq-2";
        let object_key = format!("versioning-test/{}.webp", Uuid::new_v4());
        let file_path = "/home/yuanyq/Pictures/test-7.webp";

        let link_name = format!("versioning-test/{}-link.webp", Uuid::new_v4());

        let response = client.put_object_from_file(bucket_name, &object_key, file_path, None).await;
        assert!(response.is_ok());

        let ret = response.unwrap();
        let version_id = if let PutObjectResult::ApiResponse(PutObjectApiResponse {
            request_id: _,
            etag: _,
            content_md5: _,
            hash_crc64ecma: _,
            version_id,
        }) = ret
        {
            assert!(version_id.is_some());
            version_id.clone().unwrap()
        } else {
            panic!("Unexpected response type");
        };

        log::debug!("version id: {}", version_id);

        let options = PutSymlinkOptionsBuilder::new().metadata("x-oss-meta-a", "meta value b").build();

        let response = client.put_symlink(bucket_name, &link_name, &object_key, Some(options)).await;
        assert!(response.is_ok());

        let ret = response.unwrap();
        assert!(ret.version_id.is_some());

        let response = client.get_symlink(bucket_name, &link_name, None).await;
        assert!(response.is_ok());
        let ret = response.unwrap();
        assert_eq!(ret, object_key);

        let response = client.delete_object(bucket_name, &link_name, None).await;
        assert!(response.is_ok());

        let response = client.delete_object(bucket_name, &object_key, None).await;
        assert!(response.is_ok());
    }
}
