use std::path::Path;

use async_trait::async_trait;

use crate::{
    error::ClientResult,
    object_common::{build_put_object_request, PutObjectOptions, PutObjectResult},
    Client,
};

#[async_trait]
pub trait ObjectOperations {
    ///
    /// Uploads a file to a specified bucket and object key.
    /// The file length must be greater than 0.
    ///
    async fn upload_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        P: AsRef<Path> + Send;

    ///
    /// Create a "folder"
    async fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;
}

#[async_trait]
impl ObjectOperations for Client {
    ///
    /// The `object_key` constraints:
    ///
    /// - length between [1, 1023]
    /// - must NOT starts or ends with `/` or `\`. e.g. `path/to/subfolder/some-file.txt`
    /// - the `file_path` specify full path to the file to be uploaded
    /// - the file must exist and must be readable
    /// - file length less than 5GB
    async fn upload_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        P: AsRef<Path> + Send,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let object_key = object_key.strip_prefix("/").unwrap_or(object_key);
        let object_key = object_key.strip_suffix("/").unwrap_or(object_key);

        let file_path = file_path.as_ref();

        let request = build_put_object_request(bucket_name, object_key, Some(file_path), &options)?;

        let (headers, _) = self.do_request::<()>(request).await?;

        Ok(PutObjectResult::from_headers(&headers))
    }

    ///
    /// Create a "folder".
    /// The `object_key` must ends with `/`
    async fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();
        let object_key = object_key.strip_prefix("/").unwrap_or(object_key);
        let object_key = if object_key.ends_with("/") {
            object_key.to_string()
        } else {
            format!("{}/", object_key)
        };

        let request = build_put_object_request(bucket_name, &object_key, None, &options)?;

        let (headers, _) = self.do_request::<()>(request).await?;

        Ok(PutObjectResult::from_headers(&headers))
    }
}

#[cfg(test)]
mod test_object_async {
    use std::{collections::HashMap, sync::Once};

    use crate::{object::ObjectOperations, object_common::PutObjectOptions, Client};

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[tokio::test]
    async fn test_upload_file_1() {
        setup();

        let client = Client::from_env();
        let result = client
            .upload_file(
                "yuanyq",
                "rust-sdk-test/test-pdf-output.pdf",
                "/home/yuanyq/Downloads/test-pdf-output.pdf",
                None,
            )
            .await;

        log::debug!("{:?}", result);

        assert!(result.is_ok());

        log::debug!("{:?}", result.unwrap());
    }

    #[tokio::test]
    async fn test_upload_file_2() {
        setup();

        let client = Client::from_env();

        let options = PutObjectOptions {
            tags: HashMap::from([("purpose".to_string(), "test".to_string()), ("where".to_string(), "beijing".to_string())]),

            metadata: HashMap::from([
                ("x-oss-meta-who".to_string(), "yuanyu".to_string()),
                ("x-oss-meta-when".to_string(), "now or later".to_string()),
            ]),

            ..Default::default()
        };

        let result = client
            .upload_file(
                "yuanyq",
                "rust-sdk-test/云教材发布与管理系统-用户手册.pdf",
                "/home/yuanyq/Downloads/云教材发布与管理系统-用户手册.pdf",
                Some(options),
            )
            .await;

        log::debug!("{:?}", result);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_folder_1() {
        setup();

        let client = Client::from_env();

        let result = client.create_folder("yuanyq", "rust-sdk-test/test-folder/", None).await;

        log::debug!("{:?}", result);

        assert!(result.is_ok())
    }
}
