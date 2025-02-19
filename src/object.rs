use std::path::Path;

use async_trait::async_trait;
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;

use crate::{
    error::{ClientError, ClientResult},
    object_common::{build_get_object_request, build_put_object_request, GetObjectOptions, PutObjectOptions, PutObjectResult},
    util::validate_path,
    ByteStream, Client,
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
    /// Download object and save to local file
    ///
    async fn download_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<GetObjectOptions>) -> ClientResult<()>
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

    /// Download oss object to local file.
    /// `file_path` is the full file path to save.
    /// If the `file_path` parent path does not exist, it will be created
    async fn download_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<GetObjectOptions>) -> ClientResult<()>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        P: AsRef<Path> + Send,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();
        let file_path = file_path.as_ref();

        let file_path = if file_path.is_relative() {
            file_path.canonicalize()?
        } else {
            file_path.to_path_buf()
        };

        if !validate_path(&file_path) {
            return Err(ClientError::Error(format!("invalid file path: {:?}", file_path.as_os_str().to_str())));
        }

        // check parent path
        if let Some(parent_path) = file_path.parent() {
            if !parent_path.exists() {
                std::fs::create_dir_all(parent_path)?;
            }
        }

        let request = build_get_object_request(bucket_name, object_key, &options);

        let (_, mut stream) = self.do_request::<ByteStream>(request).await?;

        let mut file = tokio::fs::File::create(&file_path).await?;

        while let Some(chunk) = stream.try_next().await? {
            file.write_all(&chunk).await?;
        }

        file.flush().await?;

        Ok(())
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

#[cfg(all(test, not(feature = "blocking")))]
mod test_object_async {
    use std::{collections::HashMap, sync::Once};

    use crate::{
        common::StorageClass,
        object::ObjectOperations,
        object_common::{GetObjectOptionsBuilder, PutObjectOptions},
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

    /// Test upload file with non-default storage class
    #[tokio::test]
    async fn test_upload_file_3() {
        setup();

        let client = Client::from_env();

        let options = PutObjectOptions {
            storage_class: Some(StorageClass::Archive),
            ..Default::default()
        };

        let result = client
            .upload_file("yuanyq", "rust-sdk-test/archived/demo.mp4", "/home/yuanyq/Pictures/demo.mp4", Some(options))
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

    /// Download full file content to local file
    /// with no options
    #[tokio::test]
    async fn test_download_file_1() {
        setup();
        let client = Client::from_env();

        let output_file = "/home/yuanyq/Downloads/ali-oss-rs-test/katex.zip";

        let result = client.download_file("yuanyq", "rust-sdk-test/katex.zip", output_file, None).await;

        assert!(result.is_ok());
    }

    /// Download range of file
    #[tokio::test]
    async fn test_download_file_2() {
        setup();
        let client = Client::from_env();

        let output_file = "/home/yuanyq/Downloads/ali-oss-rs-test/katex.zip.1";

        let options = GetObjectOptionsBuilder::new().range("bytes=0-499").build();

        let result = client.download_file("yuanyq", "rust-sdk-test/katex.zip", output_file, Some(options)).await;

        assert!(result.is_ok());

        let file_meta = std::fs::metadata(output_file).unwrap();

        assert_eq!(500, file_meta.len());
    }

    /// Test invalid output file name
    #[tokio::test]
    async fn test_download_file_3() {
        setup();
        let client = Client::from_env();

        let invalid_files = [
            "/home/yuanyq/Downloads/ali-oss-rs-test>/katex.zip.1",
            "/home/yuanyq/Downloads/ali-oss-rs-test|/katex;.zip.1",
            "/home/yuanyq/Downloads/ali-oss-rs-test\0/katex.zip.1",
        ];

        for output_file in invalid_files {
            let options = GetObjectOptionsBuilder::new().range("bytes=0-499").build();

            let result = client.download_file("yuanyq", "rust-sdk-test/katex.zip", output_file, Some(options)).await;

            assert!(result.is_err());

            log::debug!("{}", result.unwrap_err());
        }
    }
}
