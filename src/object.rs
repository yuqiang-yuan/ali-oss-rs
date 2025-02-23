use std::path::Path;

use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine};
use futures::TryStreamExt;
use reqwest::StatusCode;
use tokio::io::AsyncWriteExt;

use crate::{
    error::{Error, Result},
    object_common::{
        build_copy_object_request, build_delete_multiple_objects_request, build_get_object_request, build_head_object_request, build_put_object_request,
        AppendObjectOptions, AppendObjectResult, CopyObjectOptions, CopyObjectResult, DeleteMultipleObjectsConfig, DeleteMultipleObjectsResult,
        DeleteObjectOptions, DeleteObjectResult, GetObjectMetadataOptions, GetObjectOptions, GetObjectResult, HeadObjectOptions, ObjectMetadata,
        PutObjectOptions, PutObjectResult,
    },
    request::{RequestBuilder, RequestMethod},
    util::validate_path,
    ByteStream, Client, RequestBody,
};

#[async_trait]
pub trait ObjectOperations {
    /// Uploads a file to a specified bucket and object key.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn put_object_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        options: Option<PutObjectOptions>,
    ) -> Result<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        P: AsRef<Path> + Send;

    /// Create an object from buffer. If you are going to upload a large file, it is recommended to use `upload_file` instead.
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn put_object_from_buffer<S1, S2, B>(
        &self,
        bucket_name: S1,
        object_key: S2,
        buffer: B,
        options: Option<PutObjectOptions>,
    ) -> Result<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        B: Into<Vec<u8>> + Send;

    /// Create an object from base64 string.
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn put_object_from_base64<S1, S2, S3>(
        &self,
        bucket_name: S1,
        object_key: S2,
        base64_string: S3,
        options: Option<PutObjectOptions>,
    ) -> Result<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send;

    /// Append object.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/appendobject>
    async fn append_object_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        P: AsRef<Path> + Send;

    /// Append object from buffer. suitable for small size content
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn append_object_from_buffer<S1, S2, B>(
        &self,
        bucket_name: S1,
        object_key: S2,
        buffer: B,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        B: Into<Vec<u8>> + Send;

    /// Append object from base64 string. suitable for small size content
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn append_object_from_base64<S1, S2, S3>(
        &self,
        bucket_name: S1,
        object_key: S2,
        base64_string: S3,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send;

    /// Download object to local file
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobject>
    async fn get_object_to_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        options: Option<GetObjectOptions>,
    ) -> Result<GetObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        P: AsRef<Path> + Send;

    /// Create a "folder"
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> Result<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Get object metadata
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectmeta>
    async fn get_object_metadata<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> Result<ObjectMetadata>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Check if the object exists or not using get object metadata
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectmeta>
    async fn exists<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> Result<bool>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Head object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/headobject>
    async fn head_object<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<HeadObjectOptions>) -> Result<ObjectMetadata>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Copy files (Objects) between the same or different Buckets within the same region.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/copyobject>
    async fn copy_object<S1, S2, S3, S4>(
        &self,
        source_bucket_name: S1,
        source_object_key: S2,
        dest_bucket_name: S3,
        dest_object_key: S4,
        options: Option<CopyObjectOptions>,
    ) -> Result<CopyObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send,
        S4: AsRef<str> + Send;

    /// Delete an object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deleteobject>
    async fn delete_object<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<DeleteObjectOptions>) -> Result<DeleteObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Delete multiple objects
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletemultipleobjects>
    async fn delete_multiple_objects<'c, S1, S2>(
        &self,
        bucket_name: S1,
        config: DeleteMultipleObjectsConfig<'c, S2>,
    ) -> Result<DeleteMultipleObjectsResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send + Sync;
}

#[async_trait]
impl ObjectOperations for Client {
    /// The `object_key` constraints:
    ///
    /// - length between [1, 1023]
    /// - must NOT starts or ends with `/` or `\`. e.g. `path/to/subfolder/some-file.txt`
    /// - the `file_path` specify full path to the file to be uploaded
    /// - the file must exist and must be readable
    /// - file length less than 5GB
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn put_object_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        options: Option<PutObjectOptions>,
    ) -> Result<PutObjectResult>
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

        let with_callback = if let Some(opt) = &options { opt.callback.is_some() } else { false };

        let request = build_put_object_request(bucket_name, object_key, RequestBody::File(file_path.to_path_buf()), &options)?;

        let (headers, content) = self.do_request::<String>(request).await?;

        if with_callback {
            Ok(PutObjectResult::CallbackResponseContent(content))
        } else {
            Ok(headers.into())
        }
    }

    /// Create an object from buffer. If you are going to upload a large file, it is recommended to use `upload_file` instead.
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn put_object_from_buffer<S1, S2, B>(
        &self,
        bucket_name: S1,
        object_key: S2,
        buffer: B,
        options: Option<PutObjectOptions>,
    ) -> Result<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        B: Into<Vec<u8>> + Send,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let object_key = object_key.strip_prefix("/").unwrap_or(object_key);
        let object_key = object_key.strip_suffix("/").unwrap_or(object_key);

        let with_callback = if let Some(opt) = &options { opt.callback.is_some() } else { false };

        let request = build_put_object_request(bucket_name, object_key, RequestBody::Bytes(buffer.into()), &options)?;

        let (headers, content) = self.do_request::<String>(request).await?;

        if with_callback {
            Ok(PutObjectResult::CallbackResponseContent(content))
        } else {
            Ok(headers.into())
        }
    }

    /// Create an object from base64 string.
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn put_object_from_base64<S1, S2, S3>(
        &self,
        bucket_name: S1,
        object_key: S2,
        base64_string: S3,
        options: Option<PutObjectOptions>,
    ) -> Result<PutObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send,
    {
        let data = if let Ok(d) = BASE64_STANDARD.decode(base64_string.as_ref()) {
            d
        } else {
            return Err(Error::Other("Decoding base64 string failed".to_string()));
        };

        self.put_object_from_buffer(bucket_name, object_key, data, options).await
    }

    /// Append object.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/appendobject>
    async fn append_object_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
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

        let mut request = build_put_object_request(bucket_name, object_key, RequestBody::File(file_path.to_path_buf()), &options)?;

        // alter the request method and add append object query parameters
        request = request
            .method(RequestMethod::Post)
            .add_query("append", "")
            .add_query("position", position.to_string());

        let (headers, _) = self.do_request::<()>(request).await?;

        Ok(headers.into())
    }

    /// Append object from buffer. suitable for small size content
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn append_object_from_buffer<S1, S2, B>(
        &self,
        bucket_name: S1,
        object_key: S2,
        buffer: B,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        B: Into<Vec<u8>> + Send,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let object_key = object_key.strip_prefix("/").unwrap_or(object_key);
        let object_key = object_key.strip_suffix("/").unwrap_or(object_key);

        let mut request = build_put_object_request(bucket_name, object_key, RequestBody::Bytes(buffer.into()), &options)?;

        // alter the request method and add append object query parameters
        request = request
            .method(RequestMethod::Post)
            .add_query("append", "")
            .add_query("position", position.to_string());

        let (headers, _) = self.do_request::<()>(request).await?;

        Ok(headers.into())
    }

    /// Append object from base64 string. suitable for small size content
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn append_object_from_base64<S1, S2, S3>(
        &self,
        bucket_name: S1,
        object_key: S2,
        base64_string: S3,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send,
    {
        let data = if let Ok(d) = BASE64_STANDARD.decode(base64_string.as_ref()) {
            d
        } else {
            return Err(Error::Other("Decoding base64 string failed".to_string()));
        };

        self.append_object_from_buffer(bucket_name, object_key, data, position, options).await
    }

    /// Download oss object to local file.
    /// `file_path` is the full file path to save.
    /// If the `file_path` parent path does not exist, it will be created
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobject>
    async fn get_object_to_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        options: Option<GetObjectOptions>,
    ) -> Result<GetObjectResult>
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
            return Err(Error::Other(format!("invalid file path: {:?}", file_path.as_os_str().to_str())));
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

        Ok(GetObjectResult)
    }

    /// Create a "folder".
    /// The `object_key` must ends with `/`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    async fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> Result<PutObjectResult>
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

        let request = build_put_object_request(bucket_name, &object_key, RequestBody::Empty, &options)?;

        let (headers, _) = self.do_request::<()>(request).await?;

        Ok(headers.into())
    }

    /// Get object metadata.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectmeta>
    async fn get_object_metadata<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> Result<ObjectMetadata>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let mut request = RequestBuilder::new()
            .method(RequestMethod::Head)
            .bucket(bucket_name)
            .object(object_key)
            .add_query("objectMeta", "");

        if let Some(options) = &options {
            if let Some(s) = &options.version_id {
                request = request.add_query("versionId", s);
            }
        }

        let (headers, _) = self.do_request::<()>(request).await?;
        Ok(ObjectMetadata::from(headers))
    }

    /// Check if the object exists or not using get object metadata
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectmeta>
    async fn exists<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> Result<bool>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        match self.get_object_metadata(bucket_name, object_key, options).await {
            Ok(_) => Ok(true),
            Err(e) => match e {
                Error::StatusError(status) if status == StatusCode::NOT_FOUND => Ok(false),
                _ => Err(e),
            },
        }
    }

    /// Get more detail object metadata
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/headobject>
    async fn head_object<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<HeadObjectOptions>) -> Result<ObjectMetadata>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let request = build_head_object_request(bucket_name, object_key, &options);

        let (headers, _) = self.do_request::<()>(request).await?;
        Ok(ObjectMetadata::from(headers))
    }

    /// Copy files (Objects) between the same or different Buckets within the same region.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/copyobject>
    async fn copy_object<S1, S2, S3, S4>(
        &self,
        source_bucket_name: S1,
        source_object_key: S2,
        dest_bucket_name: S3,
        dest_object_key: S4,
        options: Option<CopyObjectOptions>,
    ) -> Result<CopyObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send,
        S4: AsRef<str> + Send,
    {
        let request = build_copy_object_request(
            source_bucket_name.as_ref(),
            source_object_key.as_ref(),
            dest_bucket_name.as_ref(),
            dest_object_key.as_ref(),
            &options,
        )?;

        let (_, _) = self.do_request::<()>(request).await?;

        Ok(CopyObjectResult)
    }

    /// Delete an object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deleteobject>
    async fn delete_object<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<DeleteObjectOptions>) -> Result<DeleteObjectResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let mut request = RequestBuilder::new()
            .method(RequestMethod::Delete)
            .bucket(bucket_name.as_ref())
            .object(object_key.as_ref());

        if let Some(options) = options {
            if let Some(s) = options.version_id {
                request = request.add_query("versionId", s);
            }
        }

        let _ = self.do_request::<()>(request).await?;

        Ok(DeleteObjectResult)
    }

    /// Delete multiple objects
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletemultipleobjects>
    async fn delete_multiple_objects<'c, S1, S2>(
        &self,
        bucket_name: S1,
        config: DeleteMultipleObjectsConfig<'c, S2>,
    ) -> Result<DeleteMultipleObjectsResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send + Sync,
    {
        let request = build_delete_multiple_objects_request(bucket_name.as_ref(), config)?;

        let (_, content) = self.do_request::<String>(request).await?;

        DeleteMultipleObjectsResult::from_xml(&content)
    }
}

#[cfg(test)]
mod test_object_async {
    use std::{collections::HashMap, sync::Once};

    use base64::{prelude::BASE64_STANDARD, Engine};

    use crate::{
        common::{ObjectType, StorageClass},
        object::ObjectOperations,
        object_common::{
            CallbackBodyParameter, CallbackBuilder, DeleteMultipleObjectsConfig, GetObjectOptionsBuilder, PutObjectOptions, PutObjectOptionsBuilder,
            PutObjectResult,
        },
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
    async fn test_upload_file_1_async() {
        setup();

        let client = Client::from_env();
        let result = client
            .put_object_from_file(
                "yuanyq",
                "rust-sdk-test/test-pdf-output.pdf",
                "/home/yuanyq/Downloads/test-pdf-output.pdf",
                None,
            )
            .await;

        log::debug!("{:?}", result);

        assert!(result.is_ok());

        let ret = result.unwrap();
        if let PutObjectResult::ApiResponseHeaders {
            request_id: _,
            etag: _,
            content_md5,
            hash_crc64ecma: _,
            version_id: _,
        } = ret
        {
            assert_eq!("u3j3ZJAf4d4uOHz4BNcXiw==", content_md5);
        }
    }

    #[tokio::test]
    async fn test_upload_file_2_async() {
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
            .put_object_from_file(
                "yuanyq",
                "rust-sdk-test/云教材发布与管理系统-用户手册.pdf",
                "/home/yuanyq/Downloads/云教材发布与管理系统-用户手册.pdf",
                Some(options),
            )
            .await;

        log::debug!("{:?}", result);

        assert!(result.is_ok());

        let ret = result.unwrap();
        if let PutObjectResult::ApiResponseHeaders {
            request_id: _,
            etag: _,
            content_md5,
            hash_crc64ecma: _,
            version_id: _,
        } = ret
        {
            assert_eq!("m6YFnp+xXeBIXkiWnqFi9w==", content_md5);
        }
    }

    /// Test upload file with non-default storage class
    #[tokio::test]
    async fn test_upload_file_3_async() {
        setup();

        let client = Client::from_env();

        let options = PutObjectOptions {
            storage_class: Some(StorageClass::Archive),
            ..Default::default()
        };

        let result = client
            .put_object_from_file("yuanyq", "rust-sdk-test/archived/demo.mp4", "/home/yuanyq/Pictures/demo.mp4", Some(options))
            .await;

        log::debug!("{:?}", result);

        assert!(result.is_ok());

        let ret = result.unwrap();
        if let PutObjectResult::ApiResponseHeaders {
            request_id: _,
            etag: _,
            content_md5,
            hash_crc64ecma: _,
            version_id: _,
        } = ret
        {
            assert_eq!("8TAE7tQlHGArvhVzfooeyw==", content_md5);
        }
    }

    #[tokio::test]
    async fn test_create_folder_1_async() {
        setup();

        let client = Client::from_env();

        let result = client.create_folder("yuanyq", "rust-sdk-test/test-folder/", None).await;

        log::debug!("{:?}", result);

        assert!(result.is_ok())
    }

    /// Download full file content to local file
    /// with no options
    #[tokio::test]
    async fn test_download_file_1_async() {
        setup();
        let client = Client::from_env();

        let output_file = "/home/yuanyq/Downloads/ali-oss-rs-test/katex.zip";

        let result = client.get_object_to_file("yuanyq", "rust-sdk-test/katex.zip", output_file, None).await;

        assert!(result.is_ok());
    }

    /// Download range of file
    #[tokio::test]
    async fn test_download_file_2_async() {
        setup();
        let client = Client::from_env();

        let output_file = "/home/yuanyq/Downloads/ali-oss-rs-test/katex.zip.1";

        let options = GetObjectOptionsBuilder::new().range("bytes=0-499").build();

        let result = client.get_object_to_file("yuanyq", "rust-sdk-test/katex.zip", output_file, Some(options)).await;

        assert!(result.is_ok());

        let file_meta = std::fs::metadata(output_file).unwrap();

        assert_eq!(500, file_meta.len());
    }

    /// Test invalid output file name
    #[tokio::test]
    async fn test_download_file_3_async() {
        setup();
        let client = Client::from_env();

        let invalid_files = [
            "/home/yuanyq/Downloads/ali-oss-rs-test>/katex.zip.1",
            "/home/yuanyq/Downloads/ali-oss-rs-test|/katex;.zip.1",
            "/home/yuanyq/Downloads/ali-oss-rs-test\0/katex.zip.1",
        ];

        for output_file in invalid_files {
            let options = GetObjectOptionsBuilder::new().range("bytes=0-499").build();

            let result = client.get_object_to_file("yuanyq", "rust-sdk-test/katex.zip", output_file, Some(options)).await;

            assert!(result.is_err());

            log::debug!("{}", result.unwrap_err());
        }
    }

    #[tokio::test]
    async fn test_get_object_metadata_async() {
        setup();
        let client = Client::from_env();

        let result = client
            .get_object_metadata("yuanyq", "rust-sdk-test/Oracle_VirtualBox_Extension_Pack-7.1.4.vbox-extpack", None)
            .await;

        assert!(result.is_ok());

        let meta = result.unwrap();
        log::debug!("{:?}", meta);
        assert_eq!(22966826, meta.content_length);
        assert_eq!("B752E1A13502E231AC4AA0E1D91F887C", meta.etag);
        assert_eq!(Some(7873641174252289613u64), meta.hash_crc64ecma);
        assert_eq!(Some("Tue, 18 Feb 2025 15:03:23 GMT".to_string()), meta.last_modified);
    }

    #[tokio::test]
    async fn test_head_object_async() {
        setup();
        let client = Client::from_env();

        let result = client
            .head_object("yuanyq", "rust-sdk-test/Oracle_VirtualBox_Extension_Pack-7.1.4.vbox-extpack", None)
            .await;

        assert!(result.is_ok());

        let meta = result.unwrap();
        log::debug!("{:#?}", meta);
        assert_eq!(22966826, meta.content_length);
        assert_eq!("B752E1A13502E231AC4AA0E1D91F887C", meta.etag);
        assert_eq!(Some(7873641174252289613), meta.hash_crc64ecma);
        assert_eq!(Some("Tue, 18 Feb 2025 15:03:23 GMT".to_string()), meta.last_modified);
        assert_eq!(Some(ObjectType::Normal), meta.object_type);
        assert_eq!(Some(StorageClass::Standard), meta.storage_class);
    }

    /// Copy object in same bucket
    #[tokio::test]
    async fn test_copy_object_1_async() {
        setup();
        let client = Client::from_env();

        let source_bucket = "yuanyq";
        let source_object = "test.php";

        let dest_bucket = "yuanyq";
        let dest_object = "test.php.bak";

        let ret = client.copy_object(source_bucket, source_object, dest_bucket, dest_object, None).await;

        assert!(ret.is_ok());

        let source_meta = client.get_object_metadata(source_bucket, source_object, None).await.unwrap();
        let dest_meta = client.get_object_metadata(dest_bucket, dest_object, None).await.unwrap();

        assert_eq!(source_meta.etag, dest_meta.etag);
    }

    /// Copy object across buckets
    #[tokio::test]
    async fn test_copy_object_2_async() {
        setup();
        let client = Client::from_env();

        let source_bucket = "yuanyq";
        let source_object = "test.php";

        let dest_bucket = "yuanyq-2";
        let dest_object = "test.php";

        let ret = client.copy_object(source_bucket, source_object, dest_bucket, dest_object, None).await;

        assert!(ret.is_ok());

        let source_meta = client.get_object_metadata(source_bucket, source_object, None).await.unwrap();
        let dest_meta = client.get_object_metadata(dest_bucket, dest_object, None).await.unwrap();

        assert_eq!(source_meta.etag, dest_meta.etag);
    }

    #[tokio::test]
    async fn test_create_object_from_buffer_async() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-from-buffer.jpg";

        let options = PutObjectOptionsBuilder::new().mime_type("image/jpeg").build();

        let buffer = std::fs::read("/home/yuanyq/Pictures/f69e41cb1642c3360bd5bb6e700a0ecb.jpeg").unwrap();

        let md5 = "1ziAOyOVKo5/xAIvbUEQJA==";

        let ret = client.put_object_from_buffer(bucket, object, buffer, Some(options)).await;

        log::debug!("{:?}", ret);

        assert!(ret.is_ok());

        let meta = client.head_object(bucket, object, None).await.unwrap();
        assert_eq!(Some(md5.to_string()), meta.content_md5);
    }

    #[tokio::test]
    async fn test_create_object_from_base64_async() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-from-base64.jpg";

        let options = PutObjectOptionsBuilder::new().mime_type("image/jpeg").build();

        let buffer = std::fs::read("/home/yuanyq/Pictures/f69e41cb1642c3360bd5bb6e700a0ecb.jpeg").unwrap();
        let base64 = BASE64_STANDARD.encode(&buffer);
        let md5 = "1ziAOyOVKo5/xAIvbUEQJA==";

        let ret = client.put_object_from_base64(bucket, object, base64, Some(options)).await;

        assert!(ret.is_ok());

        let meta = client.head_object(bucket, object, None).await.unwrap();
        assert_eq!(Some(md5.to_string()), meta.content_md5);
    }

    #[tokio::test]
    async fn test_delete_object_async() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-from-base64.jpg";

        let ret = client.delete_object(bucket, object, None).await;
        assert!(ret.is_ok());
    }

    #[tokio::test]
    async fn test_exists_async() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-from-base64.jpg";

        let ret = client.exists(bucket, object, None).await;
        assert!(ret.is_ok());
        assert!(!ret.unwrap());

        let object = "test.php";
        let ret = client.exists(bucket, object, None).await;
        assert!(ret.is_ok());
        assert!(ret.unwrap());
    }

    #[tokio::test]
    async fn test_append_object_1_async() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-appended-from-file.jpg";

        let file1 = "/home/yuanyq/Pictures/test-image-part-1.data";
        let file2 = "/home/yuanyq/Pictures/test-image-part-2.data";
        let file3 = "/home/yuanyq/Pictures/test-image-part-3.data";

        let ret1 = client.append_object_from_file(bucket, object, file1, 0, None).await;

        assert!(ret1.is_ok());

        let next_pos = ret1.unwrap().next_append_position;
        assert_eq!(61929, next_pos);

        let ret2 = client.append_object_from_file(bucket, object, file2, next_pos, None).await;
        assert!(ret2.is_ok());

        let next_pos = ret2.unwrap().next_append_position;
        assert_eq!(61929 * 2, next_pos);

        let ret3 = client.append_object_from_file(bucket, object, file3, next_pos, None).await;
        assert!(ret3.is_ok());

        let meta = client.head_object(bucket, object, None).await;
        log::debug!("{:#?}", meta.unwrap());
    }

    #[tokio::test]
    async fn test_append_object_from_buffer_async() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-appended-from-buffer.jpg";

        let file1 = "/home/yuanyq/Pictures/test-image-part-1.data";
        let file2 = "/home/yuanyq/Pictures/test-image-part-2.data";
        let file3 = "/home/yuanyq/Pictures/test-image-part-3.data";

        let buffer1 = std::fs::read(file1).unwrap();
        let buffer2 = std::fs::read(file2).unwrap();
        let buffer3 = std::fs::read(file3).unwrap();

        let ret1 = client.append_object_from_buffer(bucket, object, buffer1, 0, None).await;
        assert!(ret1.is_ok());

        let next_pos = ret1.unwrap().next_append_position;
        assert_eq!(61929, next_pos);

        let ret2 = client.append_object_from_buffer(bucket, object, buffer2, next_pos, None).await;
        assert!(ret2.is_ok());

        let next_pos = ret2.unwrap().next_append_position;
        assert_eq!(61929 * 2, next_pos);

        let ret3 = client.append_object_from_buffer(bucket, object, buffer3, next_pos, None).await;
        assert!(ret3.is_ok());

        let meta = client.head_object(bucket, object, None).await;
        log::debug!("{:#?}", meta.unwrap());
    }

    #[tokio::test]
    async fn test_append_object_from_base64_async() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-appended-from-base64.jpg";

        let file1 = "/home/yuanyq/Pictures/test-image-part-1.data";
        let file2 = "/home/yuanyq/Pictures/test-image-part-2.data";
        let file3 = "/home/yuanyq/Pictures/test-image-part-3.data";

        let buffer1 = std::fs::read(file1).unwrap();
        let buffer2 = std::fs::read(file2).unwrap();
        let buffer3 = std::fs::read(file3).unwrap();

        let s1 = BASE64_STANDARD.encode(buffer1);
        let s2 = BASE64_STANDARD.encode(buffer2);
        let s3 = BASE64_STANDARD.encode(buffer3);

        let ret1 = client.append_object_from_base64(bucket, object, s1, 0, None).await;
        assert!(ret1.is_ok());

        let next_pos = ret1.unwrap().next_append_position;
        assert_eq!(61929, next_pos);

        let ret2 = client.append_object_from_base64(bucket, object, s2, next_pos, None).await;
        assert!(ret2.is_ok());

        let next_pos = ret2.unwrap().next_append_position;
        assert_eq!(61929 * 2, next_pos);

        let ret3 = client.append_object_from_base64(bucket, object, s3, next_pos, None).await;
        assert!(ret3.is_ok());

        let meta = client.head_object(bucket, object, None).await;
        log::debug!("{:#?}", meta.unwrap());
    }

    #[tokio::test]
    async fn test_delete_multiple_objects_async() {
        setup();
        let client = Client::from_env();

        let keys = [
            "rust-sdk-test/01-01.jpg",
            "rust-sdk-test/01-02.jpg",
            "rust-sdk-test/01-03.png",
            "rust-sdk-test/01-04.jpg",
            "rust-sdk-test/01-05.png",
        ];

        let response = client.delete_multiple_objects("yuanyq", DeleteMultipleObjectsConfig::FromKeys(&keys)).await;

        log::debug!("{:#?}", response);

        assert!(response.is_ok());

        let result = response.unwrap();
        assert_eq!(5, result.items.len());

        for s in keys {
            assert!(result.items.iter().any(|item| item.key == s));
        }
    }

    #[tokio::test]
    async fn test_put_object_from_file_with_callback_async() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq".to_string();
        let object = "rust-sdk-test/test-1.webp";
        let file = "/home/yuanyq/Pictures/test-1.webp".to_string();

        let cb = CallbackBuilder::new("https://your-callback.domain.com/oss-callback-test.php")
            .body_parameter(CallbackBodyParameter::OssBucket("the_bucket"))
            .body_parameter(CallbackBodyParameter::OssObject("the_object_key"))
            .body_parameter(CallbackBodyParameter::OssETag("the_etag"))
            .body_parameter(CallbackBodyParameter::OssSize("the_size"))
            .body_parameter(CallbackBodyParameter::OssCrc64("the_crc"))
            .body_parameter(CallbackBodyParameter::OssClientIp("the_client_ip"))
            .body_parameter(CallbackBodyParameter::OssContentMd5("the_content_md5"))
            .body_parameter(CallbackBodyParameter::OssMimeType("the_mime_type"))
            .body_parameter(CallbackBodyParameter::OssImageWidth("the_image_width"))
            .body_parameter(CallbackBodyParameter::OssImageHeight("the_image_height"))
            .body_parameter(CallbackBodyParameter::OssImageFormat("the_image_format"))
            .body_parameter(CallbackBodyParameter::Custom("my-key", "my-prop", "hello world".to_string()))
            .body_parameter(CallbackBodyParameter::Constant("my-key-constant", "the-value"))
            .body_parameter(CallbackBodyParameter::Literal("k1".to_string(), "${x:v1}".to_string()))
            .custom_variable("v1", "this is value of v1")
            .build();

        let options = PutObjectOptionsBuilder::new().callback(cb).build();

        let response = client.put_object_from_file(bucket, object, &file, Some(options)).await;
        assert!(response.is_ok());

        let ret = response.unwrap();

        log::debug!("{:#?}", ret);
    }

    #[tokio::test]
    async fn test_put_object_from_buffer_with_callback_async() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq".to_string();
        let object = "rust-sdk-test/test-1.webp";
        let file = "/home/yuanyq/Pictures/test-1.webp".to_string();

        let data = std::fs::read(&file).unwrap();

        let cb = CallbackBuilder::new("https://your-callback.domain.com/oss-callback-test.php")
            .body_parameter(CallbackBodyParameter::OssBucket("the_bucket"))
            .body_parameter(CallbackBodyParameter::OssObject("the_object_key"))
            .body_parameter(CallbackBodyParameter::OssETag("the_etag"))
            .body_parameter(CallbackBodyParameter::OssSize("the_size"))
            .body_parameter(CallbackBodyParameter::OssCrc64("the_crc"))
            .body_parameter(CallbackBodyParameter::OssClientIp("the_client_ip"))
            .body_parameter(CallbackBodyParameter::OssContentMd5("the_content_md5"))
            .body_parameter(CallbackBodyParameter::OssMimeType("the_mime_type"))
            .body_parameter(CallbackBodyParameter::OssImageWidth("the_image_width"))
            .body_parameter(CallbackBodyParameter::OssImageHeight("the_image_height"))
            .body_parameter(CallbackBodyParameter::OssImageFormat("the_image_format"))
            .body_parameter(CallbackBodyParameter::Custom("my-key", "my-prop", "hello world".to_string()))
            .body_parameter(CallbackBodyParameter::Constant("my-key-constant", "the-value"))
            .body_parameter(CallbackBodyParameter::Literal("k1".to_string(), "${x:v1}".to_string()))
            .custom_variable("v1", "this is value of v1")
            .build();

        let options = PutObjectOptionsBuilder::new().callback(cb).build();

        let response = client.put_object_from_buffer(bucket, object, data, Some(options)).await;
        assert!(response.is_ok());

        let ret = response.unwrap();

        log::debug!("{:#?}", ret);
    }
}
