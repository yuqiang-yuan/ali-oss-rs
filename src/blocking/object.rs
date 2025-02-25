use std::path::Path;

use base64::{prelude::BASE64_STANDARD, Engine};
use reqwest::StatusCode;

use crate::{
    error::{Error, Result},
    object_common::{
        build_copy_object_request, build_delete_multiple_objects_request, build_get_object_request, build_head_object_request, build_put_object_request,
        build_restore_object_request, AppendObjectOptions, AppendObjectResult, CopyObjectOptions, CopyObjectResult, DeleteMultipleObjectsConfig,
        DeleteMultipleObjectsResult, DeleteObjectOptions, DeleteObjectResult, GetObjectMetadataOptions, GetObjectOptions, GetObjectResult, HeadObjectOptions,
        ObjectMetadata, PutObjectOptions, PutObjectResult, RestoreObjectRequest, RestoreObjectResult,
    },
    request::{OssRequest, RequestMethod},
    util::validate_path,
    RequestBody,
};

use super::{BytesBody, Client};

pub trait ObjectOperations {
    /// Uploads a file to a specified bucket and object key.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn put_object_from_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<PutObjectOptions>) -> Result<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>;

    /// Create an object from buffer. If you are going to upload a large file, it is recommended to use `upload_file` instead.
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn put_object_from_buffer<S1, S2, B>(&self, bucket_name: S1, object_key: S2, buffer: B, options: Option<PutObjectOptions>) -> Result<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        B: Into<Vec<u8>>;

    /// Create an object from base64 string.
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn put_object_from_base64<S1, S2, S3>(
        &self,
        bucket_name: S1,
        object_key: S2,
        base64_string: S3,
        options: Option<PutObjectOptions>,
    ) -> Result<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>;

    /// Append object.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/appendobject>
    fn append_object_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>;

    /// Append object from buffer. suitable for small size content
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn append_object_from_buffer<S1, S2, B>(
        &self,
        bucket_name: S1,
        object_key: S2,
        buffer: B,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        B: Into<Vec<u8>>;

    /// Append object from base64 string. suitable for small size content
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn append_object_from_base64<S1, S2, S3>(
        &self,
        bucket_name: S1,
        object_key: S2,
        base64_string: S3,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>;

    /// Uploads a file to a specified bucket and object key.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobject>
    fn get_object_to_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<GetObjectOptions>) -> Result<GetObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>;

    /// Create a "folder"
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> Result<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Get object metadata
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectmeta>
    fn get_object_metadata<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> Result<ObjectMetadata>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Check if the object exists or not using get object metadata
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectmeta>
    fn exists<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> Result<bool>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Head object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/headobject>
    fn head_object<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<HeadObjectOptions>) -> Result<ObjectMetadata>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Copy files (Objects) between the same or different Buckets within the same region.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/copyobject>
    fn copy_object<S1, S2, S3, S4>(
        &self,
        source_bucket_name: S1,
        source_object_key: S2,
        dest_bucket_name: S3,
        dest_object_key: S4,
        options: Option<CopyObjectOptions>,
    ) -> Result<CopyObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
        S4: AsRef<str>;

    /// Delete an object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deleteobject>
    fn delete_object<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<DeleteObjectOptions>) -> Result<DeleteObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Delete multiple objects
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletemultipleobjects>
    fn delete_multiple_objects<S1, S2>(&self, bucket_name: S1, config: DeleteMultipleObjectsConfig<'_, S2>) -> Result<DeleteMultipleObjectsResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Restore object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/restoreobject>
    fn restore_object<S1, S2>(&self, bucket_name: S1, object_key: S2, config: RestoreObjectRequest) -> Result<RestoreObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Clean retored object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/cleanrestoredobject>
    fn clean_restored_object<S1, S2>(&self, bucket_name: S1, object_key: S2) -> Result<()>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;
}

impl ObjectOperations for Client {
    /// Uploads a file to a specified bucket and object key.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn put_object_from_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<PutObjectOptions>) -> Result<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let object_key = object_key.strip_prefix("/").unwrap_or(object_key);
        let object_key = object_key.strip_suffix("/").unwrap_or(object_key);

        let file_path = file_path.as_ref();

        let with_callback = if let Some(opt) = &options { opt.callback.is_some() } else { false };

        let request = build_put_object_request(bucket_name, object_key, RequestBody::File(file_path.to_path_buf(), None), &options)?;

        let (headers, content) = self.do_request::<String>(request)?;

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
    fn put_object_from_buffer<S1, S2, B>(&self, bucket_name: S1, object_key: S2, buffer: B, options: Option<PutObjectOptions>) -> Result<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        B: Into<Vec<u8>>,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let object_key = object_key.strip_prefix("/").unwrap_or(object_key);
        let object_key = object_key.strip_suffix("/").unwrap_or(object_key);

        let with_callback = if let Some(opt) = &options { opt.callback.is_some() } else { false };

        let request = build_put_object_request(bucket_name, object_key, RequestBody::Bytes(buffer.into()), &options)?;

        let (headers, content) = self.do_request::<String>(request)?;

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
    fn put_object_from_base64<S1, S2, S3>(
        &self,
        bucket_name: S1,
        object_key: S2,
        base64_string: S3,
        options: Option<PutObjectOptions>,
    ) -> Result<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
    {
        let data = if let Ok(d) = BASE64_STANDARD.decode(base64_string.as_ref()) {
            d
        } else {
            return Err(Error::Other("Decoding base64 string failed".to_string()));
        };

        self.put_object_from_buffer(bucket_name, object_key, data, options)
    }

    /// Append object.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/appendobject>
    fn append_object_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let object_key = object_key.strip_prefix("/").unwrap_or(object_key);
        let object_key = object_key.strip_suffix("/").unwrap_or(object_key);

        let file_path = file_path.as_ref();

        let mut request = build_put_object_request(bucket_name, object_key, RequestBody::File(file_path.to_path_buf(), None), &options)?;

        // alter the request method and add append object query parameters
        request = request
            .method(RequestMethod::Post)
            .add_query("append", "")
            .add_query("position", position.to_string());

        let (headers, _) = self.do_request::<()>(request)?;

        Ok(headers.into())
    }

    /// Append object from buffer. suitable for small size content
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn append_object_from_buffer<S1, S2, B>(
        &self,
        bucket_name: S1,
        object_key: S2,
        buffer: B,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        B: Into<Vec<u8>>,
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

        let (headers, _) = self.do_request::<()>(request)?;

        Ok(headers.into())
    }

    /// Append object from base64 string. suitable for small size content
    /// And, it is recommended to set `mime_type` in `options`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn append_object_from_base64<S1, S2, S3>(
        &self,
        bucket_name: S1,
        object_key: S2,
        base64_string: S3,
        position: u64,
        options: Option<AppendObjectOptions>,
    ) -> Result<AppendObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
    {
        let data = if let Ok(d) = BASE64_STANDARD.decode(base64_string.as_ref()) {
            d
        } else {
            return Err(Error::Other("Decoding base64 string failed".to_string()));
        };

        self.append_object_from_buffer(bucket_name, object_key, data, position, options)
    }

    /// Uploads a file to a specified bucket and object key.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobject>
    fn get_object_to_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<GetObjectOptions>) -> Result<GetObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>,
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

        let (_, mut stream) = self.do_request::<BytesBody>(request)?;

        stream.save_to_file(file_path)?;

        Ok(GetObjectResult)
    }

    /// Create a "folder"
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
    fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> Result<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
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

        let (headers, _) = self.do_request::<()>(request)?;

        Ok(headers.into())
    }

    /// Get object metadata
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectmeta>
    fn get_object_metadata<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> Result<ObjectMetadata>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let mut request = OssRequest::new()
            .method(RequestMethod::Head)
            .bucket(bucket_name)
            .object(object_key)
            .add_query("objectMeta", "");

        if let Some(options) = &options {
            if let Some(s) = &options.version_id {
                request = request.add_query("versionId", s);
            }
        }

        let (headers, _) = self.do_request::<()>(request)?;
        Ok(ObjectMetadata::from(headers))
    }

    /// Check if the object exists or not using get object metadata
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectmeta>
    fn exists<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> Result<bool>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        match self.get_object_metadata(bucket_name, object_key, options) {
            Ok(_) => Ok(true),
            Err(e) => match e {
                Error::StatusError(status) if status == StatusCode::NOT_FOUND => Ok(false),
                _ => Err(e),
            },
        }
    }

    /// Head object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/headobject>
    fn head_object<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<HeadObjectOptions>) -> Result<ObjectMetadata>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        let request = build_head_object_request(bucket_name, object_key, &options);

        let (headers, _) = self.do_request::<()>(request)?;
        Ok(ObjectMetadata::from(headers))
    }

    /// Copy files (Objects) between the same or different Buckets within the same region.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/copyobject>
    fn copy_object<S1, S2, S3, S4>(
        &self,
        source_bucket_name: S1,
        source_object_key: S2,
        dest_bucket_name: S3,
        dest_object_key: S4,
        options: Option<CopyObjectOptions>,
    ) -> Result<CopyObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
        S4: AsRef<str>,
    {
        let request = build_copy_object_request(
            source_bucket_name.as_ref(),
            source_object_key.as_ref(),
            dest_bucket_name.as_ref(),
            dest_object_key.as_ref(),
            &options,
        )?;

        let (_, _) = self.do_request::<()>(request)?;

        Ok(CopyObjectResult)
    }

    /// Delete an object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deleteobject>
    fn delete_object<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<DeleteObjectOptions>) -> Result<DeleteObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let mut request = OssRequest::new()
            .method(RequestMethod::Delete)
            .bucket(bucket_name.as_ref())
            .object(object_key.as_ref());

        if let Some(options) = options {
            if let Some(s) = options.version_id {
                request = request.add_query("versionId", s);
            }
        }

        let _ = self.do_request::<()>(request)?;

        Ok(DeleteObjectResult)
    }

    /// Delete multiple objects
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletemultipleobjects>
    fn delete_multiple_objects<S1, S2>(&self, bucket_name: S1, config: DeleteMultipleObjectsConfig<'_, S2>) -> Result<DeleteMultipleObjectsResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let request = build_delete_multiple_objects_request(bucket_name.as_ref(), config)?;

        let (_, content) = self.do_request::<String>(request)?;

        DeleteMultipleObjectsResult::from_xml(&content)
    }

    /// Restore object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/restoreobject>
    fn restore_object<S1, S2>(&self, bucket_name: S1, object_key: S2, config: RestoreObjectRequest) -> Result<RestoreObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let request = build_restore_object_request(bucket_name.as_ref(), object_key.as_ref(), config)?;
        let (headers, _) = self.do_request::<()>(request)?;
        Ok(headers.into())
    }

    /// Clean retored object
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/cleanrestoredobject>
    fn clean_restored_object<S1, S2>(&self, bucket_name: S1, object_key: S2) -> Result<()>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let request = OssRequest::new()
            .method(RequestMethod::Post)
            .bucket(bucket_name.as_ref())
            .object(object_key.as_ref())
            .add_query("cleanRestoredObject", "");

        let _ = self.do_request::<()>(request)?;

        Ok(())
    }
}

#[cfg(all(test, feature = "blocking"))]
mod test_object_blocking {
    use std::{collections::HashMap, sync::Once};

    use base64::{prelude::BASE64_STANDARD, Engine};

    use crate::{
        blocking::{object::ObjectOperations, Client},
        common::{ObjectType, StorageClass},
        object_common::{DeleteMultipleObjectsConfig, GetObjectOptionsBuilder, PutObjectOptions, PutObjectOptionsBuilder},
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[test]
    fn test_upload_file_1_blocking() {
        setup();

        let client = Client::from_env();
        let result = client.put_object_from_file("yuanyq", "rust-sdk-test/katex.zip", "/home/yuanyq/Downloads/katex.zip", None);

        log::debug!("{:?}", result);

        assert!(result.is_ok());

        log::debug!("{:?}", result.unwrap());
    }

    #[test]
    fn test_upload_file_2_blocking() {
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

        let result = client.put_object_from_file(
            "yuanyq",
            "rust-sdk-test/Oracle_VirtualBox_Extension_Pack-7.1.4.vbox-extpack",
            "/home/yuanyq/Downloads/Oracle_VirtualBox_Extension_Pack-7.1.4.vbox-extpack",
            Some(options),
        );

        log::debug!("{:?}", result);

        assert!(result.is_ok());
    }

    #[test]
    fn test_create_folder_1_blocking() {
        setup();

        let client = Client::from_env();

        let result = client.create_folder("yuanyq", "rust-sdk-test/test-folder/blocking/", None);

        log::debug!("{:?}", result);

        assert!(result.is_ok())
    }

    /// Test download file with invalid options
    #[test]
    fn test_download_file_1_blocking() {
        setup();

        let client = Client::from_env();

        let options = GetObjectOptionsBuilder::new()
            .if_modified_since("Tue, 18 Feb 2025 18:03:21 GMT")
            .range("bytes=0-499")
            .build();

        let output_file = "/home/yuanyq/Downloads/ali-oss-rs-test/katex.zip.1";

        let result = client.get_object_to_file("yuanyq", "rust-sdk-test/katex.zip", output_file, Some(options));

        log::debug!("{:?}", result);

        assert!(result.is_err());
    }

    #[test]
    fn test_download_file_2_blocking() {
        setup();

        let client = Client::from_env();

        let options = GetObjectOptionsBuilder::new().range("bytes=0-499").build();

        let output_file = "/home/yuanyq/Downloads/ali-oss-rs-test/katex.zip.1";

        let result = client.get_object_to_file("yuanyq", "rust-sdk-test/katex.zip", output_file, Some(options));

        log::debug!("{:?}", result);

        assert!(result.is_ok());

        let file_meta = std::fs::metadata(output_file).unwrap();

        assert_eq!(500, file_meta.len());
    }

    #[test]
    fn test_get_object_metadata_blocking() {
        setup();
        let client = Client::from_env();

        let result = client.get_object_metadata("yuanyq", "rust-sdk-test/Oracle_VirtualBox_Extension_Pack-7.1.4.vbox-extpack", None);

        assert!(result.is_ok());

        let meta = result.unwrap();

        assert_eq!(22966826, meta.content_length);
        assert_eq!("B752E1A13502E231AC4AA0E1D91F887C", meta.etag);
        assert_eq!(Some(7873641174252289613), meta.hash_crc64ecma);
        assert_eq!(Some("Tue, 18 Feb 2025 15:03:23 GMT".to_string()), meta.last_modified);
    }

    #[test]
    fn test_head_object_blocking() {
        setup();
        let client = Client::from_env();

        let result = client.head_object("yuanyq", "rust-sdk-test/Oracle_VirtualBox_Extension_Pack-7.1.4.vbox-extpack", None);

        assert!(result.is_ok());

        let meta = result.unwrap();
        log::debug!("{:#?}", meta);
        assert_eq!(22966826, meta.content_length);
        assert_eq!("B752E1A13502E231AC4AA0E1D91F887C", meta.etag);
        assert_eq!(Some(7873641174252289613), meta.hash_crc64ecma);
        assert_eq!(Some("Tue, 18 Feb 2025 15:03:23 GMT".to_string()), meta.last_modified);
        assert_eq!(Some(ObjectType::Normal), meta.object_type);
        assert_eq!(Some(StorageClass::Standard), meta.storage_class);
        assert!(!meta.metadata.is_empty());
    }

    /// Copy object in same bucket
    #[test]
    fn test_copy_object_1_blocking() {
        setup();
        let client = Client::from_env();

        let source_bucket = "yuanyq";
        let source_object = "test.php";

        let dest_bucket = "yuanyq";
        let dest_object = "test.php.bak";

        let ret = client.copy_object(source_bucket, source_object, dest_bucket, dest_object, None);

        assert!(ret.is_ok());

        let source_meta = client.get_object_metadata(source_bucket, source_object, None).unwrap();
        let dest_meta = client.get_object_metadata(dest_bucket, dest_object, None).unwrap();

        assert_eq!(source_meta.etag, dest_meta.etag);
    }

    /// Copy object across buckets
    #[test]
    fn test_copy_object_2_blocking() {
        setup();
        let client = Client::from_env();

        let source_bucket = "yuanyq";
        let source_object = "test.php";

        let dest_bucket = "yuanyq-2";
        let dest_object = "test.php";

        let ret = client.copy_object(source_bucket, source_object, dest_bucket, dest_object, None);

        assert!(ret.is_ok());

        let source_meta = client.get_object_metadata(source_bucket, source_object, None).unwrap();
        let dest_meta = client.get_object_metadata(dest_bucket, dest_object, None).unwrap();

        assert_eq!(source_meta.etag, dest_meta.etag);
    }

    #[test]
    fn test_create_object_from_buffer_blocking() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-from-buffer.jpg";

        let options = PutObjectOptionsBuilder::new().mime_type("image/jpeg").build();

        let buffer = std::fs::read("/home/yuanyq/Pictures/f69e41cb1642c3360bd5bb6e700a0ecb.jpeg").unwrap();

        let md5 = "1ziAOyOVKo5/xAIvbUEQJA==";

        let ret = client.put_object_from_buffer(bucket, object, buffer, Some(options));

        log::debug!("{:?}", ret);

        assert!(ret.is_ok());

        let meta = client.head_object(bucket, object, None).unwrap();
        assert_eq!(Some(md5.to_string()), meta.content_md5);
    }

    #[test]
    fn test_create_object_from_base64_blocking() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-from-base64.jpg";

        let options = PutObjectOptionsBuilder::new().mime_type("image/jpeg").build();

        let buffer = std::fs::read("/home/yuanyq/Pictures/f69e41cb1642c3360bd5bb6e700a0ecb.jpeg").unwrap();
        let base64 = BASE64_STANDARD.encode(&buffer);
        let md5 = "1ziAOyOVKo5/xAIvbUEQJA==";

        let ret = client.put_object_from_base64(bucket, object, base64, Some(options));

        assert!(ret.is_ok());

        let meta = client.head_object(bucket, object, None).unwrap();
        assert_eq!(Some(md5.to_string()), meta.content_md5);
    }

    #[test]
    fn test_delete_object_blocking() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-from-buffer.jpg";

        let ret = client.delete_object(bucket, object, None);
        assert!(ret.is_ok());
    }

    #[test]
    fn test_delete_multiple_objects_blocking() {
        setup();
        let client = Client::from_env();

        let keys = [
            "rust-sdk-test/01-01.jpg",
            "rust-sdk-test/01-02.jpg",
            "rust-sdk-test/01-03.png",
            "rust-sdk-test/01-04.jpg",
            "rust-sdk-test/01-05.png",
        ];

        let response = client.delete_multiple_objects("yuanyq", DeleteMultipleObjectsConfig::FromKeys(&keys));

        log::debug!("{:#?}", response);

        assert!(response.is_ok());

        let result = response.unwrap();
        assert_eq!(5, result.items.len());

        for s in keys {
            assert!(result.items.iter().any(|item| item.key == s));
        }
    }

    #[test]
    fn test_exists_blocking() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-from-base64.jpg";

        let ret = client.exists(bucket, object, None);
        assert!(ret.is_ok());
        assert!(!ret.unwrap());

        let object = "test.php";
        let ret = client.exists(bucket, object, None);
        assert!(ret.is_ok());
        assert!(ret.unwrap());
    }

    #[test]
    fn test_append_object_blocking() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-appended-from-file-blocking.jpg";

        let file1 = "/home/yuanyq/Pictures/test-image-part-1.data";
        let file2 = "/home/yuanyq/Pictures/test-image-part-2.data";
        let file3 = "/home/yuanyq/Pictures/test-image-part-3.data";

        let ret1 = client.append_object_from_file(bucket, object, file1, 0, None);

        assert!(ret1.is_ok());

        let next_pos = ret1.unwrap().next_append_position;
        assert_eq!(61929, next_pos);

        let ret2 = client.append_object_from_file(bucket, object, file2, next_pos, None);
        assert!(ret2.is_ok());

        let next_pos = ret2.unwrap().next_append_position;
        assert_eq!(61929 * 2, next_pos);

        let ret3 = client.append_object_from_file(bucket, object, file3, next_pos, None);
        assert!(ret3.is_ok());

        let meta = client.head_object(bucket, object, None);
        log::debug!("{:#?}", meta.unwrap());
    }

    #[test]
    fn test_append_object_from_buffer_blocking() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-appended-from-buffer-blocking.jpg";

        let file1 = "/home/yuanyq/Pictures/test-image-part-1.data";
        let file2 = "/home/yuanyq/Pictures/test-image-part-2.data";
        let file3 = "/home/yuanyq/Pictures/test-image-part-3.data";

        let buffer1 = std::fs::read(file1).unwrap();
        let buffer2 = std::fs::read(file2).unwrap();
        let buffer3 = std::fs::read(file3).unwrap();

        let ret1 = client.append_object_from_buffer(bucket, object, buffer1, 0, None);
        assert!(ret1.is_ok());

        let next_pos = ret1.unwrap().next_append_position;
        assert_eq!(61929, next_pos);

        let ret2 = client.append_object_from_buffer(bucket, object, buffer2, next_pos, None);
        assert!(ret2.is_ok());

        let next_pos = ret2.unwrap().next_append_position;
        assert_eq!(61929 * 2, next_pos);

        let ret3 = client.append_object_from_buffer(bucket, object, buffer3, next_pos, None);
        assert!(ret3.is_ok());

        let meta = client.head_object(bucket, object, None);
        log::debug!("{:#?}", meta.unwrap());
    }

    #[test]
    fn test_append_object_from_base64_blocking() {
        setup();
        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/img-appended-from-base64-blocking.jpg";

        let file1 = "/home/yuanyq/Pictures/test-image-part-1.data";
        let file2 = "/home/yuanyq/Pictures/test-image-part-2.data";
        let file3 = "/home/yuanyq/Pictures/test-image-part-3.data";

        let buffer1 = std::fs::read(file1).unwrap();
        let buffer2 = std::fs::read(file2).unwrap();
        let buffer3 = std::fs::read(file3).unwrap();

        let s1 = BASE64_STANDARD.encode(buffer1);
        let s2 = BASE64_STANDARD.encode(buffer2);
        let s3 = BASE64_STANDARD.encode(buffer3);

        let ret1 = client.append_object_from_base64(bucket, object, s1, 0, None);
        assert!(ret1.is_ok());

        let next_pos = ret1.unwrap().next_append_position;
        assert_eq!(61929, next_pos);

        let ret2 = client.append_object_from_base64(bucket, object, s2, next_pos, None);
        assert!(ret2.is_ok());

        let next_pos = ret2.unwrap().next_append_position;
        assert_eq!(61929 * 2, next_pos);

        let ret3 = client.append_object_from_base64(bucket, object, s3, next_pos, None);
        assert!(ret3.is_ok());

        let meta = client.head_object(bucket, object, None);
        log::debug!("{:#?}", meta.unwrap());
    }

    // #[test]
    // fn test_delete_multiple_objects_async() {
    //     setup();
    //     let client = Client::from_env();

    //     let keys = [
    //         "rust-sdk-test/01-01.jpg",
    //         "rust-sdk-test/01-02.jpg",
    //         "rust-sdk-test/01-03.png",
    //         "rust-sdk-test/01-04.jpg",
    //         "rust-sdk-test/01-05.png"
    //     ];

    //     let response = client.delete_multiple_objects("yuanyq", DeleteMultipleObjectsConfig::FromKeys(&keys));

    //     log::debug!("{:#?}", response);

    //     assert!(response.is_ok());

    //     let result = response.unwrap();
    //     assert_eq!(5, result.items.len());

    //     for s in keys {
    //         assert!(result.items.iter().any(|item| item.key == s));
    //     }
    // }
}
