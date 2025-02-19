use std::path::Path;

use crate::{
    error::{ClientError, ClientResult},
    object_common::{
        build_get_object_request, build_put_object_request, GetObjectMetadataOptions, GetObjectOptions, ObjectMetadata, PutObjectOptions, PutObjectResult,
    },
    request::{RequestBuilder, RequestMethod},
    util::validate_path,
};

use super::{BytesBody, Client};

pub trait ObjectOperations {
    ///
    /// Uploads a file to a specified bucket and object key.
    /// The file length must be greater than 0.
    ///
    fn upload_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>;

    ///
    /// Download file to local file.
    fn download_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<GetObjectOptions>) -> ClientResult<()>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>;

    ///
    /// Create a "folder"
    fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    fn get_object_metadata<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> ClientResult<ObjectMetadata>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;
}

impl ObjectOperations for Client {
    fn upload_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
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

        let request = build_put_object_request(bucket_name, object_key, Some(file_path), &options)?;

        let (headers, _) = self.do_request::<()>(request)?;

        Ok(PutObjectResult::from_headers(&headers))
    }

    ///
    /// Download file to local file.
    fn download_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<GetObjectOptions>) -> ClientResult<()>
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
            return Err(ClientError::Error(format!("invalid file path: {:?}", file_path.as_os_str().to_str())));
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

        Ok(())
    }

    fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
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

        let request = build_put_object_request(bucket_name, &object_key, None, &options)?;

        let (headers, _) = self.do_request::<()>(request)?;

        Ok(PutObjectResult::from_headers(&headers))
    }

    fn get_object_metadata<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectMetadataOptions>) -> ClientResult<ObjectMetadata>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
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

        let (headers, _) = self.do_request::<()>(request)?;
        Ok(ObjectMetadata::from_headers(&headers))
    }
}

#[cfg(all(test, feature = "blocking"))]
mod test_object_blocking {
    use std::{collections::HashMap, sync::Once};

    use crate::{
        blocking::{object::ObjectOperations, Client},
        object_common::{GetObjectOptionsBuilder, PutObjectOptions},
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[test]
    fn test_upload_file_1() {
        setup();

        let client = Client::from_env();
        let result = client.upload_file("yuanyq", "rust-sdk-test/katex.zip", "/home/yuanyq/Downloads/katex.zip", None);

        log::debug!("{:?}", result);

        assert!(result.is_ok());

        log::debug!("{:?}", result.unwrap());
    }

    #[test]
    fn test_upload_file_2() {
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

        let result = client.upload_file(
            "yuanyq",
            "rust-sdk-test/Oracle_VirtualBox_Extension_Pack-7.1.4.vbox-extpack",
            "/home/yuanyq/Downloads/Oracle_VirtualBox_Extension_Pack-7.1.4.vbox-extpack",
            Some(options),
        );

        log::debug!("{:?}", result);

        assert!(result.is_ok());
    }

    #[test]
    fn test_create_folder_1() {
        setup();

        let client = Client::from_env();

        let result = client.create_folder("yuanyq", "rust-sdk-test/test-folder/blocking/", None);

        log::debug!("{:?}", result);

        assert!(result.is_ok())
    }

    /// Test download file with invalid options
    #[test]
    fn test_download_file_1() {
        setup();

        let client = Client::from_env();

        let options = GetObjectOptionsBuilder::new()
            .if_modified_since("Tue, 18 Feb 2025 18:03:21 GMT")
            .range("bytes=0-499")
            .build();

        let output_file = "/home/yuanyq/Downloads/ali-oss-rs-test/katex.zip.1";

        let result = client.download_file("yuanyq", "rust-sdk-test/katex.zip", output_file, Some(options));

        log::debug!("{:?}", result);

        assert!(result.is_err());
    }

    #[test]
    fn test_download_file_2() {
        setup();

        let client = Client::from_env();

        let options = GetObjectOptionsBuilder::new().range("bytes=0-499").build();

        let output_file = "/home/yuanyq/Downloads/ali-oss-rs-test/katex.zip.1";

        let result = client.download_file("yuanyq", "rust-sdk-test/katex.zip", output_file, Some(options));

        log::debug!("{:?}", result);

        assert!(result.is_ok());

        let file_meta = std::fs::metadata(output_file).unwrap();

        assert_eq!(500, file_meta.len());
    }

    #[test]
    fn test_get_object_metadata() {
        setup();
        let client = Client::from_env();

        let result = client.get_object_metadata("yuanyq", "rust-sdk-test/Oracle_VirtualBox_Extension_Pack-7.1.4.vbox-extpack", None);

        assert!(result.is_ok());

        let meta = result.unwrap();

        assert_eq!(22966826, meta.content_length);
        assert_eq!(Some("\"B752E1A13502E231AC4AA0E1D91F887C\"".to_string()), meta.etag);
        assert_eq!(Some("7873641174252289613".to_string()), meta.hash_crc64ecma);
        assert_eq!(Some("Tue, 18 Feb 2025 15:03:23 GMT".to_string()), meta.last_modified);
    }
}
