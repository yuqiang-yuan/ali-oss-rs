use std::path::Path;

use crate::{error::ClientResult, object_common::{build_put_object_request, PutObjectOptions, PutObjectResult}};

use super::Client;

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
    /// Create a "folder"
    fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;
}

impl ObjectOperations for Client {
    fn upload_file<S1, S2, P>(&self, bucket_name: S1, object_key: S2, file_path: P, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>
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

    fn create_folder<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<PutObjectOptions>) -> ClientResult<PutObjectResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>
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
}


#[cfg(all(test, feature = "blocking"))]
mod test_object_blocking {
    use std::{collections::HashMap, sync::Once};

    use crate::{blocking::{object::ObjectOperations, Client}, object_common::PutObjectOptions};

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
        let result = client
            .upload_file(
                "yuanyq",
                "rust-sdk-test/katex.zip",
                "/home/yuanyq/Downloads/katex.zip",
                None,
            );

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

        let result = client
            .upload_file(
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
}
