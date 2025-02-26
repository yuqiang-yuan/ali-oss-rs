use std::ops::Range;
use std::path::Path;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;

use crate::error::Error;
use crate::multipart_common::{
    build_complete_multipart_uploads_request, build_initiate_multipart_uploads_request, build_list_multipart_uploads_request, build_list_parts_request,
    build_upload_part_copy_request, build_upload_part_request, CompleteMultipartUploadApiResponse, CompleteMultipartUploadOptions,
    CompleteMultipartUploadRequest, CompleteMultipartUploadResult, InitiateMultipartUploadOptions, InitiateMultipartUploadResult, ListMultipartUploadsOptions,
    ListMultipartUploadsResult, ListPartsOptions, ListPartsResult, UploadPartCopyOptions, UploadPartCopyRequest, UploadPartCopyResult, UploadPartRequest,
    UploadPartResult,
};
use crate::request::{OssRequest, RequestMethod};
use crate::util::{validate_bucket_name, validate_object_key};
use crate::{RequestBody, Result};

use super::Client;

pub trait MultipartUploadsOperations {
    /// List multipart uploads which are initialized but not completed nor aborted.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listmultipartuploads>
    fn list_multipart_uploads<S>(&self, bucket_name: S, options: Option<ListMultipartUploadsOptions>) -> Result<ListMultipartUploadsResult>
    where
        S: AsRef<str>;

    /// List parts which uploaded successfully associated with the given `upload_id`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listparts>
    fn list_parts<S1, S2, S3>(&self, bucket_name: S1, object_key: S2, upload_id: S3, options: Option<ListPartsOptions>) -> Result<ListPartsResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>;

    /// Initiate multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/initiatemultipartupload>
    fn initiate_multipart_uploads<S1, S2>(
        &self,
        bucket_name: S1,
        object_key: S2,
        options: Option<InitiateMultipartUploadOptions>,
    ) -> Result<InitiateMultipartUploadResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Upload part of a file. the caller should take responsibility to make sure the range is valid.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpart>
    fn upload_part_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        range: Range<u64>,
        params: UploadPartRequest,
    ) -> Result<UploadPartResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>;

    /// Upload part from buffer.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpart>
    fn upload_part_from_buffer<S1, S2, B>(&self, bucket_name: S1, object_key: S2, buffer: B, params: UploadPartRequest) -> Result<UploadPartResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        B: Into<Vec<u8>>;

    /// Upload part from base64 string.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpart>
    fn upload_part_from_base64<S1, S2, S3>(&self, bucket_name: S1, object_key: S2, base64_string: S3, params: UploadPartRequest) -> Result<UploadPartResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>;

    /// When you want to copy a file larger than 1GB, you must use `upload_part_copy`.
    /// First, initiate a multipart upload and get `uploadId`, then call this method to upload parts of the source object.
    /// Finally complete the multipart upload by invoking `complete_multipart_uploads`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpartcopy>
    fn upload_part_copy<S1, S2>(
        &self,
        bucket_name: S1,
        dest_object_key: S2,
        data: UploadPartCopyRequest,
        options: Option<UploadPartCopyOptions>,
    ) -> Result<UploadPartCopyResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Complete multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/completemultipartupload>
    fn complete_multipart_uploads<S1, S2>(
        &self,
        bucket_name: S1,
        object_key: S2,
        data: CompleteMultipartUploadRequest,
        options: Option<CompleteMultipartUploadOptions>,
    ) -> Result<CompleteMultipartUploadResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// About multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/abortmultipartupload>
    fn abort_multipart_uploads<S1, S2, S3>(&self, bucket_name: S1, object_key: S2, upload_id: S3) -> Result<()>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>;
}

impl MultipartUploadsOperations for Client {
    /// List multipart uploads which are initialized but not completed nor aborted.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listmultipartuploads>
    fn list_multipart_uploads<S>(&self, bucket_name: S, options: Option<ListMultipartUploadsOptions>) -> Result<ListMultipartUploadsResult>
    where
        S: AsRef<str>,
    {
        if !validate_bucket_name(bucket_name.as_ref()) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name.as_ref())));
        }
        let request = build_list_multipart_uploads_request(bucket_name.as_ref(), &options)?;
        let (_, xml) = self.do_request::<String>(request)?;

        ListMultipartUploadsResult::from_xml(&xml)
    }

    /// List parts which uploaded successfully associated with the given `upload_id`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listparts>
    fn list_parts<S1, S2, S3>(&self, bucket_name: S1, object_key: S2, upload_id: S3, options: Option<ListPartsOptions>) -> Result<ListPartsResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
    {
        let request = build_list_parts_request(bucket_name.as_ref(), object_key.as_ref(), upload_id.as_ref(), &options)?;
        let (_, xml) = self.do_request::<String>(request)?;
        ListPartsResult::from_xml(&xml)
    }

    /// Initiate multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/initiatemultipartupload>
    fn initiate_multipart_uploads<S1, S2>(
        &self,
        bucket_name: S1,
        object_key: S2,
        options: Option<InitiateMultipartUploadOptions>,
    ) -> Result<InitiateMultipartUploadResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let request = build_initiate_multipart_uploads_request(bucket_name.as_ref(), object_key.as_ref(), &options)?;
        let (_, xml) = self.do_request::<String>(request)?;
        InitiateMultipartUploadResult::from_xml(&xml)
    }

    /// Upload part of a file. the caller should take responsibility to make sure the range is valid.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpart>
    fn upload_part_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        range: Range<u64>,
        params: UploadPartRequest,
    ) -> Result<UploadPartResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        P: AsRef<Path>,
    {
        let request = build_upload_part_request(
            bucket_name.as_ref(),
            object_key.as_ref(),
            RequestBody::File(file_path.as_ref().to_path_buf(), Some(range)),
            params,
        )?;

        let (headers, _) = self.do_request::<()>(request)?;

        Ok(headers.into())
    }

    /// Upload part from buffer.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpart>
    fn upload_part_from_buffer<S1, S2, B>(&self, bucket_name: S1, object_key: S2, buffer: B, params: UploadPartRequest) -> Result<UploadPartResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        B: Into<Vec<u8>>,
    {
        let request = build_upload_part_request(bucket_name.as_ref(), object_key.as_ref(), RequestBody::Bytes(buffer.into()), params)?;

        let (headers, _) = self.do_request::<()>(request)?;

        Ok(headers.into())
    }

    /// Upload part from base64 string.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpart>
    fn upload_part_from_base64<S1, S2, S3>(&self, bucket_name: S1, object_key: S2, base64_string: S3, params: UploadPartRequest) -> Result<UploadPartResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
    {
        let data = BASE64_STANDARD.decode(base64_string.as_ref())?;
        self.upload_part_from_buffer(bucket_name, object_key, data, params)
    }

    /// When you want to copy a file larger than 1GB, you must use `upload_part_copy`.
    /// First, initiate a multipart upload and get `uploadId`, then call this method to upload parts of the source object.
    /// Finally complete the multipart upload by invoking `complete_multipart_uploads`
    ///
    /// Offical document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpartcopy>
    fn upload_part_copy<S1, S2>(
        &self,
        bucket_name: S1,
        dest_object_key: S2,
        data: UploadPartCopyRequest,
        options: Option<UploadPartCopyOptions>,
    ) -> Result<UploadPartCopyResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = dest_object_key.as_ref();
        let requet = build_upload_part_copy_request(bucket_name, object_key, data, &options)?;
        let (_, xml) = self.do_request::<String>(requet)?;
        UploadPartCopyResult::from_xml(&xml)
    }

    /// Complete multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/completemultipartupload>
    fn complete_multipart_uploads<S1, S2>(
        &self,
        bucket_name: S1,
        object_key: S2,
        data: CompleteMultipartUploadRequest,
        options: Option<CompleteMultipartUploadOptions>,
    ) -> Result<CompleteMultipartUploadResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let with_callback = if let Some(opt) = &options { opt.callback.is_some() } else { false };

        let request = build_complete_multipart_uploads_request(bucket_name.as_ref(), object_key.as_ref(), data, &options)?;
        let (_, content) = self.do_request::<String>(request)?;

        if with_callback {
            Ok(CompleteMultipartUploadResult::CallbackResponse(content))
        } else {
            Ok(CompleteMultipartUploadResult::ApiResponse(CompleteMultipartUploadApiResponse::from_xml(
                &content,
            )?))
        }
    }

    /// About multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/abortmultipartupload>
    fn abort_multipart_uploads<S1, S2, S3>(&self, bucket_name: S1, object_key: S2, upload_id: S3) -> Result<()>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
    {
        let bucket_name = bucket_name.as_ref();
        let object_key = object_key.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        if !validate_object_key(object_key) {
            return Err(Error::Other(format!("invalid object key: {}", object_key)));
        }

        if upload_id.as_ref().is_empty() {
            return Err(Error::Other("invalid upload id: [empty]".to_string()));
        }

        let request = OssRequest::new()
            .method(RequestMethod::Delete)
            .bucket(bucket_name)
            .object(object_key)
            .add_query("uploadId", upload_id);

        self.do_request::<()>(request)?;

        Ok(())
    }
}

#[cfg(all(test, feature = "blocking"))]
mod test_multipart_blocking {
    use std::{
        io::{Read, Seek},
        ops::Range,
        sync::Once,
    };

    use uuid::Uuid;

    use crate::{
        blocking::{multipart::MultipartUploadsOperations, object::ObjectOperations, Client},
        multipart_common::{
            CompleteMultipartUploadOptions, CompleteMultipartUploadRequest, CompleteMultipartUploadResult, UploadPartCopyOptionsBuilder, UploadPartCopyRequest,
            UploadPartRequest,
        },
        object_common::{CallbackBodyParameter, CallbackBuilder},
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[allow(dead_code)]
    fn setup_comp() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::from_filename(".env.comp").unwrap();
        });
    }

    // #[test]
    // fn test_list_multipart_uploads_blocking_1() {
    //     setup_comp();

    //     let client = Client::from_env();
    //     let response = client.list_multipart_uploads("mi-dev-public", None);
    //     assert!(response.is_ok());

    //     let ret = response.unwrap();
    //     log::debug!("{:#?}", ret);

    //     assert!(ret.max_uploads > 0);
    // }

    // #[test]
    // fn test_list_multipart_uploads_blocking_2() {
    //     setup_comp();

    //     let client = Client::from_env();
    //     let options = ListMultipartUploadsOptionsBuilder::new()
    //         .prefix("builder/editor/2023/000-278/videos/c29s08f01-032-663b31e15a44347d59de9e75/")
    //         .delimiter('/')
    //         .max_uploads(20)
    //         .build();

    //     let response = client.list_multipart_uploads("mi-dev-public", Some(options));
    //     assert!(response.is_ok());

    //     let ret = response.unwrap();
    //     log::debug!("{:#?}", ret);

    //     assert!(ret.max_uploads > 0);
    // }

    #[test]
    fn test_multipart_uploads_from_file_blocking() {
        setup();

        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = format!("rust-sdk-test/multipart-{}.deb", Uuid::new_v4());
        let file = "/home/yuanyq/Downloads/sourcegit_2025.06-1_amd64.deb";

        let meta = std::fs::metadata(file).unwrap();

        let slice_len: u64 = 5 * 1024 * 1024;
        let mut ranges = vec![];
        let mut c = 0;
        loop {
            let end = (c + 1) * slice_len;
            let r = Range {
                start: c * slice_len,
                end: end.min(meta.len()),
            };

            ranges.push(r);

            if end >= meta.len() {
                break;
            }

            c += 1;
        }

        log::debug!("{:#?}", ranges);

        let init_response = client.initiate_multipart_uploads(bucket, &object, None);
        assert!(init_response.is_ok());

        let init_result = init_response.unwrap();
        let upload_id = init_result.upload_id.clone();
        log::debug!("upload id = {}", upload_id);

        let mut upload_results = vec![];

        for (i, rng) in ranges.iter().enumerate() {
            let upload_data = UploadPartRequest {
                part_number: (i + 1) as u32,
                upload_id: upload_id.clone(),
            };

            log::debug!("begin to upload part {}", i);

            let upload_response = client.upload_part_from_file(bucket, &object, file, rng.clone(), upload_data);

            log::debug!("{:#?}", upload_response);

            assert!(upload_response.is_ok());

            let upload_result = upload_response.unwrap();
            upload_results.push(((i + 1) as u32, upload_result.etag));
        }

        log::debug!("all parts uploaded, check it");
        let resp = client.list_parts(bucket, &object, &upload_id, None);
        assert!(resp.is_ok());

        let ret = resp.unwrap();
        assert_eq!(ranges.len(), ret.parts.len());

        log::debug!("going to complete multipart upload for upload id: {}", upload_id);

        let comp_response = client.complete_multipart_uploads(
            bucket,
            &object,
            CompleteMultipartUploadRequest {
                upload_id,
                parts: upload_results,
            },
            None,
        );

        log::debug!("{:#?}", comp_response);

        log::debug!("multipart uploads completed");

        client.delete_object(bucket, &object, None).unwrap();
    }

    #[test]
    fn test_upload_part_from_buffer_blocking() {
        setup();

        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = format!("rust-sdk-test/multipart-{}.deb", Uuid::new_v4());
        let file = "/home/yuanyq/Downloads/sourcegit_2025.06-1_amd64.deb";

        let meta = std::fs::metadata(file).unwrap();

        let slice_len: u64 = 10 * 1024 * 1024;
        let mut ranges = vec![];
        let mut c = 0;
        loop {
            let end = (c + 1) * slice_len;
            let r = Range {
                start: c * slice_len,
                end: end.min(meta.len()),
            };

            ranges.push(r);

            if end >= meta.len() {
                break;
            }

            c += 1;
        }

        log::debug!("{:#?}", ranges);

        let init_response = client.initiate_multipart_uploads(bucket, &object, None);
        assert!(init_response.is_ok());

        let init_result = init_response.unwrap();
        let upload_id = init_result.upload_id.clone();
        log::debug!("upload id = {}", upload_id);

        let mut upload_results = vec![];

        for (i, rng) in ranges.iter().enumerate() {
            let part_no = (i + 1) as u32;

            log::debug!("begin to upload part {}", i);

            let mut buf = Vec::new();
            let mut stream = std::fs::File::open(file).unwrap();
            stream.seek(std::io::SeekFrom::Start(rng.start)).unwrap();
            let mut partial = stream.take(rng.end - rng.start);
            partial.read_to_end(&mut buf).unwrap();

            let upload_data = UploadPartRequest {
                part_number: part_no,
                upload_id: upload_id.clone(),
            };

            let upload_response = client.upload_part_from_buffer(bucket, &object, buf, upload_data);

            log::debug!("{:#?}", upload_response);

            assert!(upload_response.is_ok());

            let upload_result = upload_response.unwrap();
            upload_results.push(((i + 1) as u32, upload_result.etag));
        }

        let list_parts_response = client.list_parts(bucket, &object, upload_id.clone(), None);
        log::debug!("{:#?}", list_parts_response);
        assert!(list_parts_response.is_ok());
        let list_parts_result = list_parts_response.unwrap();

        assert_eq!(ranges.len(), list_parts_result.parts.len());

        let abort_response = client.abort_multipart_uploads(bucket, &object, upload_id.clone());
        log::debug!("{:#?}", abort_response);
        assert!(abort_response.is_ok());

        let resp = client.exists(bucket, &object, None);
        assert!(resp.is_ok());
        assert!(!resp.unwrap());
    }

    #[test]
    fn test_upload_part_copy_blocking() {
        setup();

        let client = Client::from_env();

        let bucket = "yuanyq";

        let source_object_key = "rust-sdk-test/img-appended-from-file.jpg";
        let dest_object_key = format!("rust-sdk-test/img-{}.jpg", Uuid::new_v4());

        let init_response = client.initiate_multipart_uploads(bucket, &dest_object_key, None);
        assert!(init_response.is_ok());

        let upload_id = init_response.unwrap().upload_id.clone();

        // part 1
        let upload_response = client.upload_part_copy(
            bucket,
            &dest_object_key,
            UploadPartCopyRequest::new(1, &upload_id, source_object_key),
            Some(UploadPartCopyOptionsBuilder::new().copy_source_range("bytes=0-185000").build()),
        );
        assert!(upload_response.is_ok());
        log::debug!("upload response 1: {:#?}", upload_response);

        let etag1 = upload_response.unwrap().etag;

        let upload_response = client.upload_part_copy(
            bucket,
            &dest_object_key,
            UploadPartCopyRequest::new(2, &upload_id, source_object_key),
            Some(UploadPartCopyOptionsBuilder::new().copy_source_range("bytes=185001-").build()),
        );
        assert!(upload_response.is_ok());
        log::debug!("upload response 2: {:#?}", upload_response);

        let etag2 = upload_response.unwrap().etag;

        let comp_data = CompleteMultipartUploadRequest {
            upload_id,
            parts: vec![(1, etag1), (2, etag2)],
        };

        let comp_response = client.complete_multipart_uploads(bucket, &dest_object_key, comp_data, None);
        log::debug!("complete multipart upload response: {:#?}", comp_response);

        client.delete_object(bucket, &dest_object_key, None).unwrap();
    }

    #[test]
    fn test_multipart_upload_with_callback_blocking() {
        log::debug!("test multipart upload with callback while completing");
        setup();

        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = format!("rust-sdk-test/multipart-{}.deb", Uuid::new_v4());
        let file = "/home/yuanyq/Downloads/sourcegit_2025.06-1_amd64.deb";

        let meta = std::fs::metadata(file).unwrap();

        let slice_len: u64 = 5 * 1024 * 1024;
        let mut ranges = vec![];
        let mut c = 0;
        loop {
            let end = (c + 1) * slice_len;
            let r = Range {
                start: c * slice_len,
                end: end.min(meta.len()),
            };

            ranges.push(r);

            if end >= meta.len() {
                break;
            }

            c += 1;
        }

        log::debug!("{:#?}", ranges);

        let init_response = client.initiate_multipart_uploads(bucket, &object, None);
        assert!(init_response.is_ok());

        let init_result = init_response.unwrap();
        let upload_id = init_result.upload_id.clone();
        log::debug!("upload id = {}", upload_id);

        let mut upload_results = vec![];

        for (i, rng) in ranges.iter().enumerate() {
            let upload_data = UploadPartRequest {
                part_number: (i + 1) as u32,
                upload_id: upload_id.clone(),
            };

            log::debug!("begin to upload part {}", i);

            let upload_response = client.upload_part_from_file(bucket, &object, file, rng.clone(), upload_data);

            log::debug!("{:#?}", upload_response);

            assert!(upload_response.is_ok());

            let upload_result = upload_response.unwrap();
            upload_results.push(((i + 1) as u32, upload_result.etag));
        }

        log::debug!("all parts uploaded, check it");
        let resp = client.list_parts(bucket, &object, &upload_id, None);
        assert!(resp.is_ok());

        let ret = resp.unwrap();
        assert_eq!(ranges.len(), ret.parts.len());

        log::debug!("going to complete multipart upload for upload id: {}", upload_id);

        let cb = CallbackBuilder::new("https://dev.mbook.cc/oss-callback-test.php")
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

        let options = CompleteMultipartUploadOptions { callback: Some(cb) };

        let comp_response = client.complete_multipart_uploads(
            bucket,
            &object,
            CompleteMultipartUploadRequest {
                upload_id,
                parts: upload_results,
            },
            Some(options),
        );

        log::debug!("{:#?}", comp_response);

        log::debug!("multipart uploads completed");

        let ret = comp_response.unwrap();

        if let CompleteMultipartUploadResult::CallbackResponse(s) = ret {
            assert!(s.contains(&serde_json::to_string(&object).unwrap()));
        } else {
            panic!("no callback json content returned");
        }

        client.delete_object(bucket, &object, None).unwrap();
    }
}
