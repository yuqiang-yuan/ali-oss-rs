use std::ops::Range;
use std::path::Path;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;

use crate::error::Error;
use crate::multipart_common::{
    build_complete_multipart_uploads_request, build_initiate_multipart_uploads_request, build_list_multipart_uploads_request, build_list_parts_request,
    build_upload_part_copy_request, build_upload_part_request, CompleteMultipartUploadRequest, CompleteMultipartUploadResult, InitiateMultipartUploadOptions,
    InitiateMultipartUploadResult, ListMultipartUploadsOptions, ListMultipartUploadsResult, ListPartsOptions, ListPartsResult, UploadPartCopyOptions,
    UploadPartCopyRequest, UploadPartCopyResult, UploadPartRequest, UploadPartResult,
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
    fn complete_multipart_uploads<S1, S2>(&self, bucket_name: S1, object_key: S2, data: CompleteMultipartUploadRequest) -> Result<CompleteMultipartUploadResult>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let request = build_complete_multipart_uploads_request(bucket_name.as_ref(), object_key.as_ref(), data)?;
        let (_, xml) = self.do_request::<String>(request)?;
        CompleteMultipartUploadResult::from_xml(&xml)
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
