//! Mutlipart uploads related operations module

use std::{ops::Range, path::Path};

use async_trait::async_trait;

use crate::{
    error::Result,
    multipart_common::{
        build_complete_multipart_uploads_request, build_initiate_multipart_uploads_request, build_list_multipart_uploads_request, build_upload_part_request,
        CompleteMultipartUploadRequest, CompleteMultipartUploadResult, InitiateMultipartUploadOptions, InitiateMultipartUploadResult,
        ListMultipartUploadsOptions, ListMultipartUploadsResult, UploadPartRequest, UploadPartResult,
    },
    Client, RequestBody,
};

#[async_trait]
pub trait MultipartUploadsOperations {
    /// List multipart uploads which are initialized but not completed nor aborted.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listmultipartuploads>
    async fn list_multipart_uploads<S>(&self, bucket_name: S, options: Option<ListMultipartUploadsOptions>) -> Result<ListMultipartUploadsResult>
    where
        S: AsRef<str> + Send;

    /// Initiate multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/initiatemultipartupload>
    async fn initiate_multipart_uploads<S1, S2>(
        &self,
        bucket_name: S1,
        object_key: S2,
        options: Option<InitiateMultipartUploadOptions>,
    ) -> Result<InitiateMultipartUploadResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Upload part of a file. the caller should take responsibility to make sure the range is valid.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpart>
    async fn upload_part_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        range: Range<u64>,
        params: UploadPartRequest,
    ) -> Result<UploadPartResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        P: AsRef<Path> + Send;

    /// Complete multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/completemultipartupload>
    async fn complete_multipart_uploads<S1, S2>(
        &self,
        bucket_name: S1,
        object_key: S2,
        data: CompleteMultipartUploadRequest,
    ) -> Result<CompleteMultipartUploadResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;
}

#[async_trait]
impl MultipartUploadsOperations for Client {
    /// List multipart uploads which are initialized but not completed nor aborted.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listmultipartuploads>
    async fn list_multipart_uploads<S>(&self, bucket_name: S, options: Option<ListMultipartUploadsOptions>) -> Result<ListMultipartUploadsResult>
    where
        S: AsRef<str> + Send,
    {
        let request = build_list_multipart_uploads_request(bucket_name.as_ref(), &options)?;
        let (_, xml) = self.do_request::<String>(request).await?;

        ListMultipartUploadsResult::from_xml(&xml)
    }

    /// Initiate multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/initiatemultipartupload>
    async fn initiate_multipart_uploads<S1, S2>(
        &self,
        bucket_name: S1,
        object_key: S2,
        options: Option<InitiateMultipartUploadOptions>,
    ) -> Result<InitiateMultipartUploadResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let request = build_initiate_multipart_uploads_request(bucket_name.as_ref(), object_key.as_ref(), &options)?;
        let (_, xml) = self.do_request::<String>(request).await?;
        InitiateMultipartUploadResult::from_xml(&xml)
    }

    /// Upload part of a file. the caller should take responsibility to make sure the range is valid.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/uploadpart>
    async fn upload_part_from_file<S1, S2, P>(
        &self,
        bucket_name: S1,
        object_key: S2,
        file_path: P,
        range: Range<u64>,
        params: UploadPartRequest,
    ) -> Result<UploadPartResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        P: AsRef<Path> + Send,
    {
        let request = build_upload_part_request(
            bucket_name.as_ref(),
            object_key.as_ref(),
            RequestBody::File(file_path.as_ref().to_path_buf(), Some(range)),
            params,
        )?;

        let (headers, _) = self.do_request::<()>(request).await?;

        Ok(headers.into())
    }

    /// Complete multipart uploads
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/completemultipartupload>
    async fn complete_multipart_uploads<S1, S2>(
        &self,
        bucket_name: S1,
        object_key: S2,
        data: CompleteMultipartUploadRequest,
    ) -> Result<CompleteMultipartUploadResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let request = build_complete_multipart_uploads_request(bucket_name.as_ref(), object_key.as_ref(), data)?;
        let (_, xml) = self.do_request::<String>(request).await?;
        CompleteMultipartUploadResult::from_xml(&xml)
    }
}

#[cfg(test)]
mod test_multipart_async {
    use std::{ops::Range, sync::Once};

    use crate::{
        multipart::MultipartUploadsOperations,
        multipart_common::{CompleteMultipartUploadRequest, ListMultipartUploadsOptionsBuilder, UploadPartRequest},
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
    async fn test_list_multipart_uploads_async_1() {
        setup();

        let client = Client::from_env();
        let response = client.list_multipart_uploads("mi-dev-public", None).await;
        assert!(response.is_ok());

        let ret = response.unwrap();
        log::debug!("{:#?}", ret);

        assert!(ret.max_uploads > 0);
    }

    #[tokio::test]
    async fn test_list_multipart_uploads_async_2() {
        setup();

        let client = Client::from_env();
        let options = ListMultipartUploadsOptionsBuilder::new()
            .prefix("builder/editor/2023/000-278/videos/c29s08f01-032-663b31e15a44347d59de9e75/")
            .delimiter('/')
            .max_uploads(20)
            .build();

        let response = client.list_multipart_uploads("mi-dev-public", Some(options)).await;
        assert!(response.is_ok());

        let ret = response.unwrap();
        log::debug!("{:#?}", ret);

        assert!(ret.max_uploads > 0);
    }

    #[tokio::test]
    async fn test_multipart_uploads_from_file_async() {
        setup();

        let client = Client::from_env();

        let bucket = "yuanyq";
        let object = "rust-sdk-test/multipart-uploads-from-file.zip";
        let file = "/home/yuanyq/Downloads/ubuntu-latest-builds.zip";

        let meta = std::fs::metadata(file).unwrap();

        let slice_len: u64 = 50 * 1024 * 1024;
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

        let init_response = client.initiate_multipart_uploads(bucket, object, None).await;
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

            let upload_response = client.upload_part_from_file(bucket, object, file, rng.clone(), upload_data).await;

            log::debug!("{:#?}", upload_response);

            assert!(upload_response.is_ok());

            let upload_result = upload_response.unwrap();
            upload_results.push(((i + 1) as u32, upload_result.etag));
        }

        log::debug!("going to complete multipart upload for upload id: {}", upload_id);

        let comp_response = client
            .complete_multipart_uploads(
                bucket,
                object,
                CompleteMultipartUploadRequest {
                    upload_id,
                    parts: upload_results,
                },
            )
            .await;

        log::debug!("{:#?}", comp_response);
    }
}
