//! Common types for pre-signing

use std::collections::HashMap;

use crate::{common, request::{RequestBuilder, RequestMethod}, util};

/// Presign options for GET
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct PresignGetOptions {
    /// Time to live for this URL in seconds.
    ///
    /// The minimum value is `1`, the maximum value is `604800` seconds (7 days) for regular access key id and secret.
    /// If you use STSToken to generate a signed url, the maximum value is `43200` seconds (12 hours).
    pub expire_seconds: u32,

    pub response_content_type: Option<String>,
    pub response_content_language: Option<String>,
    pub response_content_disposition: Option<String>,
    pub response_content_encoding: Option<String>,

    pub version_id: Option<String>,

    /// OSS process for images, documents and so on.
    /// e.g. if you have a image style with name 'test-img-process',
    /// you should pass `style/test-img-process` as this query parameter value.
    pub process: Option<String>,

    /// Additional query parameters added to the presigned url
    pub query_parameters: HashMap<String, String>,
}

/// Builder for `PresignGetOptions`
#[derive(Debug, Default)]
pub struct PresignGetOptionsBuilder {
    expire_seconds: u32,
    response_content_type: Option<String>,
    response_content_language: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    version_id: Option<String>,
    process: Option<String>,
    query_parameters: HashMap<String, String>,
}

impl PresignGetOptionsBuilder {
    pub fn new(expire_seconds: u32) -> Self {
        Self {
            expire_seconds,
            ..Default::default()
        }
    }

    pub fn expires_seconds(mut self, expires_seconds: u32) -> Self {
        self.expire_seconds = expires_seconds;
        self
    }

    pub fn response_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.response_content_type = Some(content_type.into());
        self
    }

    pub fn response_content_language(mut self, language: impl Into<String>) -> Self {
        self.response_content_language = Some(language.into());
        self
    }

    pub fn response_content_disposition(mut self, disposition: impl Into<String>) -> Self {
        self.response_content_disposition = Some(disposition.into());
        self
    }

    pub fn response_content_encoding(mut self, encoding: impl Into<String>) -> Self {
        self.response_content_encoding = Some(encoding.into());
        self
    }

    pub fn version_id(mut self, version_id: impl Into<String>) -> Self {
        self.version_id = Some(version_id.into());
        self
    }

    pub fn process(mut self, process: impl Into<String>) -> Self {
        self.process = Some(process.into());
        self
    }

    pub fn query_parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query_parameters.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> PresignGetOptions {
        PresignGetOptions {
            expire_seconds: self.expire_seconds,
            response_content_type: self.response_content_type,
            response_content_language: self.response_content_language,
            response_content_disposition: self.response_content_disposition,
            response_content_encoding: self.response_content_encoding,
            version_id: self.version_id,
            process: self.process,
            query_parameters: self.query_parameters,
        }
    }
}

pub(crate) fn build_presign_get_request(bucket_name: &str, object_key: &str, options: &PresignGetOptions) -> RequestBuilder {
    let mut request = RequestBuilder::new()
        .method(RequestMethod::Get)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("x-oss-expires", options.expire_seconds.to_string())
        .add_query("x-oss-signature-version", common::SIGNATURE_VERSION);

    // move the `x-oss-date` from header which is set in `RequestBuilder::new()`
    let oss_date = request.headers_mut().remove("x-oss-date").unwrap_or(util::get_iso8601_date_time_string());
    request = request.add_query("x-oss-date", oss_date);

    // clear all other headers because this is a get request
    // and we do not support additional request header included in signature calculation so far.
    request.headers_mut().clear();

    if let Some(s) = &options.response_content_type {
        request = request.add_query("response-content-type", s);
    }

    if let Some(s) = &options.response_content_encoding {
        request = request.add_query("response-content-encoding", s);
    }

    if let Some(s) = &options.response_content_language {
        request = request.add_query("response-content-language", s);
    }

    if let Some(s) = &options.response_content_disposition {
        request = request.add_query("response-content-disposition", s);
    }

    if let Some(s) = &options.process {
        request = request.add_query("x-oss-process", s);
    }

    if let Some(s) = &options.version_id {
        request = request.add_query("versionId", s);
    }

    if !options.query_parameters.is_empty() {
        for (k, v) in &options.query_parameters {
            request = request.add_query(k, v);
        }
    }

    request
}
