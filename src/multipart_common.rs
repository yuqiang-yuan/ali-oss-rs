//! Multipart upload types

use std::collections::HashMap;

use base64::{prelude::BASE64_STANDARD, Engine};
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};

use crate::{
    common,
    error::Error,
    object_common::{build_put_object_request, Callback, PutObjectOptions, PutObjectOptionsBuilder},
    request::{OssRequest, RequestMethod},
    util::{sanitize_etag, validate_bucket_name, validate_object_key},
    RequestBody, Result,
};

pub type InitiateMultipartUploadOptions = PutObjectOptions;
pub type InitiateMultipartUploadOptionsBuilder = PutObjectOptionsBuilder;

/// Initiate mutlipart upload result
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct InitiateMultipartUploadResult {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

impl InitiateMultipartUploadResult {
    pub(crate) fn from_xml(xml: &str) -> Result<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut tag = String::new();
        let mut data = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
                Event::Text(s) => {
                    let text = s.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "Bucket" => data.bucket = text,
                        "Key" => data.key = text,
                        "UploadId" => data.upload_id = text,
                        _ => {}
                    }
                }
                Event::End(_) => tag.clear(),
                _ => {}
            }
        }

        Ok(data)
    }
}

/// Request data for upload part
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct UploadPartRequest {
    /// 每一个上传的 Part 都有一个标识它的号码（partNumber）。
    ///
    /// 取值：1~10000
    ///
    /// 单个 Part 的大小限制为 100 KB~5 GB。
    /// MultipartUpload 事件中除最后一个 Part 以外，其他 Part 的大小都要大于或等于 100 KB。
    /// 因不确定是否为最后一个 Part，
    /// UploadPart 接口并不会立即校验上传 Part 的大小，只有当 CompleteMultipartUpload 时才会校验。
    pub part_number: u32,

    /// The upload id returned from InitiateMultipartUpload
    pub upload_id: String,
}

impl UploadPartRequest {
    pub fn new<S>(part_number: u32, upload_id: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            part_number,
            upload_id: upload_id.as_ref().to_string(),
        }
    }
}

/// Upload part result.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct UploadPartResult {
    pub request_id: String,

    /// Used when call `CompleteMultipartUpload`
    pub etag: String,
}

impl From<HashMap<String, String>> for UploadPartResult {
    fn from(mut headers: HashMap<String, String>) -> Self {
        Self {
            request_id: headers.remove("x-oss-request-id").unwrap_or_default(),
            etag: sanitize_etag(headers.remove("etag").unwrap_or_default()),
        }
    }
}

/// Data required for upload part copy for copying object larger than 1GB
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct UploadPartCopyRequest {
    /// 每一个上传的 Part 都有一个标识它的号码（partNumber）。
    ///
    /// 取值：1~10000
    ///
    /// 单个 Part 的大小限制为 100 KB~5 GB。
    /// MultipartUpload 事件中除最后一个 Part 以外，其他 Part 的大小都要大于或等于 100 KB。
    /// 因不确定是否为最后一个 Part，
    /// UploadPart 接口并不会立即校验上传 Part 的大小，只有当 CompleteMultipartUpload 时才会校验。
    pub part_number: u32,

    /// The upload id returned from InitiateMultipartUpload
    pub upload_id: String,

    /// The object key **without** bucket part because UploadPartCopy can copy objects in the same bucket only. e.g. `path/to/sub-path/obj_key.zip`.
    pub source_object_key: String,
}

impl UploadPartCopyRequest {
    pub fn new<S1, S2>(part_number: u32, upload_id: S1, source_object_key: S2) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        Self {
            part_number,
            upload_id: upload_id.as_ref().to_string(),
            source_object_key: source_object_key.as_ref().to_string(),
        }
    }
}

/// Other options for upload part copy
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct UploadPartCopyOptions {
    /// 如果源 Object 开启了版本控制，这里可以指明版本，实现从 Object 的指定版本进行拷贝
    pub source_object_version_id: Option<String>,

    /// 源 Object 的拷贝范围。例如设置 `bytes=0-9`，表示拷贝 `0` 到 `9` 这 `10` 个字节。
    /// 这个范围是两端闭区间的 `[start, end]`，并且下标从 `0` 开始。也就是最大的取值范围是 `[0, file_length - 1]`
    ///
    /// - 不指定该请求头时，表示拷贝整个源 Object。
    /// - 当指定的范围不符合规范时，则拷贝整个源 Object
    pub copy_source_range: Option<String>,

    pub copy_source_if_match: Option<String>,
    pub copy_source_if_none_match: Option<String>,
    pub copy_source_if_unmodified_since: Option<String>,
    pub copy_source_if_modified_since: Option<String>,
}

#[derive(Debug, Default)]
pub struct UploadPartCopyOptionsBuilder {
    options: UploadPartCopyOptions,
}

impl UploadPartCopyOptionsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn source_object_version_id<S: Into<String>>(mut self, version_id: S) -> Self {
        self.options.source_object_version_id = Some(version_id.into());
        self
    }

    pub fn copy_source_range<S: Into<String>>(mut self, range: S) -> Self {
        self.options.copy_source_range = Some(range.into());
        self
    }

    pub fn copy_source_if_match<S: Into<String>>(mut self, etag: S) -> Self {
        self.options.copy_source_if_match = Some(etag.into());
        self
    }

    pub fn copy_source_if_none_match<S: Into<String>>(mut self, etag: S) -> Self {
        self.options.copy_source_if_none_match = Some(etag.into());
        self
    }

    pub fn copy_source_if_unmodified_since<S: Into<String>>(mut self, timestamp: S) -> Self {
        self.options.copy_source_if_unmodified_since = Some(timestamp.into());
        self
    }

    pub fn copy_source_if_modified_since<S: Into<String>>(mut self, timestamp: S) -> Self {
        self.options.copy_source_if_modified_since = Some(timestamp.into());
        self
    }

    pub fn build(self) -> UploadPartCopyOptions {
        self.options
    }
}

/// Result for upload part copy
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct UploadPartCopyResult {
    pub last_modified: String,
    pub etag: String,
}

impl UploadPartCopyResult {
    pub(crate) fn from_xml(xml: &str) -> Result<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut tag = String::new();
        let mut data = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
                Event::Text(text) => {
                    let s = text.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "LastModified" => data.last_modified = s,
                        "ETag" => data.etag = sanitize_etag(s),
                        _ => {}
                    }
                }
                Event::End(_) => tag.clear(),
                _ => {}
            }
        }

        Ok(data)
    }
}

/// Request data for complete multipart upload
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct CompleteMultipartUploadRequest {
    pub upload_id: String,
    /// `.0` is the `part_number` while upload part,
    /// `.1` is the returned ETag after upload part done successfully.
    pub parts: Vec<(u32, String)>,
}

impl CompleteMultipartUploadRequest {
    /// Consume self and generate XML string for sending request
    pub(crate) fn into_xml(self) -> Result<String> {
        let Self { upload_id: _, parts } = self;

        let mut writer = quick_xml::Writer::new(Vec::new());

        writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

        writer.write_event(Event::Start(BytesStart::new("CompleteMultipartUpload")))?;

        for (n, s) in parts.into_iter() {
            writer.write_event(Event::Start(BytesStart::new("Part")))?;

            writer.write_event(Event::Start(BytesStart::new("PartNumber")))?;
            writer.write_event(Event::Text(BytesText::new(&n.to_string())))?;
            writer.write_event(Event::End(BytesEnd::new("PartNumber")))?;

            writer.write_event(Event::Start(BytesStart::new("ETag")))?;
            let etag = if s.starts_with("\"") { s } else { format!("\"{}", s) };

            let etag = if etag.ends_with("\"") { etag } else { format!("{}\"", etag) };

            writer.write_event(Event::Text(BytesText::new(&etag)))?;
            writer.write_event(Event::End(BytesEnd::new("ETag")))?;

            writer.write_event(Event::End(BytesEnd::new("Part")))?;
        }

        writer.write_event(Event::End(BytesEnd::new("CompleteMultipartUpload")))?;
        Ok(String::from_utf8(writer.into_inner())?)
    }
}

/// Options for complete multipart uploads
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct CompleteMultipartUploadOptions {
    pub callback: Option<Callback>,
}

/// Complete multipart upload result
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum CompleteMultipartUploadResult {
    /// This is response headers from aliyun oss api when you put object with no callback specified
    #[cfg_attr(feature = "serde-camelcase", serde(rename = "apiResponse", rename_all = "camelCase"))]
    ApiResponse(CompleteMultipartUploadApiResponse),

    /// This is your callback response content string when you put object with callback specified.
    /// `.0` should be a valid JSON string.
    #[cfg_attr(feature = "serde-camelcase", serde(rename = "callbackResponse"))]
    CallbackResponse(String),
}

/// Request data for complete multipart upload
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct CompleteMultipartUploadApiResponse {
    pub bucket: String,
    pub key: String,
    pub etag: String,
}

impl CompleteMultipartUploadApiResponse {
    pub(crate) fn from_xml(xml: &str) -> Result<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut tag = String::new();
        let mut data = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
                Event::Text(s) => {
                    let text = s.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "Bucket" => data.bucket = text,
                        "Key" => data.key = text,
                        "ETag" => data.etag = sanitize_etag(text),
                        _ => {}
                    }
                }
                Event::End(_) => tag.clear(),
                _ => {}
            }
        }

        Ok(data)
    }
}

/// query options for listing all multipart uploads which is initialized but not completed nor aborted.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct ListMultipartUploadsOptions {
    /// 用于对 Object 名称进行分组的字符。所有名称包含指定的前缀且首次出现 delimiter 字符之间的 Object 作为一组元素 CommonPrefixes。
    pub delimiter: Option<char>,

    /// 限定此次返回 Multipart Upload 事件的最大个数，默认值为 1000。最大值为 1000。
    pub max_uploads: Option<u32>,

    /// 与 upload-id-marker 参数配合使用，用于指定返回结果的起始位置
    pub key_marker: Option<String>,

    /// 与 upload-id-marker 参数配合使用，用于指定返回结果的起始位置。
    ///
    /// - 如果未设置 upload-id-marker 参数，查询结果中包含所有 Object 名称的字典序大于 key-marker 参数值的 Multipart Upload 事件。
    /// - 如果设置了 upload-id-marker 参数，查询结果中包含：
    ///   - 所有 Object 名称的字典序大于 key-marker 参数值的 Multipart Upload 事件。
    ///   - Object 名称等于 key-marker 参数值，但是 UploadId 比 upload-id-marker 参数值大的 Multipart Upload 事件。
    pub upload_id_marker: Option<String>,

    /// 限定返回的 Object Key 必须以 prefix 作为前缀。注意使用 prefix 查询时，返回的 Key 中仍会包含 prefix。
    pub prefix: Option<String>,
}

#[derive(Debug, Default)]
pub struct ListMultipartUploadsOptionsBuilder {
    options: ListMultipartUploadsOptions,
}

impl ListMultipartUploadsOptionsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn delimiter(mut self, delimiter: char) -> Self {
        self.options.delimiter = Some(delimiter);
        self
    }

    pub fn max_uploads(mut self, max_uploads: u32) -> Self {
        self.options.max_uploads = Some(max_uploads);
        self
    }

    pub fn key_marker<S: Into<String>>(mut self, key_marker: S) -> Self {
        self.options.key_marker = Some(key_marker.into());
        self
    }

    pub fn upload_id_marker<S: Into<String>>(mut self, upload_id_marker: S) -> Self {
        self.options.upload_id_marker = Some(upload_id_marker.into());
        self
    }

    pub fn prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.options.prefix = Some(prefix.into());
        self
    }

    pub fn build(self) -> ListMultipartUploadsOptions {
        self.options
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct ListMultipartUploadsResultItem {
    pub key: String,
    pub upload_id: String,

    /// Multipart Upload 事件初始化的时间。示例：`2012-02-23T04:18:23.000Z`
    pub initiated: String,
}

impl ListMultipartUploadsResultItem {
    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> Result<Self> {
        let mut tag = String::new();
        let mut item = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
                Event::Text(s) => {
                    let s = s.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "Key" => item.key = s,
                        "UploadId" => item.upload_id = s,
                        "Initiated" => item.initiated = s,
                        _ => {}
                    }
                }
                Event::End(t) => {
                    tag.clear();
                    if t.local_name().as_ref() == b"Upload" {
                        break;
                    }
                }
                _ => {}
            }
        }

        Ok(item)
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct ListMultipartUploadsResult {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<char>,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
    pub max_uploads: u32,
    pub is_truncated: bool,
    pub uploads: Vec<ListMultipartUploadsResultItem>,
    pub common_prefixes: Vec<String>,
}

impl ListMultipartUploadsResult {
    pub(crate) fn from_xml(xml: &str) -> Result<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut tag = String::new();
        let mut level = 0;

        let mut ret = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => {
                    if t.local_name().as_ref() == b"Upload" {
                        ret.uploads.push(ListMultipartUploadsResultItem::from_xml_reader(&mut reader)?);
                    } else {
                        level += 1;
                        tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string();
                    }
                }
                Event::Text(s) => {
                    let text = s.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "Bucket" => ret.bucket = text,
                        "KeyMarker" => ret.key_marker = if text.is_empty() { None } else { Some(text) },
                        "UploadIdMarker" => ret.upload_id_marker = if text.is_empty() { None } else { Some(text) },
                        "NextKeyMarker" => ret.next_key_marker = if text.is_empty() { None } else { Some(text) },
                        "NextUploadIdMarker" => ret.next_upload_id_marker = if text.is_empty() { None } else { Some(text) },
                        "Prefix" if level == 2 => ret.prefix = if text.is_empty() { None } else { Some(text) },
                        "Prefix" if level == 3 => ret.common_prefixes.push(text),
                        "Delimiter" => ret.delimiter = text.chars().next(),
                        "MaxUploads" => ret.max_uploads = text.parse::<u32>().unwrap_or_default(),
                        "IsTruncated" => ret.is_truncated = text == "true",
                        _ => {}
                    }
                }
                Event::End(_) => {
                    tag.clear();
                    level -= 1;
                }
                _ => {}
            }
        }

        Ok(ret)
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct ListPartsOptions {
    /// Maximum parts number in response data. Valida data range: `[1, 1000]`
    pub max_parts: Option<u32>,

    /// The start part number. only parts which numbers are greater than the give `part_number_marker` will be returned
    pub part_number_marker: Option<u32>,
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct ListPartsResultItem {
    pub etag: String,
    pub part_number: u32,
    pub size: u64,
    pub last_modified: String,
}

impl ListPartsResultItem {
    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> Result<Self> {
        let mut tag = String::new();
        let mut data = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
                Event::Text(text) => {
                    let s = text.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "PartNumber" => data.part_number = s.parse()?,
                        "Size" => data.size = s.parse()?,
                        "ETag" => data.etag = sanitize_etag(s),
                        "LastModified" => data.last_modified = s,
                        _ => {}
                    }
                }
                Event::End(t) => {
                    tag.clear();
                    if t.local_name().as_ref() == b"Part" {
                        break;
                    }
                }
                _ => {}
            }
        }

        Ok(data)
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct ListPartsResult {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub max_parts: Option<u32>,
    pub part_number_marker: Option<u32>,
    pub next_part_number_marker: Option<u32>,
    pub is_truncated: bool,
    pub parts: Vec<ListPartsResultItem>,
}

impl ListPartsResult {
    pub(crate) fn from_xml(xml: &str) -> Result<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut tag = String::new();
        let mut data = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => {
                    if t.local_name().as_ref() == b"Part" {
                        data.parts.push(ListPartsResultItem::from_xml_reader(&mut reader)?);
                    } else {
                        tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string();
                    }
                }
                Event::Text(text) => {
                    let s = text.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "Bucket" => data.bucket = s,
                        "Key" => data.key = s,
                        "UploadId" => data.upload_id = s,
                        "MaxParts" => data.max_parts = if s.is_empty() { None } else { Some(s.parse()?) },
                        "PartNumberMarker" => data.part_number_marker = if s.is_empty() { None } else { Some(s.parse()?) },
                        "NextPartNumberMarker" => data.next_part_number_marker = if s.is_empty() { None } else { Some(s.parse()?) },
                        "IsTruncated" => data.is_truncated = s == "true",
                        _ => {}
                    }
                }
                Event::End(_) => {
                    tag.clear();
                }
                _ => {}
            }
        }

        Ok(data)
    }
}

pub(crate) fn build_initiate_multipart_uploads_request(
    bucket_name: &str,
    object_key: &str,
    options: &Option<InitiateMultipartUploadOptions>,
) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    let mut request = build_put_object_request(bucket_name, object_key, RequestBody::Empty, options)?;

    request = request
        .method(RequestMethod::Post)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("uploads", "");

    Ok(request)
}

pub(crate) fn build_upload_part_request(bucket_name: &str, object_key: &str, body: RequestBody, params: UploadPartRequest) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    let UploadPartRequest { part_number, upload_id } = params;

    if !(1..=10000).contains(&part_number) {
        return Err(Error::Other(format!(
            "invalid part number: {}. part number should be in range [1, 10000]",
            part_number
        )));
    }

    if upload_id.is_empty() {
        return Err(Error::Other("invalid upload id. upload id must not be empty".to_string()));
    }

    let request = OssRequest::new()
        .method(RequestMethod::Put)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("partNumber", part_number.to_string())
        .add_query("uploadId", upload_id)
        .body(body);

    Ok(request)
}

pub(crate) fn build_upload_part_copy_request(
    bucket_name: &str,
    object_key: &str,
    data: UploadPartCopyRequest,
    options: &Option<UploadPartCopyOptions>,
) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid destination object key: {}", object_key)));
    }

    if !validate_object_key(&data.source_object_key) {
        return Err(Error::Other(format!("invalid source object key: {}", data.source_object_key)));
    }

    if !(1..=10000).contains(&data.part_number) {
        return Err(Error::Other(format!("invalid part number: {}", data.part_number)));
    }

    let UploadPartCopyRequest {
        part_number,
        upload_id,
        source_object_key,
    } = data;

    if upload_id.is_empty() {
        return Err(Error::Other("invalid upload id: must not be empty".to_string()));
    }

    if !validate_object_key(&source_object_key) {
        return Err(Error::Other(format!("invalid source object key: {}", source_object_key)));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Put)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("uploadId", upload_id)
        .add_query("partNumber", part_number.to_string());

    let mut copy_source = format!("/{}/{}", bucket_name, source_object_key);
    if let Some(opt) = options {
        if let Some(v) = &opt.source_object_version_id {
            copy_source = format!("{}?versionId={}", copy_source, v);
        }
    }

    request = request.add_header("x-oss-copy-source", &copy_source);

    if let Some(options) = options {
        if let Some(s) = &options.copy_source_range {
            request = request.add_header("x-oss-copy-source-range", s);
        }

        if let Some(s) = &options.copy_source_if_match {
            request = request.add_header("x-oss-copy-source-if-match", s);
        }

        if let Some(s) = &options.copy_source_if_none_match {
            request = request.add_header("x-oss-copy-source-if-none-match", s);
        }

        if let Some(s) = &options.copy_source_if_modified_since {
            request = request.add_header("x-oss-copy-source-if-modified-since", s);
        }

        if let Some(s) = &options.copy_source_if_unmodified_since {
            request = request.add_header("x-oss-copy-source-if-unmodified-since", s);
        }
    }

    Ok(request)
}

pub(crate) fn build_complete_multipart_uploads_request(
    bucket_name: &str,
    object_key: &str,
    data: CompleteMultipartUploadRequest,
    options: &Option<CompleteMultipartUploadOptions>,
) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    if data.upload_id.is_empty() {
        return Err(Error::Other("upload id must not be empty".to_string()));
    }

    if data.parts.is_empty() {
        return Err(Error::Other("multipart uploads items must not be empty".to_string()));
    }

    let upload_id = data.upload_id.clone();

    let xml = data.into_xml()?;

    let mut request = OssRequest::new()
        .method(RequestMethod::Post)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("uploadId", &upload_id)
        .content_type(common::MIME_TYPE_XML)
        .text_body(xml);

    if let Some(options) = options {
        if let Some(cb) = &options.callback {
            // custom variable values are not serialized
            let callback_json = serde_json::to_string(cb)?;
            let callback_base64 = BASE64_STANDARD.encode(&callback_json);
            request = request.add_header("x-oss-callback", callback_base64);

            if !cb.custom_variables.is_empty() {
                let callback_vars_json = serde_json::to_string(&cb.custom_variables)?;
                let callback_vars_base64 = BASE64_STANDARD.encode(&callback_vars_json);
                request = request.add_header("x-oss-callback-var", callback_vars_base64);
            }
        }
    }

    Ok(request)
}

pub(crate) fn build_list_multipart_uploads_request(bucket_name: &str, options: &Option<ListMultipartUploadsOptions>) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other("invalid bucket name".to_string()));
    }

    let mut request = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("uploads", "");

    if let Some(options) = options {
        if let Some(c) = options.delimiter {
            request = request.add_query("delimiter", c.to_string());
        }

        if let Some(n) = options.max_uploads {
            request = request.add_query("max-uploads", n.to_string());
        }

        if let Some(s) = &options.key_marker {
            request = request.add_query("key-marker", s);
        }

        if let Some(s) = &options.upload_id_marker {
            request = request.add_query("upload-id-marker", s);
        }

        if let Some(s) = &options.prefix {
            request = request.add_query("prefix", s);
        }
    }
    Ok(request)
}

pub(crate) fn build_list_parts_request(bucket_name: &str, object_key: &str, upload_id: &str, options: &Option<ListPartsOptions>) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    if upload_id.is_empty() {
        return Err(Error::Other("upload id must not be empty".to_string()));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Get)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("uploadId", upload_id);

    if let Some(options) = options {
        if let Some(n) = options.max_parts {
            request = request.add_query("max-parts", n.to_string());
        }

        if let Some(n) = options.part_number_marker {
            request = request.add_query("part-number-marker", n.to_string());
        }
    }

    Ok(request)
}

#[cfg(test)]
mod test_multipart_common {
    use super::ListMultipartUploadsResult;

    #[test]
    fn test_list_multipart_uploads_result() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListMultipartUploadsResult xmlns="http://doc.oss-cn-hangzhou.aliyuncs.com">
            <Bucket>oss-example</Bucket>
            <KeyMarker></KeyMarker>
            <UploadIdMarker></UploadIdMarker>
            <NextKeyMarker>oss.avi</NextKeyMarker>
            <NextUploadIdMarker>0004B99B8E707874FC2D692FA5D77D3F</NextUploadIdMarker>
            <Delimiter></Delimiter>
            <Prefix></Prefix>
            <MaxUploads>1000</MaxUploads>
            <IsTruncated>false</IsTruncated>
            <Upload>
                <Key>multipart.data</Key>
                <UploadId>0004B999EF518A1FE585B0C9360DC4C8</UploadId>
                <Initiated>2012-02-23T04:18:23.000Z</Initiated>
            </Upload>
            <Upload>
                <Key>multipart.data</Key>
                <UploadId>0004B999EF5A239BB9138C6227D6****</UploadId>
                <Initiated>2012-02-23T04:18:23.000Z</Initiated>
            </Upload>
            <Upload>
                <Key>oss.avi</Key>
                <UploadId>0004B99B8E707874FC2D692FA5D7****</UploadId>
                <Initiated>2012-02-23T06:14:27.000Z</Initiated>
            </Upload>
            <CommonPrefixes>
                <Prefix>a/b/</Prefix>
            </CommonPrefixes>
        </ListMultipartUploadsResult>"#;

        let data = ListMultipartUploadsResult::from_xml(xml).unwrap();
        assert_eq!(Some("oss.avi".to_string()), data.next_key_marker);
        assert_eq!(Some("0004B99B8E707874FC2D692FA5D77D3F".to_string()), data.next_upload_id_marker);
        assert_eq!(1000, data.max_uploads);
        assert_eq!(3, data.uploads.len());

        assert_eq!("multipart.data", data.uploads[0].key);
        assert_eq!("0004B999EF518A1FE585B0C9360DC4C8", &data.uploads[0].upload_id);
        assert_eq!("2012-02-23T04:18:23.000Z", &data.uploads[0].initiated);

        assert_eq!(1, data.common_prefixes.len());
        assert_eq!("a/b/", &data.common_prefixes[0]);

        println!("{:#?}", data);
    }
}
