use std::collections::HashMap;

use base64::prelude::{Engine, BASE64_STANDARD};
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};

use crate::{
    common::{self, build_tag_string, ObjectAcl, MetadataDirective, ObjectType, ServerSideEncryptionAlgorithm, StorageClass, TagDirective, MIME_TYPE_XML},
    error::Error,
    request::{OssRequest, RequestMethod},
    util::{sanitize_etag, validate_bucket_name, validate_meta_key, validate_object_key, validate_tag_key, validate_tag_value},
    RequestBody, Result,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum ContentEncoding {
    /// 表示 Object 未经过压缩或编码
    #[default]
    #[cfg_attr(feature = "serde-support", serde(rename = "identity"))]
    Identity,

    /// 表示 Object 采用 Lempel-Ziv（LZ77） 压缩算法以及 32 位 CRC 校验的编码方式。
    #[cfg_attr(feature = "serde-support", serde(rename = "gzip"))]
    Gzip,

    /// 表示 Object 采用 zlib 结构和 deflate 压缩算法的编码方式。
    #[cfg_attr(feature = "serde-support", serde(rename = "deflate"))]
    Deflate,

    /// 表示 Object 采用 Lempel-Ziv-Welch（LZW） 压缩算法的编码方式。
    #[cfg_attr(feature = "serde-support", serde(rename = "compress"))]
    Compress,

    /// 表示 Object 采用 Brotli 压缩算法的编码方式。
    #[cfg_attr(feature = "serde-support", serde(rename = "br"))]
    Brotli,
}

impl ContentEncoding {
    pub fn as_str(&self) -> &str {
        match self {
            ContentEncoding::Identity => "identity",
            ContentEncoding::Gzip => "gzip",
            ContentEncoding::Deflate => "deflate",
            ContentEncoding::Compress => "compress",
            ContentEncoding::Brotli => "br",
        }
    }
}

impl TryFrom<&str> for ContentEncoding {
    type Error = Error;

    /// Try to create a ContentEncoding from a string.
    /// Acceptable values are "identity", "gzip", "deflate", "compress", and "br".
    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "identity" => Ok(ContentEncoding::Identity),
            "gzip" => Ok(ContentEncoding::Gzip),
            "deflate" => Ok(ContentEncoding::Deflate),
            "compress" => Ok(ContentEncoding::Compress),
            "br" => Ok(ContentEncoding::Brotli),
            _ => Err(Error::Other(format!("invalid content encoding: {}", value))),
        }
    }
}

impl TryFrom<&String> for ContentEncoding {
    type Error = Error;

    /// See [`try_from(value: &str)`] for more details.
    fn try_from(value: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<String> for ContentEncoding {
    type Error = Error;

    /// See [`try_from(value: &str)`] for more details.
    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

/// Options for putting object
///
/// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobject>
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct PutObjectOptions {
    /// 文件的 mime_type。如果不指定，则从文件名猜测。如果猜测不到，则使用 application/octet-stream
    /// 如果是直接从字节数组创建 Object 的，则不会猜测这个值，建议显式指定
    pub mime_type: Option<String>,

    /// 指定该Object被下载时网页的缓存行为。取值如下：
    ///
    /// - `no-cache`：不可直接使用缓存，而是先到服务端验证 Object 是否已更新。如果 Object 已更新，表明缓存已过期，需从服务端重新下载 Object；如果 Object 未更新，表明缓存未过期，此时将使用本地缓存。
    /// - `no-store`：所有内容都不会被缓存。
    /// - `public`：所有内容都将被缓存。
    /// - `private`：所有内容只在客户端缓存。
    /// - `max-age=<seconds>`：缓存内容的相对过期时间，单位为秒。此选项仅在 HTTP 1.1 中可用。示例：`max-age=3600`
    pub cache_control: Option<String>,

    /// 指定Object的展示形式。取值如下：
    ///
    /// - `inline`: 直接预览文件内容。
    /// - `attachment`: 以原文件名的形式下载到浏览器指定路径。
    /// - `attachment; filename="yourFileName"`: 以自定义文件名的形式下载到浏览器指定路径。 `yourFileName` 用于自定义下载后的文件名称，例如 `example.jpg`。
    ///
    /// 注意：
    ///
    /// - 如果 Object 名称包含星号（`*`）、正斜线（`/`）等特殊字符时，可能会出现特殊字符转义的情况。例如，下载 `example*.jpg` 到本地时，`example*.jpg` 可能会转义为`example_.jpg`。
    /// - 如需确保下载名称中包含中文字符的 Object 到本地指定路径后，文件名称不出现乱码的现象，您需要将名称中包含的中文字符进行 URL 编码。例如，将`测试.txt` 从 OSS 下载到本地后，需要保留文件名为 `测试.txt`，需按照 `"attachment;filename="+URLEncoder.encode("测试","UTF-8")+".txt;filename*=UTF-8''"+URLEncoder.encode("测试","UTF-8")+".txt"` 的格式设置 `Content-Disposition`，即 `attachment;filename=%E6%B5%8B%E8%AF%95.txt;filename*=UTF-8''%E6%B5%8B%E8%AF%95.txt`
    pub content_disposition: Option<String>,

    pub content_encoding: Option<ContentEncoding>,

    /// 上传内容的 MD5 摘要算法结果的 base64 字符串。用于检查消息内容是否与发送时一致。Content-MD5 是由 MD5 算法生成的值。上传了 Content-MD5 请求头后，OSS 会计算消息体的 Content-MD5 并检查一致性。
    pub content_md5: Option<String>,

    /// 过期时间。例如：`Wed, 08 Jul 2015 16:57:01 GMT`
    pub expires: Option<String>,

    /// 指定 PutObject 操作时是否覆盖同名 Object。
    ///
    /// 当目标 Bucket 处于已开启或已暂停的版本控制状态时，
    /// `x-oss-forbid-overwrite` 请求 Header 设置无效，即允许覆盖同名 Object。
    ///
    /// - 不指定 `x-oss-forbid-overwrite` 或者指定 `x-oss-forbid-overwrite` 为 `false` 时，表示允许覆盖同名 Object。
    /// - 指定 `x-oss-forbid-overwrite` 为 `true` 时，表示禁止覆盖同名 Object。
    ///
    /// **设置 `x-oss-forbid-overwrite` 请求 Header 导致 QPS 处理性能下降，如果您有大量的操作需要使用 `x-oss-forbid-overwrite` 请求 Header（QPS > 1000），请联系技术支持，避免影响您的业务。**
    pub forbid_overwrite: Option<bool>,

    /// 创建 Object 时，指定服务器端加密方式。
    /// 指定此选项后，在响应头中会返回此选项，OSS 会对上传的 Object 进行加密编码存储。当下载该 Object 时，响应头中会包含 `x-oss-server-side-encryption`，且该值会被设置成此 Object 的加密算法。
    pub server_side_encryption: Option<ServerSideEncryptionAlgorithm>,

    /// 指定Object的加密算法。如果未指定此选项，表明 Object 使用 AES256 加密算法。此选项仅当 `x-oss-server-side-encryption` 为 KMS 时有效。
    pub server_side_data_encryption: Option<ServerSideEncryptionAlgorithm>,

    /// KMS托管的用户主密钥。此选项仅在 `x-oss-server-side-encryption` 为 KMS 时有效。
    pub server_side_encryption_key_id: Option<String>,

    /// 如果不指定，则默认采用 Bucket 的 ACL。
    pub object_acl: Option<ObjectAcl>,

    /// 如果不指定，则默认采用 Bucket 的存储类型。
    pub storage_class: Option<StorageClass>,

    /// 使用 PutObject 接口时，如果配置以 `x-oss-meta-` 为前缀的参数，则该参数视为元数据，例如 `x-oss-meta-location`。
    /// 一个 Object 可以有多个类似的参数，但所有的元数据总大小不能超过 8 KB。
    /// 元数据支持短划线（`-`）、数字、英文字母（`a~z`）。英文字符的大写字母会被转成小写字母，不支持下划线（`_`）在内的其他字符。
    ///
    /// **注意：Key 必须是 `x-oss-meta-` 开头的**
    pub metadata: HashMap<String, String>,

    /// Object 标签
    /// 签合法字符集包括大小写字母、数字、空格和下列符号：`+ - = . _ : /`。
    pub tags: HashMap<String, String>,

    /// For `put_object` only.
    pub callback: Option<Callback>,
}

pub struct PutObjectOptionsBuilder {
    mime_type: Option<String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<ContentEncoding>,
    content_md5: Option<String>,
    expires: Option<String>,
    forbid_overwrite: Option<bool>,
    server_side_encryption: Option<ServerSideEncryptionAlgorithm>,
    server_side_data_encryption: Option<ServerSideEncryptionAlgorithm>,
    server_side_encryption_key_id: Option<String>,
    object_acl: Option<ObjectAcl>,
    storage_class: Option<StorageClass>,
    metadata: HashMap<String, String>,
    tags: HashMap<String, String>,
    callback: Option<Callback>,
}

impl PutObjectOptionsBuilder {
    pub fn new() -> Self {
        Self {
            mime_type: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_md5: None,
            expires: None,
            forbid_overwrite: None,
            server_side_encryption: None,
            server_side_data_encryption: None,
            server_side_encryption_key_id: None,
            object_acl: None,
            storage_class: None,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            callback: None,
        }
    }

    pub fn mime_type(mut self, mime_type: impl Into<String>) -> Self {
        self.mime_type = Some(mime_type.into());
        self
    }

    pub fn cache_control(mut self, cache_control: impl Into<String>) -> Self {
        self.cache_control = Some(cache_control.into());
        self
    }

    pub fn content_disposition(mut self, content_disposition: impl Into<String>) -> Self {
        self.content_disposition = Some(content_disposition.into());
        self
    }

    pub fn content_encoding(mut self, content_encoding: ContentEncoding) -> Self {
        self.content_encoding = Some(content_encoding);
        self
    }

    pub fn content_md5(mut self, content_md5: impl Into<String>) -> Self {
        self.content_md5 = Some(content_md5.into());
        self
    }

    pub fn expires(mut self, expires: impl Into<String>) -> Self {
        self.expires = Some(expires.into());
        self
    }

    pub fn forbid_overwrite(mut self, forbid_overwrite: bool) -> Self {
        self.forbid_overwrite = Some(forbid_overwrite);
        self
    }

    pub fn server_side_encryption(mut self, algorithm: ServerSideEncryptionAlgorithm) -> Self {
        self.server_side_encryption = Some(algorithm);
        self
    }

    pub fn server_side_data_encryption(mut self, algorithm: ServerSideEncryptionAlgorithm) -> Self {
        self.server_side_data_encryption = Some(algorithm);
        self
    }

    pub fn server_side_encryption_key_id(mut self, key_id: impl Into<String>) -> Self {
        self.server_side_encryption_key_id = Some(key_id.into());
        self
    }

    pub fn object_acl(mut self, acl: ObjectAcl) -> Self {
        self.object_acl = Some(acl);
        self
    }

    pub fn storage_class(mut self, storage_class: StorageClass) -> Self {
        self.storage_class = Some(storage_class);
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    pub fn callback(mut self, cb: Callback) -> Self {
        self.callback = Some(cb);
        self
    }

    pub fn build(self) -> PutObjectOptions {
        PutObjectOptions {
            mime_type: self.mime_type,
            cache_control: self.cache_control,
            content_disposition: self.content_disposition,
            content_encoding: self.content_encoding,
            content_md5: self.content_md5,
            expires: self.expires,
            forbid_overwrite: self.forbid_overwrite,
            server_side_encryption: self.server_side_encryption,
            server_side_data_encryption: self.server_side_data_encryption,
            server_side_encryption_key_id: self.server_side_encryption_key_id,
            object_acl: self.object_acl,
            storage_class: self.storage_class,
            metadata: self.metadata,
            tags: self.tags,
            callback: self.callback,
        }
    }
}

impl Default for PutObjectOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Options for getting object
///
/// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobject>
pub struct GetObjectOptions {
    // The following fields are header items
    /// 指定文件传输的范围。
    ///
    /// - 如果指定的范围符合规范，返回消息中会包含整个 Object 的大小和此次返回 Object 的范围。例如：`Content-Range: bytes 0~9/44`，表示整个 Object 大小为 `44`，此次返回的范围为 `0~9`。
    /// - 如果指定的范围不符合规范，则传送整个 Object，并且结果中不包含 `Content-Range`。
    pub range: Option<String>,

    /// GMT 日期时间字符串，例如：`Fri, 13 Nov 2015 14:47:53 GMT`
    ///
    /// 如果指定的时间早于实际修改时间或指定的时间不符合规范，则直接返回 Object，并返回 `200 OK`；
    /// 如果指定的时间等于或者晚于实际修改时间，则返回 `304 Not Modified`。
    pub if_modified_since: Option<String>,

    /// GMT 日期时间字符串，例如：`Fri, 13 Nov 2015 14:47:53 GMT`
    ///
    /// 如果指定的时间等于或者晚于 Object 实际修改时间，则正常传输 Object，并返回 `200 OK`；
    /// 如果指定的时间早于实际修改时间，则返回 `412 Precondition Failed`。
    /// `If-Modified-Since` 和 `If-Unmodified-Since` 可以同时使用。
    pub if_unmodified_since: Option<String>,

    /// ETag 值
    ///
    /// 如果传入的 `ETag` 和 Object 的 `ETag` 匹配，则正常传输 Object，并返回 `200 OK`；
    /// 如果传入的 `ETag` 和 Object 的 `ETag` 不匹配，则返回 `412 Precondition Failed`。
    pub if_match: Option<String>,

    /// ETag 值
    ///
    /// 如果传入的 `ETag` 值和 `Object` 的 `ETag` 不匹配，则正常传输 Object，并返回 `200 OK`；
    /// 如果传入的 `ETag` 和 `Object` 的 `ETag` 匹配，则返回 `304 Not Modified`。
    ///
    /// `If-Match` 和 `If-None-Match` 可以同时使用。
    pub if_none_match: Option<String>,

    /// 指定客户端的编码类型。例如：gzip
    ///
    /// 如果要对返回内容进行 Gzip 压缩传输，您需要在请求头中以显示方式加入 `Accept-Encoding:gzip`。
    /// OSS 会根据 Object 的 Content-Type 和 Object 大小（不小于1KB），
    /// 判断传输过程中是否对数据进行 Gzip 压缩。满足条件时，数据以压缩形式传输，否则，数据以原始形式传输。
    ///
    /// 注意：
    ///
    /// - 如果采用了 Gzip 压缩且压缩生效，则不会附带 `ETag` 和 `Content-Length` 信息。
    /// - 目前 OSS 支持对以下 `Content-Type` 类型的数据进行 Gzip 压缩：
    ///   - text/cache-manifest
    ///   - text/xml
    ///   - text/css
    ///   - text/html
    ///   - text/plain
    ///   - application/javascript
    ///   - application/x-javascript
    ///   - application/rss+xml
    ///   - application/json
    ///   - text/json
    pub accept_encoding: Option<String>,

    // The following fields are query parameters
    /// Add `Content-Language` to response header
    pub response_content_language: Option<String>,

    /// Add `Expires` to response header
    pub response_expires: Option<String>,

    /// Add `Cache-Control` to response header
    pub response_cache_control: Option<String>,

    /// Add `Content-Disposition` to response header
    pub response_content_disposition: Option<String>,

    /// Add `Content-Encoding` to response header
    pub response_content_encoding: Option<ContentEncoding>,

    /// The version to retreive
    pub version_id: Option<String>,
}

pub struct GetObjectOptionsBuilder {
    range: Option<String>,
    if_modified_since: Option<String>,
    if_unmodified_since: Option<String>,
    if_match: Option<String>,
    if_non_match: Option<String>,
    accept_encoding: Option<String>,
    response_content_language: Option<String>,
    response_expires: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<ContentEncoding>,
    version_id: Option<String>,
}

impl GetObjectOptionsBuilder {
    pub fn new() -> Self {
        Self {
            range: None,
            if_modified_since: None,
            if_unmodified_since: None,
            if_match: None,
            if_non_match: None,
            accept_encoding: None,
            response_content_language: None,
            response_expires: None,
            response_cache_control: None,
            response_content_disposition: None,
            response_content_encoding: None,
            version_id: None,
        }
    }

    pub fn range(mut self, range: impl Into<String>) -> Self {
        self.range = Some(range.into());
        self
    }

    pub fn if_modified_since(mut self, if_modified_since: impl Into<String>) -> Self {
        self.if_modified_since = Some(if_modified_since.into());
        self
    }

    pub fn if_unmodified_since(mut self, if_unmodified_since: impl Into<String>) -> Self {
        self.if_unmodified_since = Some(if_unmodified_since.into());
        self
    }

    pub fn if_match(mut self, if_match: impl Into<String>) -> Self {
        self.if_match = Some(if_match.into());
        self
    }

    pub fn if_non_match(mut self, if_non_match: impl Into<String>) -> Self {
        self.if_non_match = Some(if_non_match.into());
        self
    }

    pub fn accept_encoding(mut self, accept_encoding: impl Into<String>) -> Self {
        self.accept_encoding = Some(accept_encoding.into());
        self
    }

    pub fn response_content_language(mut self, content_language: impl Into<String>) -> Self {
        self.response_content_language = Some(content_language.into());
        self
    }

    pub fn response_expires(mut self, expires: impl Into<String>) -> Self {
        self.response_expires = Some(expires.into());
        self
    }

    pub fn response_cache_control(mut self, cache_control: impl Into<String>) -> Self {
        self.response_cache_control = Some(cache_control.into());
        self
    }

    pub fn response_content_disposition(mut self, content_disposition: impl Into<String>) -> Self {
        self.response_content_disposition = Some(content_disposition.into());
        self
    }

    pub fn response_content_encoding(mut self, content_encoding: ContentEncoding) -> Self {
        self.response_content_encoding = Some(content_encoding);
        self
    }

    pub fn version_id(mut self, version_id: impl Into<String>) -> Self {
        self.version_id = Some(version_id.into());
        self
    }

    pub fn build(self) -> GetObjectOptions {
        GetObjectOptions {
            range: self.range,
            if_modified_since: self.if_modified_since,
            if_unmodified_since: self.if_unmodified_since,
            if_match: self.if_match,
            if_none_match: self.if_non_match,
            accept_encoding: self.accept_encoding,
            response_content_language: self.response_content_language,
            response_expires: self.response_expires,
            response_cache_control: self.response_cache_control,
            response_content_disposition: self.response_content_disposition,
            response_content_encoding: self.response_content_encoding,
            version_id: self.version_id,
        }
    }
}

impl Default for GetObjectOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A "placeholder" struct for adding more fields in the future
#[derive(Debug)]
pub struct GetObjectResult;

pub(crate) fn build_put_object_request(
    bucket_name: &str,
    object_key: &str,
    request_body: RequestBody,
    options: &Option<PutObjectOptions>,
) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    // check for metadata and tags
    if let Some(options) = &options {
        for (k, v) in &options.metadata {
            if k.is_empty() || !validate_meta_key(k) || v.is_empty() {
                return Err(Error::Other(format!("invalid meta data: \"{}: {}\". the key must starts with `x-oss-meta-`, and only `[0-9a-z\\-]` are allowed; the key and value must not be empty", k, v)));
            }
        }

        for (k, v) in &options.tags {
            if k.is_empty() || !validate_tag_key(k) || (!v.is_empty() && !validate_tag_value(v)) {
                return Err(Error::Other(format!(
                    "invalid tagging data: \"{}={}\". only `[0-9a-zA-Z\\+\\-=\\.:/]` and space character are allowed",
                    k, v
                )));
            }
        }
    }

    let mut request = OssRequest::new().method(RequestMethod::Put).bucket(bucket_name).object(object_key);

    let content_length = match &request_body {
        RequestBody::Empty => 0u64,
        RequestBody::Text(s) => s.len() as u64,
        RequestBody::Bytes(bytes) => bytes.len() as u64,
        RequestBody::File(file_path, range) => {
            if let Some(r) = range {
                r.end - r.start
            } else {
                if !file_path.exists() || !file_path.is_file() {
                    return Err(Error::Other(format!(
                        "{} does not exist or is not a regular file",
                        file_path.as_os_str().to_str().unwrap_or("UNKNOWN")
                    )));
                }

                let file_meta = std::fs::metadata(file_path)?;

                file_meta.len()
            }
        }
    };

    // max file size for putting object is 5GB
    if content_length > 5_368_709_120 {
        return Err(Error::Other(format!("length {} exceeds limitation. max allowed is 5GB", content_length)));
    }

    request = request.content_length(content_length);

    // if no `content-type` specified, try to guess from file
    if let RequestBody::File(file_path, _) = &request_body {
        request = request.content_type(mime_guess::from_path(file_path).first_or_octet_stream().as_ref());
    }

    // move the body to request
    request = request.body(request_body);

    if let Some(options) = options {
        // if `mime_type` is specified, overwrite it's value which guess from file (maybe)
        if let Some(s) = &options.mime_type {
            request = request.add_header("content-type", s);
        }

        if let Some(s) = &options.cache_control {
            request = request.add_header("cache-control", s);
        }

        if let Some(s) = &options.content_disposition {
            request = request.add_header("content-disposition", s);
        }

        if let Some(enc) = &options.content_encoding {
            request = request.add_header("content-encoding", enc.as_str());
        }

        if let Some(s) = &options.expires {
            request = request.add_header("expires", s);
        }

        if let Some(b) = &options.forbid_overwrite {
            if *b {
                request = request.add_header("x-oss-forbid-overwrite", "true");
            }
        }

        if let Some(a) = &options.server_side_encryption {
            request = request.add_header("x-oss-server-side-encryption", a.as_str());
        }

        if let Some(a) = &options.server_side_data_encryption {
            request = request.add_header("x-oss-server-side-data-encryption", a.as_str());
        }

        if let Some(s) = &options.server_side_encryption_key_id {
            request = request.add_header("x-oss-server-side-encryption-key-id", s);
        }

        if let Some(acl) = &options.object_acl {
            request = request.add_header("x-oss-object-acl", acl.as_str());
        }

        if let Some(store) = &options.storage_class {
            request = request.add_header("x-oss-storage-class", store.as_str());
        }

        for (k, v) in &options.metadata {
            request = request.add_header(k, v);
        }

        if !options.tags.is_empty() {
            request = request.add_header("x-oss-tagging", build_tag_string(&options.tags));
        }

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

/// Options for getting object metadata
///
/// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectmeta>
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct GetObjectMetadataOptions {
    pub version_id: Option<String>,
}

/// Options for heading object
///
/// Official document: <https://help.aliyun.com/zh/oss/developer-reference/headobject>
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct HeadObjectOptions {
    pub version_id: Option<String>,
    pub if_modified_since: Option<String>,
    pub if_unmodified_since: Option<String>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
}

pub struct HeadObjectOptionsBuilder {
    version_id: Option<String>,
    if_modified_since: Option<String>,
    if_unmodified_since: Option<String>,
    if_match: Option<String>,
    if_none_match: Option<String>,
}

impl HeadObjectOptionsBuilder {
    pub fn new() -> Self {
        Self {
            version_id: None,
            if_modified_since: None,
            if_unmodified_since: None,
            if_match: None,
            if_none_match: None,
        }
    }

    pub fn version_id(mut self, version_id: impl Into<String>) -> Self {
        self.version_id = Some(version_id.into());
        self
    }

    pub fn if_modified_since(mut self, if_modified_since: impl Into<String>) -> Self {
        self.if_modified_since = Some(if_modified_since.into());
        self
    }

    pub fn if_unmodified_since(mut self, if_unmodified_since: impl Into<String>) -> Self {
        self.if_unmodified_since = Some(if_unmodified_since.into());
        self
    }

    pub fn if_match(mut self, if_match: impl Into<String>) -> Self {
        self.if_match = Some(if_match.into());
        self
    }

    pub fn if_none_match(mut self, if_none_match: impl Into<String>) -> Self {
        self.if_none_match = Some(if_none_match.into());
        self
    }

    pub fn build(self) -> HeadObjectOptions {
        HeadObjectOptions {
            version_id: self.version_id,
            if_modified_since: self.if_modified_since,
            if_unmodified_since: self.if_unmodified_since,
            if_match: self.if_match,
            if_none_match: self.if_none_match,
        }
    }
}

impl Default for HeadObjectOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct ObjectMetadata {
    pub request_id: String,
    pub content_length: u64,

    /// 已经移除了首尾双引号（`"`）之后的字符串
    pub etag: String,
    pub hash_crc64ecma: Option<u64>,

    /// Object 通过生命周期规则转储为冷归档或者深度冷归档存储类型的时间。
    pub transition_time: Option<String>,

    /// Object 的最后一次访问时间。时间格式为 HTTP 1.1 协议中规定的 GMT 时间。
    /// 开启访问跟踪时，该字段的值会随着文件被访问的时间持续更新。
    /// 如果开启后关闭了访问跟踪，该字段的值保留为上一次最后更新的值。
    /// 示例： `Tue, 30 Mar 2021 06:07:48 GMT`
    pub last_access_time: Option<String>,

    /// 时间格式为 HTTP 1.1 协议中规定的 GMT 时间。
    pub last_modified: Option<String>,

    pub version_id: Option<String>,

    pub server_side_encryption: Option<ServerSideEncryptionAlgorithm>,
    pub server_side_encryption_key_id: Option<String>,
    pub storage_class: Option<StorageClass>,
    pub object_type: Option<ObjectType>,

    /// 对于 `Appendable` 类型的 Object 会返回此 Header，指明下一次请求应当提供的 `position`。
    pub next_append_position: Option<u64>,

    /// 配置了生命周期规则的Bucket中Object的过期时间。
    pub expiration: Option<String>,

    /// 如果 Object 存储类型为 `Archive`、`ColdArchive` 或者 `DeepColdArchive`，
    /// 且您已提交 Restore 请求，则响应头中会以 `x-oss-restore` 返回该 Object 的 Restore 状态，分如下几种情况：
    ///
    /// - 如果没有提交 Restore 或者 Restore 已经超时，则不返回该字段。
    /// - 如果已经提交 Restore，且 Restore 没有完成，则返回的 `x-oss-restore` 值为 `ongoing-request="true"`。
    /// - 如果已经提交 Restore，且 Restore 已经完成，则返回的 `x-oss-restore` 值为 `ongoing-request="false", expiry-date="Sun, 16 Apr 2017 08:12:33 GMT"`，其中 `expiry-date` 是 Restore 完成后 Object 进入可读状态的过期时间。
    pub restore: Option<String>,

    /// 当用户通过轻量消息队列 SMQ 创建 OSS 事件通知后，
    /// 在进行请求 OSS 相关操作时如果有匹配的事件通知规则，
    /// 则响应中会携带这个 Header，值为经过 Base64 编码 JSON 格式的事件通知结果。
    pub process_status: Option<String>,

    /// 当 Object 所属的 Bucket 被设置为请求者付费模式，
    /// 且请求者不是 Bucket 的拥有者时，响应中将携带此 Header，值为 `requester`。
    pub request_charged: Option<String>,

    /// - 对于 `Normal` 类型的 Object，根据 RFC 1864 标准对消息内容（不包括Header）计算 Md5 值获得 128 比特位数字，对该数字进行 Base64 编码作为一个消息的 Content-Md5 值。
    /// - `Multipart` 和 `Appendable` 类型的文件不会返回这个 Header。
    pub content_md5: Option<String>,

    /// 当 Object 所在的 Bucket 配置了 CORS 规则，且请求的 Origin 满足指定的 CORS 规则时会在响应中包含这个 Origin。
    pub access_control_allow_origin: Option<String>,

    /// 当 Object 所在的 Bucket 配置了 CORS 规则，且请求的 `Access-Control-Request-Method` 满足指定的CORS规则时会在响应中包含允许的 Methods。
    pub access_control_allow_methods: Option<String>,

    /// 当 Object 所在的 Bucket 配置了 CORS 规则，且请求满足 Bucket 配置的 CORS 规则时会在响应中包含 `MaxAgeSeconds`。
    pub access_control_allow_max_age: Option<String>,

    /// 当 Object 所在的 Bucket 配置了 CORS 规则，且请求满足指定的 CORS 规则时会在响应中包含这些 Headers。
    pub access_control_allow_headers: Option<String>,

    /// 表示允许访问客户端 JavaScript 程序的 headers 列表。当 Object 所在的 Bucket 配置了 CORS 规则，且请求满足指定的CORS规则时会在响应中包含 ExposeHeader。
    pub access_control_expose_headers: Option<String>,

    /// 对象关联的标签个数。仅当用户有读取标签权限时返回。
    pub tag_count: Option<u32>,

    /// `x-oss-meta-` 开头的用户自定义属性
    pub metadata: HashMap<String, String>,
}

impl From<HashMap<String, String>> for ObjectMetadata {
    /// Consumes the headers map and return ObjectMetadata
    fn from(mut headers: HashMap<String, String>) -> Self {
        Self {
            request_id: headers.remove("x-oss-request-id").unwrap_or("".to_string()),
            content_length: headers.remove("content-length").unwrap_or("0".to_string()).parse().unwrap_or(0),
            etag: sanitize_etag(headers.remove("etag").unwrap_or_default()),
            hash_crc64ecma: headers.remove("x-oss-hash-crc64ecma").map(|s| s.parse::<u64>().unwrap_or(0)),
            transition_time: headers.remove("x-oss-transition-time"),
            last_access_time: headers.remove("x-oss-last-access-time"),
            last_modified: headers.remove("last-modified"),
            version_id: headers.remove("x-oss-version-id"),
            server_side_encryption: if let Some(s) = headers.remove("x-oss-server-side-encryption") {
                // Not good...
                if let Ok(v) = s.try_into() {
                    Some(v)
                } else {
                    None
                }
            } else {
                None
            },
            server_side_encryption_key_id: headers.remove("x-oss-server-side-encryption-key-id"),
            storage_class: if let Some(s) = headers.remove("x-oss-storage-class") {
                if let Ok(v) = s.try_into() {
                    Some(v)
                } else {
                    None
                }
            } else {
                None
            },
            object_type: if let Some(s) = headers.remove("x-oss-object-type") {
                if let Ok(v) = s.try_into() {
                    Some(v)
                } else {
                    None
                }
            } else {
                None
            },
            next_append_position: headers.remove("x-oss-next-append-position").map(|s| s.parse().unwrap_or(0)),
            expiration: headers.remove("x-oss-expiration"),
            restore: headers.remove("x-oss-restore"),
            process_status: headers.remove("x-oss-process-status"),
            request_charged: headers.remove("x-oss-request-charged"),
            content_md5: headers.remove("content-md5"),
            access_control_allow_origin: headers.remove("access-control-allow-origin"),
            access_control_allow_methods: headers.remove("access-control-allow-methods"),
            access_control_allow_headers: headers.remove("access-control-allow-headers"),
            access_control_allow_max_age: headers.remove("access-control-max-age"),
            access_control_expose_headers: headers.remove("access-control-expose-headers"),
            tag_count: headers.remove("x-oss-tagging-count").map(|s| s.parse().unwrap_or(0)),

            // CAUTION!! must be the last field to handle because `drain` consumes all the entries left in the map
            metadata: headers.drain().filter(|(k, _)| k.starts_with("x-oss-meta-")).collect(),
        }
    }
}

/// Options for copying objects
///
/// Official document: <https://help.aliyun.com/zh/oss/developer-reference/copyobject>
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct CopyObjectOptions {
    /// 指定 CopyObject 操作时是否覆盖同名目标 Object。
    /// 当目标 Bucket 处于已开启或已暂停版本控制状态时，`x-oss-forbid-overwrite` 请求 Header 设置无效，即允许覆盖同名Object。
    ///
    /// - 未指定 `x-oss-forbid-overwrite` 或者指定 `x-oss-forbid-overwrite` 为 `false` 时，表示允许覆盖同名目标 Object。
    /// - 指定 `x-oss-forbid-overwrite` 为 `true` 时，表示禁止覆盖同名 Object。
    ///
    /// 设置`x-oss-forbid-overwrite` 请求 Header 会导致 QPS 处理性能下降，
    /// 如果您有大量的操作需要使用 `x-oss-forbid-overwrite` 请求 Header（QPS>1000），请联系技术支持，避免影响您的业务。
    pub forbid_overwrite: Option<bool>,

    /// 默认复制源 Object 的当前版本。如果需要复制指定的版本，请设置此参数
    pub source_version_id: Option<String>,

    /// 如果源 Object 的 ETag 值和您提供的 ETag 相等，则执行拷贝操作，并返回 200 OK。
    pub copy_source_if_match: Option<String>,

    /// 如果源 Object 的 ETag 值和您提供的 ETag 不相等，则执行拷贝操作，并返回 200 OK。
    pub copy_source_if_none_match: Option<String>,

    /// 如果指定的时间等于或者晚于文件实际修改时间，则正常拷贝文件，并返回 200 OK。
    /// e.g. `Mon, 11 May 2020 08:16:23 GMT`
    pub copy_source_if_unmodified_since: Option<String>,

    /// 如果指定的时间早于文件实际修改时间，则正常拷贝文件，并返回200 OK。
    /// e.g. `Mon, 11 May 2020 08:16:23 GMT`
    pub copy_source_if_modified_since: Option<String>,

    /// 指定如何设置目标 Object 的元数据。
    pub metadata_directive: Option<MetadataDirective>,

    /// Key 以 `x-oss-meta-` 开头
    pub metadata: HashMap<String, String>,

    pub server_side_encryption: Option<ServerSideEncryptionAlgorithm>,
    pub server_side_encryption_key_id: Option<String>,

    /// 指定 OSS 创建目标 Object 时的访问权限。
    pub object_acl: Option<ObjectAcl>,

    /// 指定 OSS 创建目标 Object 时的存储类型
    pub storage_class: Option<StorageClass>,

    pub tags: HashMap<String, String>,
    pub tag_directive: Option<TagDirective>,
}

pub struct CopyObjectOptionsBuilder {
    forbid_overwrite: Option<bool>,
    source_version_id: Option<String>,
    copy_source_if_match: Option<String>,
    copy_source_if_none_match: Option<String>,
    copy_source_if_unmodified_since: Option<String>,
    copy_source_if_modified_since: Option<String>,
    metadata_directive: Option<MetadataDirective>,
    metadata: HashMap<String, String>,
    server_side_encryption: Option<ServerSideEncryptionAlgorithm>,
    server_side_encryption_key_id: Option<String>,
    object_acl: Option<ObjectAcl>,
    storage_class: Option<StorageClass>,
    tags: HashMap<String, String>,
    tag_directive: Option<TagDirective>,
}

impl CopyObjectOptionsBuilder {
    pub fn new() -> Self {
        Self {
            forbid_overwrite: None,
            source_version_id: None,
            copy_source_if_match: None,
            copy_source_if_none_match: None,
            copy_source_if_unmodified_since: None,
            copy_source_if_modified_since: None,
            metadata_directive: None,
            metadata: HashMap::new(),
            server_side_encryption: None,
            server_side_encryption_key_id: None,
            object_acl: None,
            storage_class: None,
            tags: HashMap::new(),
            tag_directive: None,
        }
    }

    pub fn forbid_overwrite(mut self, forbid_overwrite: bool) -> Self {
        self.forbid_overwrite = Some(forbid_overwrite);
        self
    }

    pub fn source_version_id(mut self, version_id: impl Into<String>) -> Self {
        self.source_version_id = Some(version_id.into());
        self
    }

    pub fn copy_source_if_match(mut self, copy_source_if_match: impl Into<String>) -> Self {
        self.copy_source_if_match = Some(copy_source_if_match.into());
        self
    }

    pub fn copy_source_if_none_match(mut self, copy_source_if_none_match: impl Into<String>) -> Self {
        self.copy_source_if_none_match = Some(copy_source_if_none_match.into());
        self
    }

    pub fn copy_source_if_unmodified_since(mut self, copy_source_if_unmodified_since: impl Into<String>) -> Self {
        self.copy_source_if_unmodified_since = Some(copy_source_if_unmodified_since.into());
        self
    }

    pub fn copy_source_if_modified_since(mut self, copy_source_if_modified_since: impl Into<String>) -> Self {
        self.copy_source_if_modified_since = Some(copy_source_if_modified_since.into());
        self
    }

    pub fn metadata_directive(mut self, metadata_directive: MetadataDirective) -> Self {
        self.metadata_directive = Some(metadata_directive);
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn server_side_encryption(mut self, algorithm: ServerSideEncryptionAlgorithm) -> Self {
        self.server_side_encryption = Some(algorithm);
        self
    }

    pub fn server_side_encryption_key_id(mut self, key_id: impl Into<String>) -> Self {
        self.server_side_encryption_key_id = Some(key_id.into());
        self
    }

    pub fn object_acl(mut self, acl: ObjectAcl) -> Self {
        self.object_acl = Some(acl);
        self
    }

    pub fn storage_class(mut self, storage_class: StorageClass) -> Self {
        self.storage_class = Some(storage_class);
        self
    }

    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    pub fn tag_directive(mut self, tag_directive: TagDirective) -> Self {
        self.tag_directive = Some(tag_directive);
        self
    }

    pub fn build(self) -> CopyObjectOptions {
        CopyObjectOptions {
            forbid_overwrite: self.forbid_overwrite,
            source_version_id: self.source_version_id,
            copy_source_if_match: self.copy_source_if_match,
            copy_source_if_none_match: self.copy_source_if_none_match,
            copy_source_if_unmodified_since: self.copy_source_if_unmodified_since,
            copy_source_if_modified_since: self.copy_source_if_modified_since,
            metadata_directive: self.metadata_directive,
            metadata: self.metadata,
            server_side_encryption: self.server_side_encryption,
            server_side_encryption_key_id: self.server_side_encryption_key_id,
            object_acl: self.object_acl,
            storage_class: self.storage_class,
            tags: self.tags,
            tag_directive: self.tag_directive,
        }
    }
}

impl Default for CopyObjectOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Options for deleting object
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct DeleteObjectOptions {
    pub version_id: Option<String>,
}

/// A "placeholder" struct for adding more fields in the future
pub struct DeleteObjectResult;

/// A "placeholder" struct for adding more fields in the future
pub struct CopyObjectResult;

pub(crate) fn build_copy_object_request(
    source_bucket_name: &str,
    source_object_key: &str,
    dest_bucket_name: &str,
    dest_object_key: &str,
    options: &Option<CopyObjectOptions>,
) -> Result<OssRequest> {
    if !validate_bucket_name(source_bucket_name) {
        return Err(Error::Other(format!("invalid source bucket name: {}", source_bucket_name)));
    }

    if !validate_object_key(source_object_key) {
        return Err(Error::Other(format!("invalid source object key: {}", source_object_key)));
    }

    if !validate_bucket_name(dest_bucket_name) {
        return Err(Error::Other(format!("invalid destination bucket name: {}", dest_bucket_name)));
    }

    if !validate_object_key(dest_object_key) {
        return Err(Error::Other(format!("invalid destination object key: {}", dest_object_key)));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Put)
        .bucket(dest_bucket_name)
        .object(dest_object_key)
        .add_header(
            "x-oss-copy-source",
            format!("/{}/{}", urlencoding::encode(source_bucket_name), urlencoding::encode(source_object_key)),
        );

    if let Some(options) = options {
        // validate metadata key and taggings
        for (k, _) in options.metadata.iter() {
            if !validate_meta_key(k) {
                return Err(Error::Other(format!("invalid metadata key: {}", k)));
            }
        }

        for (k, v) in options.tags.iter() {
            if !validate_tag_key(k) || !validate_tag_value(v) {
                return Err(Error::Other(format!("invalid tagging data: {}={}", k, v)));
            }
        }

        if let Some(s) = &options.source_version_id {
            request = request.add_query("versionId", s);
        }

        if let Some(b) = options.forbid_overwrite {
            request = request.add_header("x-oss-forbid-overwrite", b.to_string())
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

        if let Some(md) = options.metadata_directive {
            request = request.add_header("x-oss-metadata-directive", md);
        }

        if let Some(a) = &options.server_side_encryption {
            request = request.add_header("x-oss-server-side-encryption", a);
        }

        if let Some(s) = &options.server_side_encryption_key_id {
            request = request.add_header("x-oss-server-side-encryption-key-id", s);
        }

        if let Some(acl) = options.object_acl {
            request = request.add_header("x-oss-object-acl", acl);
        }

        if let Some(sc) = options.storage_class {
            request = request.add_header("x-oss-storage-class", sc);
        }

        if let Some(td) = options.tag_directive {
            request = request.add_header("x-oss-tag-directive", td);
        }

        if !options.tags.is_empty() {
            request = request.add_header("x-oss-tagging", build_tag_string(&options.tags));
        }

        for (key, value) in options.metadata.iter() {
            request = request.add_header(key, value);
        }
    }

    Ok(request)
}

pub(crate) fn build_get_object_request(bucket_name: &str, object_key: &str, options: &Option<GetObjectOptions>) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    let mut request = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).object(object_key);

    if let Some(options) = options {
        if let Some(s) = &options.range {
            request = request.add_header("range", s);
        }

        if let Some(s) = &options.if_modified_since {
            request = request.add_header("if-modified-since", s);
        }

        if let Some(s) = &options.if_unmodified_since {
            request = request.add_header("if-unmodified-since", s);
        }

        if let Some(s) = &options.if_match {
            request = request.add_header("if-match", s);
        }

        if let Some(s) = &options.if_none_match {
            request = request.add_header("if-none-match", s);
        }

        if let Some(s) = &options.accept_encoding {
            request = request.add_header("accept-encoding", s);
        }

        if let Some(s) = &options.response_content_language {
            request = request.add_query("response-content-language", s);
        }

        if let Some(s) = &options.response_expires {
            request = request.add_query("response-expires", s);
        }

        if let Some(s) = &options.response_cache_control {
            request = request.add_query("response-cache-control", s);
        }

        if let Some(s) = &options.response_content_disposition {
            request = request.add_query("response-content-disposition", s);
        }

        if let Some(ce) = options.response_content_encoding {
            request = request.add_query("response-content-encoding", ce.as_str());
        }

        if let Some(s) = &options.version_id {
            request = request.add_query("versionId", s);
        }
    }

    Ok(request)
}

pub(crate) fn build_head_object_request(bucket_name: &str, object_key: &str, options: &Option<HeadObjectOptions>) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    let mut request = OssRequest::new().method(RequestMethod::Head).bucket(bucket_name).object(object_key);

    if let Some(options) = options {
        if let Some(s) = &options.if_modified_since {
            request = request.add_header("if-modified-since", s);
        }

        if let Some(s) = &options.if_unmodified_since {
            request = request.add_header("if-unmodified-since", s);
        }

        if let Some(s) = &options.if_match {
            request = request.add_header("if-match", s);
        }

        if let Some(s) = &options.if_none_match {
            request = request.add_header("if-none-match", s);
        }

        if let Some(s) = &options.version_id {
            request = request.add_query("versionId", s);
        }
    }

    Ok(request)
}

/// Put object result enumeration
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub enum PutObjectResult {
    /// This is response headers from aliyun oss api when you put object with no callback specified
    #[cfg_attr(feature = "serde-camelcase", serde(rename = "apiResponse", rename_all = "camelCase"))]
    ApiResponse(PutObjectApiResponse),

    /// This is your callback response content string when you put object with callback specified.
    /// `.0` should be a valid JSON string.
    #[cfg_attr(feature = "serde-camelcase", serde(rename = "callbackResponse"))]
    CallbackResponse(String),
}

/// The response headers from aliyun oss put object api
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct PutObjectApiResponse {
    pub request_id: String,

    /// 已经移除了首尾双引号（`"`）之后的字符串
    pub etag: String,

    /// 文件 MD5 值，Base64 编码的字符串
    pub content_md5: String,

    /// 文件 CRC64 值
    pub hash_crc64ecma: u64,

    /// 表示文件的版本 ID。仅当您将文件上传至已开启版本控制状态的 Bucket 时，会返回该响应头。
    pub version_id: Option<String>,
}

/// If you put object without callback, parse aliyun oss api headers into `PutObjectResult::WithoutCallback` enum variant
impl From<HashMap<String, String>> for PutObjectApiResponse {
    fn from(mut headers: HashMap<String, String>) -> Self {
        Self {
            request_id: headers.remove("x-oss-request-id").unwrap_or_default(),
            etag: sanitize_etag(headers.remove("etag").unwrap_or_default()),
            content_md5: headers.remove("content-md5").unwrap_or_default(),
            hash_crc64ecma: headers.remove("x-oss-hash-crc64ecma").unwrap_or("0".to_string()).parse().unwrap_or(0),
            version_id: headers.remove("x-oss-version-id"),
        }
    }
}

/// Options for appending object
pub type AppendObjectOptions = PutObjectOptions;
pub type AppendObjectOptionsBuilder = PutObjectOptionsBuilder;

pub struct AppendObjectResult {
    pub request_id: String,
    pub next_append_position: u64,
}

impl From<HashMap<String, String>> for AppendObjectResult {
    fn from(mut headers: HashMap<String, String>) -> Self {
        Self {
            request_id: headers.remove("x-oss-request-id").unwrap_or_default(),
            next_append_position: headers.remove("x-oss-next-append-position").unwrap_or("0".to_string()).parse().unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct DeleteMultipleObjectsItem {
    pub key: String,
    pub version_id: Option<String>,
}

/// Payload while call delete multiple objects in one request
///
/// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletemultipleobjects>
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct DeleteMultipleObjectsRequest {
    pub quiet: Option<bool>,

    /// Object keys to delete
    pub objects: Vec<DeleteMultipleObjectsItem>,
}

impl DeleteMultipleObjectsRequest {
    /// Consumes data and generate xml content
    pub(crate) fn into_xml(self) -> Result<String> {
        let mut writer = quick_xml::Writer::new(Vec::new());
        writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

        writer.write_event(Event::Start(BytesStart::new("Delete")))?;

        if let Some(b) = self.quiet {
            writer.write_event(Event::Start(BytesStart::new("Quiet")))?;
            writer.write_event(Event::Text(BytesText::new(&b.to_string())))?;
            writer.write_event(Event::End(BytesEnd::new("Quiet")))?;
        }

        for item in self.objects {
            writer.write_event(Event::Start(BytesStart::new("Object")))?;

            writer.write_event(Event::Start(BytesStart::new("Key")))?;
            writer.write_event(Event::Text(BytesText::new(&item.key)))?;
            writer.write_event(Event::End(BytesEnd::new("Key")))?;

            if let Some(s) = item.version_id {
                writer.write_event(Event::Start(BytesStart::new("VersionId")))?;
                writer.write_event(Event::Text(BytesText::new(&s)))?;
                writer.write_event(Event::End(BytesEnd::new("VersionId")))?;
            }

            writer.write_event(Event::End(BytesEnd::new("Object")))?;
        }

        writer.write_event(Event::End(BytesEnd::new("Delete")))?;

        Ok(String::from_utf8(writer.into_inner())?)
    }
}

impl<T> From<&[T]> for DeleteMultipleObjectsRequest
where
    T: AsRef<str>,
{
    fn from(object_keys: &[T]) -> Self {
        Self {
            objects: object_keys
                .iter()
                .map(|s| DeleteMultipleObjectsItem {
                    key: s.as_ref().to_string(),
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
            ..Default::default()
        }
    }
}

#[derive(Debug)]
pub enum DeleteMultipleObjectsConfig<'a, T: AsRef<str> + 'a> {
    /// A simpler mode: only keys to delete are specified
    FromKeys(&'a [T]),

    /// User custom full request
    FullRequest(DeleteMultipleObjectsRequest),
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct DeleteMultipleObjectsResultItem {
    pub key: String,
    pub version_id: Option<String>,
    pub delete_marker: Option<String>,
    pub delete_marker_version_id: Option<String>,
}

/// Result of deleting multiple objects ()
///
/// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deletemultipleobjects>
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct DeleteMultipleObjectsResult {
    pub items: Vec<DeleteMultipleObjectsResultItem>,
}

impl DeleteMultipleObjectsResult {
    pub fn from_xml(xml_content: &str) -> Result<Self> {
        let mut reader = quick_xml::Reader::from_str(xml_content);
        let mut tag = String::new();
        let mut items = Vec::new();

        let mut current_item = DeleteMultipleObjectsResultItem::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => {
                    tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string();
                    if tag == "Deleted" {
                        current_item = DeleteMultipleObjectsResultItem::default();
                    }
                }

                Event::Text(e) => {
                    let s = e.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "Key" => current_item.key = s,
                        "VersionId" => current_item.version_id = Some(s),
                        "DeleteMarker" => current_item.delete_marker = Some(s),
                        "DeleteMarkerVersionId" => current_item.delete_marker_version_id = Some(s),
                        _ => {}
                    }
                }

                Event::End(t) => {
                    if t.local_name().as_ref() == b"Deleted" {
                        items.push(current_item.clone());
                    }
                    tag.clear();
                }

                _ => {}
            }
        }

        Ok(Self { items })
    }
}

pub(crate) fn build_delete_multiple_objects_request<S>(bucket_name: &str, config: DeleteMultipleObjectsConfig<S>) -> Result<OssRequest>
where
    S: AsRef<str>,
{
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Post)
        .bucket(bucket_name)
        .add_query("delete", "")
        .content_type(MIME_TYPE_XML);

    let items_len = match &config {
        DeleteMultipleObjectsConfig::FromKeys(items) => items.len(),
        DeleteMultipleObjectsConfig::FullRequest(cfg) => cfg.objects.len(),
    };

    if items_len > common::DELETE_MULTIPLE_OBJECTS_LIMIT {
        return Err(Error::Other(format!(
            "{} exceeds the items count limits while deleting multiple objects",
            items_len
        )));
    }

    let payload = match config {
        DeleteMultipleObjectsConfig::FromKeys(items) => DeleteMultipleObjectsRequest::from(items),
        DeleteMultipleObjectsConfig::FullRequest(delete_multiple_objects_request) => delete_multiple_objects_request,
    };

    let xml_content = payload.into_xml()?;
    let content_md5 = BASE64_STANDARD.encode(*md5::compute(xml_content.as_bytes()));

    request = request
        .content_length(xml_content.len() as u64)
        .add_header("content-md5", content_md5)
        .text_body(xml_content);

    Ok(request)
}

/// 发起回调请求的 `Content-Type`
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CallbackBodyType {
    #[default]
    #[serde(rename = "application/x-www-form-urlencoded")]
    FormUrlEncoded,

    #[serde(rename = "application/json")]
    Json,
}

/// The callback while call
///
/// - `put_object_from_file`
/// - `put_object_from_buffer`
/// - `put_object_from_base64`
/// - `complete_multipart_upload`
///
/// to create an object, if create object successfully,
/// the OSS server will call your server according to this callback config with `POST` request method.
///
/// Official document: <https://help.aliyun.com/zh/oss/developer-reference/callback>
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Callback {
    /// 创建 Object 成功之后，OSS 服务器会 `POST` 到这个 URL。
    /// 该 URL 调用后的要求有：
    ///
    /// - 响应 `HTTP/1.1 200 OK`
    /// - 响应头 `Content-Length` 必须是合法的值
    /// - 响应体为 JSON 格式
    /// - 响应体最大为 3MB
    ///
    /// 另外：
    ///
    /// - 支持同时配置最多 5 个 URL，多个 URL 间以分号（;）分隔。OSS 会依次发送请求，直到第一个回调请求成功返回。
    /// - 支持 HTTPS 协议地址。
    /// - **不支持**填写 IPV6 地址，也**不支持**填写指向 IPV6 地址的域名。
    /// - 为了保证正确处理中文等情况，此 URL 需做编码处理，
    ///   例如 `https://example.com/中文.php?key=value&中文名称=中文值`
    ///   需要编码为 `https://example.com/%E4%B8%AD%E6%96%87.php?key=value&%E4%B8%AD%E6%96%87%E5%90%8D%E7%A7%B0=%E4%B8%AD%E6%96%87%E5%80%BC`。
    #[serde(rename = "callbackUrl")]
    pub url: String,

    /// 发起回调请求时 `Host` 头的值，格式为域名或 IP 地址。仅在设置了 `url` 时有效。
    /// 如果没有配置 `host`，则解析 `url` 中的 URL，并将解析的 `Host` 填充到 `Host` 请求头中。
    #[serde(rename = "callbackHost", skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    /// 发起回调时请求Body的值，例如 `key=${object}&etag=${etag}&my_var=${x:my_var}`。
    ///
    /// 支持：
    ///
    /// - OSS系统参数，要写成 `${var_name}` 格式
    ///   - `bucket`: The bucket name
    ///   - `object`: The object key
    ///   - `etag`: The ETag of the object
    ///   - `size`: 以字节为单位的 Object 大小。调用 `complete_multipart_upload` 时，`size` 为整个 Object 的大小
    ///   - `mimeType`: 资源类型，例如 jpeg 图片的资源类型为 `image/jpeg`
    ///   - `imageInfo.height`: 图片高度。该变量仅适用于图片格式，对于非图片格式，该变量的值为空
    ///   - `imageInfo.width`: 图片宽度。该变量仅适用于图片格式，对于非图片格式，该变量的值为空
    ///   - `imageInfo.format`: 图片格式，例如 JPG、PNG 等。该变量仅适用于图片格式，对于非图片格式，该变量的值为空
    ///   - `crc64`: 与上传文件后返回的 `x-oss-hash-crc64ecma` 头内容一致
    ///   - `contentMd5`: 与上传文件后返回的 `Content-MD5` 头内容一致。仅在 `put_object_from_xxx` 时候该变量的值不为空
    ///   - `vpcId`: 发起请求的客户端所在的 VpcId。如果不是通过 VPC 发起请求，则该变量的值为空
    ///   - `clientIp`: 发起请求的客户端 IP 地址
    ///   - `reqId`: 发起请求的 RequestId
    ///   - 发起请求的接口名称，例如 `PutObject`、 `PostObject` 等
    /// - 自定义参数，其使用格式为 `${x:var_name}`。参数值放到 `custom_variables` 中
    /// - 常量（字面量）
    #[serde(rename = "callbackBody")]
    pub body: String,

    /// 客户端发起回调请求时，OSS是否向地址发送服务器名称指示 SNI（Server Name Indication）。
    /// 是否发送 SNI 取决于服务器的配置和需求。
    /// 对于使用同一个 IP 地址来托管多个 TLS/SSL 证书的服务器的情况，建议选择发送 SNI
    #[serde(rename = "callbackSNI", skip_serializing_if = "Option::is_none")]
    pub sni: Option<bool>,

    /// 发起回调请求时候的 `Content-Type`。默认是：`application/x-www-form-urlencoded`
    #[serde(rename = "callbackBodyType", skip_serializing_if = "Option::is_none")]
    pub body_type: Option<CallbackBodyType>,

    /// 在回调请求体 (`body`) 中携带的数据，如果有自定义参数，请使用此属性容纳自定义参数的值。
    /// Key 为自定义变量名，但是不包含 `x:` 前缀。在生成 `body` 的时候会自动增加
    /// 注意：这里我使用了 Map 来接受自定义参数，也就是说，这里**不支持**多个同名的自定义参数来表示集合数据类型
    #[serde(skip_serializing)]
    pub custom_variables: HashMap<String, String>,
}

/// 回调请求数据枚举值
///
/// `Oss` 开头的，其中 `.0` 是此参数值对应的参数名。
/// 因为大部分时候，传入的参数名是固定的，自定参数的 body 参数名和自定义参数的参数值、常量参数值，一般都是静态字符串，
/// 所以这里用了 `&'a str` 的形态。如果 `&'a str` 不满足你的需求，请使用 `CallbackBodyParameter::Literal(String, String)`
///
/// # Example
///
/// ```rust
/// use ali_oss_rs::object_common::CallbackBodyParameter;
///
///
/// assert_eq!("foo=${bucket}", CallbackBodyParameter::OssBucket("foo").to_body_string());
/// assert_eq!("foo=${x:bar}", CallbackBodyParameter::Custom(
///     "foo",
///     "bar",
///     "Are you OK?".to_string()
/// ).to_body_string());
/// assert_eq!("foo=bar", CallbackBodyParameter::Constant("foo", "bar").to_body_string());
/// assert_eq!(
///     "foo=${x:bar}",
///     CallbackBodyParameter::Literal(
///         "foo".to_string(),
///         "${x:bar}".to_string()
///     ).to_body_string()
/// );
///
/// ```
pub enum CallbackBodyParameter<'a> {
    OssBucket(&'a str),
    OssObject(&'a str),
    OssETag(&'a str),
    OssSize(&'a str),
    OssMimeType(&'a str),
    OssImageHeight(&'a str),
    OssImageWidth(&'a str),
    OssImageFormat(&'a str),
    OssCrc64(&'a str),
    OssContentMd5(&'a str),
    OssVpcId(&'a str),
    OssClientIp(&'a str),
    OssRequestId(&'a str),
    OssOperation(&'a str),

    /// - `.0` 是在回调中的参数名，
    /// - `.1` 是在回调中参数值对应的自定义变量的名字，也就是 `${x:custom_var_name}` 中的 `custom_var_name`。
    ///   例如：`CallbackBodyParameter::Custom("foo", "bar", "Are you OK?".to_string())`，
    ///   在请求体中就是 `foo=${x:bar}`
    /// - `.2` 是在自定义参数表中的自定参数对应的值。例如，上面的例子中，会在 `custom_variables` 中加入一个 `("x:bar", "Are you OK?")` 的条目
    ///
    /// 这么设计是为了防止在 body 中增加了自定义参数，但是忘记了传递自定义参数的值
    Custom(&'a str, &'a str, String),

    /// `.0` 是在回调中的参数名，`.1` 是在回调中的参数值，不做解析，在回调的时候原封不动插入给定的字符。
    ///
    /// 例如：`CallbackBodyParameter::Constant("hello", "world")`，
    /// 在请求体中就是 `hello=world`
    Constant(&'a str, &'a str),

    /// 这是一个兜底的，如果上面 `&'str` 不满足需求，就使用这个。
    /// 因为它可以获取所有权。在生成回调请求体的时候，会按照字符串直接拼接。
    ///
    /// 例如：`CallbackBodyParameter::Literal("foo".to_string(), "${x:bar}")`，
    /// 在请求体中就是 `foo=${x:bar}`
    Literal(String, String),
}

impl CallbackBodyParameter<'_> {
    /// 转换成 callback body 中的格式
    pub fn to_body_string(&self) -> String {
        match self {
            CallbackBodyParameter::OssBucket(k) => format!("{}=${{bucket}}", k),
            CallbackBodyParameter::OssObject(k) => format!("{}=${{object}}", k),
            CallbackBodyParameter::OssETag(k) => format!("{}=${{etag}}", k),
            CallbackBodyParameter::OssSize(k) => format!("{}=${{size}}", k),
            CallbackBodyParameter::OssMimeType(k) => format!("{}=${{mimeType}}", k),
            CallbackBodyParameter::OssImageHeight(k) => format!("{}=${{imageInfo.height}}", k),
            CallbackBodyParameter::OssImageWidth(k) => format!("{}=${{imageInfo.width}}", k),
            CallbackBodyParameter::OssImageFormat(k) => format!("{}=${{imageInfo.format}}", k),
            CallbackBodyParameter::OssCrc64(k) => format!("{}=${{crc64}}", k),
            CallbackBodyParameter::OssContentMd5(k) => format!("{}=${{contentMd5}}", k),
            CallbackBodyParameter::OssVpcId(k) => format!("{}=${{vpcId}}", k),
            CallbackBodyParameter::OssClientIp(k) => format!("{}=${{clientIp}}", k),
            CallbackBodyParameter::OssRequestId(k) => format!("{}=${{reqId}}", k),
            CallbackBodyParameter::OssOperation(k) => format!("{}=${{operation}}", k),
            CallbackBodyParameter::Custom(k, v, _) => format!("{}=${{x:{}}}", k, v),
            CallbackBodyParameter::Constant(k, v) => format!("{}={}", k, v),
            CallbackBodyParameter::Literal(k, v) => format!("{}={}", k, v),
        }
    }
}

/// Callback builder, hope it's helpful when you building your callback
pub struct CallbackBuilder<'a> {
    url: String,
    host: Option<String>,
    sni: Option<bool>,
    body_type: Option<CallbackBodyType>,
    body_parameters: Vec<CallbackBodyParameter<'a>>,
    custom_variables: HashMap<String, String>,
}

impl<'a> CallbackBuilder<'a> {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            host: None,
            sni: None,
            body_type: None,
            body_parameters: vec![],
            custom_variables: HashMap::new(),
        }
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn sni(mut self, sni: bool) -> Self {
        self.sni = Some(sni);
        self
    }

    pub fn body_type(mut self, body_type: CallbackBodyType) -> Self {
        self.body_type = Some(body_type);
        self
    }

    pub fn body_parameter(mut self, param: CallbackBodyParameter<'a>) -> Self {
        if let CallbackBodyParameter::Custom(_, custom_var_name, custom_var_value) = &param {
            self.custom_variables.insert(
                format!("x:{}", custom_var_name.strip_prefix("x:").unwrap_or(custom_var_name)),
                custom_var_value.clone(),
            );
        }

        self.body_parameters.push(param);
        self
    }

    /// 一般情况下，不需要直接调用这个函数。 `body_parameter` 已经处理了自定义参数的情况了。
    /// 除非你使用 `CallbackBodyParameter::Literal`，并且涉及到自定义参数的，就需要使用这个函数把自定义参数值加入进来。
    /// `k` 无需携带 `x:` 前缀，在插入的时候自动追加前缀
    pub fn custom_variable(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        let key: String = k.into();
        self.custom_variables
            .insert(format!("x:{}", key.strip_prefix("x:").unwrap_or(key.as_str())), v.into());
        self
    }

    pub fn build(self) -> Callback {
        let body_string = self.body_parameters.into_iter().map(|bp| bp.to_body_string()).collect::<Vec<_>>().join("&");

        Callback {
            url: self.url,
            host: self.host,
            body: body_string,
            sni: self.sni,
            body_type: self.body_type,
            custom_variables: self.custom_variables,
        }
    }
}

/// Job parameters tier for restoring object
///
/// 冷归档、深度冷归档类型 Object 解冻优先级。取值范围如下：
///
/// - 冷归档类型 Object
///   - 高优先级（Expedited）：表示 1 小时内完成解冻。
///   - 标准（Standard，默认值）：表示 2~5 小时内完成解冻。
///   - 批量（Bulk）：表示 5~12 小时内完成解冻。
/// - 深度冷归档类型 Object
///   - 高优先级（Expedited）：表示 12 小时内完成解冻。
///   - 标准（Standard，默认值）：表示48小时内完成解冻。
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum RestoreJobTier {
    #[cfg_attr(feature = "serde-support", serde(rename = "Standard"))]
    #[default]
    Standard,

    #[cfg_attr(feature = "serde-support", serde(rename = "Expedited"))]
    Expedited,

    #[cfg_attr(feature = "serde-support", serde(rename = "Bulk"))]
    Bulk,
}

impl RestoreJobTier {
    pub fn as_str(&self) -> &str {
        match self {
            RestoreJobTier::Standard => "Standard",
            RestoreJobTier::Expedited => "Expedited",
            RestoreJobTier::Bulk => "Bulk",
        }
    }
}

impl AsRef<str> for RestoreJobTier {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<&str> for RestoreJobTier {
    type Error = Error;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        match s {
            "Standard" => Ok(Self::Standard),
            "Expedited" => Ok(Self::Expedited),
            "Bulk" => Ok(Self::Bulk),
            _ => Err(Error::Other(format!("invalid job tier: {}", s))),
        }
    }
}

impl TryFrom<&String> for RestoreJobTier {
    type Error = Error;

    fn try_from(s: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

impl TryFrom<String> for RestoreJobTier {
    type Error = Error;

    fn try_from(s: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

/// Restore object request
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct RestoreObjectRequest {
    /// 设置归档、冷归档以及深度冷归档类型 Object 的解冻天数。
    ///
    /// - 归档类型 Object 解冻天数的取值范围为 1~7，单位为天
    /// - 冷归档以及深度冷归档类型 Object 解冻天数的取值范围为 1~365，单位为天。
    pub days: u16,

    /// 对于开启版本控制的 Bucket，Object 的各个版本可以对应不同的存储类型。
    /// 默认解冻 Object 当前版本，您可以通过指定 `versionId` 的方式来解冻 Object 指定版本。
    pub version_id: Option<String>,

    /// 冷归档、深度冷归档类型Object解冻优先级. 参见 [`RestoreJobTier`]
    pub tier: Option<RestoreJobTier>,
}

impl RestoreObjectRequest {
    /// Consume value and build XML content for requesting
    pub(crate) fn into_xml(self) -> Result<String> {
        let mut writer = quick_xml::Writer::new(Vec::new());

        writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

        writer.write_event(Event::Start(BytesStart::new("RestoreRequest")))?;

        writer.write_event(Event::Start(BytesStart::new("Days")))?;
        writer.write_event(Event::Text(BytesText::new(self.days.to_string().as_str())))?;
        writer.write_event(Event::End(BytesEnd::new("Days")))?;

        if let Some(t) = self.tier {
            writer.write_event(Event::Start(BytesStart::new("JobParameters")))?;
            writer.write_event(Event::Start(BytesStart::new("Tier")))?;
            writer.write_event(Event::Text(BytesText::new(t.as_str())))?;
            writer.write_event(Event::End(BytesEnd::new("Tier")))?;
            writer.write_event(Event::End(BytesEnd::new("JobParameters")))?;
        }

        writer.write_event(Event::End(BytesEnd::new("RestoreRequest")))?;

        Ok(String::from_utf8(writer.into_inner())?)
    }
}

/// Restore object result
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct RestoreObjectResult {
    pub request_id: String,
    pub object_restore_priority: Option<String>,
    pub version_id: Option<String>,
}

impl From<HashMap<String, String>> for RestoreObjectResult {
    fn from(mut headers: HashMap<String, String>) -> Self {
        Self {
            request_id: headers.remove("x-oss-request-id").unwrap_or_default(),
            object_restore_priority: headers.remove("x-oss-object-restore-priority"),
            version_id: headers.remove("x-oss-version-id"),
        }
    }
}

pub(crate) fn build_restore_object_request(bucket_name: &str, object_key: &str, config: RestoreObjectRequest) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Post)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("restore", "");

    if let Some(s) = &config.version_id {
        request = request.add_query("versionId", s);
    }

    let xml = config.into_xml()?;
    request = request.content_type("application/xml").content_length(xml.len() as u64).text_body(xml);

    Ok(request)
}

#[cfg(test)]
mod test_object_common {
    use crate::object_common::CallbackBodyParameter;

    #[cfg(feature = "serde-support")]
    use super::PutObjectResult;

    #[test]
    fn test_callback_body_parameter() {
        assert_eq!("foo=${bucket}", CallbackBodyParameter::OssBucket("foo").to_body_string());
        assert_eq!(
            "foo=${x:bar}",
            CallbackBodyParameter::Custom("foo", "bar", "Are you OK?".to_string()).to_body_string()
        );

        assert_eq!("foo=bar", CallbackBodyParameter::Constant("foo", "bar").to_body_string());
        assert_eq!(
            "foo=${x:bar}",
            CallbackBodyParameter::Literal("foo".to_string(), "${x:bar}".to_string()).to_body_string()
        );
    }

    #[test]
    #[cfg(feature = "serde-support")]
    fn test_put_object_result_serde() {
        use crate::object_common::PutObjectApiResponse;

        let ret = PutObjectResult::ApiResponse(PutObjectApiResponse {
            request_id: "abc".to_string(),
            etag: "1232".to_string(),
            content_md5: "abcsdf".to_string(),
            hash_crc64ecma: 1232344,
            version_id: None,
        });

        let s = serde_json::to_string(&ret).unwrap();
        println!("{}", s);
    }
}
