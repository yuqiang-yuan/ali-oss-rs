use std::{collections::HashMap, path::Path};

use crate::{
    common::{build_tag_string, Acl, MetadataDirective, ObjectType, ServerSideEncryptionAlgorithm, StorageClass, TagDirective},
    error::{ClientError, ClientResult},
    request::{RequestBuilder, RequestMethod},
    util::{validate_folder_object_key, validate_meta_key, validate_object_key, validate_tag_key, validate_tag_value},
};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ContentEncoding {
    /// 表示 Object 未经过压缩或编码
    #[default]
    #[cfg_attr(feature = "serde", serde(rename = "identity"))]
    Identity,

    /// 表示 Object 采用 Lempel-Ziv（LZ77） 压缩算法以及 32 位 CRC 校验的编码方式。
    #[cfg_attr(feature = "serde", serde(rename = "gzip"))]
    Gzip,

    /// 表示 Object 采用 zlib 结构和 deflate 压缩算法的编码方式。
    #[cfg_attr(feature = "serde", serde(rename = "deflate"))]
    Deflate,

    /// 表示 Object 采用 Lempel-Ziv-Welch（LZW） 压缩算法的编码方式。
    #[cfg_attr(feature = "serde", serde(rename = "compress"))]
    Compress,

    /// 表示 Object 采用 Brotli 压缩算法的编码方式。
    #[cfg_attr(feature = "serde", serde(rename = "br"))]
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
    type Error = ClientError;

    /// Try to create a ContentEncoding from a string.
    /// Acceptable values are "identity", "gzip", "deflate", "compress", and "br".
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "identity" => Ok(ContentEncoding::Identity),
            "gzip" => Ok(ContentEncoding::Gzip),
            "deflate" => Ok(ContentEncoding::Deflate),
            "compress" => Ok(ContentEncoding::Compress),
            "br" => Ok(ContentEncoding::Brotli),
            _ => Err(ClientError::Error(format!("invalid content encoding: {}", value))),
        }
    }
}

impl TryFrom<&String> for ContentEncoding {
    type Error = ClientError;

    /// See [`try_from(value: &str)`] for more details.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<String> for ContentEncoding {
    type Error = ClientError;

    /// See [`try_from(value: &str)`] for more details.
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
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

    /// 过期事件。例如：`Wed, 08 Jul 2015 16:57:01 GMT`
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
    pub object_acl: Option<Acl>,

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
    object_acl: Option<Acl>,
    storage_class: Option<StorageClass>,
    metadata: HashMap<String, String>,
    tags: HashMap<String, String>,
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

    pub fn object_acl(mut self, acl: Acl) -> Self {
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
        }
    }
}

impl Default for PutObjectOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

///
/// Get object options
///
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

pub(crate) fn build_put_object_request(
    bucket_name: &str,
    object_key: &str,
    file_path: Option<&Path>,
    options: &Option<PutObjectOptions>,
) -> ClientResult<RequestBuilder> {
    if bucket_name.is_empty() || object_key.is_empty() {
        return Err(ClientError::Error("bucket_name and object_key cannot be empty".to_string()));
    }

    // 普通文件的验证规则
    if file_path.is_some() && !validate_object_key(object_key) {
        return Err(ClientError::Error(format!("invalid object key: {}", object_key)));
    }

    if let Some(options) = &options {
        for (k, v) in &options.metadata {
            if k.is_empty() || !validate_meta_key(k) || v.is_empty() {
                return Err(ClientError::Error(format!("invalid meta data: \"{}: {}\". the key must starts with `x-oss-meta-`, and only `[0-9a-z\\-]` are allowed; the key and value must not be empty", k, v)));
            }
        }

        for (k, v) in &options.tags {
            if k.is_empty() || !validate_tag_key(k) || (!v.is_empty() && !validate_tag_value(v)) {
                return Err(ClientError::Error(format!(
                    "invalid tagging data: \"{}={}\". only `[0-9a-zA-Z\\+\\-=\\.:/]` and space character are allowed",
                    k, v
                )));
            }
        }
    }

    let mut request = RequestBuilder::new().method(RequestMethod::Put).bucket(bucket_name).object(object_key);

    if let Some(file) = file_path {
        if !file.exists() || !file.is_file() {
            return Err(ClientError::Error(format!(
                "{} does not exist or is not a regular file",
                file.as_os_str().to_str().unwrap_or("UNKNOWN")
            )));
        }

        let file_meta = std::fs::metadata(file)?;

        if file_meta.len() > 5368709120 {
            return Err(ClientError::Error(format!(
                "file {} length is larger than 5GB, can not put to oss",
                file.as_os_str().to_str().unwrap_or("UNKNOWN")
            )));
        }

        request = request
            .add_header("content-type", mime_guess::from_path(file).first_or_octet_stream())
            .add_header("content-length", file_meta.len().to_string())
            .file_body(file);
    } else {
        // creating folder
        request = request.add_header("content-length", "0");
    }

    if let Some(options) = options {
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
    }

    Ok(request)
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct GetObjectMetadataOptions {
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct ObjectMetadata {
    pub request_id: String,
    pub content_length: u64,
    pub etag: Option<String>,
    pub hash_crc64ecma: Option<String>,

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
            etag: headers.remove("etag"),
            hash_crc64ecma: headers.remove("x-oss-hash-crc64ecma"),
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

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
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
    pub object_acl: Option<Acl>,

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
    object_acl: Option<Acl>,
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

    pub fn object_acl(mut self, acl: Acl) -> Self {
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

pub(crate) fn build_copy_object_request(
    source_bucket_name: &str,
    source_object_key: &str,
    dest_bucket_name: &str,
    dest_object_key: &str,
    options: &Option<CopyObjectOptions>,
) -> ClientResult<RequestBuilder> {
    if !validate_object_key(dest_object_key) {
        return Err(ClientError::Error(format!("invalid object key: {}", dest_object_key)));
    }

    let mut request = RequestBuilder::new()
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
                return Err(ClientError::Error(format!("invalid metadata key: {}", k)));
            }
        }

        for (k, v) in options.tags.iter() {
            if !validate_tag_key(k) || !validate_tag_value(v) {
                return Err(ClientError::Error(format!("invalid tagging data: {}={}", k, v)));
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
            request = request.add_header("x-oss-metadata-directive", md.to_string());
        }

        if let Some(a) = &options.server_side_encryption {
            request = request.add_header("x-oss-server-side-encryption", a.to_string());
        }

        if let Some(s) = &options.server_side_encryption_key_id {
            request = request.add_header("x-oss-server-side-encryption-key-id", s);
        }

        if let Some(acl) = options.object_acl {
            request = request.add_header("x-oss-object-acl", acl.to_string());
        }

        if let Some(storage_class) = options.storage_class {
            request = request.add_header("x-oss-storage-class", storage_class.to_string());
        }

        if let Some(td) = options.tag_directive {
            request = request.add_header("x-oss-tag-directive", td.to_string());
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

pub(crate) fn build_get_object_request(bucket_name: &str, object_key: &str, options: &Option<GetObjectOptions>) -> RequestBuilder {
    let mut request = RequestBuilder::new().method(RequestMethod::Get).bucket(bucket_name).object(object_key);

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

    request
}

pub(crate) fn build_head_object_request(bucket_name: &str, object_key: &str, options: &Option<HeadObjectOptions>) -> RequestBuilder {
    let mut request = RequestBuilder::new().method(RequestMethod::Head).bucket(bucket_name).object(object_key);

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

    request
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct PutObjectResult {
    /// ETag
    pub etag: Option<String>,

    /// 文件 MD5 值，Base64 编码的字符串
    pub content_md5: Option<String>,

    /// 文件 CRC64 值，16 进制字符串
    pub hash_crc64ecma: Option<String>,

    /// 表示文件的版本 ID。仅当您将文件上传至已开启版本控制状态的 Bucket 时，会返回该响应头。
    pub version_id: Option<String>,
}

impl PutObjectResult {
    pub fn from_headers(headers: &HashMap<String, String>) -> Self {
        let etag = headers.get("etag").map(|v| v.to_string());
        let content_md5 = headers.get("content-md5").map(|v| v.to_string());
        let hash_crc64ecma = headers.get("x-oss-hash-crc64ecma").map(|v| v.to_string());
        let version_id = headers.get("x-oss-version-id").map(|v| v.to_string());

        Self {
            etag,
            content_md5,
            hash_crc64ecma,
            version_id,
        }
    }
}
