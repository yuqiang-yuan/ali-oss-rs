use std::{collections::HashMap, path::Path};

use crate::{
    common::{Acl, ServerSideEncryptionAlgorithm, StorageClass},
    error::{ClientError, ClientResult},
    request::{RequestBuilder, RequestMethod},
    util::{validate_folder_object_key, validate_meta_key, validate_object_key, validate_tag_key, validate_tag_value},
};

// #[derive(Debug, Copy, Clone, PartialEq, Eq)]
// #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// pub enum CacheControl {
//     NoCache,
//     NoStore,
//     Private,
//     MaxAge(u32),
// }

// impl CacheControl {
//     pub fn to_string(&self) -> String {
//         match self {
//             CacheControl::NoCache => "no-cache".to_string(),
//             CacheControl::NoStore => "no-store".to_string(),
//             CacheControl::Private => "private".to_string(),
//             CacheControl::MaxAge(age) => format!("max-age={}", age),
//         }
//     }
// }

// impl TryFrom<&str> for CacheControl {
//     type Error = ClientError;

//     /// Try to parse a CacheControl from a string.
//     ///
//     /// Acceptable values are:
//     ///
//     /// - "no-cache"
//     /// - "no-store"
//     /// - "private"
//     /// - "max-age=<seconds>". which `seconds` is a positive integer. for example: `max-age=3600`.
//     ///
//     fn try_from(value: &str) -> Result<Self, Self::Error> {
//         match value {
//             "no-cache" => Ok(CacheControl::NoCache),
//             "no-store" => Ok(CacheControl::NoStore),
//             "private" => Ok(CacheControl::Private),
//             s if s.starts_with("max-age=") => {
//                 if let Some(age) = s[8..].parse::<u32>().ok() {
//                     Ok(CacheControl::MaxAge(age))
//                 } else {
//                     Err(ClientError::Error(format!("invalid cache control value: {}", value)))
//                 }
//             }
//             _ => Err(ClientError::Error(format!("invalid cache control value: {}", value))),
//         }
//     }
// }

// impl TryFrom<&String> for CacheControl {
//     type Error = ClientError;

//     fn try_from(value: &String) -> Result<Self, Self::Error> {
//         Self::try_from(value.as_str())
//     }
// }

// impl TryFrom<String> for CacheControl {
//     type Error = ClientError;

//     fn try_from(value: String) -> Result<Self, Self::Error> {
//         Self::try_from(value.as_str())
//     }
// }

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub enum ContentDisposition {
//     Inline,
//     Attachment,
//     AttachmentWithFilename(String),
// }

// impl ContentDisposition {
//     pub fn to_string(&self) -> String {
//         match self {
//             ContentDisposition::Inline => "inline".to_string(),
//             ContentDisposition::Attachment => "attachment".to_string(),
//             ContentDisposition::AttachmentWithFilename(filename) => format!(
//                 "attachment; filename=\"{}\";filename*=UTF-8''{}",
//                 urlencoding::encode(filename),
//                 urlencoding::encode(filename)
//             ),
//         }
//     }
// }

// impl TryFrom<&str> for ContentDisposition {
//     type Error = ClientError;

//     /// Try to parse a ContentDisposition from a string.
//     ///
//     /// Acceptable values are:
//     ///
//     /// - "inline"
//     /// - "attachment"
//     /// - "attachment; filename=\"filename\"". for example: `attachment;filename=\"%E4%B8%AD%20abc.txt\"`. which `filename` is encoded using UTF-8 (like `encodeURIComponent` in javascript).
//     fn try_from(value: &str) -> Result<Self, Self::Error> {
//         let regex = regex::RegexBuilder::new(r#"^attachment;\s*filename\s*=\s*"(.+)""#)
//             .case_insensitive(true)
//             .build()
//             .unwrap();

//         match value.to_lowercase().as_str() {
//             "inline" => Ok(ContentDisposition::Inline),
//             "attachment" => Ok(ContentDisposition::Attachment),
//             s if regex.is_match(s) => {
//                 let captures = regex.captures(value).unwrap();
//                 let filename = captures.get(1).unwrap().as_str();
//                 Ok(ContentDisposition::AttachmentWithFilename(urlencoding::decode(filename)?.to_string()))
//             }
//             _ => Err(ClientError::Error(format!("invalid content disposition: {}", value))),
//         }
//     }
// }

// impl TryFrom<&String> for ContentDisposition {
//     type Error = ClientError;

//     fn try_from(value: &String) -> Result<Self, Self::Error> {
//         Self::try_from(value.as_str())
//     }
// }

// impl TryFrom<String> for ContentDisposition {
//     type Error = ClientError;

//     fn try_from(value: String) -> Result<Self, Self::Error> {
//         Self::try_from(&value)
//     }
// }

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
    ///
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

    // 文件夹的验证规则
    if file_path.is_none() && !validate_folder_object_key(object_key) {
        return Err(ClientError::Error(format!("invalid object key as a folder: {}", object_key)));
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
            let s = options
                .tags
                .iter()
                .map(|(k, v)| {
                    if v.is_empty() {
                        urlencoding::encode(k).to_string()
                    } else {
                        format!("{}={}", urlencoding::encode(k), urlencoding::encode(v))
                    }
                })
                .collect::<Vec<_>>()
                .join("&");

            request = request.add_header("x-oss-tagging", &s);
        }
    }

    Ok(request)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct PutObjectResult {
    /// 文件 MD5 值，Base64 编码的字符串
    pub content_md5: Option<String>,

    /// 文件 CRC64 值，16 进制字符串
    pub hash_crc64ecma: Option<String>,

    /// 表示文件的版本 ID。仅当您将文件上传至已开启版本控制状态的 Bucket 时，会返回该响应头。
    pub version_id: Option<String>,
}

impl PutObjectResult {
    pub fn from_headers(headers: &HashMap<String, String>) -> Self {
        let content_md5 = headers.get("content-md5").map(|v| v.to_string());
        let hash_crc64ecma = headers.get("x-oss-hash-crc64ecma").map(|v| v.to_string());
        let version_id = headers.get("x-oss-version-id").map(|v| v.to_string());

        Self {
            content_md5,
            hash_crc64ecma,
            version_id,
        }
    }
}
