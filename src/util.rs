use std::path::{Component, Path};

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::common;

/// Get UTC date time string for aliyun oss API.
/// e.g. 20231203T121212Z
pub(crate) fn get_iso8601_date_time_string() -> String {
    // 获取当前 UTC 时间
    let now: DateTime<Utc> = Utc::now();

    // 格式化为 ISO8601 格式
    // 使用 Z 表示 UTC 时区
    now.format("%Y%m%dT%H%M%SZ").to_string()
}

/// Get date string for aliyun oss API.
/// e.g. 20231203
#[allow(unused)]
pub(crate) fn get_iso8601_date_string() -> String {
    // 获取当前 UTC 时间
    let now: DateTime<Utc> = Utc::now();

    // 格式化为 ISO8601 格式
    // 使用 Z 表示 UTC 时区
    now.format("%Y%m%d").to_string()
}

pub(crate) fn get_http_date() -> String {
    // HTTP Date 格式必须使用 UTC 时间
    let now: DateTime<Utc> = Utc::now();

    // 格式化为 HTTP Date 格式
    // 例如: "Sun, 06 Nov 1994 08:49:37 GMT"
    now.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Find region from endpoint string
pub(crate) fn get_region_from_endpoint<S: AsRef<str>>(endpoint: S) -> Result<String, String> {
    match endpoint.as_ref().find(".") {
        Some(idx) => Ok(endpoint.as_ref()[..idx - 1].replace("oss-", "").to_string()),
        None => Err(format!("can not extract region id from endpoint: {}", endpoint.as_ref())),
    }
}

/// Hmac-SHA256 digest
pub(crate) fn hmac_sha256(key_data: &[u8], msg_data: &[u8]) -> Vec<u8> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key_data).unwrap();
    mac.update(msg_data);
    let ret = mac.finalize();
    ret.into_bytes().to_vec()
}

pub(crate) fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let data = hasher.finalize();
    data.to_vec()
}

/// Consumes the ETag string and remove the prefix and suffix double quotation mark
pub(crate) fn sanitize_etag(s: String) -> String {
    let tag = s.strip_prefix("\"").unwrap_or(s.as_str());
    let tag = tag.strip_suffix("\"").unwrap_or(tag);
    tag.to_string()
}

/// Debug request
#[allow(dead_code)]
pub(crate) fn debug_request(req: &reqwest::Request) {
    log::debug!("Request Details:");
    log::debug!("---------------");

    // Method and URL
    log::debug!("Method: {}", req.method());
    log::debug!("URL: {}", req.url());

    // Headers
    log::debug!("\nHeaders:");
    for (name, value) in req.headers() {
        log::debug!("  {}: {}", name, value.to_str().unwrap_or("[invalid]"));
    }
    // Version
    log::debug!("\nVersion: {:?}", req.version());
}

#[allow(dead_code)]
pub(crate) fn debug_blocking_request(req: &reqwest::blocking::Request) {
    log::debug!("Request Details:");
    log::debug!("---------------");

    // Method and URL
    log::debug!("Method: {}", req.method());
    log::debug!("URL: {}", req.url());

    // Headers
    log::debug!("\nHeaders:");
    for (name, value) in req.headers() {
        log::debug!("  {}: {}", name, value.to_str().unwrap_or("[invalid]"));
    }
    // Version
    log::debug!("\nVersion: {:?}", req.version());
}

///
/// Bucket name validation.
///
/// - lenght between [3, 63]
/// - only lowercase letters, digits, and hyphens are allowed
/// - must start and end with a letter or digit (not a hyphen)
///
pub(crate) fn validate_bucket_name(name: &str) -> bool {
    // 检查长度
    if name.len() < common::MIN_BUCKET_NAME_LENGTH || name.len() > common::MAX_BUCKET_NAME_LENGTH {
        return false;
    }

    // 检查是否以短横线开头或结尾
    if name.starts_with('-') || name.ends_with('-') {
        return false;
    }

    // 检查字符是否合法
    name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// object key for regular file
pub(crate) fn validate_object_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 1023 && !key.starts_with("/") && !key.starts_with("\\") && !key.ends_with("/") && !key.ends_with("\\")
}

// /// object key for folder
// pub(crate) fn validate_folder_object_key(key: &str) -> bool {
//     !key.is_empty() && key.len() <= 1023 && !key.starts_with("/") && !key.starts_with("\\") && key.ends_with("/")
// }

/// Validate oss tagging key and value
/// 签合法字符集包括大小写字母、数字、空格和下列符号：`+ - = . _ : /`。
pub(crate) fn validate_tag(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_alphanumeric() || c == ' ' || "+-=._:/".contains(c))
}

/// Tagging key length must between [1, 128]
pub(crate) fn validate_tag_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 128 && validate_tag(key)
}

/// Tagging value length must between [1, 256]
pub(crate) fn validate_tag_value(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && validate_tag(value)
}

/// Starts with `x-oss-meta-` and only supports ascii alphabets or numbers or hyphen (`-`)
pub(crate) fn validate_meta_key(key: &str) -> bool {
    key.starts_with("x-oss-meta-") && key.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// Check if the file name contains invalid characters.
/// note: valid file name has different rules on windows and linux and macOS
/// TODO: check file length
pub(crate) fn validate_file_name(s: &str) -> bool {
    !s.is_empty()
        && !s.contains(['<', '>', ':', '"', '/', '\\', '|', '?', '*', '\0'])
        && ![
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6",
            "LPT7", "LPT8", "LPT9",
        ]
        .contains(&s)
}

/// Validate each normal component (folder name) in the given path
/// the path must be an absolute path.
/// TODO: check path length
pub(crate) fn validate_path(p: impl AsRef<Path>) -> bool {
    if p.as_ref().is_relative() {
        return false;
    }

    // check the folder name and file name
    if !p.as_ref().components().all(|comp| match comp {
        Component::Normal(os_str) => {
            if let Some(s) = os_str.to_str() {
                validate_file_name(s)
            } else {
                false
            }
        }
        _ => true,
    }) {
        return false;
    }

    // check the file stem only
    if let Some(stem) = p.as_ref().file_stem() {
        if let Some(s) = stem.to_str() {
            validate_file_name(s)
        } else {
            false
        }
    } else {
        false
    }
}

/// Calculate file md5 and returns base64 string
#[cfg(test)]
pub(crate) fn file_md5(file: impl AsRef<Path>) -> String {
    use base64::Engine;

    let mut hasher = md5::Context::new();
    let mut file = std::fs::File::open(file).unwrap();
    std::io::copy(&mut file, &mut hasher).unwrap();
    let data = hasher.compute();
    base64::prelude::BASE64_STANDARD.encode(data.0)
}

#[cfg(test)]
mod test_util {
    use crate::util::{get_http_date, get_iso8601_date_string};

    use super::get_iso8601_date_time_string;

    #[test]
    fn test_iso8601() {
        let s = get_iso8601_date_time_string();
        println!("ISO8601 date time string: {}", s);

        let s = get_iso8601_date_string();
        println!("ISO8601 date string: {}", s);

        let s = get_http_date();
        println!("HTTP Date header: {}", s);
    }

    #[test]
    fn test_validate_path() {
        use super::validate_path;

        // Test invalid relative paths
        assert!(!validate_path("relative/path"));
        assert!(!validate_path("./relative/path"));
        assert!(!validate_path("../relative/path"));

        // Test valid absolute paths on Unix
        assert!(validate_path("/absolute/path"));
        assert!(validate_path("/absolute/path/file.txt"));
        assert!(validate_path("/absolute/path/with space/file.txt"));

        // Test invalid characters in path components
        assert!(!validate_path("/path/with/</invalid"));
        assert!(!validate_path("/path/with/>/invalid"));
        assert!(!validate_path("/path/with/:/invalid"));
        assert!(!validate_path("/path/with/\\/invalid"));
        assert!(!validate_path("/path/with/|/invalid"));
        assert!(!validate_path("/path/with/?/invalid"));
        assert!(!validate_path("/path/with/*/invalid"));

        // Test reserved names on Windows
        assert!(!validate_path("/path/COM1/file"));
        assert!(!validate_path("/path/PRN/file"));
        assert!(!validate_path("/path/AUX/file"));
        assert!(!validate_path("/path/NUL/file"));

        // Test path component length
        assert!(validate_path("/path/normal_length_folder/file.txt"));
        assert!(!validate_path("/path/very_long_folder_name_that_exceeds_255_characters_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/file.txt"));
    }
}
