use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

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

// pub(crate) fn debug_request(req: &reqwest::Request) {
//     debug!("Request Details:");
//     debug!("---------------");

//     // Method and URL
//     debug!("Method: {}", req.method());
//     debug!("URL: {}", req.url());

//     // Headers
//     debug!("\nHeaders:");
//     for (name, value) in req.headers() {
//         debug!("  {}: {}", name, value.to_str().unwrap_or("[invalid]"));
//     }
//     // Version
//     debug!("\nVersion: {:?}", req.version());
// }

///
/// Bucket name validation.
///
/// - lenght between [3, 63]
/// - only lowercase letters, digits, and hyphens are allowed
/// - must start and end with a letter or digit (not a hyphen)
///
pub(crate) fn validate_bucket_name(name: &str) -> bool {
    // 检查长度
    if name.len() < 3 || name.len() > 63 {
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

/// object key for folder
pub(crate) fn validate_folder_object_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 1023 && !key.starts_with("/") && !key.starts_with("\\") && key.ends_with("/")
}

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
}
