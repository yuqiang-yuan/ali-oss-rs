use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use log::debug;
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

pub(crate) fn debug_request(req: &reqwest::Request) {
    debug!("Request Details:");
    debug!("---------------");

    // Method and URL
    debug!("Method: {}", req.method());
    debug!("URL: {}", req.url());

    // Headers
    debug!("\nHeaders:");
    for (name, value) in req.headers() {
        debug!("  {}: {}", name, value.to_str().unwrap_or("[invalid]"));
    }
    // Version
    debug!("\nVersion: {:?}", req.version());
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

#[cfg(test)]
mod test {
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
