#![doc = include_str!("../README.md")]
pub mod acl;
pub mod acl_common;
pub mod bucket;
pub mod bucket_common;
pub mod cname;
pub mod cname_common;
pub mod common;
pub mod error;
pub mod multipart;
pub mod multipart_common;
pub mod object;
pub mod object_common;
pub mod presign;
pub mod presign_common;
pub mod progress;
pub mod request;
pub mod symlink;
pub mod symlink_common;
pub mod tagging;
pub mod tagging_common;

#[cfg(feature = "blocking")]
pub mod blocking;

mod util;

use std::{collections::HashMap, pin::Pin, str::FromStr};

use async_trait::async_trait;
use bytes::Bytes;
use error::{Error, ErrorResponse};
use futures::{Stream, StreamExt};
use request::RequestBody;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Body,
};

pub use reqwest;
pub use serde;
pub use serde_json;
pub use tokio;

use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};
use url::Url;
use util::{get_region_from_endpoint, hmac_sha256};

pub type Result<T> = std::result::Result<T, crate::error::Error>;

/// Builder for `Client`.
#[derive(Debug, Default)]
pub struct ClientBuilder {
    access_key_id: String,
    access_key_secret: String,
    endpoint: String,
    region: Option<String>,
    scheme: Option<String>,
    sts_token: Option<String>,
    client: Option<reqwest::Client>,
}

impl ClientBuilder {
    /// `endpoint` could be: `oss-cn-hangzhou.aliyuncs.com` without scheme part.
    /// or you can include scheme part in the `endpoint`: `https://oss-cn-hangzhou.aliyuncs.com`.
    /// if no scheme specified, use `https` by default.
    ///
    /// # Examples
    ///
    /// ```
    /// let client = ali_oss_rs::ClientBuilder::new(
    ///     "your access key id",
    ///     "your acess key secret",
    ///     "oss-cn-hangzhou.aliyuncs.com"
    /// ).build();
    /// ```
    pub fn new<S1, S2, S3>(access_key_id: S1, access_key_secret: S2, endpoint: S3) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
    {
        Self {
            access_key_id: access_key_id.as_ref().to_string(),
            access_key_secret: access_key_secret.as_ref().to_string(),
            endpoint: endpoint.as_ref().to_string(),
            ..Default::default()
        }
    }

    /// Set region id explicitly. e.g. `cn-beijing`, `cn-hangzhou`.
    /// **CAUTION** no `oss-` prefix for region.
    /// If no region is set, I will be guessed from `endpoint`.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set scheme. should be: `https` or `http`.
    pub fn scheme(mut self, scheme: impl Into<String>) -> Self {
        self.scheme = Some(scheme.into());
        self
    }

    /// For sts token mode.
    pub fn sts_token(mut self, sts_token: impl Into<String>) -> Self {
        self.sts_token = Some(sts_token.into());
        self
    }

    /// You can build your own `reqwest::Client` and set to the OSS client.
    /// I do not expose each option of `reqwest::Client` because there are many options to build a `reqwest::Client`.
    pub fn client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Build the client.
    ///
    /// # Errors
    ///
    /// If `region` is not set and can not guessed from `endpoint`, returns error.
    pub fn build(self) -> std::result::Result<crate::Client, String> {
        let ClientBuilder {
            access_key_id,
            access_key_secret,
            endpoint,
            region,
            scheme,
            sts_token,
            client,
        } = self;

        let scheme = if let Some(s) = scheme {
            s
        } else if endpoint.starts_with("http://") {
            "http".to_string()
        } else {
            "https".to_string()
        };

        let lc_endpoint = endpoint.as_str();
        // remove the scheme part from the endpoint if there was one
        let lc_endpoint = if let Some(s) = lc_endpoint.strip_prefix("http://") {
            s.to_string()
        } else {
            lc_endpoint.to_string()
        };

        let lc_endpoint = if let Some(s) = lc_endpoint.strip_prefix("https://") {
            s.to_string()
        } else {
            lc_endpoint.to_string()
        };

        let region = if let Some(r) = region { r } else { get_region_from_endpoint(&lc_endpoint)? };

        Ok(Client {
            access_key_id,
            access_key_secret,
            endpoint: lc_endpoint,
            region,
            scheme,
            sts_token,
            http_client: if let Some(c) = client { c } else { reqwest::Client::new() },
        })
    }
}

/// An asynchronous OSS client.
pub struct Client {
    access_key_id: String,
    access_key_secret: String,
    region: String,
    endpoint: String,
    scheme: String,
    sts_token: Option<String>,
    http_client: reqwest::Client,
}

impl Client {
    /// Creates a new client from environment variables.
    ///
    /// - `ALI_ACCESS_KEY_ID` The access key id
    /// - `ALI_ACCESS_KEY_SECRET` The access key secret
    /// - `ALI_OSS_ENDPOINT` The endpoint of the OSS service. e.g. `oss-cn-hangzhou.aliyuncs.com`. Or, you can write full URL `http://oss-cn-hangzhou.aliyuncs.com` or `https://oss-cn-hangzhou.aliyuncs.com` with scheme `http` or `https`.
    /// - `ALI_OSS_REGION` Optional. The region id of the OSS service e.g. `cn-hangzhou`, `cn-beijing`. If not present, It will be inferred from `ALI_OSS_ENDPOINT` env.
    ///
    pub fn from_env() -> Self {
        let access_key_id = std::env::var("ALI_ACCESS_KEY_ID").expect("env var ALI_ACCESS_KEY_ID is missing");
        let access_key_secret = std::env::var("ALI_ACCESS_KEY_SECRET").expect("env var ALI_ACCESS_KEY_SECRET is missing");
        let endpoint = std::env::var("ALI_OSS_ENDPOINT").expect("env var ALI_OSS_ENDPOINT is missing");
        let region = match std::env::var("ALI_OSS_REGION") {
            Ok(s) => s,
            Err(e) => match e {
                std::env::VarError::NotPresent => match util::get_region_from_endpoint(&endpoint) {
                    Ok(s) => s,
                    Err(e) => {
                        panic!("{}", e)
                    }
                },
                _ => panic!("env var ALI_OSS_REGION is missing or misconfigured"),
            },
        };

        Self::new(access_key_id, access_key_secret, region, endpoint)
    }

    /// Create a new client.
    ///
    /// See [`Self::from_env`] for more details about the arguments.
    ///
    /// If you need highly cusomtized `reqwest::Client` to setup this struct,
    /// Please check [`ClientBuilder`]
    pub fn new<S1, S2, S3, S4>(access_key_id: S1, access_key_secret: S2, region: S3, endpoint: S4) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
        S4: AsRef<str>,
    {
        let lc_endpoint = endpoint.as_ref().to_string().to_lowercase();

        let scheme = if lc_endpoint.starts_with("http://") {
            "http".to_string()
        } else {
            "https".to_string()
        };

        // remove the scheme part from the endpoint if there was one
        let lc_endpoint = if let Some(s) = lc_endpoint.strip_prefix("http://") {
            s.to_string()
        } else {
            lc_endpoint
        };

        let lc_endpoint = if let Some(s) = lc_endpoint.strip_prefix("https://") {
            s.to_string()
        } else {
            lc_endpoint
        };

        Self {
            access_key_id: access_key_id.as_ref().to_string(),
            access_key_secret: access_key_secret.as_ref().to_string(),
            region: region.as_ref().to_string(),
            endpoint: lc_endpoint,
            sts_token: None,
            scheme,
            http_client: reqwest::Client::new(),
        }
    }

    fn calculate_signature(&self, string_to_sign: &str, date_string: &str) -> String {
        let key_string = format!("aliyun_v4{}", &self.access_key_secret);

        let date_key = hmac_sha256(key_string.as_bytes(), date_string.as_bytes());
        let date_region_key = hmac_sha256(&date_key, self.region.as_bytes());
        let date_region_service_key = hmac_sha256(&date_region_key, "oss".as_bytes());
        let signing_key = hmac_sha256(&date_region_service_key, "aliyun_v4_request".as_bytes());

        hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()))
    }

    /// Some of the strings are used multiple times,
    /// So I put them in this method to prevent re-generating
    /// and better debugging output.
    /// And add some default headers to the request builder.
    async fn do_request<T>(&self, mut oss_request: crate::request::OssRequest) -> Result<(HashMap<String, String>, T)>
    where
        T: FromResponse,
    {
        // check if sign `host` header
        if oss_request.additional_headers.contains("host") {
            let host = if oss_request.bucket_name.is_empty() {
                self.endpoint.clone()
            } else {
                format!("{}.{}", oss_request.bucket_name, self.endpoint)
            };

            oss_request.headers_mut().insert("host".to_string(), host);
        }

        if let Some(s) = &self.sts_token {
            oss_request.headers_mut().insert("x-oss-security-token".to_string(), s.to_string());
        }

        let date_time_string = oss_request.headers.get("x-oss-date").unwrap();
        let date_string = &date_time_string[..8];

        let additional_headers = oss_request.build_additional_headers();

        let string_to_sign = oss_request.build_string_to_sign(&self.region);

        log::debug!("string to sign: \n--------\n{}\n--------", string_to_sign);

        let sig = self.calculate_signature(&string_to_sign, date_string);

        log::debug!("signature: {}", sig);

        let auth_string = format!(
            "OSS4-HMAC-SHA256 Credential={}/{}/{}/oss/aliyun_v4_request,{}Signature={}",
            self.access_key_id,
            date_string,
            self.region,
            if additional_headers.is_empty() {
                "".to_string()
            } else {
                format!("{},", additional_headers)
            },
            sig
        );

        let mut header_map = HeaderMap::new();

        for (k, v) in oss_request.headers.iter() {
            header_map.insert(HeaderName::from_str(k)?, HeaderValue::from_str(v)?);
        }

        let http_date = util::get_http_date();

        header_map.insert(HeaderName::from_static("authorization"), HeaderValue::from_str(&auth_string)?);
        header_map.insert(HeaderName::from_static("date"), HeaderValue::from_str(&http_date)?);

        let uri = oss_request.build_request_uri();
        let query_string = oss_request.build_canonical_query_string();

        let domain_name = if oss_request.bucket_name.is_empty() {
            format!("{}://{}{}", self.scheme, self.endpoint, uri)
        } else {
            format!("{}://{}.{}{}", self.scheme, oss_request.bucket_name, self.endpoint, uri)
        };

        let full_url = if query_string.is_empty() {
            domain_name
        } else {
            format!("{}?{}", domain_name, query_string)
        };

        log::debug!("full url: {}", full_url);

        let mut req_builder = self.http_client.request(oss_request.method.into(), Url::parse(&full_url)?).headers(header_map);

        let progress = oss_request.progress.take();

        // 内存中的 body 没法分块上报进度，只能等整个 body 交给底层传输之后再补一次完成事件。
        // 这里记录 (回调, 总长度)，在 execute 之后触发。
        let mut in_memory_progress = None;

        // 根据 body 类型设置请求体
        req_builder = match oss_request.body {
            RequestBody::Empty => req_builder,
            RequestBody::Text(text) => {
                if let Some(cb) = progress {
                    let len = text.len() as u64;
                    cb(0, Some(len));
                    in_memory_progress = Some((cb, len));
                }

                req_builder.body(text)
            }
            RequestBody::Bytes(bytes) => {
                // 注意：这里刻意不把 `Bytes` 转成 stream。reqwest 对内存 body 的 `try_reuse()`
                // 返回 `Some`，可以重放 307/308 重定向；换成 `wrap_stream` 会静默失去这个能力，
                // 而对已经在内存里的数据来说也没有任何信息增益。
                if let Some(cb) = progress {
                    let len = bytes.len() as u64;
                    cb(0, Some(len));
                    in_memory_progress = Some((cb, len));
                }

                req_builder.body(bytes)
            }
            RequestBody::File(path, range) => match progress {
                None => {
                    if let Some(rng) = range {
                        let mut file = tokio::fs::File::open(path).await?;
                        file.seek(tokio::io::SeekFrom::Start(rng.start)).await?;
                        let limited_reader = file.take(rng.end - rng.start);
                        // Create a stream from the limited reader
                        let stream = FramedRead::new(limited_reader, BytesCodec::new()).map(|r| r.map(|bytes| bytes.freeze()));
                        req_builder.body(Body::wrap_stream(stream))
                    } else {
                        req_builder.body(tokio::fs::File::open(path).await?)
                    }
                }
                Some(cb) => {
                    let mut file = tokio::fs::File::open(path).await?;

                    // 不从 `content-length` 头取长度：直接用文件元数据 / range，避免依赖调用方
                    // 是否设置了那个头。
                    let (start, len) = match range {
                        Some(rng) => (rng.start, rng.end - rng.start),
                        None => (0, file.metadata().await?.len()),
                    };

                    if start > 0 {
                        file.seek(tokio::io::SeekFrom::Start(start)).await?;
                    }

                    cb(0, Some(len));

                    let limited_reader = file.take(len);
                    let stream = FramedRead::with_capacity(limited_reader, BytesCodec::new(), progress::CHUNK_SIZE).map(|r| r.map(|bytes| bytes.freeze()));

                    req_builder.body(Body::wrap_stream(progress::ProgressStream::new(stream, cb, Some(len))))
                }
            },
        };

        let req = req_builder.build()?;

        for (k, v) in req.headers() {
            log::debug!(">> headers: {}: {}", k, v.to_str().unwrap_or_default());
        }

        let response = self.http_client.execute(req).await?;

        if let Some((cb, len)) = in_memory_progress {
            cb(len, Some(len));
        }

        let mut response_headers = HashMap::new();

        // 阿里云 OSS API 中的响应头的值都是可表示的字符串
        for (key, value) in response.headers() {
            log::debug!("<< headers: {}: {}", key, value.to_str().unwrap_or("ERROR-PARSE-HEADER-VALUE"));
            response_headers.insert(key.to_string(), value.to_str().unwrap_or("").to_string());
        }

        if !response.status().is_success() {
            let status = response.status();

            match response.text().await {
                Ok(s) => {
                    log::error!("{}", s);
                    if s.is_empty() {
                        log::error!("call api failed with status: \"{}\". full url: {}", status, full_url);
                        Err(Error::StatusError(status))
                    } else {
                        let error_response = ErrorResponse::from_xml(&s)?;
                        Err(Error::ApiError(Box::new(error_response)))
                    }
                }
                Err(_) => {
                    log::error!("call api failed with status: \"{}\". full url: {}", status, full_url);
                    Err(Error::StatusError(status))
                }
            }
        } else {
            Ok((response_headers, T::from_response(response).await?))
        }
    }

    /// Clone a new client instance with the same security data and different region.
    /// This is helpful if you are operation on buckets across multiple regions with a single pair of access key id and secret.
    pub fn clone_to<S1, S2>(&self, region: S1, endpoint: S2) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let endpoint = endpoint.as_ref();

        let endpoint = if let Some(s) = endpoint.strip_prefix("http://") { s } else { endpoint };

        let endpoint = if let Some(s) = endpoint.strip_prefix("https://") { s } else { endpoint };

        Self {
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            region: region.as_ref().to_string(),
            endpoint: endpoint.to_string(),
            scheme: self.scheme.clone(),
            sts_token: self.sts_token.clone(),
            http_client: self.http_client.clone(),
        }
    }
}

#[async_trait]
pub(crate) trait FromResponse: Sized {
    async fn from_response(response: reqwest::Response) -> Result<Self>;
}

#[async_trait]
impl FromResponse for String {
    async fn from_response(response: reqwest::Response) -> Result<Self> {
        let text = response.text().await?;
        Ok(text)
    }
}

#[async_trait]
impl FromResponse for () {
    async fn from_response(_: reqwest::Response) -> Result<Self> {
        Ok(())
    }
}

// Define a type alias for the byte stream
pub(crate) type ByteStream = Pin<Box<dyn Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send>>;

#[async_trait]
impl FromResponse for ByteStream {
    async fn from_response(response: reqwest::Response) -> Result<Self> {
        // Convert the response body into a byte stream
        let stream = response.bytes_stream();
        Ok(Box::pin(stream))
    }
}

#[test]
fn test_client_build() {
    let config = ClientBuilder::new("access_key_id", "access_key_secret", "https://oss-cn-hangzhou.aliyuncs.com").build().unwrap();
    assert_eq!(config.region, "cn-hangzhou");
    assert_eq!(config.scheme, "https");
    assert_eq!(config.endpoint, "oss-cn-hangzhou.aliyuncs.com");
}

/// 这些测试起一个本地 TCP 服务器当 OSS，不依赖网络和凭证，可以直接进 CI。
#[cfg(test)]
mod test_upload_progress {
    use std::{net::SocketAddr, sync::Arc};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::{
        progress::ProgressFn,
        request::{OssRequest, RequestMethod},
        Client, RequestBody, Result,
    };

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    /// 起一个只处理一个连接的 HTTP 服务器，返回它收到的 `(原始请求头, 请求体)`。
    async fn spawn_server() -> (SocketAddr, tokio::task::JoinHandle<(String, Vec<u8>)>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            let mut buf = Vec::new();
            let mut tmp = [0u8; 8192];

            // 先读到请求头结束
            let head_end = loop {
                let n = socket.read(&mut tmp).await.unwrap();
                assert!(n > 0, "连接在请求头读完之前就关闭了");

                buf.extend_from_slice(&tmp[..n]);

                if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                    break pos + 4;
                }
            };

            let head = String::from_utf8_lossy(&buf[..head_end]).to_string();

            let content_length: usize = head
                .lines()
                .find_map(|line| {
                    let (k, v) = line.split_once(':')?;
                    if k.eq_ignore_ascii_case("content-length") {
                        v.trim().parse().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);

            // 再读满请求体
            while buf.len() < head_end + content_length {
                let n = socket.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }

            let body = buf[head_end..].to_vec();

            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await.unwrap();
            socket.flush().await.unwrap();

            (head, body)
        });

        (addr, handle)
    }

    /// 构造一个指向本地服务器的 Client。
    fn client_for(addr: SocketAddr) -> Client {
        Client {
            access_key_id: "test_access_key_id".to_string(),
            access_key_secret: "test_access_key_secret".to_string(),
            region: "cn-hangzhou".to_string(),
            endpoint: addr.to_string(),
            scheme: "http".to_string(),
            sts_token: None,
            http_client: reqwest::Client::new(),
        }
    }

    /// 收集到的进度事件。
    type Events = Arc<std::sync::Mutex<Vec<(u64, Option<u64>)>>>;

    fn recorder() -> (Events, ProgressFn) {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let cloned = events.clone();

        let callback: ProgressFn = Arc::new(move |transferred, total| {
            cloned.lock().unwrap().push((transferred, total));
        });

        (events, callback)
    }

    /// 关键回归测试：文件上传的 body 是「未知长度的流 + 显式 content-length 头」，
    /// 必须仍然以固定的 Content-Length 发送，**不能**降级成 `Transfer-Encoding: chunked`。
    ///
    /// 同时验证进度回调的首尾事件和单调性。
    #[tokio::test]
    async fn test_file_upload_progress_keeps_content_length_framing() {
        let (addr, server) = spawn_server().await;

        // 200 KiB，保证会被切成多个 64 KiB 的块
        let payload = vec![b'x'; 200 * 1024];

        let dir = std::env::temp_dir().join(format!("ali-oss-rs-progress-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("payload.bin");
        std::fs::write(&file_path, &payload).unwrap();

        let (events, callback) = recorder();

        let request = OssRequest::new()
            .method(RequestMethod::Put)
            .object("payload.bin")
            .content_length(payload.len() as u64)
            .body(RequestBody::File(file_path.clone(), None))
            .progress(callback);

        let result: Result<(std::collections::HashMap<String, String>, String)> = client_for(addr).do_request(request).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let (head, body) = server.await.unwrap();

        let lower = head.to_lowercase();
        assert!(lower.contains(&format!("content-length: {}", payload.len())), "请求头里没有正确的 content-length:\n{}", head);
        assert!(!lower.contains("transfer-encoding"), "请求被降级成了 chunked:\n{}", head);
        assert_eq!(body, payload);

        let events = events.lock().unwrap();
        let total = payload.len() as u64;

        assert_eq!(events.first(), Some(&(0, Some(total))), "首个事件应该是 (0, total)：{:?}", *events);
        assert_eq!(events.last(), Some(&(total, Some(total))), "最后一个事件应该是 (total, total)：{:?}", *events);
        assert!(events.windows(2).all(|w| w[0].0 < w[1].0), "进度不是单调递增的：{:?}", *events);
        assert!(events.len() > 2, "文件 body 应该产生多于两个事件：{:?}", *events);

        std::fs::remove_file(&file_path).ok();
    }

    /// range 上传（分片上传用的路径）同样要保持 framing 正确。
    #[tokio::test]
    async fn test_ranged_upload_progress() {
        let (addr, server) = spawn_server().await;

        let payload = vec![b'y'; 100 * 1024];

        let dir = std::env::temp_dir().join(format!("ali-oss-rs-progress-range-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("payload.bin");
        std::fs::write(&file_path, &payload).unwrap();

        let (events, callback) = recorder();

        // 只上传 [1024, 1024 + 40KiB) 这一段
        let range = 1024u64..(1024 + 40 * 1024);
        let expected = payload[range.start as usize..range.end as usize].to_vec();

        let request = OssRequest::new()
            .method(RequestMethod::Put)
            .object("payload.bin")
            .content_length(expected.len() as u64)
            .body(RequestBody::File(file_path.clone(), Some(range.clone())))
            .progress(callback);

        let result: Result<(std::collections::HashMap<String, String>, String)> = client_for(addr).do_request(request).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let (head, body) = server.await.unwrap();

        let lower = head.to_lowercase();
        assert!(lower.contains(&format!("content-length: {}", expected.len())), "请求头里没有正确的 content-length:\n{}", head);
        assert!(!lower.contains("transfer-encoding"), "请求被降级成了 chunked:\n{}", head);
        assert_eq!(body, expected);

        let events = events.lock().unwrap();
        let total = expected.len() as u64;
        assert_eq!(events.first(), Some(&(0, Some(total))));
        assert_eq!(events.last(), Some(&(total, Some(total))));

        std::fs::remove_file(&file_path).ok();
    }

    /// 内存 body 不走流式路径，只上报首尾两个事件。
    #[tokio::test]
    async fn test_buffer_upload_progress_reports_boundaries_only() {
        let (addr, server) = spawn_server().await;

        let payload = vec![b'z'; 32 * 1024];
        let (events, callback) = recorder();

        let request = OssRequest::new()
            .method(RequestMethod::Put)
            .object("payload.bin")
            .content_length(payload.len() as u64)
            .body(RequestBody::Bytes(payload.clone()))
            .progress(callback);

        let result: Result<(std::collections::HashMap<String, String>, String)> = client_for(addr).do_request(request).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let (_, body) = server.await.unwrap();
        assert_eq!(body, payload);

        let events = events.lock().unwrap();
        let total = payload.len() as u64;
        assert_eq!(*events, vec![(0, Some(total)), (total, Some(total))]);
    }

    /// 没有请求体的请求（例如 initiate_multipart_uploads）不应该触发任何回调。
    #[tokio::test]
    async fn test_empty_body_triggers_no_progress() {
        let (addr, server) = spawn_server().await;

        let (events, callback) = recorder();

        let request = OssRequest::new()
            .method(RequestMethod::Post)
            .object("payload.bin")
            .body(RequestBody::Empty)
            .progress(callback);

        let result: Result<(std::collections::HashMap<String, String>, String)> = client_for(addr).do_request(request).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let _ = server.await.unwrap();

        assert!(events.lock().unwrap().is_empty(), "空请求体不应该触发进度回调");
    }
}
