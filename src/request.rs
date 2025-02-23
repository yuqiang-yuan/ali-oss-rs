use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    path::PathBuf,
};

use crate::util;

// 首先定义 RequestBody 枚举
#[derive(Debug, Default)]
pub(crate) enum RequestBody {
    #[default]
    Empty,
    Text(String),
    Bytes(Vec<u8>),
    File(PathBuf),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RequestMethod {
    Get,
    Put,
    Post,
    Delete,
    Head,
}

impl Display for RequestMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestMethod::Get => write!(f, "GET"),
            RequestMethod::Put => write!(f, "PUT"),
            RequestMethod::Post => write!(f, "POST"),
            RequestMethod::Delete => write!(f, "DELETE"),
            RequestMethod::Head => write!(f, "HEAD"),
        }
    }
}

impl From<RequestMethod> for reqwest::Method {
    fn from(value: RequestMethod) -> Self {
        match value {
            RequestMethod::Get => reqwest::Method::GET,
            RequestMethod::Put => reqwest::Method::PUT,
            RequestMethod::Post => reqwest::Method::POST,
            RequestMethod::Delete => reqwest::Method::DELETE,
            RequestMethod::Head => reqwest::Method::HEAD,
        }
    }
}

pub(crate) struct RequestBuilder {
    pub bucket_name: String,
    pub object_key: String,
    pub method: RequestMethod,
    pub headers: HashMap<String, String>,

    // 这个是根据官方文档的名字来取的属性名。
    // 实际上这个表示除了 Content-Type、Content-MD5 之外还需要需要参与签名的请求头的名字
    // 由于构建 Canonical Headers 的时候，请求头都是小写的，所以这里在存入 Set 的时候就转换小写。
    pub additional_headers: HashSet<String>,
    pub query: HashMap<String, String>,

    pub body: RequestBody,
}

impl RequestBuilder {
    pub fn new() -> Self {
        let date_time_string = util::get_iso8601_date_time_string();
        Self {
            bucket_name: "".to_string(),
            object_key: "".to_string(),
            method: RequestMethod::Get,
            headers: HashMap::from([
                ("x-sdk-client".to_string(), "ali-oss-rs/0.1.0".to_string()),
                ("x-oss-content-sha256".to_string(), "UNSIGNED-PAYLOAD".to_string()),
                ("x-oss-date".to_string(), date_time_string),
            ]),
            additional_headers: HashSet::new(),
            query: HashMap::new(),
            body: RequestBody::Empty,
        }
    }

    pub fn method(mut self, m: RequestMethod) -> Self {
        self.method = m;
        self
    }

    pub fn bucket<S: Into<String>>(mut self, bucket_name: S) -> Self {
        self.bucket_name = bucket_name.into();
        self
    }

    pub fn object<S: Into<String>>(mut self, object_key: S) -> Self {
        self.object_key = object_key.into();
        self
    }

    /// Add header to the builder.
    /// and DO NOT treat this header as additional header
    pub fn add_header<S1, S2>(self, k: S1, v: S2) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        self.add_header_ext(k, v, false)
    }

    /// Add header to the builder
    ///
    /// `addtional_header` identifies if the header name should be added to additional header,
    /// and being used when calculating canonical request and signature
    pub fn add_header_ext<S1, S2>(mut self, k: S1, v: S2, addtional_header: bool) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        self.headers.insert(k.as_ref().to_string(), v.as_ref().to_string());
        if addtional_header {
            self.additional_headers.insert(k.as_ref().to_lowercase());
        }

        self
    }

    pub fn add_query<S1, S2>(mut self, k: S1, v: S2) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        self.query.insert(k.as_ref().to_string(), v.as_ref().to_string());
        self
    }

    /// Add only the header name to additional headers
    #[allow(dead_code)]
    pub fn add_additional_header_name<S>(mut self, name: S) -> Self
    where
        S: AsRef<str>,
    {
        self.additional_headers.insert(name.as_ref().to_lowercase());
        self
    }

    // 添加设置 body 的便捷方法
    pub fn body(mut self, body: RequestBody) -> Self {
        self.body = body;
        self
    }

    /// helper method for [`body`]. only the body is set and left `content-length`, `content-type` untouched
    pub fn text_body(self, text: impl Into<String>) -> Self {
        self.body(RequestBody::Text(text.into()))
    }

    #[allow(dead_code)]
    /// helper method for [`body`]. only the body is set and left `content-length`, `content-type` untouched
    pub fn bytes_body(self, bytes: impl Into<Vec<u8>>) -> Self {
        self.body(RequestBody::Bytes(bytes.into()))
    }

    #[allow(dead_code)]
    /// helper method for [`body`]. only the body is set and left `content-length`, `content-type` untouched
    pub fn file_body(self, file_path: impl Into<PathBuf>) -> Self {
        self.body(RequestBody::File(file_path.into()))
    }

    pub fn content_type(mut self, content_type: &str) -> Self {
        self.headers.insert("content-type".to_string(), content_type.to_string());
        self
    }

    pub fn content_length(mut self, len: u64) -> Self {
        self.headers.insert("content-length".to_string(), len.to_string());
        self
    }

    #[allow(dead_code)]
    pub fn headers_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.headers
    }

    #[allow(dead_code)]
    pub fn additional_headers_mut(&mut self) -> &mut HashSet<String> {
        &mut self.additional_headers
    }

    #[allow(dead_code)]
    pub fn query_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.query
    }

    ///
    /// 官方文档：https://help.aliyun.com/zh/oss/developer-reference/recommend-to-use-signature-version-4?spm=a2c4g.11186623.help-menu-31815.d_5_1_0_1_0_0.5dbf37b36yQEjT#c33cf04dcb1c3
    ///
    /// - 如果请求的 URI 中既包含 Bucket 也包含 Object，则 Canonical URI 填写示例为: `/examplebucket/exampleobject`
    /// - 如果请求的 URI 中只包含 Bucket 不包含 Object，则 Canonical URI 填写示例为： `/examplebucket/`
    /// - 如果请求的 URI 中不包含 Bucket 且不包含 Object，则 Canonical URI 填写示例为： `/`
    ///
    pub(crate) fn build_canonical_uri(&self) -> String {
        match (self.bucket_name.is_empty(), self.object_key.is_empty()) {
            (true, true) => "/".to_string(),
            (true, false) => format!("/{}/", urlencoding::encode(&self.bucket_name)),
            (_, _) => {
                format!(
                    "/{}/{}",
                    urlencoding::encode(&self.bucket_name),
                    self.object_key.split("/").map(|s| urlencoding::encode(s)).collect::<Vec<_>>().join("/")
                )
            }
        }
    }

    /// Build the uri part of real http request
    /// The returned string starts with "/"
    pub(crate) fn build_request_uri(&self) -> String {
        if self.object_key.is_empty() {
            return "/".to_string();
        }

        let s = self.object_key.split("/").collect::<Vec<_>>().join("/");
        format!("/{}", s)
    }

    /// 按 QueryString 的 key 进行排序。
    /// - 先编码，再排序
    /// - 如果有多个相同的 key，按照原来添加的顺序放置即可
    /// - 中间使用 `&`进行连接。
    /// - 只有 key 没有 value 的情况下，只添加 key即可
    /// - 如果没有 QueryString，则只需要放置空字符串 “”，末尾仍然需要有换行符。
    ///
    pub(crate) fn build_canonical_query_string(&self) -> String {
        if self.query.is_empty() {
            return "".to_string();
        }

        let mut pairs = self
            .query
            .iter()
            .map(|(k, v)| (urlencoding::encode(k), urlencoding::encode(v)))
            .collect::<Vec<(_, _)>>();

        pairs.sort_by(|e1, e2| e1.0.cmp(&e2.0));

        pairs
            .iter()
            .map(|(k, v)| if v.is_empty() { k.to_string() } else { format!("{}={}", k, v) })
            .collect::<Vec<_>>()
            .join("&")
    }

    /// 对请求 Header 的列表格式化后的字符串，各个 Header 之间需要添加换行符分隔。
    ///
    /// - 单个 Header 中的 key 和 value 通过冒号 `:` 分隔， Header 与 Header 之间通过换行符分隔。
    /// - Header 的key必须小写，value 必须经过Trim（去除头尾的空格）。
    /// - 按 Header 中 key 的字典序进行排列。
    /// - 请求时间通过 `x-oss-date` 来描述，要求格式必须是 ISO8601 标准时间格式（示例值为 `20231203T121212Z`）。
    ///
    /// Canonical  Headers 包含以下两类：
    ///
    /// - 必须存在且参与签名的 Header 包括：
    ///   - `x-oss-content-sha256`（其值为 `UNSIGNED-PAYLOAD`）
    ///   - Additional Header 指定必须存在且参与签名的 Header
    /// - 如果存在则加入签名的 Header 包括：
    ///   - `Content-Type`
    ///   - `Content-MD5`
    ///   - `x-oss-*`
    pub(crate) fn build_canonical_headers(&self) -> String {
        if self.headers.is_empty() {
            return "\n".to_string();
        }

        let mut pairs = self
            .headers
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .filter(|(k, _)| k == "content-type" || k == "content-md5" || k.starts_with("x-oss-") || self.additional_headers.contains(k))
            .collect::<Vec<_>>();

        pairs.sort_by(|a, b| a.0.cmp(&b.0));

        let s = pairs.iter().map(|(k, v)| format!("{}:{}", k, v.trim())).collect::<Vec<_>>().join("\n");

        // 不知道为什么这里要多一个空行
        format!("{}\n", s)
    }

    pub(crate) fn build_additional_headers(&self) -> String {
        if self.additional_headers.is_empty() {
            return "".to_string();
        }

        let mut keys = self.additional_headers.iter().map(|k| k.to_lowercase()).collect::<Vec<_>>();

        keys.sort();

        keys.join(";")
    }

    pub(crate) fn build_canonical_request(&self) -> String {
        let canonical_uri = self.build_canonical_uri();
        let canonical_query = self.build_canonical_query_string();
        let canonical_headers = self.build_canonical_headers();
        let additional_headers = self.build_additional_headers();
        let method = self.method.to_string();

        format!("{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{additional_headers}\nUNSIGNED-PAYLOAD")
    }

    /// 在构建要签名的认证字符串的时候，才生成 `x-oss-date` 头，并放到 headers 里面
    pub(crate) fn build_string_to_sign(&self, region: &str) -> String {
        let date_time_string = self.headers.get("x-oss-date").unwrap();
        let date_string = &date_time_string[..8];

        let canonical_request = self.build_canonical_request();

        log::debug!("canonical request: \n--------\n{}\n--------", canonical_request);

        let canonical_request_hash = util::sha256(canonical_request.as_bytes());

        format!(
            "OSS4-HMAC-SHA256\n{}\n{}/{}/oss/aliyun_v4_request\n{}",
            date_time_string,
            date_string,
            region,
            hex::encode(&canonical_request_hash)
        )
    }
}
