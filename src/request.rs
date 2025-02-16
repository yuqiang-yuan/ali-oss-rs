use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    path::PathBuf,
};

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
        Self {
            bucket_name: "".to_string(),
            object_key: "".to_string(),
            method: RequestMethod::Get,
            headers: HashMap::from([
                ("x-sdk-client".to_string(), "ali-oss-rs/0.1.0".to_string()),
                ("x-oss-content-sha256".to_string(), "UNSIGNED-PAYLOAD".to_string()),
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

    pub fn bucket<S: AsRef<str>>(mut self, bucket_name: S) -> Self {
        self.bucket_name = bucket_name.as_ref().to_string();
        self
    }

    pub fn object<S: AsRef<str>>(mut self, object_key: S) -> Self {
        self.object_key = object_key.as_ref().to_string();
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

    pub fn text_body<S: Into<String>>(self, text: S) -> Self {
        self.body(RequestBody::Text(text.into()))
    }

    pub fn bytes_body<B: Into<Vec<u8>>>(self, bytes: B) -> Self {
        self.body(RequestBody::Bytes(bytes.into()))
    }

    pub fn file_body<P: Into<PathBuf>>(self, path: P) -> Self {
        self.body(RequestBody::File(path.into()))
    }

    pub fn headers_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.headers
    }

    pub fn additional_headers_mut(&mut self) -> &mut HashSet<String> {
        &mut self.additional_headers
    }

    pub fn query_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.query
    }
}
