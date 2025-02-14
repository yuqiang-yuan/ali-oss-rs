use std::str::FromStr;

use log::{debug, error};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Url,
};

use crate::{
    error::{ClientError, ClientResult, ErrorResponse},
    request::RequestMethod,
    util::{self, hmac_sha256},
};

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

pub struct Client {
    pub access_key_id: String,
    access_key_secret: String,
    pub region: String,
    pub endpoint: String,
    pub scheme: String,

    http_client: reqwest::Client,

    #[cfg(feature = "blocking")]
    http_client_sync: reqwest::blocking::Client,
}

impl Client {
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
            scheme,
            http_client: reqwest::Client::new(),

            #[cfg(feature = "blocking")]
            http_client_sync: reqwest::blocking::Client::new(),
        }
    }

    /// 官方文档：https://help.aliyun.com/zh/oss/developer-reference/recommend-to-use-signature-version-4?spm=a2c4g.11186623.help-menu-31815.d_5_1_0_1_0_0.5dbf37b36yQEjT#c33cf04dcb1c3
    ///
    /// - 如果请求的 URI 中既包含 Bucket 也包含 Object，则 Canonical URI 填写示例为: `/examplebucket/exampleobject`
    /// - 如果请求的 URI 中只包含 Bucket 不包含 Object，则 Canonical URI 填写示例为： `/examplebucket/`
    /// - 如果请求的 URI 中不包含 Bucket 且不包含 Object，则 Canonical URI 填写示例为： `/`
    ///
    fn build_canonical_uri(&self, request_builder: &crate::request::RequestBuilder) -> String {
        match (request_builder.bucket_name.is_empty(), request_builder.object_key.is_empty()) {
            (true, true) => "/".to_string(),
            (true, false) => format!("/{}/", urlencoding::encode(&request_builder.bucket_name)),
            (_, _) => {
                format!(
                    "/{}/{}",
                    urlencoding::encode(&request_builder.bucket_name),
                    request_builder
                        .object_key
                        .split("/")
                        .filter(|s| !s.is_empty())
                        .map(|s| urlencoding::encode(s))
                        .collect::<Vec<_>>()
                        .join("/")
                )
            }
        }
    }

    /// Build the uri part of real http request
    fn build_request_uri(&self, request_builder: &crate::request::RequestBuilder) -> String {
        if request_builder.object_key.is_empty() {
            return "/".to_string();
        }

        request_builder.object_key.split("/").filter(|s| !s.is_empty()).collect::<Vec<_>>().join("/")
    }

    /// 按 QueryString 的 key 进行排序。
    /// - 先编码，再排序
    /// - 如果有多个相同的 key，按照原来添加的顺序放置即可
    /// - 中间使用 `&`进行连接。
    /// - 只有 key 没有 value 的情况下，只添加 key即可
    /// - 如果没有 QueryString，则只需要放置空字符串 “”，末尾仍然需要有换行符。
    ///
    fn build_canonical_query_string(&self, request_builder: &crate::request::RequestBuilder) -> String {
        if request_builder.query.is_empty() {
            return "".to_string();
        }

        let mut pairs = request_builder
            .query
            .iter()
            .map(|(k, v)| (urlencoding::encode(k), urlencoding::encode(v)))
            .collect::<Vec<(_, _)>>();

        pairs.sort_by(|e1, e2| e1.0.cmp(&e2.0));

        pairs.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&")
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
    fn build_canonical_headers(&self, request_builder: &crate::request::RequestBuilder) -> String {
        if request_builder.headers.is_empty() {
            return "\n".to_string();
        }

        let mut pairs = request_builder
            .headers
            .iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .filter(|(k, _)| k == "content-type" || k == "content-md5" || k.starts_with("x-oss-") || request_builder.additional_headers.contains(k))
            .collect::<Vec<_>>();

        pairs.sort_by(|a, b| a.0.cmp(&b.0));

        let s = pairs.iter().map(|(k, v)| format!("{}:{}", k, v.trim())).collect::<Vec<_>>().join("\n");

        // 不知道为什么这里要多一个空行
        format!("{}\n", s)
    }

    fn build_additional_headers(&self, request_builder: &crate::request::RequestBuilder) -> String {
        if request_builder.additional_headers.is_empty() {
            return "".to_string();
        }

        let mut keys = request_builder.additional_headers.iter().map(|k| k.to_lowercase()).collect::<Vec<_>>();

        keys.sort();

        keys.join(";")
    }

    fn calculate_signature<S1, S2>(&self, string_to_sign: S1, date_string: S2) -> String
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let key_string = format!("aliyun_v4{}", &self.access_key_secret);

        let date_key = hmac_sha256(key_string.as_bytes(), date_string.as_ref().as_bytes());
        let date_region_key = hmac_sha256(&date_key, self.region.as_bytes());
        let date_region_service_key = hmac_sha256(&date_region_key, "oss".as_bytes());
        let signing_key = hmac_sha256(&date_region_service_key, "aliyun_v4_request".as_bytes());

        hex::encode(hmac_sha256(&signing_key, string_to_sign.as_ref().as_bytes()))
    }

    fn prepare_request(&self, request_builder: &mut crate::request::RequestBuilder) {
        let date_time_string = util::get_iso8601_date_time_string();
        let date_string = &date_time_string[..8];

        request_builder.headers.insert("x-oss-date".to_string(), date_time_string.clone());

        let canonical_uri = self.build_canonical_uri(request_builder);
        let canonical_query = self.build_canonical_query_string(request_builder);
        let canonical_headers = self.build_canonical_headers(request_builder);
        let additional_headers = self.build_additional_headers(request_builder);
        let method = request_builder.method.to_string();

        let canonical_request = format!("{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{additional_headers}\nUNSIGNED-PAYLOAD");

        debug!("canonical request: \n--------\n{}\n--------", canonical_request);

        let canonical_request_hash = util::sha256(canonical_request.as_bytes());

        let string_to_sign = format!(
            "OSS4-HMAC-SHA256\n{}\n{}/{}/oss/aliyun_v4_request\n{}",
            date_time_string,
            date_string,
            &self.region,
            hex::encode(&canonical_request_hash)
        );

        debug!("string to sign: \n--------\n{}\n--------", string_to_sign);

        let sig = self.calculate_signature(string_to_sign, date_string);

        debug!("signature: {}", sig);

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

        debug!("authorization string: {}", auth_string);

        request_builder.headers_mut().insert("Authorization".to_string(), auth_string);
        request_builder.headers_mut().insert("Date".to_string(), util::get_http_date());
    }

    /// Some of the strings are used multiple times,
    /// So I put them in this method to prevent re-generating
    /// and better debuging output.
    /// And add some default headers to the request builder.
    pub(crate) async fn do_request(&self, mut request_builder: crate::request::RequestBuilder) -> ClientResult<String> {
        self.prepare_request(&mut request_builder);

        let mut header_map = HeaderMap::new();

        for (k, v) in request_builder.headers.iter() {
            header_map.insert(HeaderName::from_str(k)?, HeaderValue::from_str(v)?);
        }

        let uri = self.build_request_uri(&request_builder);
        let query_string = self.build_canonical_query_string(&request_builder);

        let domain_name = if request_builder.bucket_name.is_empty() {
            format!("{}://{}{}", self.scheme, self.endpoint, uri)
        } else {
            format!("{}://{}.{}{}", request_builder.bucket_name, self.scheme, self.endpoint, uri)
        };

        let full_url = if query_string.is_empty() {
            domain_name
        } else {
            format!("{}?{}", domain_name, query_string)
        };

        debug!("full url: {}", full_url);

        let req_builder = self
            .http_client
            .request(request_builder.method.into(), Url::parse(&full_url)?)
            .headers(header_map);

        let req = req_builder.build()?;

        let response = self.http_client.execute(req).await?;

        if response.status() != reqwest::StatusCode::OK {
            let status = response.status();

            match response.text().await {
                Ok(s) => {
                    error!("{}", s);
                    if s.is_empty() {
                        Err(ClientError::Error(format!("API call failed with status code: {}", status.as_str())))
                    } else {
                        let error_response = ErrorResponse::from_xml(&s)?;
                        Err(ClientError::ApiError(Box::new(error_response)))
                    }
                },
                Err(_) => {
                    Err(ClientError::Error(format!("API call failed with status code: {}", status.as_str())))
                }
            }
        } else {
            let s = response.text().await?;
            debug!("\n-------- \n{}--------", s);
            Ok(s)
        }
    }

    /// Do request synchronized
    #[cfg(feature = "blocking")]
    pub(crate) fn do_request_sync(&self, mut request_builder: crate::request::RequestBuilder) -> ClientResult<String> {
        self.prepare_request(&mut request_builder);

        let mut header_map = HeaderMap::new();

        for (k, v) in request_builder.headers.iter() {
            header_map.insert(HeaderName::from_str(k)?, HeaderValue::from_str(v)?);
        }

        let uri = self.build_request_uri(&request_builder);
        let query_string = self.build_canonical_query_string(&request_builder);

        let domain_name = if request_builder.bucket_name.is_empty() {
            format!("{}://{}{}", self.scheme, self.endpoint, uri)
        } else {
            format!("{}://{}.{}{}", request_builder.bucket_name, self.scheme, self.endpoint, uri)
        };

        let full_url = if query_string.is_empty() {
            domain_name
        } else {
            format!("{}?{}", domain_name, query_string)
        };

        let req_builder = self
            .http_client_sync
            .request(request_builder.method.into(), Url::parse(&full_url)?)
            .headers(header_map);

        let req = req_builder.build()?;

        let response = self.http_client_sync.execute(req)?;

        if response.status() != reqwest::StatusCode::OK {
            let status = response.status();

            match response.text() {
                Ok(s) => {
                    error!("{}", s);

                    if s.is_empty() {
                        Err(ClientError::Error(format!("API call failed with status code: {}", status.as_str())))
                    } else {
                        let error_response = ErrorResponse::from_xml(&s)?;
                        Err(ClientError::ApiError(Box::new(error_response)))
                    }
                },
                Err(_) => {
                    Err(ClientError::Error(format!("API call failed with status code: {}", status.as_str())))
                }
            }
        } else {
            let s = response.text()?;
            debug!("\n-------- \n{}--------", s);
            Ok(s)
        }

    }
}
