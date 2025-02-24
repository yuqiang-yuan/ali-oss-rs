#![doc = include_str!("../README.md")]
pub mod bucket;
pub mod bucket_common;
pub mod common;
pub mod error;
pub mod multipart;
pub mod multipart_common;
pub mod object;
pub mod object_common;
pub mod presign;
pub mod presign_common;
pub mod request;

#[cfg(feature = "blocking")]
pub mod blocking;

mod util;

use std::{collections::HashMap, pin::Pin, str::FromStr};

use async_trait::async_trait;
use bytes::Bytes;
use error::{Error, ErrorResponse, Result};
use futures::{Stream, StreamExt};
use request::RequestBody;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Body,
};

pub use serde;
pub use serde_json;

use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};
use url::Url;
use util::{get_region_from_endpoint, hmac_sha256};

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
    /// ## Error:
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
            endpoint,
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
    /// and better debuging output.
    /// And add some default headers to the request builder.
    async fn do_request<T>(&self, mut request_builder: crate::request::RequestBuilder) -> Result<(HashMap<String, String>, T)>
    where
        T: FromResponse,
    {
        if let Some(s) = &self.sts_token {
            request_builder.headers_mut().insert("x-oss-security-token".to_string(), s.to_string());
        }

        let date_time_string = request_builder.headers.get("x-oss-date").unwrap();
        let date_string = &date_time_string[..8];

        let additional_headers = request_builder.build_additional_headers();

        let string_to_sign = request_builder.build_string_to_sign(&self.region);

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

        for (k, v) in request_builder.headers.iter() {
            header_map.insert(HeaderName::from_str(k)?, HeaderValue::from_str(v)?);
        }

        let http_date = util::get_http_date();

        header_map.insert(HeaderName::from_static("authorization"), HeaderValue::from_str(&auth_string)?);
        header_map.insert(HeaderName::from_static("date"), HeaderValue::from_str(&http_date)?);

        let uri = request_builder.build_request_uri();
        let query_string = request_builder.build_canonical_query_string();

        let domain_name = if request_builder.bucket_name.is_empty() {
            format!("{}://{}{}", self.scheme, self.endpoint, uri)
        } else {
            format!("{}://{}.{}{}", self.scheme, request_builder.bucket_name, self.endpoint, uri)
        };

        let full_url = if query_string.is_empty() {
            domain_name
        } else {
            format!("{}?{}", domain_name, query_string)
        };

        log::debug!("full url: {}", full_url);

        let mut req_builder = self
            .http_client
            .request(request_builder.method.into(), Url::parse(&full_url)?)
            .headers(header_map);

        // 根据 body 类型设置请求体
        req_builder = match request_builder.body {
            RequestBody::Empty => req_builder,
            RequestBody::Text(text) => req_builder.body(text),
            RequestBody::Bytes(bytes) => req_builder.body(bytes),
            RequestBody::File(path, range) => {
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
        };

        let req = req_builder.build()?;

        for (k, v) in req.headers() {
            log::debug!(">> headers: {}: {}", k, v.to_str().unwrap_or_default());
        }

        let response = self.http_client.execute(req).await?;

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
