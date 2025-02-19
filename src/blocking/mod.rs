use std::{collections::HashMap, fs::File, path::Path, str::FromStr};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use url::Url;

use crate::{
    error::{ClientError, ClientResult, ErrorResponse},
    hmac_sha256, util, RequestBody,
};

pub mod bucket;
pub mod object;

pub struct Client {
    access_key_id: String,
    access_key_secret: String,
    pub region: String,
    pub endpoint: String,
    pub scheme: String,
    blocking_http_client: reqwest::blocking::Client,
}

impl Client {
    /// Creates a new client from environment variables.
    ///
    /// - `ALI_ACCESS_KEY_ID` The access key id
    /// - `ALI_ACCESS_KEY_SECRET` The access key secret
    /// - `ALI_OSS_ENDPOINT` The endpoint of the OSS service. e.g. `oss-cn-hangzhou.aliyuncs.com`. Or, you can write full URL `http://oss-cn-hangzhou.aliyuncs.com` or `https://oss-cn-hangzhou.aliyuncs.com` with scheme `http` or `https`.
    /// - `ALI_OSS_REGION` Optional. The region of the OSS service. If not present, It will be inferred from the `ALI_OSS_ENDPOINT` env.
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
            blocking_http_client: reqwest::blocking::Client::new(),
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
    pub(crate) fn do_request<T>(&self, request_builder: crate::request::RequestBuilder) -> ClientResult<(HashMap<String, String>, T)>
    where
        T: FromResponse,
    {
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
            .blocking_http_client
            .request(request_builder.method.into(), Url::parse(&full_url)?)
            .headers(header_map);

        // 根据 body 类型设置请求体
        req_builder = match request_builder.body {
            RequestBody::Empty => req_builder,
            RequestBody::Text(text) => req_builder.body(text),
            RequestBody::Bytes(bytes) => req_builder.body(bytes),
            RequestBody::File(path) => {
                let file = File::open(path)?;
                req_builder.body(file)
            }
        };

        let req = req_builder.build()?;

        let response = self.blocking_http_client.execute(req)?;

        let mut response_headers = HashMap::new();

        // 阿里云 OSS API 中的响应头的值都是可表示的字符串
        for (key, value) in response.headers() {
            log::debug!("<< headers: {}: {}", key, value.to_str().unwrap_or("ERROR-PARSE-HEADER-VALUE"));
            response_headers.insert(key.to_string(), value.to_str().unwrap_or("").to_string());
        }

        if !response.status().is_success() {
            let status = response.status();

            match response.text() {
                Ok(s) => {
                    log::error!("{}", s);
                    if s.is_empty() {
                        log::error!("call api failed with status: \"{}\". full url: {}", status, full_url);
                        Err(ClientError::StatusError(status))
                    } else {
                        let error_response = ErrorResponse::from_xml(&s)?;
                        Err(ClientError::ApiError(Box::new(error_response)))
                    }
                }
                Err(_) => {
                    log::error!("call api failed with status: \"{}\". full url: {}", status, full_url);
                    Err(ClientError::StatusError(status))
                }
            }
        } else {
            Ok((response_headers, T::from_response(response)?))
        }
    }
}

pub(crate) trait FromResponse: Sized {
    fn from_response(response: reqwest::blocking::Response) -> ClientResult<Self>;
}

impl FromResponse for String {
    fn from_response(response: reqwest::blocking::Response) -> ClientResult<Self> {
        let text = response.text()?;
        Ok(text)
    }
}

impl FromResponse for Vec<u8> {
    fn from_response(response: reqwest::blocking::Response) -> ClientResult<Self> {
        let bytes = response.bytes()?;
        Ok(bytes.to_vec())
    }
}

/// This is a wrapper around `reqwest::blocking::Response` that provides a convenient way to access the response body as bytes.
pub(crate) struct BytesBody(reqwest::blocking::Response);

impl BytesBody {
    pub fn save_to_file<P: AsRef<Path>>(&mut self, path: P) -> ClientResult<()> {
        let mut file = File::create(path)?;
        self.0.copy_to(&mut file)?;
        Ok(())
    }
}

impl FromResponse for BytesBody {
    fn from_response(response: reqwest::blocking::Response) -> ClientResult<Self> {
        Ok(Self(response))
    }
}

impl FromResponse for () {
    fn from_response(_: reqwest::blocking::Response) -> ClientResult<Self> {
        Ok(())
    }
}
