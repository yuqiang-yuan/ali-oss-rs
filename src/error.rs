use std::fmt::Display;

use thiserror::Error;

//
// Aliyun OSS API error response
//
// ```xml
// <?xml version="1.0" ?>
// <Error xmlns=”http://doc.oss-cn-hangzhou.aliyuncs.com”>
//   <Code>MalformedXML</Code>
//   <Message>The XML you provided was not well-formed or did not validate against our published schema.</Message>
//   <RequestId>57ABD896CCB80C366955****</RequestId>
//   <HostId>oss-cn-hangzhou.aliyuncs.com</HostId>
//   <EC>0031-00000001</EC>
//   <RecommendDoc>https://api.aliyun.com/troubleshoot?q=0031-00000001</RecommendDoc>
// </Error>
// ```

#[derive(Debug, Default)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub request_id: String,
    pub host_id: String,
    pub ec: String,
    pub recommend_doc: String,
}

impl ErrorResponse {
    pub fn from_xml(xml_content: &str) -> ClientResult<Self> {
        let mut reader = quick_xml::Reader::from_str(xml_content);
        let mut ret = Self::default();

        let mut current_tag = String::new();

        loop {
            match reader.read_event()? {
                quick_xml::events::Event::Eof => break,

                quick_xml::events::Event::Start(e) => {
                    current_tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                }

                quick_xml::events::Event::Text(t) => match current_tag.as_str() {
                    "Code" => ret.code = String::from_utf8_lossy(t.as_ref()).to_string(),
                    "Message" => ret.message = String::from_utf8_lossy(t.as_ref()).to_string(),
                    "RequestId" => ret.request_id = String::from_utf8_lossy(t.as_ref()).to_string(),
                    "HostId" => ret.host_id = String::from_utf8_lossy(t.as_ref()).to_string(),
                    "EC" => ret.ec = String::from_utf8_lossy(t.as_ref()).to_string(),
                    "RecommendDoc" => ret.recommend_doc = String::from_utf8_lossy(t.as_ref()).to_string(),
                    _ => {}
                },

                quick_xml::events::Event::End(_) => {
                    current_tag.clear();
                }

                _ => {}
            }
        }

        Ok(ret)
    }
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Code: {}, Message: {}, Request Id: {}", self.code, self.message, self.request_id)
    }
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("{0}")]
    InvalidHeaderName(#[from] reqwest::header::InvalidHeaderName),

    #[error("{0}")]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),

    #[error("{0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("{0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("{0}")]
    XmlParseError(#[from] quick_xml::Error),

    #[error("{0}")]
    ApiError(Box<ErrorResponse>),

    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("{0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("{0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("{0}")]
    Error(String),
}

pub type ClientResult<T> = Result<T, ClientError>;
