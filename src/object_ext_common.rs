use quick_xml::events::Event;

use crate::error::Error;
use crate::object_common::ObjectAcl;
use crate::request::{OssRequest, RequestMethod};
use crate::util::{validate_bucket_name, validate_object_key};
use crate::Result;

/// Options for putting object acl
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct PutObjectAclOptions {
    pub version_id: Option<String>
}

/// Options for getting object acl
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct GetObjectAclOptions {
    pub version_id: Option<String>
}

pub(crate) fn parse_objcect_acl_from_xml(xml: &str) -> Result<ObjectAcl> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut tag = String::new();
    let mut acl_string = String::new();

    loop {
        match reader.read_event()? {
            Event::Eof => break,
            Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
            Event::Text(text) if tag.as_str() == "Grant" => acl_string = text.unescape()?.trim().to_string(),
            Event::End(_) => tag.clear(),
            _ => {}
        }
    }

    ObjectAcl::try_from(acl_string)
}

pub(crate) fn build_get_object_acl_request(bucket_name: &str, object_key: &str, options: &Option<GetObjectAclOptions>) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Get)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("acl", "");

    if let Some(options) = options {
        if let Some(s) = &options.version_id {
            request = request.add_query("versionId", s);
        }
    }

    Ok(request)
}

pub(crate) fn build_put_object_acl_request(bucket_name: &str, object_key: &str, acl: ObjectAcl, options: &Option<GetObjectAclOptions>) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Put)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("acl", "")
        .add_header("x-oss-object-acl", acl.as_str());

    if let Some(options) = options {
        if let Some(s) = &options.version_id {
            request = request.add_query("versionId", s);
        }
    }

    Ok(request)
}
