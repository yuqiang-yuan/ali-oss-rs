use std::collections::HashMap;

use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};

use crate::common::{self, StorageClass, VersionIdOnlyOptions};
use crate::error::Error;
use crate::object_common::ObjectAcl;
use crate::request::{OssRequest, RequestMethod};
use crate::util::{validate_bucket_name, validate_meta_key, validate_object_key, validate_tag_key, validate_tag_value};
use crate::Result;

pub type PutObjectAclOptions = VersionIdOnlyOptions;
pub type GetObjectAclOptions = VersionIdOnlyOptions;

/// Options for putting symbolic link
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct PutSymlinkOptions {
    pub object_acl: Option<ObjectAcl>,
    pub storage_class: Option<StorageClass>,
    pub forbid_overwrite: Option<bool>,

    /// user defined metadata of the symbolic link whose key starts with `x-oss-meta-`
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Default)]
pub struct PutSymlinkOptionsBuilder {
    options: PutSymlinkOptions,
}

impl PutSymlinkOptionsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn object_acl(mut self, acl: ObjectAcl) -> Self {
        self.options.object_acl = Some(acl);
        self
    }

    pub fn storage_class(mut self, storage_class: StorageClass) -> Self {
        self.options.storage_class = Some(storage_class);
        self
    }

    pub fn forbid_overwrite(mut self, forbid: bool) -> Self {
        self.options.forbid_overwrite = Some(forbid);
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.metadata.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> PutSymlinkOptions {
        self.options
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct PutSymlinkResult {
    pub request_id: Option<String>,

    /// The version id of the symbol link
    pub version_id: Option<String>,
}

impl From<HashMap<String, String>> for PutSymlinkResult {
    fn from(mut value: HashMap<String, String>) -> Self {
        Self {
            request_id: value.remove("x-oss-request-id"),
            version_id: value.remove("x-oss-version-id"),
        }
    }
}

pub type GetSymlinkOptions = VersionIdOnlyOptions;

pub(crate) fn build_put_symlink_request(
    bucket_name: &str,
    symlink_object_key: &str,
    target_object_key: &str,
    options: &Option<PutSymlinkOptions>,
) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(symlink_object_key) {
        return Err(Error::Other(format!("invalid object key: {}", symlink_object_key)));
    }

    if !validate_object_key(target_object_key) {
        return Err(Error::Other(format!("invalid object key: {}", target_object_key)));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Put)
        .bucket(bucket_name)
        .object(symlink_object_key)
        .add_query("symlink", "")
        .add_header("x-oss-symlink-target", target_object_key);

    if let Some(opt) = options {
        if let Some(acl) = opt.object_acl {
            request = request.add_header("x-oss-object-acl", acl.as_str());
        }

        if let Some(sc) = opt.storage_class {
            request = request.add_header("x-oss-storage-class", sc.as_str());
        }

        if let Some(b) = opt.forbid_overwrite {
            if b {
                request = request.add_header("x-oss-forbid-overwrite", "true")
            }
        }

        if !opt.metadata.is_empty() {
            for (k, v) in &opt.metadata {
                if !validate_meta_key(k) {
                    return Err(Error::Other(format!("invalid meta key: {}", k)));
                }

                request = request.add_header(k, v);
            }
        }
    }

    Ok(request)
}

pub type PutObjectTagOptions = VersionIdOnlyOptions;
pub type GetObjectTagOptions = VersionIdOnlyOptions;
pub type DeleteObjectTagOptions = VersionIdOnlyOptions;

pub(crate) fn tags_to_xml(tags: &HashMap<String, String>) -> Result<String> {
    let mut writer = quick_xml::Writer::new(Vec::new());
    writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

    writer.write_event(Event::Start(BytesStart::new("Tagging")))?;
    writer.write_event(Event::Start(BytesStart::new("TagSet")))?;
    for (k, v) in tags {
        writer.write_event(Event::Start(BytesStart::new("Tag")))?;

        writer.write_event(Event::Start(BytesStart::new("Key")))?;
        writer.write_event(Event::Text(BytesText::new(k)))?;
        writer.write_event(Event::End(BytesEnd::new("Key")))?;

        writer.write_event(Event::Start(BytesStart::new("Value")))?;
        writer.write_event(Event::Text(BytesText::new(v)))?;
        writer.write_event(Event::End(BytesEnd::new("Value")))?;

        writer.write_event(Event::End(BytesEnd::new("Tag")))?;
    }
    writer.write_event(Event::End(BytesEnd::new("TagSet")))?;
    writer.write_event(Event::End(BytesEnd::new("Tagging")))?;

    Ok(String::from_utf8(writer.into_inner())?)
}

pub(crate) fn parse_tags_from_xml(xml: String) -> Result<HashMap<String, String>> {
    let mut reader = quick_xml::Reader::from_str(&xml);
    let mut tag_name = String::new();
    let mut key = String::new();
    let mut value = String::new();

    let mut tags = HashMap::<String, String>::new();

    loop {
        match reader.read_event()? {
            Event::Eof => break,
            Event::Start(t) => tag_name = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
            Event::Text(text) => {
                let s = text.unescape()?.trim().to_string();
                match tag_name.as_str() {
                    "Key" => key = s,
                    "Value" => value = s,
                    _ => {}
                }
            }
            Event::End(t) => {
                if t.local_name().as_ref() == b"Tag" {
                    tags.insert(key.clone(), value.clone());
                    key.clear();
                    value.clear();
                }
                tag_name.clear();
            }
            _ => {}
        }
    }

    Ok(tags)
}

pub(crate) fn build_get_object_tag_request(bucket_name: &str, object_key: &str, options: &Option<GetObjectTagOptions>) -> Result<OssRequest> {
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
        .add_query("tagging", "");

    if let Some(opt) = options {
        if let Some(s) = &opt.version_id {
            request = request.add_query("versionId", s);
        }
    }

    Ok(request)
}

pub(crate) fn build_put_object_tag_request(
    bucket_name: &str,
    object_key: &str,
    tags: &HashMap<String, String>,
    options: &Option<PutObjectTagOptions>,
) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    if tags.is_empty() {
        return Err(Error::Other("tags cannot be empty".to_string()));
    }

    for (k, v) in tags {
        if !validate_tag_key(k) {
            return Err(Error::Other(format!("invalid tag key: {}", k)));
        }

        if !validate_tag_value(v) {
            return Err(Error::Other(format!("invalid tag value: {}", v)));
        }
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Put)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("tagging", "");

    if let Some(opt) = options {
        if let Some(s) = &opt.version_id {
            request = request.add_query("versionId", s);
        }
    }

    let xml = tags_to_xml(tags)?;
    request = request.content_type(common::MIME_TYPE_XML).text_body(xml);

    Ok(request)
}

pub(crate) fn build_delete_object_tag_request(bucket_name: &str, object_key: &str, options: &Option<DeleteObjectTagOptions>) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(object_key) {
        return Err(Error::Other(format!("invalid object key: {}", object_key)));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Delete)
        .bucket(bucket_name)
        .object(object_key)
        .add_query("tagging", "");

    if let Some(opt) = options {
        if let Some(s) = &opt.version_id {
            request = request.add_query("versionId", s);
        }
    }

    Ok(request)
}

pub(crate) fn build_get_symlink_request(bucket_name: &str, symlink_object_key: &str, options: &Option<GetSymlinkOptions>) -> Result<OssRequest> {
    if !validate_bucket_name(bucket_name) {
        return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
    }

    if !validate_object_key(symlink_object_key) {
        return Err(Error::Other(format!("invalid object key: {}", symlink_object_key)));
    }

    let mut request = OssRequest::new()
        .method(RequestMethod::Get)
        .bucket(bucket_name)
        .object(symlink_object_key)
        .add_query("symlink", "");

    if let Some(opt) = options {
        if let Some(s) = &opt.version_id {
            request = request.add_query("versionId", s);
        }
    }

    Ok(request)
}

pub(crate) fn parse_object_acl_from_xml(xml: &str) -> Result<ObjectAcl> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut tag = String::new();
    let mut acl_string = String::new();

    loop {
        match reader.read_event()? {
            Event::Eof => break,
            Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
            Event::Text(text) if tag.as_str() == "Grant" => {
                acl_string = text.unescape()?.trim().to_string();
                break;
            }
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
