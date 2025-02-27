use std::collections::HashMap;

use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};

use crate::common::VersionIdOnlyOptions;
use crate::error::Error;
use crate::request::{OssRequest, RequestMethod};
use crate::util::{validate_bucket_name, validate_object_key, validate_tag_key, validate_tag_value};
use crate::{common, Result};

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
