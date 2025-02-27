use std::collections::HashMap;

use crate::{
    common::{StorageClass, VersionIdOnlyOptions},
    error::Error,
    object_common::ObjectAcl,
    request::{OssRequest, RequestMethod},
    util::{validate_bucket_name, validate_meta_key, validate_object_key},
    Result,
};

pub type GetSymlinkOptions = VersionIdOnlyOptions;

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
