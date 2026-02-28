//! Common types: structs and enumerations
use std::{collections::HashMap, fmt::Display};

use quick_xml::events::Event;

use crate::error::Error;
use crate::Result;

pub const VERSION: &str = "0.2.5";

pub const MIME_TYPE_XML: &str = "application/xml";
pub const DELETE_MULTIPLE_OBJECTS_LIMIT: usize = 1000;
pub const SIGNATURE_VERSION: &str = "OSS4-HMAC-SHA256";
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const MIN_BUCKET_NAME_LENGTH: usize = 3;
pub const MAX_BUCKET_NAME_LENGTH: usize = 63;
pub const MAX_LIST_OBJECTS_LIMIT: u32 = 1000;

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct Owner {
    pub id: String,
    pub display_name: String,
}

impl Owner {
    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> Result<Self> {
        let mut current_tag = "".to_string();
        let mut owner = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,

                Event::Start(e) => {
                    current_tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                }

                Event::Text(e) => match current_tag.as_str() {
                    "ID" => owner.id = e.unescape()?.trim().to_string(),
                    "DisplayName" => owner.display_name = e.unescape()?.trim().to_string(),
                    _ => {}
                },

                Event::End(e) => {
                    current_tag.clear();
                    if e.local_name().as_ref() == b"Owner" {
                        break;
                    }
                }

                _ => {}
            }
        }

        Ok(owner)
    }
}

/// Represents the storage class for an object in Aliyun OSS.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum StorageClass {
    /// 标准存储
    #[default]
    #[cfg_attr(feature = "serde-support", serde(rename = "Standard"))]
    Standard,

    /// 低频访问
    #[cfg_attr(feature = "serde-support", serde(rename = "IA"))]
    IA,

    /// 归档
    #[cfg_attr(feature = "serde-support", serde(rename = "Archive"))]
    Archive,

    /// 冷归档
    #[cfg_attr(feature = "serde-support", serde(rename = "ColdArchive"))]
    ColdArchive,

    /// 深度冷归档
    #[cfg_attr(feature = "serde-support", serde(rename = "DeepColdArchive"))]
    DeepColdArchive,
}

impl StorageClass {
    pub fn as_str(&self) -> &str {
        match self {
            StorageClass::Standard => "Standard",
            StorageClass::IA => "IA",
            StorageClass::Archive => "Archive",
            StorageClass::ColdArchive => "ColdArchive",
            StorageClass::DeepColdArchive => "DeepColdArchive",
        }
    }
}

impl Display for StorageClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageClass::Standard => write!(f, "Standard"),
            StorageClass::IA => write!(f, "IA"),
            StorageClass::Archive => write!(f, "Archive"),
            StorageClass::ColdArchive => write!(f, "ColdArchive"),
            StorageClass::DeepColdArchive => write!(f, "DeepColdArchive"),
        }
    }
}

impl AsRef<str> for StorageClass {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<&str> for StorageClass {
    type Error = crate::error::Error;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        match s {
            "Standard" => Ok(StorageClass::Standard),
            "IA" => Ok(StorageClass::IA),
            "Archive" => Ok(StorageClass::Archive),
            "ColdArchive" => Ok(StorageClass::ColdArchive),
            "DeepColdArchive" => Ok(StorageClass::DeepColdArchive),
            _ => Err(Error::Other(format!("Invalid StorageClass value: {}", s))),
        }
    }
}

impl TryFrom<String> for StorageClass {
    type Error = crate::error::Error;

    fn try_from(s: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

impl TryFrom<&String> for StorageClass {
    type Error = crate::error::Error;

    fn try_from(s: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum DataRedundancyType {
    #[default]
    #[cfg_attr(feature = "serde-support", serde(rename = "LRS"))]
    LRS,

    #[cfg_attr(feature = "serde-support", serde(rename = "ZRS"))]
    ZRS,
}

impl DataRedundancyType {
    pub fn as_str(&self) -> &str {
        match self {
            DataRedundancyType::LRS => "LRS",
            DataRedundancyType::ZRS => "ZRS",
        }
    }
}

impl Display for DataRedundancyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataRedundancyType::LRS => write!(f, "LRS"),
            DataRedundancyType::ZRS => write!(f, "ZRS"),
        }
    }
}

impl AsRef<str> for DataRedundancyType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<&str> for DataRedundancyType {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "LRS" => Ok(DataRedundancyType::LRS),
            "ZRS" => Ok(DataRedundancyType::ZRS),
            _ => Err(Error::Other(format!("Invalid DataRedundancyType value: {}", value))),
        }
    }
}

impl TryFrom<String> for DataRedundancyType {
    type Error = crate::error::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for DataRedundancyType {
    type Error = crate::error::Error;

    fn try_from(value: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

pub struct KvPair {
    pub key: String,
    pub value: String,
}

///
/// Many aliyun ON/OFF settings are represented as strings.
///
/// - `Enabled`
/// - `Disabled`
///
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum OnOff {
    #[cfg_attr(feature = "serde-support", serde(rename = "Enabled"))]
    Enabled,

    #[default]
    #[cfg_attr(feature = "serde-support", serde(rename = "Disabled"))]
    Disabled,
}

impl OnOff {
    pub fn as_str(&self) -> &str {
        match self {
            OnOff::Enabled => "Enabled",
            OnOff::Disabled => "Disabled",
        }
    }
}

impl AsRef<str> for OnOff {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for OnOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for OnOff {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "Enabled" => Ok(OnOff::Enabled),
            "Disabled" => Ok(OnOff::Disabled),
            _ => Err(Error::Other(format!("Invalid CrossRegionReplication value: {}", value))),
        }
    }
}

impl TryFrom<String> for OnOff {
    type Error = crate::error::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for OnOff {
    type Error = crate::error::Error;

    fn try_from(value: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

/// Type aliases for some On/Off types
pub type CrossRegionReplication = OnOff;
pub type TransferAcceleration = OnOff;
pub type AccessMonitor = OnOff;

///
/// Versioning enumeration
///
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum Versioning {
    #[cfg_attr(feature = "serde-support", serde(rename = "Enabled"))]
    Enabled,

    #[default]
    #[cfg_attr(feature = "serde-support", serde(rename = "Suspended"))]
    Suspended,
}

impl Versioning {
    pub fn as_str(&self) -> &str {
        match self {
            Versioning::Enabled => "Enabled",
            Versioning::Suspended => "Disabled",
        }
    }
}

impl AsRef<str> for Versioning {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for Versioning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for Versioning {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "Enabled" => Ok(Versioning::Enabled),
            "Disabled" => Ok(Versioning::Suspended),
            _ => Err(Error::Other(format!("Invalid Versioning value: {}", value))),
        }
    }
}

impl TryFrom<String> for Versioning {
    type Error = crate::error::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for Versioning {
    type Error = crate::error::Error;

    fn try_from(value: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum ServerSideEncryptionAlgorithm {
    #[default]
    #[cfg_attr(feature = "serde-support", serde(rename = "KMS"))]
    KMS,

    #[cfg_attr(feature = "serde-support", serde(rename = "AES256"))]
    AES256,

    #[cfg_attr(feature = "serde-support", serde(rename = "SM4"))]
    SM4,
}

impl ServerSideEncryptionAlgorithm {
    pub fn as_str(&self) -> &str {
        match self {
            ServerSideEncryptionAlgorithm::KMS => "KMS",
            ServerSideEncryptionAlgorithm::AES256 => "AES256",
            ServerSideEncryptionAlgorithm::SM4 => "SM4",
        }
    }
}

impl AsRef<str> for ServerSideEncryptionAlgorithm {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for ServerSideEncryptionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for ServerSideEncryptionAlgorithm {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "KMS" => Ok(ServerSideEncryptionAlgorithm::KMS),
            "AES256" => Ok(ServerSideEncryptionAlgorithm::AES256),
            "SM4" => Ok(ServerSideEncryptionAlgorithm::SM4),
            _ => Err(Error::Other(format!("Invalid ServerSideEncryptionAlgorithm value: {}", value))),
        }
    }
}

impl TryFrom<String> for ServerSideEncryptionAlgorithm {
    type Error = crate::error::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for ServerSideEncryptionAlgorithm {
    type Error = crate::error::Error;

    fn try_from(value: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct ServerSideEncryptionRule {
    pub sse_algorithm: ServerSideEncryptionAlgorithm,

    /// Only present when sse_algorithm is `KMS`
    pub kms_master_key_id: Option<String>,
    pub kms_data_encryption: Option<String>,
}

/// Object type enumeration
#[derive(Debug, Clone, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum ObjectType {
    /// 通过简单上传生成的 Object
    #[default]
    #[cfg_attr(feature = "serde-support", serde(rename = "Normal"))]
    Normal,

    /// 通过分片上传生成的 Object
    #[cfg_attr(feature = "serde-support", serde(rename = "Multipart"))]
    Multipart,

    /// 通过追加上传生成的 Object
    #[cfg_attr(feature = "serde-support", serde(rename = "Appendable"))]
    Appendable,

    /// 符号链接
    #[cfg_attr(feature = "serde-support", serde(rename = "Symlink"))]
    Symlink,
}

impl ObjectType {
    pub fn as_str(&self) -> &str {
        match self {
            ObjectType::Normal => "Normal",
            ObjectType::Multipart => "Multipart",
            ObjectType::Appendable => "Appendable",
            ObjectType::Symlink => "Symlink",
        }
    }
}

impl AsRef<str> for ObjectType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<&str> for ObjectType {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "Normal" => Ok(ObjectType::Normal),
            "Multipart" => Ok(ObjectType::Multipart),
            "Appendable" => Ok(ObjectType::Appendable),
            "Symlink" => Ok(ObjectType::Symlink),
            _ => Err(Error::Other(format!("Invalid ObjectType value: {}", value))),
        }
    }
}

impl TryFrom<String> for ObjectType {
    type Error = crate::error::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for ObjectType {
    type Error = crate::error::Error;

    fn try_from(value: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

/// How to apply metadata rule while coping object
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum MetadataDirective {
    /// 复制源 Object 的元数据到目标 Object。
    /// OSS 不会复制源 Object 的 `x-oss-server-side-encryption` 属性配置到目标 Object。
    /// 目标 Object 的服务器端加密编码方式取决于当前拷贝操作是否指定了 `x-oss-server-side-encryption`。
    #[default]
    #[cfg_attr(feature = "serde-support", serde(rename = "COPY"))]
    Copy,

    /// 忽略源 Object 的元数据，直接采用请求中指定的元数据
    #[cfg_attr(feature = "serde-support", serde(rename = "REPLACE"))]
    Replace,
}

impl MetadataDirective {
    pub fn as_str(&self) -> &str {
        match self {
            MetadataDirective::Copy => "COPY",
            MetadataDirective::Replace => "REPLACE",
        }
    }
}

impl AsRef<str> for MetadataDirective {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for MetadataDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataDirective::Copy => write!(f, "COPY"),
            MetadataDirective::Replace => write!(f, "REPLACE"),
        }
    }
}

impl TryFrom<&str> for MetadataDirective {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "COPY" => Ok(MetadataDirective::Copy),
            "REPLACE" => Ok(MetadataDirective::Replace),
            _ => Err(Error::Other(format!("Invalid MetadataDirective value: {}", value))),
        }
    }
}

impl TryFrom<String> for MetadataDirective {
    type Error = crate::error::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for MetadataDirective {
    type Error = crate::error::Error;

    fn try_from(value: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

/// How to apply taggings rule while coping object
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
pub enum TagDirective {
    /// 复制源 Object 的标签数据到目标 Object。
    #[default]
    #[cfg_attr(feature = "serde-support", serde(rename = "Copy"))]
    Copy,

    /// 忽略源 Object 的对象标签，直接采用请求中指定的对象标签。
    #[cfg_attr(feature = "serde-support", serde(rename = "Replace"))]
    Replace,
}

impl TagDirective {
    pub fn as_str(&self) -> &str {
        match self {
            TagDirective::Copy => "Copy",
            TagDirective::Replace => "Replace",
        }
    }
}

impl AsRef<str> for TagDirective {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for TagDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TagDirective::Copy => write!(f, "Copy"),
            TagDirective::Replace => write!(f, "Replace"),
        }
    }
}

impl TryFrom<&str> for TagDirective {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "Copy" => Ok(TagDirective::Copy),
            "Replace" => Ok(TagDirective::Replace),
            _ => Err(Error::Other(format!("Invalid MetadataDirective value: {}", value))),
        }
    }
}

impl TryFrom<String> for TagDirective {
    type Error = crate::error::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for TagDirective {
    type Error = crate::error::Error;

    fn try_from(value: &String) -> std::result::Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct VersionIdOnlyOptions {
    pub version_id: Option<String>,
}

/// Build tags string
pub(crate) fn build_tag_string(tags: &HashMap<String, String>) -> String {
    tags.iter()
        .map(|(k, v)| {
            if v.is_empty() {
                urlencoding::encode(k).to_string()
            } else {
                format!("{}={}", urlencoding::encode(k), urlencoding::encode(v))
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}
