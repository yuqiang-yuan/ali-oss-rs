//! Common types: structs and enumerations
use std::fmt::Display;

use quick_xml::events::Event;

use crate::error::{ClientError, ClientResult};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct Owner {
    pub id: String,
    pub display_name: String,
}

impl Owner {
    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> ClientResult<Self> {
        let mut current_tag = "".to_string();
        let mut owner = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,

                Event::Start(e) => {
                    current_tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                }

                Event::Text(e) => match current_tag.as_str() {
                    "ID" => owner.id = e.unescape()?.to_string(),
                    "DisplayName" => owner.display_name = e.unescape()?.to_string(),
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

///
/// Represents the access control list (ACL) for an object in Aliyun OSS.
///
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Acl {
    #[cfg_attr(feature = "serde", serde(rename = "public-read-write"))]
    PublicReadWrite,

    #[cfg_attr(feature = "serde", serde(rename = "public-read"))]
    PublicRead,

    #[default]
    #[cfg_attr(feature = "serde", serde(rename = "private"))]
    Private,
}

impl Acl {
    pub fn as_str(&self) -> &str {
        match self {
            Acl::PublicReadWrite => "public-read-write",
            Acl::PublicRead => "public-read",
            Acl::Private => "private",
        }
    }
}

impl Display for Acl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Acl::PublicReadWrite => write!(f, "public-read-write"),
            Acl::PublicRead => write!(f, "public-read"),
            Acl::Private => write!(f, "private"),
        }
    }
}

impl AsRef<str> for Acl {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<&str> for Acl {
    type Error = ClientError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "public-read-write" => Ok(Acl::PublicReadWrite),
            "public-read" => Ok(Acl::PublicRead),
            "private" => Ok(Acl::Private),
            _ => Err(ClientError::Error(format!("Invalid ACL value: {}", s))),
        }
    }
}

impl TryFrom<String> for Acl {
    type Error = ClientError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

impl TryFrom<&String> for Acl {
    type Error = ClientError;

    fn try_from(s: &String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

///
/// Represents the storage class for an object in Aliyun OSS.
///
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum StorageClass {
    #[default]
    #[cfg_attr(feature = "serde", serde(rename = "Standard"))]
    Standard,

    #[cfg_attr(feature = "serde", serde(rename = "IA"))]
    IA,

    #[cfg_attr(feature = "serde", serde(rename = "Archive"))]
    Archive,

    #[cfg_attr(feature = "serde", serde(rename = "ColdArchive"))]
    ColdArchive,

    #[cfg_attr(feature = "serde", serde(rename = "DeepColdArchive"))]
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
    type Error = ClientError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "Standard" => Ok(StorageClass::Standard),
            "IA" => Ok(StorageClass::IA),
            "Archive" => Ok(StorageClass::Archive),
            "ColdArchive" => Ok(StorageClass::ColdArchive),
            "DeepColdArchive" => Ok(StorageClass::DeepColdArchive),
            _ => Err(ClientError::Error(format!("Invalid StorageClass value: {}", s))),
        }
    }
}

impl TryFrom<String> for StorageClass {
    type Error = ClientError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

impl TryFrom<&String> for StorageClass {
    type Error = ClientError;

    fn try_from(s: &String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DataRedundancyType {
    #[default]
    #[cfg_attr(feature = "serde", serde(rename = "LRS"))]
    LRS,

    #[cfg_attr(feature = "serde", serde(rename = "ZRS"))]
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
    type Error = ClientError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "LRS" => Ok(DataRedundancyType::LRS),
            "ZRS" => Ok(DataRedundancyType::ZRS),
            _ => Err(ClientError::Error(format!("Invalid DataRedundancyType value: {}", value))),
        }
    }
}

impl TryFrom<String> for DataRedundancyType {
    type Error = ClientError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for DataRedundancyType {
    type Error = ClientError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OnOff {
    #[cfg_attr(feature = "serde", serde(rename = "Enabled"))]
    Enabled,

    #[default]
    #[cfg_attr(feature = "serde", serde(rename = "Disabled"))]
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

impl Display for OnOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for OnOff {
    type Error = ClientError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Enabled" => Ok(OnOff::Enabled),
            "Disabled" => Ok(OnOff::Disabled),
            _ => Err(ClientError::Error(format!("Invalid CrossRegionReplication value: {}", value))),
        }
    }
}

impl TryFrom<String> for OnOff {
    type Error = ClientError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for OnOff {
    type Error = ClientError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Versioning {
    #[cfg_attr(feature = "serde", serde(rename = "Enabled"))]
    Enabled,

    #[default]
    #[cfg_attr(feature = "serde", serde(rename = "Suspended"))]
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

impl Display for Versioning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for Versioning {
    type Error = ClientError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Enabled" => Ok(Versioning::Enabled),
            "Disabled" => Ok(Versioning::Suspended),
            _ => Err(ClientError::Error(format!("Invalid Versioning value: {}", value))),
        }
    }
}

impl TryFrom<String> for Versioning {
    type Error = ClientError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for Versioning {
    type Error = ClientError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ServerSideEncryptionAlgorithm {
    #[default]
    #[cfg_attr(feature = "serde", serde(rename = "KMS"))]
    KMS,

    #[cfg_attr(feature = "serde", serde(rename = "AES256"))]
    AES256,

    #[cfg_attr(feature = "serde", serde(rename = "SM4"))]
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

impl Display for ServerSideEncryptionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for ServerSideEncryptionAlgorithm {
    type Error = ClientError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "KMS" => Ok(ServerSideEncryptionAlgorithm::KMS),
            "AES256" => Ok(ServerSideEncryptionAlgorithm::AES256),
            "SM4" => Ok(ServerSideEncryptionAlgorithm::SM4),
            _ => Err(ClientError::Error(format!("Invalid ServerSideEncryptionAlgorithm value: {}", value))),
        }
    }
}

impl TryFrom<String> for ServerSideEncryptionAlgorithm {
    type Error = ClientError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for ServerSideEncryptionAlgorithm {
    type Error = ClientError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct ServerSideEncryptionRule {
    pub sse_algorithm: ServerSideEncryptionAlgorithm,

    /// Only present when sse_algorithm is `KMS`
    pub kms_master_key_id: Option<String>,
    pub kms_data_encryption: Option<String>,
}

///
/// Object type enumeration
///
#[derive(Debug, Clone, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub enum ObjectType {
    #[default]
    Normal,
    Multipart,
    Appendable,
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

impl TryFrom<&str> for ObjectType {
    type Error = ClientError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Normal" => Ok(ObjectType::Normal),
            "Multipart" => Ok(ObjectType::Multipart),
            "Appendable" => Ok(ObjectType::Appendable),
            "Symlink" => Ok(ObjectType::Symlink),
            _ => Err(ClientError::Error(format!("Invalid ObjectType value: {}", value))),
        }
    }
}

impl TryFrom<String> for ObjectType {
    type Error = ClientError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for ObjectType {
    type Error = ClientError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}
