use std::fmt::Display;

use crate::error::ClientError;

///
/// Represents the access control list (ACL) for an object in Aliyun OSS.
///
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub enum Acl {
    PublicReadWrite,
    PublicRead,
    #[default]
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
pub enum StorageClass {
    #[default]
    Standard,
    IA,
    Archive,
    ColdArchive,
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
pub enum DataRedundancyType {
    #[default]
    LRS,
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
pub enum OnOff {
    Enabled,

    #[default]
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
pub enum Versioning {
    Enabled,

    #[default]
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
pub enum ServerSideEncryptionAlgorithm {
    #[default]
    KMS,
    AES256,
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
pub struct ServerSideEncryptionRule {
    pub sse_algorithm: ServerSideEncryptionAlgorithm,

    /// Only present when sse_algorithm is `KMS`
    pub kms_master_key_id: Option<String>,
    pub kms_data_encryption: Option<String>
}
