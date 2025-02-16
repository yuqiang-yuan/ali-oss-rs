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
