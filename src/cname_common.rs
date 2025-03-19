use quick_xml::events::Event;

use crate::common::OnOff;
use crate::Result;

pub type CnameStatus = OnOff;

// /// Cname certificate
// #[derive(Debug, Clone, Default)]
// #[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
// #[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
// pub struct CnameCertificate {

//     /// 证书来源
//     ///
//     /// - `CAS`: 证书中心
//     /// - `Upload`: 用户自行上传的
//     pub cert_type: String,

//     pub cert_id: String,
//     pub status: OnOff,
//     pub creation_date: String,

//     /// 证书签名。e.g. `DE:01:CF:EC:7C:A7:98:CB:D8:6E:FB:1D:97:EB:A9:64:1D:4E:**:**`.
//     pub fingerprint: String,
//     pub valid_start_date: String,
//     pub valid_end_date: String,
// }

/// Bucket cname data
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct CnameInfo {
    pub domain: String,
    pub last_modified: String,
    pub status: CnameStatus,
    pub is_purge_cdn_cache: bool,
}

impl CnameInfo {
    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> Result<Self> {
        let mut tag = String::new();
        let mut data = CnameInfo::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => {
                    tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string();
                }
                Event::Text(text) => {
                    let s = text.unescape()?.trim().to_string();

                    match tag.as_str() {
                        "Domain" => data.domain = s,
                        "LastModified" => data.last_modified = s,
                        "Status" => data.status = CnameStatus::try_from(s)?,
                        "IsPurgeCdnCache" => data.is_purge_cdn_cache = s == "true",
                        _ => {}
                    }
                }
                Event::End(t) => {
                    tag.clear();
                    if t.local_name().as_ref() == b"Cname" {
                        break;
                    }
                }
                _ => {}
            }
        }

        Ok(data)
    }
}

/// List cname result.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde-support", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde-camelcase", serde(rename_all = "camelCase"))]
pub struct ListCnameResult {
    pub cnames: Vec<CnameInfo>,
}

impl ListCnameResult {
    /// Parse cnames from xml
    pub(crate) fn from_xml(xml: &str) -> Result<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut cnames = vec![];

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) if t.local_name().as_ref() == b"Cname" => cnames.push(CnameInfo::from_xml_reader(&mut reader)?),
                _ => {}
            }
        }

        Ok(Self { cnames })
    }
}
