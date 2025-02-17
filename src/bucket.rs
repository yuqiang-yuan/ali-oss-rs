//! Basic bucket operations

use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};

use crate::{
    common::{
        AccessMonitor, Acl, CrossRegionReplication, DataRedundancyType, ObjectType, Owner, ServerSideEncryptionAlgorithm, ServerSideEncryptionRule,
        StorageClass, TransferAcceleration, Versioning,
    },
    error::{ClientError, ClientResult},
    request::{RequestBody, RequestBuilder, RequestMethod},
    util::validate_bucket_name,
};

/// Summary information of a bucket.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct BucketSummary {
    pub name: String,
    pub location: String,
    pub creation_date: String,
    pub extranet_endpoint: String,
    pub intranet_endpoint: String,
    pub region: String,
    pub storage_class: StorageClass,
    pub resource_group_id: Option<String>,
    pub comment: Option<String>,
}

impl BucketSummary {
    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> ClientResult<Self> {
        let mut bucket = Self::default();
        let mut current_tag = "".to_string();

        loop {
            match reader.read_event()? {
                Event::Eof => break,

                Event::Start(e) => {
                    current_tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                }

                Event::Text(e) => match current_tag.as_str() {
                    "Name" => bucket.name = e.unescape()?.to_string(),
                    "CreationDate" => bucket.creation_date = e.unescape()?.to_string(),
                    "Location" => bucket.location = e.unescape()?.to_string(),
                    "ExtranetEndpoint" => bucket.extranet_endpoint = e.unescape()?.to_string(),
                    "IntranetEndpoint" => bucket.intranet_endpoint = e.unescape()?.to_string(),
                    "Region" => bucket.region = e.unescape()?.to_string(),
                    "StorageClass" => bucket.storage_class = StorageClass::try_from(e.unescape()?.to_string())?,
                    "ResourceGroupId" => bucket.resource_group_id = Some(e.unescape()?.to_string()),
                    "Comment" => bucket.comment = Some(e.unescape()?.to_string()),
                    _ => {}
                },

                Event::End(e) => {
                    current_tag.clear();

                    if e.local_name().as_ref() == b"Bucket" {
                        break;
                    }
                }

                _ => {}
            }
        }

        Ok(bucket)
    }
}

///
/// Bucket policy
///
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct BucketPolicy {
    pub log_bucket: String,
    pub log_prefix: String,
}

///
/// Bucket detail information
///
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct BucketDetail {
    pub name: String,
    pub location: String,
    pub creation_date: String,
    pub extranet_endpoint: String,
    pub intranet_endpoint: String,
    pub region: String,
    pub storage_class: StorageClass,
    pub data_redundancy_type: DataRedundancyType,
    pub access_monitor: AccessMonitor,
    pub block_public_access: bool,
    pub transfer_acceleration: TransferAcceleration,
    pub cross_region_acceleration: CrossRegionReplication,
    pub resource_group_id: Option<String>,
    pub comment: Option<String>,
    pub versioning: Option<Versioning>,
    pub access_control_list: Vec<Acl>,
    pub bucket_policy: BucketPolicy,
    pub server_side_encryption_rule: Option<ServerSideEncryptionRule>,

    pub owner: Owner,
}

impl BucketDetail {
    /// Parse from response XML content
    pub(crate) fn from_xml(xml: &str) -> ClientResult<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut current_tag = "".to_string();

        let mut bucket_detail = BucketDetail::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(e) => match e.local_name().as_ref() {
                    b"Bucket" => bucket_detail = Self::from_xml_reader(&mut reader)?,
                    _ => {}
                },
                Event::End(_) => {
                    current_tag.clear();
                }
                _ => {}
            }
        }

        Ok(bucket_detail)
    }

    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> ClientResult<Self> {
        let mut bucket = Self::default();
        let mut current_tag = "".to_string();

        let mut tags = vec![];

        let mut sse_algorithm = "".to_string();
        let mut kms_master_key_id = "".to_string();
        let mut kms_data_encryption = "".to_string();

        loop {
            match reader.read_event()? {
                Event::Eof => break,

                Event::Start(e) => match e.local_name().as_ref() {
                    b"Owner" => {
                        bucket.owner = Owner::from_xml_reader(reader)?;
                    }

                    _ => {
                        current_tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                        tags.push(current_tag.clone());
                    }
                },

                Event::Text(e) => match current_tag.as_str() {
                    "Name" => bucket.name = e.unescape()?.to_string(),
                    "CreationDate" => bucket.creation_date = e.unescape()?.to_string(),
                    "Location" => bucket.location = e.unescape()?.to_string(),
                    "ExtranetEndpoint" => bucket.extranet_endpoint = e.unescape()?.to_string(),
                    "IntranetEndpoint" => bucket.intranet_endpoint = e.unescape()?.to_string(),
                    "Region" => bucket.region = e.unescape()?.to_string(),
                    "StorageClass" => bucket.storage_class = StorageClass::try_from(e.unescape()?.to_string())?,
                    "ResourceGroupId" => bucket.resource_group_id = Some(e.unescape()?.to_string()),
                    "Comment" => bucket.comment = Some(e.unescape()?.to_string()),
                    "AccessMonitor" => bucket.access_monitor = AccessMonitor::try_from(e.unescape()?.to_string())?,
                    "DataRedundancyType" => bucket.data_redundancy_type = DataRedundancyType::try_from(e.unescape()?.to_string())?,
                    "CrossRegionReplication" => bucket.cross_region_acceleration = CrossRegionReplication::try_from(e.unescape()?.to_string())?,
                    "TransferAcceleration" => bucket.transfer_acceleration = TransferAcceleration::try_from(e.unescape()?.to_string())?,
                    "Grant" if tags.get(tags.len() - 2) == Some(&"AccessControlList".to_string()) => {
                        bucket.access_control_list.push(Acl::try_from(e.unescape()?.to_string())?)
                    }
                    "SSEAlgorithm" if tags.get(tags.len() - 2) == Some(&"ServerSideEncryptionRule".to_string()) => sse_algorithm = e.unescape()?.to_string(),
                    "KMSMasterKeyID" if tags.get(tags.len() - 2) == Some(&"ServerSideEncryptionRule".to_string()) => {
                        kms_master_key_id = e.unescape()?.to_string()
                    }
                    "KMSDataEncryption" if tags.get(tags.len() - 2) == Some(&"ServerSideEncryptionRule".to_string()) => {
                        kms_data_encryption = e.unescape()?.to_string()
                    }
                    "BlockPublicAccess" => bucket.block_public_access = e.unescape()? == "true",
                    "LogBucket" => bucket.bucket_policy.log_bucket = e.unescape()?.to_string(),
                    "LogPrefix" => bucket.bucket_policy.log_prefix = e.unescape()?.to_string(),
                    _ => {}
                },

                Event::End(e) => {
                    current_tag.clear();
                    tags.pop();

                    if e.local_name().as_ref() == b"Bucket" {
                        break;
                    }
                }

                _ => {}
            }
        }

        if sse_algorithm == "None" {
            bucket.server_side_encryption_rule = None;
        } else {
            bucket.server_side_encryption_rule = Some(ServerSideEncryptionRule {
                sse_algorithm: ServerSideEncryptionAlgorithm::try_from(&sse_algorithm)?,
                kms_master_key_id: Some(kms_master_key_id),
                kms_data_encryption: Some(kms_data_encryption),
            });
        }

        Ok(bucket)
    }
}

/// The response data of list buckets.
/// If all buckets are returned, the following fields will be `None`:
///
/// - `prefix`
/// - `marker`
/// - `max_keys`
/// - `is_truncated`
/// - `next_marker`
///
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct ListBucketsResult {
    pub prefix: Option<String>,
    pub marker: Option<String>,
    pub max_keys: Option<String>,
    pub is_truncated: Option<bool>,
    pub next_marker: Option<String>,
    pub owner: Owner,
    pub buckets: Vec<BucketSummary>,
}

impl ListBucketsResult {
    pub(crate) fn from_xml(xml_content: &str) -> ClientResult<Self> {
        let mut reader = quick_xml::Reader::from_str(xml_content);
        reader.config_mut().trim_text(true);

        let mut ret = ListBucketsResult::default();

        let mut current_tag = "".to_string();

        loop {
            match reader.read_event()? {
                Event::Eof => break,

                Event::Start(e) => match e.local_name().as_ref() {
                    b"Owner" => {
                        ret.owner = Owner::from_xml_reader(&mut reader)?;
                    }

                    b"Bucket" => {
                        ret.buckets.push(BucketSummary::from_xml_reader(&mut reader)?);
                    }

                    _ => {
                        current_tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                    }
                },

                Event::Text(e) => match current_tag.as_str() {
                    "Prefix" => ret.prefix = Some(e.unescape()?.to_string()),
                    "Marker" => ret.marker = Some(e.unescape()?.to_string()),
                    "MaxKeys" => ret.max_keys = Some(e.unescape()?.to_string()),
                    "IsTruncated" => ret.is_truncated = Some(e.unescape()? == "true"),
                    "NextMarker" => ret.next_marker = Some(e.unescape()?.to_string()),
                    _ => {}
                },

                Event::End(_) => {
                    current_tag.clear();
                }

                _ => {}
            }
        }

        Ok(ret)
    }
}

#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct ListBucketsOptions {
    pub prefix: Option<String>,
    pub marker: Option<String>,
    pub max_keys: Option<String>,
    pub resource_group_id: Option<String>,
}

#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct PutBucketConfiguration {
    pub storage_class: Option<StorageClass>,
    pub data_redundancy_type: Option<DataRedundancyType>,
}

impl PutBucketConfiguration {
    pub fn to_xml(&self) -> ClientResult<String> {
        let mut writer = quick_xml::Writer::new(Vec::new());
        writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

        writer.write_event(Event::Start(BytesStart::new("CreateBucketConfiguration")))?;

        if let Some(storage_class) = &self.storage_class {
            writer.write_event(Event::Start(BytesStart::new("StorageClass")))?;
            writer.write_event(Event::Text(BytesText::new(storage_class.as_str())))?;
            writer.write_event(Event::End(BytesEnd::new("StorageClass")))?;
        }

        if let Some(data_redundancy_type) = &self.data_redundancy_type {
            writer.write_event(Event::Start(BytesStart::new("DataRedundancyType")))?;
            writer.write_event(Event::Text(BytesText::new(data_redundancy_type.as_str())))?;
            writer.write_event(Event::End(BytesEnd::new("DataRedundancyType")))?;
        }

        writer.write_event(Event::End(BytesEnd::new("CreateBucketConfiguration")))?;

        Ok(String::from_utf8(writer.into_inner())?)
    }
}

#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct PutBucketOptions {
    pub acl: Option<Acl>,
    pub resource_group_id: Option<String>,
    pub tags: Option<Vec<(String, String)>>,
}

///
/// Extract bucket location from XML response.
///
fn extract_bucket_location(xml: &str) -> ClientResult<String> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut tag = "".to_string();
    let mut location = "".to_string();
    loop {
        match reader.read_event()? {
            Event::Eof => break,
            Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
            Event::Text(s) => {
                if tag == "LocationConstraint" {
                    location = s.unescape()?.to_string();
                }
            }
            Event::End(_) => tag.clear(),
            _ => {}
        }
    }

    Ok(location)
}

///
/// Bucket statistics data. All statistical items are counted in bytes
///
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct BucketStat {
    pub storage: u64,
    pub object_count: u64,
    pub multipart_upload_count: u64,
    pub live_channel_count: u64,

    /// 获取到的存储信息的时间点，格式为时间戳，单位为秒。
    pub last_modified_time: u64,
    pub standard_storage: u64,
    pub standard_object_count: u64,

    pub infrequent_access_storage: u64,
    pub infrequent_access_real_storage: u64,
    pub infrequent_access_object_count: u64,

    pub archive_storage: u64,
    pub archive_real_storage: u64,
    pub archive_object_count: u64,

    pub cold_archive_storage: u64,
    pub cold_archive_real_storage: u64,
    pub cold_archive_object_count: u64,

    pub deep_cold_archive_storage: u64,
    pub deep_cold_archive_real_storage: u64,
    pub deep_cold_archive_object_count: u64,
}

impl BucketStat {
    pub(crate) fn from_xml(xml: &str) -> ClientResult<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut tag = String::new();
        let mut data = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
                Event::Text(s) => match tag.as_str() {
                    "Storage" => data.storage = s.unescape()?.to_string().parse()?,
                    "ObjectCount" => data.object_count = s.unescape()?.to_string().parse()?,
                    "MultipartUploadCount" => data.multipart_upload_count = s.unescape()?.to_string().parse()?,
                    "LiveChannelCount" => data.live_channel_count = s.unescape()?.to_string().parse()?,
                    "LastModifiedTime" => data.last_modified_time = s.unescape()?.to_string().parse()?,
                    "StandardStorage" => data.standard_storage = s.unescape()?.to_string().parse()?,
                    "StandardObjectCount" => data.standard_object_count = s.unescape()?.to_string().parse()?,
                    "InfrequentAccessStorage" => data.infrequent_access_storage = s.unescape()?.to_string().parse()?,
                    "InfrequentAccessRealStorage" => data.infrequent_access_real_storage = s.unescape()?.to_string().parse()?,
                    "InfrequentAccessObjectCount" => data.infrequent_access_object_count = s.unescape()?.to_string().parse()?,
                    "ArchiveStorage" => data.archive_storage = s.unescape()?.to_string().parse()?,
                    "ArchiveRealStorage" => data.archive_real_storage = s.unescape()?.to_string().parse()?,
                    "ArchiveObjectCount" => data.archive_object_count = s.unescape()?.to_string().parse()?,
                    "ColdArchiveStorage" => data.cold_archive_storage = s.unescape()?.to_string().parse()?,
                    "ColdArchiveRealStorage" => data.cold_archive_real_storage = s.unescape()?.to_string().parse()?,
                    "ColdArchiveObjectCount" => data.cold_archive_object_count = s.unescape()?.to_string().parse()?,
                    "DeepColdArchiveStorage" => data.deep_cold_archive_storage = s.unescape()?.to_string().parse()?,
                    "DeepColdArchiveRealStorage" => data.deep_cold_archive_real_storage = s.unescape()?.to_string().parse()?,
                    "DeepColdArchiveObjectCount" => data.deep_cold_archive_object_count = s.unescape()?.to_string().parse()?,
                    _ => {}
                },
                Event::End(_) => tag.clear(),
                _ => {}
            }
        }

        Ok(data)
    }
}

///
/// Object summary data for list objects v2
///
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct ObjectSummary {
    pub key: String,

    /// e.g. `2012-02-24T08:42:32.000Z`
    pub last_modified: String,

    /// This etag is starts and ends with double quotation characters (`"`)
    /// and maybe includes hyphen (`-`) if there are multiple files have the same hash value.
    pub etag: String,
    pub object_type: ObjectType,
    pub size: u64,
    pub storage_class: StorageClass,

    /// Only presents when query with `fetch_owner` is set to `true`
    pub owner: Option<Owner>,

    pub restore_info: Option<String>,
}

impl ObjectSummary {
    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> ClientResult<Self> {
        let mut tag = String::new();

        let mut data = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => match t.local_name().as_ref() {
                    b"Owner" => data.owner = Some(Owner::from_xml_reader(reader)?),
                    _ => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string()
                },
                Event::Text(s) => match tag.as_str() {
                    "Key" => data.key = s.unescape()?.to_string(),
                    "LastModified" => data.last_modified = s.unescape()?.to_string(),
                    "ETag" => data.etag = s.unescape()?.to_string(),
                    "Type" => data.object_type = ObjectType::try_from(s.unescape()?.to_string())?,
                    "Size" => data.size = s.unescape()?.to_string().parse()?,
                    "StorageClass" => data.storage_class = StorageClass::try_from(s.unescape()?.to_string())?,
                    "RestoreInfo" => data.restore_info = Some(s.unescape()?.to_string()),
                    _ => {}
                },
                Event::End(t) => {
                    if t.local_name().as_ref() == b"Contents" {
                        break;
                    }
                    tag.clear();
                }
                _ => {}
            }
        }

        Ok(data)
    }
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct ListObjectsResult {
    pub name: String,
    pub prefix: String,
    pub max_keys: u32,
    pub delimiter: String,
    pub start_after: Option<String>,
    pub is_truncated: bool,
    pub key_count: u64,
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,
    pub common_prefixes: Vec<String>,
    pub contents: Vec<ObjectSummary>,
}

impl ListObjectsResult {
    pub(crate) fn from_xml(xml: &str) -> ClientResult<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut tag = String::new();
        let mut data = Self::default();

        let mut tags = vec![];

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => match t.local_name().as_ref() {
                    b"Contents" => data.contents.push(ObjectSummary::from_xml_reader(&mut reader)?),
                    _ => {
                        tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string();
                        tags.push(tag.clone());
                    }
                }
                Event::Text(s) => match tag.as_str() {
                    "Name" => data.name = s.unescape()?.to_string(),
                    "StartAfter" => data.start_after = Some(s.unescape()?.to_string()),
                    "MaxKeys" => data.max_keys = s.unescape()?.to_string().parse()?,
                    "Delimiter" => data.delimiter = s.unescape()?.to_string(),
                    "IsTruncated" => data.is_truncated = s.unescape()? == "true",
                    "KeyCount" => data.key_count = s.unescape()?.to_string().parse()?,
                    "ContinuationToken" => data.continuation_token = Some(s.unescape()?.to_string()),
                    "NextContinuationToken" => data.next_continuation_token = Some(s.unescape()?.to_string()),
                    "Prefix" => {
                        // there 2 elements named `Prefix`, one is under root element, the other is `root/CommPrefixes`
                        let prefix = s.unescape()?.to_string();
                        if tags.len() == 2 {
                            data.prefix = prefix;
                        } else if tags.len() == 3 {
                            data.common_prefixes.push(prefix);
                        }
                    },
                    _ => {}
                },
                Event::End(_) => {
                    tags.pop();
                    tag.clear();
                },
                _ => {}
            }
        }

        Ok(data)
    }
}

/// Query options for listing objects in a bucket
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct ListObjectOptions {
    /// 对 Object 名字进行分组的字符。所有名字包含指定的前缀且第一次出现 `delimiter` 字符之间的 Object 作为一组元素 `common_prefixes`
    pub delimiter: Option<String>,

    /// 设定从 `start_after` 之后按字母排序开始返回 Object。
    /// `start_after` 用来实现分页显示效果，参数的长度必须小于 1024 字节。
    /// 做条件查询时，即使 `start_after` 在列表中不存在，也会从符合 `start_after` 字母排序的下一个开始打印。
    pub start_after: Option<String>,

    /// 指定 List 操作需要从此 `token` 开始。您可从请求结果中的 `next_continuation_token` 获取此 `token`。
    pub continuation_token: Option<String>,

    /// 指定返回 Object 的最大数。取值：大于 0 小于等于 1000
    pub max_keys: Option<u32>,

    /// 限定返回文件的Key必须以 `prefix` 作为前缀。
    /// 如果把 `prefix` 设为某个文件夹名，则列举以此 `prefix` 开头的文件，即该文件夹下递归的所有文件和子文件夹。
    /// 在设置 `prefix` 的基础上，将 `delimiter` 设置为正斜线（`/`）时，返回值就只列举该文件夹下的文件，文件夹下的子文件夹名返回在 `CommonPrefixes` 中，子文件夹下递归的所有文件和文件夹不显示。
    /// 例如，一个 Bucket 中有三个 Object，分别为
    ///
    /// - `fun/test.jpg`
    /// - `fun/movie/001.avi`
    /// - `fun/movie/007.avi`
    ///
    /// 如果设定 `prefix` 为 `fun/`，则返回三个 Object；如果在 `prefix` 设置为 `fun/` 的基础上，将 `delimiter` 设置为正斜线（`/`），
    /// 则返回 `fun/test.jpg` 和 `fun/movie/`。
    pub prefix: Option<String>,

    /// 指定是否在返回结果中包含 `owner` 信息
    pub fetch_owner: Option<bool>,
}

impl crate::oss::Client {
    fn build_put_bucket_request(&self, bucket_name: &str, config: &PutBucketConfiguration, options: &Option<PutBucketOptions>) -> ClientResult<RequestBuilder> {
        let xml = config.to_xml()?;

        let mut request = crate::request::RequestBuilder::new()
            .method(RequestMethod::Put)
            .bucket(bucket_name)
            .content_type("application/xml")
            .content_length(xml.len())
            .body(RequestBody::Text(xml));

        if let Some(options) = options {
            if let Some(acl) = &options.acl {
                request = request.add_header("x-oss-acl", acl.as_str());
            }

            if let Some(resource_group_id) = &options.resource_group_id {
                request = request.add_header("x-oss-resource-group-id", resource_group_id);
            }

            if let Some(tags) = &options.tags {
                if !tags.is_empty() {
                    let tags_string = tags
                        .iter()
                        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                        .collect::<Vec<String>>()
                        .join("&");

                    request = request.add_header("x-oss-bucket-tagging", &tags_string);
                }
            }
        }

        Ok(request)
    }

    fn build_list_buckets_request(&self, options: &Option<ListBucketsOptions>) -> RequestBuilder {
        let mut request = RequestBuilder::new();

        if let Some(opt) = options {
            if let Some(prefix) = &opt.prefix {
                request = request.add_query("prefix", prefix);
            }
            if let Some(marker) = &opt.marker {
                request = request.add_query("marker", marker);
            }
            if let Some(max_keys) = &opt.max_keys {
                request = request.add_query("max-keys", max_keys);
            }
            if let Some(oss_resource_group_id) = &opt.resource_group_id {
                request = request.add_header("x-oss-resource-group-id", oss_resource_group_id)
            }
        }

        request
    }

    fn build_list_objects_request(&self, options: &Option<ListObjectOptions>) -> RequestBuilder {
        let mut request = RequestBuilder::new().method(RequestMethod::Get).add_query("list-type", "2");

        if let Some(options) = options {
            if let Some(s) = &options.delimiter {
                request = request.add_query("delimiter", s);
            }

            if let Some(s) = &options.prefix {
                request = request.add_query("prefix", s);
            }

            if let Some(s) = &options.max_keys {
                request = request.add_query("max-keys", s.to_string());
            }

            if let Some(s) = &options.start_after {
                request = request.add_query("start-after", s);
            }

            if let Some(s) = &options.continuation_token {
                request = request.add_query("continuation-token", s);
            }

            if let Some(b) = &options.fetch_owner {
                request = request.add_query("fetch-owner", b.to_string());
            }
        }

        request
    }

    ///
    /// Create a bucket.
    ///
    /// `bucket_name` constraint:
    ///
    /// - 3 to 63 characters length
    /// - only lower case ascii, numbers and hyphen (`-`) are allowed
    /// - not starts or ends with hyphen character
    ///
    pub async fn put_bucket<S: AsRef<str>>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> ClientResult<()> {
        if !validate_bucket_name(bucket_name.as_ref()) {
            return Err(ClientError::Error(format!(
                "invalid bucket name: {}. please see the official document for more details",
                bucket_name.as_ref()
            )));
        }

        let request_builder = self.build_put_bucket_request(bucket_name.as_ref(), &config, &options)?;

        self.do_request(request_builder).await?;

        Ok(())
    }

    // List buckets response XML:
    //
    // ```xml
    // <?xml version="1.0" encoding="UTF-8"?>
    // <ListAllMyBucketsResult>
    //   <Owner>
    //     <ID>1447573407570489</ID>
    //     <DisplayName>1447573407570489</DisplayName>
    //   </Owner>
    //   <Buckets>
    //     <Bucket>
    //       <Comment></Comment>
    //       <CreationDate>2023-02-14T08:10:05.000Z</CreationDate>
    //       <ExtranetEndpoint>oss-cn-beijing.aliyuncs.com</ExtranetEndpoint>
    //       <IntranetEndpoint>oss-cn-beijing-internal.aliyuncs.com</IntranetEndpoint>
    //       <Location>oss-cn-beijing</Location>
    //       <Name>yuanyq</Name>
    //       <Region>cn-beijing</Region>
    //       <StorageClass>Standard</StorageClass>
    //     </Bucket>
    //   </Buckets>
    // </ListAllMyBucketsResult>
    // ```

    ///
    /// See official document for more details: <https://help.aliyun.com/zh/oss/developer-reference/listbuckets?spm=a2c4g.11186623.help-menu-31815.d_5_1_1_3_0.4a08b930Bo8bEt>
    ///
    pub async fn list_buckets(&self, options: Option<ListBucketsOptions>) -> ClientResult<ListBucketsResult> {
        let request_builder = self.build_list_buckets_request(&options);

        let content = self.do_request(request_builder).await?;

        ListBucketsResult::from_xml(&content)
    }

    ///
    /// Delte a bucket. Only non-empty bucket can be deleted
    ///
    pub async fn delete_bucket<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<()> {
        let request_builder = RequestBuilder::new().method(RequestMethod::Delete).bucket(bucket_name.as_ref());

        self.do_request(request_builder).await?;

        Ok(())
    }

    ///
    /// Get bucket info
    ///
    pub async fn get_bucket_info<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<BucketDetail> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("bucketInfo", "");

        let content = self.do_request(request_builder).await?;

        BucketDetail::from_xml(&content)
    }

    ///
    /// Get bucket location
    ///
    pub async fn get_bucket_location<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<String> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("location", "");

        let content = self.do_request(request_builder).await?;

        extract_bucket_location(content.as_str())
    }

    ///
    /// Get bucket statistics data
    ///
    pub async fn get_bucket_stat<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<BucketStat> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("stat", "");

        let content = self.do_request(request_builder).await?;

        BucketStat::from_xml(&content)
    }

    ///
    /// List objects in a bucket
    ///
    pub async fn list_objects<S: AsRef<str>>(&self, bucket_name: S, options: Option<ListObjectOptions>) -> ClientResult<ListObjectsResult> {
        let request = self.build_list_objects_request(&options).bucket(bucket_name);

        let content = self.do_request(request).await?;

        ListObjectsResult::from_xml(&content)
    }

    ///
    /// With `blocking` feature enabled
    ///
    #[cfg(feature = "blocking")]
    #[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
    pub async fn put_bucket_sync<S: AsRef<str>>(&self, bucket_name: S, config: PutBucketConfiguration, options: Option<PutBucketOptions>) -> ClientResult<()> {
        if !validate_bucket_name(bucket_name.as_ref()) {
            return Err(ClientError::Error(format!(
                "invalid bucket name: {}. please see the official document for more details",
                bucket_name.as_ref()
            )));
        }

        let request = self.build_put_bucket_request(bucket_name.as_ref(), &config, &options)?;

        self.do_request_sync(request)?;

        Ok(())
    }

    ///
    /// With `blocking` feature enabled
    ///
    #[cfg(feature = "blocking")]
    #[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
    pub fn list_buckets_sync(&self, options: Option<ListBucketsOptions>) -> ClientResult<ListBucketsResult> {
        let request = self.build_list_buckets_request(&options);

        let content = self.do_request_sync(request)?;

        ListBucketsResult::from_xml(&content)
    }

    ///
    /// With `blocking` feature enabled
    ///
    #[cfg(feature = "blocking")]
    #[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
    pub fn delete_bucket_sync<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<()> {
        let request_builder = RequestBuilder::new().method(RequestMethod::Delete).bucket(bucket_name.as_ref());

        self.do_request_sync(request_builder)?;

        Ok(())
    }

    ///
    /// Get bucket info
    ///
    #[cfg(feature = "blocking")]
    #[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
    pub fn get_bucket_info_sync<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<BucketDetail> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("bucketInfo", "");

        let content = self.do_request_sync(request_builder)?;

        BucketDetail::from_xml(&content)
    }

    #[cfg(feature = "blocking")]
    #[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
    pub fn get_bucket_location_sync<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<String> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("location", "");

        let content = self.do_request_sync(request_builder)?;

        extract_bucket_location(content.as_str())
    }

    ///
    /// Get bucket statistics data
    ///
    #[cfg(feature = "blocking")]
    #[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
    pub fn get_bucket_stat_sync<S: AsRef<str>>(&self, bucket_name: S) -> ClientResult<BucketStat> {
        let request_builder = RequestBuilder::new()
            .method(RequestMethod::Get)
            .bucket(bucket_name.as_ref())
            .add_query("stat", "");

        let content = self.do_request_sync(request_builder)?;

        BucketStat::from_xml(&content)
    }
}

#[cfg(all(test, not(feature = "blocking")))]
#[cfg(test)]
mod test_bucket {
    use std::sync::Once;

    use log::debug;

    use crate::{
        bucket::{ListBucketsOptions, ListObjectOptions},
        common::{Acl, TransferAcceleration},
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    // #[tokio::test]
    // async fn test_put_bucket() {
    //     setup();

    //     let client = crate::oss::Client::from_env();

    //     let response = client.put_bucket("yuanyq-test", PutBucketConfiguration::default(), None).await;

    //     assert!(response.is_ok());
    // }

    // #[tokio::test]
    // async fn test_update_bucket() {
    //     setup();

    //     let client = crate::oss::Client::from_env();
    //     let options = PutBucketOptions {
    //         acl: Some(Acl::PublicRead),
    //         tags: Some(vec![
    //             ("purpose".to_string(), "rust sdk test".to_string())
    //         ]),
    //         ..Default::default()
    //     };

    //     let response = client.put_bucket("yuanyq-test", PutBucketConfiguration::default(), Some(options)).await;

    //     assert!(response.is_ok());
    // }

    #[tokio::test]
    async fn test_list_buckets() {
        setup();
        let client = crate::oss::Client::from_env();

        let response = client.list_buckets(None).await;

        assert!(response.is_ok());

        let result = response.unwrap();
        assert!(!result.buckets.is_empty());

        let bucket = &result.buckets[0];
        assert!(!bucket.name.is_empty());

        debug!("{:?}", result);
    }

    #[tokio::test]
    async fn test_list_buckets_with_options() {
        setup();

        let client = crate::oss::Client::from_env();

        debug!("test list buckets with options: prefix");

        let options = ListBucketsOptions {
            prefix: Some("mi-builder".to_string()),
            ..Default::default()
        };

        let response = client.list_buckets(Some(options)).await;

        assert!(response.is_ok());

        let result = response.unwrap();
        if !result.buckets.is_empty() {
            for bucket in &result.buckets {
                assert!(!bucket.name.is_empty());
                assert!(bucket.name.starts_with("mi-builder"));
            }
        }

        debug!("test list buckets with options: resource group id");

        let options = ListBucketsOptions {
            resource_group_id: Some("rg-1234567890".to_string()),
            ..Default::default()
        };

        let response = client.list_buckets(Some(options)).await;

        assert!(response.is_ok());

        let result = response.unwrap();
        assert!(result.buckets.is_empty());

        // debug!("test list buckets with options: max-keys");

        // let options = ListBucketsOptions {
        //     max_keys: Some("2".to_string()),
        //     ..Default::default()
        // };

        // let response = client.list_buckets(Some(options)).await;

        // assert!(response.is_ok());

        // let result = response.unwrap();
        // assert!(result.next_marker.is_some());
        // assert_eq!(Some(true), result.is_truncated);
        // assert_eq!(2, result.buckets.len());
    }

    // #[tokio::test]
    // async fn test_delete_bucket() {
    //     setup();

    //     let client = crate::oss::Client::from_env();

    //     let response = client.delete_bucket("yuanyu-1").await;

    //     println!("{:?}", response);

    //     assert!(response.is_ok());
    // }

    #[tokio::test]
    async fn test_get_bucket_info() {
        setup();

        let client = crate::oss::Client::from_env();

        let response = client.get_bucket_info("yuanyq").await;

        println!("{:?}", response);

        assert!(response.is_ok());

        let bucket_info = response.unwrap();
        assert_eq!(None, bucket_info.server_side_encryption_rule);

        assert_eq!(1, bucket_info.access_control_list.len());
        assert_eq!(Some(&Acl::Private), bucket_info.access_control_list.get(0));
        assert_eq!(TransferAcceleration::Disabled, bucket_info.transfer_acceleration);
        assert_eq!(Some(&"rg-acfmyr4t7stjuoi".to_string()), bucket_info.resource_group_id.as_ref());
        assert_eq!("oss-cn-beijing", bucket_info.location);
        assert_eq!(false, bucket_info.block_public_access);
    }

    #[tokio::test]
    async fn test_get_bucket_location() {
        setup();

        let client = crate::oss::Client::from_env();

        let response = client.get_bucket_location("yuanyq").await;

        assert!(response.is_ok());

        let location = response.unwrap();
        assert_eq!("oss-cn-beijing", &location);
    }

    #[tokio::test]
    async fn test_get_bucket_stat() {
        setup();

        let client = crate::oss::Client::from_env();

        let response = client.get_bucket_stat("yuanyq").await;

        assert!(response.is_ok());

        let stat = response.unwrap();
        debug!("{:?}", stat);
    }

    #[tokio::test]
    async fn test_list_objects_1() {
        setup();

        let client = crate::oss::Client::from_env();

        let response = client.list_objects("mi-dev-public", None).await;
        assert!(response.is_ok());

        let result = response.unwrap();
        debug!("{:?}", result);
    }

    #[tokio::test]
    async fn test_list_objects_2() {
        setup();

        let client = crate::oss::Client::from_env();

        let options = ListObjectOptions {
            delimiter: Some("/".to_string()),
            prefix: Some("yuanyu-test/".to_string()),
            fetch_owner: Some(true),
            ..Default::default()
        };

        let response = client.list_objects("mi-dev-public", Some(options)).await;
        assert!(response.is_ok());

        let result = response.unwrap();
        debug!("{:?}", result);
    }
}

#[cfg(all(test, feature = "blocking"))]
mod test_bucket_sync {
    use crate::bucket::PutBucketConfiguration;
    use crate::common::Acl;
    use crate::common::TransferAcceleration;
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[test]
    fn test_list_buckets_sync() {
        simple_logger::init_with_level(log::Level::Debug).unwrap();
        dotenvy::dotenv().unwrap();

        let client = crate::oss::Client::from_env();

        let response = client.list_buckets_sync(None);

        assert!(response.is_ok());

        let result = response.unwrap();
        assert!(!result.buckets.is_empty());

        let bucket = &result.buckets[0];
        assert!(!bucket.name.is_empty());
    }

    #[test]
    fn test_put_bucket_config_xml() {
        let payload = PutBucketConfiguration { ..Default::default() };

        let s = payload.to_xml();
        println!("{}", s.unwrap());
    }

    #[test]
    fn test_get_bucket_info_sync() {
        setup();

        let client = crate::oss::Client::from_env();

        let response = client.get_bucket_info_sync("yuanyq");

        println!("{:?}", response);

        assert!(response.is_ok());

        let bucket_info = response.unwrap();
        assert_eq!(None, bucket_info.server_side_encryption_rule);

        assert_eq!(1, bucket_info.access_control_list.len());
        assert_eq!(Some(&Acl::Private), bucket_info.access_control_list.get(0));
        assert_eq!(TransferAcceleration::Disabled, bucket_info.transfer_acceleration);
        assert_eq!(Some(&"rg-acfmyr4t7stjuoi".to_string()), bucket_info.resource_group_id.as_ref());
        assert_eq!("oss-cn-beijing", bucket_info.location);
        assert_eq!(false, bucket_info.block_public_access);
    }

    #[test]
    fn test_get_bucket_location_sync() {
        setup();

        let client = crate::oss::Client::from_env();

        let response = client.get_bucket_location_sync("yuanyq");

        assert!(response.is_ok());

        let location = response.unwrap();
        assert_eq!("oss-cn-beijing", &location);
    }
}

#[cfg(test)]
mod test_bucket_xml {
    use crate::common::StorageClass;

    use super::ListBucketsResult;

    #[test]
    fn test_parse_bucket_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListAllMyBucketsResult>
            <Owner>
                <ID>1447573407570489</ID>
                <DisplayName>1447573407570489</DisplayName>
            </Owner>
            <Buckets>
                <Bucket>
                    <Comment></Comment>
                    <CreationDate>2023-02-14T08:10:05.000Z</CreationDate>
                    <ExtranetEndpoint>oss-cn-beijing.aliyuncs.com</ExtranetEndpoint>
                    <IntranetEndpoint>oss-cn-beijing-internal.aliyuncs.com</IntranetEndpoint>
                    <Location>oss-cn-beijing</Location>
                    <Name>yuanyq</Name>
                    <Region>cn-beijing</Region>
                    <StorageClass>Standard</StorageClass>
                </Bucket>
            </Buckets>
        </ListAllMyBucketsResult>"#;

        let response = ListBucketsResult::from_xml(xml);

        assert!(response.is_ok());

        let response = response.unwrap();
        println!("{:?}", response);

        assert_eq!("1447573407570489", response.owner.id);
        assert_eq!("1447573407570489", response.owner.display_name);

        assert_eq!(1, response.buckets.len());

        let bucket = &response.buckets[0];
        assert_eq!("yuanyq", bucket.name);
        assert_eq!("cn-beijing", bucket.region);
        assert_eq!(StorageClass::Standard, bucket.storage_class);
        assert_eq!("2023-02-14T08:10:05.000Z", bucket.creation_date);
        assert_eq!("oss-cn-beijing.aliyuncs.com", bucket.extranet_endpoint);
        assert_eq!("oss-cn-beijing-internal.aliyuncs.com", bucket.intranet_endpoint);
        assert_eq!("oss-cn-beijing", bucket.location);
    }
}
