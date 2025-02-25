use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};

use crate::{
    common::{
        self, AccessMonitor, Acl, CrossRegionReplication, DataRedundancyType, ObjectType, Owner, ServerSideEncryptionAlgorithm, ServerSideEncryptionRule,
        StorageClass, TransferAcceleration, Versioning,
    },
    error::Error,
    request::{OssRequest, RequestMethod},
    Result,
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
    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> Result<Self> {
        let mut bucket = Self::default();
        let mut current_tag = "".to_string();

        loop {
            match reader.read_event()? {
                Event::Eof => break,

                Event::Start(e) => {
                    current_tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                }

                Event::Text(e) => {
                    let s = e.unescape()?.trim().to_string();
                    match current_tag.as_str() {
                        "Name" => bucket.name = s,
                        "CreationDate" => bucket.creation_date = s,
                        "Location" => bucket.location = s,
                        "ExtranetEndpoint" => bucket.extranet_endpoint = s,
                        "IntranetEndpoint" => bucket.intranet_endpoint = s,
                        "Region" => bucket.region = s,
                        "StorageClass" => bucket.storage_class = StorageClass::try_from(s)?,
                        "ResourceGroupId" => bucket.resource_group_id = if s.is_empty() { None } else { Some(s) },
                        "Comment" => bucket.comment = if s.is_empty() { None } else { Some(s) },
                        _ => {}
                    }
                }

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

/// Bucket policy
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct BucketPolicy {
    pub log_bucket: String,
    pub log_prefix: String,
}

/// Bucket detail information
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
    pub(crate) fn from_xml(xml: &str) -> Result<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut current_tag = "".to_string();

        let mut bucket_detail = BucketDetail::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) if t.local_name().as_ref() == b"Bucket" => bucket_detail = Self::from_xml_reader(&mut reader)?,
                Event::End(_) => {
                    current_tag.clear();
                }
                _ => {}
            }
        }

        Ok(bucket_detail)
    }

    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> Result<Self> {
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

                Event::Text(e) => {
                    let s = e.unescape()?.trim().to_string();
                    match current_tag.as_str() {
                        "Name" => bucket.name = s,
                        "CreationDate" => bucket.creation_date = s,
                        "Location" => bucket.location = s,
                        "ExtranetEndpoint" => bucket.extranet_endpoint = s,
                        "IntranetEndpoint" => bucket.intranet_endpoint = s,
                        "Region" => bucket.region = s,
                        "StorageClass" => bucket.storage_class = StorageClass::try_from(s)?,
                        "ResourceGroupId" => bucket.resource_group_id = if s.is_empty() { None } else { Some(s) },
                        "Comment" => bucket.comment = if s.is_empty() { None } else { Some(s) },
                        "AccessMonitor" => bucket.access_monitor = AccessMonitor::try_from(s)?,
                        "DataRedundancyType" => bucket.data_redundancy_type = DataRedundancyType::try_from(s)?,
                        "CrossRegionReplication" => bucket.cross_region_acceleration = CrossRegionReplication::try_from(s)?,
                        "TransferAcceleration" => bucket.transfer_acceleration = TransferAcceleration::try_from(s)?,
                        "Grant" if tags.get(tags.len() - 2) == Some(&"AccessControlList".to_string()) => bucket.access_control_list.push(Acl::try_from(s)?),
                        "SSEAlgorithm" if tags.get(tags.len() - 2) == Some(&"ServerSideEncryptionRule".to_string()) => sse_algorithm = s,
                        "KMSMasterKeyID" if tags.get(tags.len() - 2) == Some(&"ServerSideEncryptionRule".to_string()) => kms_master_key_id = s,
                        "KMSDataEncryption" if tags.get(tags.len() - 2) == Some(&"ServerSideEncryptionRule".to_string()) => kms_data_encryption = s,
                        "BlockPublicAccess" => bucket.block_public_access = e.unescape()? == "true",
                        "LogBucket" => bucket.bucket_policy.log_bucket = s,
                        "LogPrefix" => bucket.bucket_policy.log_prefix = s,
                        _ => {}
                    }
                }

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
                kms_master_key_id: if kms_master_key_id.is_empty() { None } else { Some(kms_master_key_id) },
                kms_data_encryption: if kms_data_encryption.is_empty() { None } else { Some(kms_data_encryption) },
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
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde_camelcase", serde(rename_all = "camelCase"))]
pub struct ListBucketsResult {
    pub prefix: Option<String>,
    pub marker: Option<String>,
    pub max_keys: Option<String>,
    pub is_truncated: bool,
    pub next_marker: Option<String>,
    pub owner: Owner,
    pub buckets: Vec<BucketSummary>,
}

impl ListBucketsResult {
    pub(crate) fn from_xml(xml_content: &str) -> Result<Self> {
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

                Event::Text(e) => {
                    let s = e.unescape()?.trim().to_string();
                    match current_tag.as_str() {
                        "Prefix" => ret.prefix = if s.is_empty() { None } else { Some(s) },
                        "Marker" => ret.marker = if s.is_empty() { None } else { Some(s) },
                        "MaxKeys" => ret.max_keys = if s.is_empty() { None } else { Some(s) },
                        "IsTruncated" => ret.is_truncated = s == "true",
                        "NextMarker" => ret.next_marker = if s.is_empty() { None } else { Some(s) },
                        _ => {}
                    }
                }

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
    pub(crate) fn to_xml(&self) -> Result<String> {
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

/// Extract bucket location from XML response.
pub(crate) fn extract_bucket_location(xml: &str) -> Result<String> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut tag = "".to_string();
    let mut location = "".to_string();
    loop {
        match reader.read_event()? {
            Event::Eof => break,
            Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
            Event::Text(s) => {
                if tag == "LocationConstraint" {
                    location = s.unescape()?.trim().to_string();
                }
            }
            Event::End(_) => tag.clear(),
            _ => {}
        }
    }

    Ok(location)
}

/// Bucket statistics data. All statistical items are counted in bytes
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
    pub(crate) fn from_xml(xml: &str) -> Result<Self> {
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut tag = String::new();
        let mut data = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
                Event::Text(text) => {
                    let s = text.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "Storage" => data.storage = s.parse()?,
                        "ObjectCount" => data.object_count = s.parse()?,
                        "MultipartUploadCount" => data.multipart_upload_count = s.parse()?,
                        "LiveChannelCount" => data.live_channel_count = s.parse()?,
                        "LastModifiedTime" => data.last_modified_time = s.parse()?,
                        "StandardStorage" => data.standard_storage = s.parse()?,
                        "StandardObjectCount" => data.standard_object_count = s.parse()?,
                        "InfrequentAccessStorage" => data.infrequent_access_storage = s.parse()?,
                        "InfrequentAccessRealStorage" => data.infrequent_access_real_storage = s.parse()?,
                        "InfrequentAccessObjectCount" => data.infrequent_access_object_count = s.parse()?,
                        "ArchiveStorage" => data.archive_storage = s.parse()?,
                        "ArchiveRealStorage" => data.archive_real_storage = s.parse()?,
                        "ArchiveObjectCount" => data.archive_object_count = s.parse()?,
                        "ColdArchiveStorage" => data.cold_archive_storage = s.parse()?,
                        "ColdArchiveRealStorage" => data.cold_archive_real_storage = s.parse()?,
                        "ColdArchiveObjectCount" => data.cold_archive_object_count = s.parse()?,
                        "DeepColdArchiveStorage" => data.deep_cold_archive_storage = s.parse()?,
                        "DeepColdArchiveRealStorage" => data.deep_cold_archive_real_storage = s.parse()?,
                        "DeepColdArchiveObjectCount" => data.deep_cold_archive_object_count = s.parse()?,
                        _ => {}
                    }
                }
                Event::End(_) => tag.clear(),
                _ => {}
            }
        }

        Ok(data)
    }
}

/// Object summary data for list objects v2
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
    pub(crate) fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> Result<Self> {
        let mut tag = String::new();

        let mut data = Self::default();

        loop {
            match reader.read_event()? {
                Event::Eof => break,
                Event::Start(t) => match t.local_name().as_ref() {
                    b"Owner" => data.owner = Some(Owner::from_xml_reader(reader)?),
                    _ => tag = String::from_utf8_lossy(t.local_name().as_ref()).to_string(),
                },
                Event::Text(text) => {
                    let s = text.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "Key" => data.key = s,
                        "LastModified" => data.last_modified = s,
                        "ETag" => data.etag = s,
                        "Type" => data.object_type = ObjectType::try_from(s)?,
                        "Size" => data.size = s.parse()?,
                        "StorageClass" => data.storage_class = StorageClass::try_from(s)?,
                        "RestoreInfo" => data.restore_info = if s.is_empty() { None } else { Some(s) },
                        _ => {}
                    }
                }
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
    pub(crate) fn from_xml(xml: &str) -> Result<Self> {
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
                },
                Event::Text(text) => {
                    let s = text.unescape()?.trim().to_string();
                    match tag.as_str() {
                        "Name" => data.name = s,
                        "StartAfter" => data.start_after = if s.is_empty() { None } else { Some(s) },
                        "MaxKeys" => data.max_keys = s.parse()?,
                        "Delimiter" => data.delimiter = s,
                        "IsTruncated" => data.is_truncated = s == "true",
                        "KeyCount" => data.key_count = s.parse()?,
                        "ContinuationToken" => data.continuation_token = if s.is_empty() { None } else { Some(s) },
                        "NextContinuationToken" => data.next_continuation_token = if s.is_empty() { None } else { Some(s) },
                        "Prefix" => {
                            // there 2 elements named `Prefix`, one is under root element, the other is `root/CommPrefixes`
                            if tags.len() == 2 {
                                data.prefix = s;
                            } else if tags.len() == 3 {
                                data.common_prefixes.push(s);
                            }
                        }
                        _ => {}
                    }
                }
                Event::End(_) => {
                    tags.pop();
                    tag.clear();
                }
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
pub struct ListObjectsOptions {
    /// 对 Object 名字进行分组的字符。所有名字包含指定的前缀且第一次出现 `delimiter` 字符之间的 Object 作为一组元素 `common_prefixes`
    pub delimiter: Option<char>,

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

#[derive(Default)]
pub struct ListObjectsOptionsBuilder {
    options: ListObjectsOptions,
}

impl ListObjectsOptionsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn delimiter(mut self, delimiter: char) -> Self {
        self.options.delimiter = Some(delimiter);
        self
    }

    pub fn start_after<T: Into<String>>(mut self, start_after: T) -> Self {
        self.options.start_after = Some(start_after.into());
        self
    }

    pub fn continuation_token<T: Into<String>>(mut self, continuation_token: T) -> Self {
        self.options.continuation_token = Some(continuation_token.into());
        self
    }

    pub fn max_keys(mut self, max_keys: u32) -> Self {
        self.options.max_keys = Some(max_keys);
        self
    }

    pub fn prefix<T: Into<String>>(mut self, prefix: T) -> Self {
        self.options.prefix = Some(prefix.into());
        self
    }

    pub fn fetch_owner(mut self, fetch_owner: bool) -> Self {
        self.options.fetch_owner = Some(fetch_owner);
        self
    }

    pub fn build(self) -> ListObjectsOptions {
        self.options
    }
}

pub(crate) fn build_put_bucket_request(bucket_name: &str, config: &PutBucketConfiguration, options: &Option<PutBucketOptions>) -> Result<OssRequest> {
    let xml = config.to_xml()?;

    let mut request = crate::request::OssRequest::new()
        .method(RequestMethod::Put)
        .bucket(bucket_name)
        .content_type(common::MIME_TYPE_XML)
        .text_body(xml);

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

pub(crate) fn build_list_buckets_request(options: &Option<ListBucketsOptions>) -> OssRequest {
    let mut request = OssRequest::new();

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

pub(crate) fn build_list_objects_request(bucket_name: &str, options: &Option<ListObjectsOptions>) -> Result<OssRequest> {
    let mut request = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("list-type", "2");

    if let Some(options) = options {
        if let Some(c) = options.delimiter {
            request = request.add_query("delimiter", c.to_string());
        }

        if let Some(s) = &options.prefix {
            request = request.add_query("prefix", s);
        }

        if let Some(u) = options.max_keys {
            if u == 0 || u > 1000 {
                return Err(Error::Other(format!("invalid max-keys: {}. must between 1 and 1000", u)));
            }
            request = request.add_query("max-keys", u.to_string());
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

    Ok(request)
}
