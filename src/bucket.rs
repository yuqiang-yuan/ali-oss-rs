use quick_xml::events::Event;

use crate::{
    error::ClientResult,
    request::RequestBuilder,
};

#[derive(Debug, Clone, Default)]
pub struct Owner {
    id: String,
    display_name: String,
}

impl Owner {
    pub fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> ClientResult<Self> {
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

/// Summary information of a bucket.
#[derive(Debug, Clone, Default)]
pub struct BucketSummary {
    name: String,
    location: String,
    creation_date: String,
    extranet_endpoint: String,
    intranet_endpoint: String,
    region: String,
    storage_class: String,
    resource_group_id: Option<String>,
    comment: Option<String>,
}

impl BucketSummary {
    pub fn from_xml_reader(reader: &mut quick_xml::Reader<&[u8]>) -> ClientResult<Self> {
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
                    "StorageClass" => bucket.storage_class = e.unescape()?.to_string(),
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
    pub fn from_xml(xml_content: &str) -> ClientResult<Self> {
        let mut reader = quick_xml::Reader::from_str(xml_content);
        reader.config_mut().trim_text(true);

        let mut ret = ListBucketsResult::default();

        let mut current_tag = "".to_string();

        loop {
            match reader.read_event()? {
                Event::Eof => break,

                Event::Start(e) => {
                    match e.local_name().as_ref() {
                        b"Owner" => {
                            ret.owner = Owner::from_xml_reader(&mut reader)?;
                        }

                        b"Bucket" => {
                            ret.buckets.push(BucketSummary::from_xml_reader(&mut reader)?);
                        }

                        _ => {
                            current_tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                        }
                    }
                }

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
pub struct ListBucketsOptions {
    pub prefix: Option<String>,
    pub marker: Option<String>,
    pub max_keys: Option<String>,
    pub resource_group_id: Option<String>,
}

impl crate::oss::Client {

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

    /// List buckets response XML:
    ///
    /// ```xml
    /// <?xml version="1.0" encoding="UTF-8"?>
    /// <ListAllMyBucketsResult>
    ///   <Owner>
    ///     <ID>1447573407570489</ID>
    ///     <DisplayName>1447573407570489</DisplayName>
    ///   </Owner>
    ///   <Buckets>
    ///     <Bucket>
    ///       <Comment></Comment>
    ///       <CreationDate>2023-02-14T08:10:05.000Z</CreationDate>
    ///       <ExtranetEndpoint>oss-cn-beijing.aliyuncs.com</ExtranetEndpoint>
    ///       <IntranetEndpoint>oss-cn-beijing-internal.aliyuncs.com</IntranetEndpoint>
    ///       <Location>oss-cn-beijing</Location>
    ///       <Name>yuanyq</Name>
    ///       <Region>cn-beijing</Region>
    ///       <StorageClass>Standard</StorageClass>
    ///     </Bucket>
    ///   </Buckets>
    /// </ListAllMyBucketsResult>
    /// ```
    pub async fn list_buckets(&self, options: Option<ListBucketsOptions>) -> ClientResult<ListBucketsResult> {
        let request = self.build_list_buckets_request(&options);

        let content = self.do_request(request).await?;

        Ok(ListBucketsResult::from_xml(&content)?)
    }

    #[cfg(feature = "blocking")]
    pub fn list_buckets_sync(&self, options: Option<ListBucketsOptions>) -> ClientResult<ListBucketsResult> {
        let request = self.build_list_buckets_request(&options);

        let content = self.do_request_sync(request)?;

        Ok(ListBucketsResult::from_xml(&content)?)
    }
}


#[cfg(all(test, not(feature = "blocking")))]
#[cfg(test)]
mod test_bucket {
    use std::sync::Once;

    use log::debug;

    use crate::bucket::ListBucketsOptions;

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
                dotenvy::dotenv().unwrap();
        });
    }

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


        debug!("test list buckets with options: max-keys");

        let options = ListBucketsOptions {
            max_keys: Some("2".to_string()),
            ..Default::default()
        };

        let response = client.list_buckets(Some(options)).await;

        assert!(response.is_ok());

        let result = response.unwrap();
        assert!(result.next_marker.is_some());
        assert_eq!(Some(true), result.is_truncated);
        assert_eq!(2, result.buckets.len());

    }
}

#[cfg(all(test, feature = "blocking"))]
mod test_bucket_sync {
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
}

#[cfg(test)]
mod test_bucket_xml {
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
        assert_eq!("Standard", bucket.storage_class);
        assert_eq!("2023-02-14T08:10:05.000Z", bucket.creation_date);
        assert_eq!("oss-cn-beijing.aliyuncs.com", bucket.extranet_endpoint);
        assert_eq!("oss-cn-beijing-internal.aliyuncs.com", bucket.intranet_endpoint);
        assert_eq!("oss-cn-beijing", bucket.location);
    }
}
