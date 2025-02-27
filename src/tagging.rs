//! Object tagging module

use std::collections::HashMap;

use async_trait::async_trait;

use crate::tagging_common::{
    build_delete_object_tag_request, build_get_object_tag_request, build_put_object_tag_request, parse_tags_from_xml, DeleteObjectTagOptions,
    GetObjectTagOptions, PutObjectTagOptions,
};
use crate::{Client, Result};

#[async_trait]
pub trait ObjectTagOperations {
    /// Get object taggings
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjecttagging>
    async fn get_object_tags<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectTagOptions>) -> Result<HashMap<String, String>>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Put object taggings
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobjecttagging>
    async fn put_object_tags<S1, S2>(&self, bucket_name: S1, object_key: S2, tags: HashMap<String, String>, options: Option<PutObjectTagOptions>) -> Result<()>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Delete object taggings
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deleteobjecttagging>
    async fn delete_object_tags<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<DeleteObjectTagOptions>) -> Result<()>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;
}

#[async_trait]
impl ObjectTagOperations for Client {
    /// Get object taggings
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjecttagging>
    async fn get_object_tags<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectTagOptions>) -> Result<HashMap<String, String>>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let request = build_get_object_tag_request(bucket_name.as_ref(), object_key.as_ref(), &options)?;
        let (_, xml) = self.do_request::<String>(request).await?;
        parse_tags_from_xml(xml)
    }

    /// Put object taggings
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobjecttagging>
    async fn put_object_tags<S1, S2>(&self, bucket_name: S1, object_key: S2, tags: HashMap<String, String>, options: Option<PutObjectTagOptions>) -> Result<()>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let request = build_put_object_tag_request(bucket_name.as_ref(), object_key.as_ref(), &tags, &options)?;
        let _ = self.do_request::<()>(request).await?;
        Ok(())
    }

    /// Delete object taggings
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/deleteobjecttagging>
    async fn delete_object_tags<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<DeleteObjectTagOptions>) -> Result<()>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let request = build_delete_object_tag_request(bucket_name.as_ref(), object_key.as_ref(), &options)?;
        let _ = self.do_request::<()>(request).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test_tagging_async {
    use std::{collections::HashMap, sync::Once};

    use uuid::Uuid;

    use crate::{
        object::ObjectOperations,
        object_common::{HeadObjectOptionsBuilder, PutObjectApiResponse, PutObjectOptionsBuilder, PutObjectResult},
        tagging::ObjectTagOperations,
        tagging_common::{GetObjectTagOptions, PutObjectTagOptions},
        Client,
    };

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[tokio::test]
    async fn test_tagging_async() {
        log::debug!("test object tagging");
        setup();
        let client = Client::from_env();

        let bucket_name = "yuanyq-2";
        let object_key = format!("ali-oss-rs-test/{}.webp", Uuid::new_v4());
        let file_path = "/home/yuanyq/Pictures/test-8.webp";

        let options = PutObjectOptionsBuilder::new().tag("tag-a", "tag-value-a").build();

        let res = client.put_object_from_file(bucket_name, &object_key, file_path, Some(options)).await;
        assert!(res.is_ok());

        let ret = res.unwrap();

        if let PutObjectResult::ApiResponse(PutObjectApiResponse {
            request_id: _,
            etag: _,
            content_md5: _,
            hash_crc64ecma: _,
            version_id,
        }) = ret
        {
            assert!(version_id.is_some());
        } else {
            panic!("response type does not match");
        }

        log::debug!("create a new version");

        let options = PutObjectOptionsBuilder::new().tag("tag-a", "tag-value-a").build();

        let res = client.put_object_from_file(bucket_name, &object_key, file_path, Some(options)).await;
        assert!(res.is_ok());

        let ret = res.unwrap();

        let version_id = if let PutObjectResult::ApiResponse(PutObjectApiResponse {
            request_id: _,
            etag: _,
            content_md5: _,
            hash_crc64ecma: _,
            version_id,
        }) = ret
        {
            assert!(version_id.is_some());
            version_id.unwrap()
        } else {
            panic!("response type does not match");
        };

        log::debug!("last version id: {}", version_id);

        let res = client
            .head_object(bucket_name, &object_key, Some(HeadObjectOptionsBuilder::new().version_id(&version_id).build()))
            .await;

        let ret = res.unwrap();
        assert_eq!(Some(1), ret.tag_count);

        let res = client
            .get_object_tags(
                bucket_name,
                &object_key,
                Some(GetObjectTagOptions {
                    version_id: Some(version_id.clone()),
                }),
            )
            .await;
        log::debug!("get object tag response: {:#?}", res);
        assert!(res.is_ok());
        let ret = res.unwrap();
        assert!(ret.contains_key("tag-a"));
        assert_eq!("tag-value-a", ret.get("tag-a").unwrap());

        let new_tags = HashMap::from([
            ("tag-b".to_string(), "tag-value-b".to_string()),
            ("tag-c".to_string(), "tag-value-c".to_string()),
        ]);

        let _ = client
            .put_object_tags(
                bucket_name,
                &object_key,
                new_tags,
                Some(PutObjectTagOptions {
                    version_id: Some(version_id.clone()),
                }),
            )
            .await;

        let res = client
            .get_object_tags(
                bucket_name,
                &object_key,
                Some(GetObjectTagOptions {
                    version_id: Some(version_id.clone()),
                }),
            )
            .await;
        log::debug!("get object tag response: {:#?}", res);
        assert!(res.is_ok());

        let ret = res.unwrap();
        assert!(ret.contains_key("tag-b"));
        assert_eq!("tag-value-b", ret.get("tag-b").unwrap());

        assert!(ret.contains_key("tag-c"));
        assert_eq!("tag-value-c", ret.get("tag-c").unwrap());

        let _ = client.delete_object_tags(bucket_name, &object_key, None).await;

        let res = client
            .get_object_tags(
                bucket_name,
                &object_key,
                Some(GetObjectTagOptions {
                    version_id: Some(version_id.clone()),
                }),
            )
            .await;
        log::debug!("get object tag response: {:#?}", res);
        assert!(res.is_ok());
        let ret = res.unwrap();
        assert!(ret.is_empty());

        let _ = client.delete_object(bucket_name, &object_key, None).await;
    }
}
