//! The extension operations on an object. e.g. acl control, tagging and so on

use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    object_common::ObjectAcl,
    object_ext_common::{
        build_delete_object_tag_request, build_get_object_acl_request, build_get_object_tag_request, build_get_symlink_request, build_put_object_acl_request,
        build_put_object_tag_request, build_put_symlink_request, parse_object_acl_from_xml, parse_tags_from_xml, DeleteObjectTagOptions, GetObjectAclOptions,
        GetObjectTagOptions, GetSymlinkOptions, PutObjectTagOptions, PutSymlinkOptions, PutSymlinkResult,
    },
    Result,
};

#[async_trait]
pub trait ObjectExtOperations {
    /// Get and object's acl.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectacl>
    async fn get_object_acl<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectAclOptions>) -> Result<ObjectAcl>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Put and object's acl. If you want to restore the object's acl to follow bucket acl settings, pass acl as `ObjectAcl::Default`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobjectacl>
    async fn put_object_acl<S1, S2>(&self, bucket_name: S1, object_key: S2, acl: ObjectAcl, options: Option<GetObjectAclOptions>) -> Result<()>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

    /// Put a symlink object.
    ///
    /// `target_object_key` should be a full and valid object key.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putsymlink>
    async fn put_symlink<S1, S2, S3>(
        &self,
        bucket_name: S1,
        symlink_object_key: S2,
        target_object_key: S3,
        options: Option<PutSymlinkOptions>,
    ) -> Result<PutSymlinkResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send;

    /// Get a symlink object. The returned string is the target object key
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getsymlink>
    async fn get_symlink<S1, S2>(&self, bucket_name: S1, symlink_object_key: S2, options: Option<GetSymlinkOptions>) -> Result<String>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send;

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
impl ObjectExtOperations for crate::Client {
    /// Get an object's acl.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectacl>
    async fn get_object_acl<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectAclOptions>) -> Result<ObjectAcl>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let request = build_get_object_acl_request(bucket_name.as_ref(), object_key.as_ref(), &options)?;
        let (_, content) = self.do_request::<String>(request).await?;
        parse_object_acl_from_xml(&content)
    }

    /// Put an object's acl. If you want to restore the object's acl to follow bucket acl settings, pass acl as `ObjectAcl::Default`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobjectacl>
    async fn put_object_acl<S1, S2>(&self, bucket_name: S1, object_key: S2, acl: ObjectAcl, options: Option<GetObjectAclOptions>) -> Result<()>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let request = build_put_object_acl_request(bucket_name.as_ref(), object_key.as_ref(), acl, &options)?;
        let _ = self.do_request::<()>(request).await?;
        Ok(())
    }

    /// Put a symlink object.
    ///
    /// `target_object_key` should be a full and valid object key.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putsymlink>
    async fn put_symlink<S1, S2, S3>(
        &self,
        bucket_name: S1,
        symlink_object_key: S2,
        target_object_key: S3,
        options: Option<PutSymlinkOptions>,
    ) -> Result<PutSymlinkResult>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
        S3: AsRef<str> + Send,
    {
        let request = build_put_symlink_request(bucket_name.as_ref(), symlink_object_key.as_ref(), target_object_key.as_ref(), &options)?;
        let (headers, _) = self.do_request::<()>(request).await?;
        Ok(headers.into())
    }

    /// Get a symlink object. The returned string is the target object key
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getsymlink>
    async fn get_symlink<S1, S2>(&self, bucket_name: S1, symlink_object_key: S2, options: Option<GetSymlinkOptions>) -> Result<String>
    where
        S1: AsRef<str> + Send,
        S2: AsRef<str> + Send,
    {
        let request = build_get_symlink_request(bucket_name.as_ref(), symlink_object_key.as_ref(), &options)?;
        let (headers, _) = self.do_request::<()>(request).await?;
        let object_key = headers.get("x-oss-symlink-target").unwrap_or(&String::new()).to_string();

        Ok(urlencoding::decode(&object_key)?.to_string())
    }

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
mod test_object_ext_operations_async {
    use std::{collections::HashMap, sync::Once};

    use reqwest::StatusCode;
    use uuid::Uuid;

    use crate::{
        object::ObjectOperations,
        object_common::{HeadObjectOptionsBuilder, ObjectAcl, PutObjectApiResponse, PutObjectOptionsBuilder, PutObjectResult},
        object_ext::ObjectExtOperations,
        object_ext_common::{GetObjectTagOptions, PutObjectTagOptions, PutSymlinkOptionsBuilder},
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
    async fn test_object_acl_async_1() {
        log::debug!("test a private object update to public");
        setup();

        let client = Client::from_env();

        let bucket_name = "yuanyq";
        let object_key = format!("rust-sdk-test/{}.jpg", Uuid::new_v4());
        let file_path = "/home/yuanyq/Pictures/01-cover.jpg";

        let response = client.put_object_from_file(bucket_name, &object_key, file_path, None).await;

        assert!(response.is_ok());

        let response = client.put_object_acl(bucket_name, &object_key, ObjectAcl::PublicRead, None).await;
        assert!(response.is_ok());

        let response = client.get_object_acl(bucket_name, &object_key, None).await;
        assert!(response.is_ok());
        let ret = response.unwrap();
        assert_eq!(ObjectAcl::PublicRead, ret);

        let url = format!("https://{}.oss-cn-beijing.aliyuncs.com/{}", bucket_name, object_key);
        let status = reqwest::get(&url).await.unwrap().status();
        assert_eq!(StatusCode::OK, status);

        let response = client.put_object_acl(bucket_name, &object_key, ObjectAcl::Default, None).await;
        assert!(response.is_ok());

        let response = client.get_object_acl(bucket_name, &object_key, None).await;
        assert!(response.is_ok());
        let ret = response.unwrap();
        assert_eq!(ObjectAcl::Default, ret);

        let url = format!("https://{}.oss-cn-beijing.aliyuncs.com/{}", bucket_name, object_key);
        let status = reqwest::get(&url).await.unwrap().status();
        assert_eq!(StatusCode::FORBIDDEN, status);

        client.delete_object(bucket_name, &object_key, None).await.unwrap();
    }

    #[tokio::test]
    async fn test_symlink_async() {
        log::debug!("test symlink");
        setup();

        let client = Client::from_env();
        let bucket_name = "yuanyq-2";
        let object_key = format!("versioning-test/{}.webp", Uuid::new_v4());
        let file_path = "/home/yuanyq/Pictures/test-7.webp";

        let link_name = format!("versioning-test/{}-link.webp", Uuid::new_v4());

        let response = client.put_object_from_file(bucket_name, &object_key, file_path, None).await;
        assert!(response.is_ok());

        let ret = response.unwrap();
        let version_id = if let PutObjectResult::ApiResponse(PutObjectApiResponse {
            request_id: _,
            etag: _,
            content_md5: _,
            hash_crc64ecma: _,
            version_id,
        }) = ret
        {
            assert!(version_id.is_some());
            version_id.clone().unwrap()
        } else {
            panic!("Unexpected response type");
        };

        log::debug!("version id: {}", version_id);

        let options = PutSymlinkOptionsBuilder::new().metadata("x-oss-meta-a", "meta value b").build();

        let response = client.put_symlink(bucket_name, &link_name, &object_key, Some(options)).await;
        assert!(response.is_ok());

        let ret = response.unwrap();
        assert!(ret.version_id.is_some());

        let response = client.get_symlink(bucket_name, &link_name, None).await;
        assert!(response.is_ok());
        let ret = response.unwrap();
        assert_eq!(ret, object_key);

        let response = client.delete_object(bucket_name, &link_name, None).await;
        assert!(response.is_ok());

        let response = client.delete_object(bucket_name, &object_key, None).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_tagging_async() {
        log::debug!("test object tagging");
        setup();
        let client = Client::from_env();

        let bucket_name = "yuanyq-2";
        let object_key = format!("ali-oss-rs-test/{}.webp", Uuid::new_v4());
        let file_path = "/home/yuanyq/Pictures/test-8.webp";

        let options = PutObjectOptionsBuilder::new()
            .tag("tag-a", "tag-value-a")
            .build();

        let res = client.put_object_from_file(bucket_name, &object_key, file_path, Some(options)).await;
        assert!(res.is_ok());

        let ret = res.unwrap();

        if let PutObjectResult::ApiResponse(PutObjectApiResponse {
            request_id: _,
            etag: _,
            content_md5: _,
            hash_crc64ecma: _,
            version_id
        }) = ret {
            assert!(version_id.is_some());
        } else {
            panic!("response type does not match");
        }

        log::debug!("create a new version");

        let options = PutObjectOptionsBuilder::new()
            .tag("tag-a", "tag-value-a")
            .build();

        let res = client.put_object_from_file(bucket_name, &object_key, file_path, Some(options)).await;
        assert!(res.is_ok());

        let ret = res.unwrap();

        let version_id = if let PutObjectResult::ApiResponse(PutObjectApiResponse {
            request_id: _,
            etag: _,
            content_md5: _,
            hash_crc64ecma: _,
            version_id
        }) = ret {
            assert!(version_id.is_some());
            version_id.unwrap()
        } else {
            panic!("response type does not match");
        };

        log::debug!("last version id: {}", version_id);

        let res = client.head_object(
            bucket_name,
            &object_key,
            Some(HeadObjectOptionsBuilder::new().version_id(&version_id).build())
        ).await;

        let ret = res.unwrap();
        assert_eq!(Some(1), ret.tag_count);

        let res = client.get_object_tags(
            bucket_name,
            &object_key,
            Some(GetObjectTagOptions {version_id: Some(version_id.clone())})
        ).await;
        log::debug!("get object tag response: {:#?}", res);
        assert!(res.is_ok());
        let ret = res.unwrap();
        assert!(ret.contains_key("tag-a"));
        assert_eq!("tag-value-a", ret.get("tag-a").unwrap());

        let new_tags = HashMap::from([
            ("tag-b".to_string(), "tag-value-b".to_string()),
            ("tag-c".to_string(), "tag-value-c".to_string())
        ]);

        let _ = client.put_object_tags(
            bucket_name,
            &object_key,
            new_tags,
            Some(PutObjectTagOptions{version_id: Some(version_id.clone())})
        ).await;

        let res = client.get_object_tags(
            bucket_name,
            &object_key,
            Some(GetObjectTagOptions {version_id: Some(version_id.clone())})
        ).await;
        log::debug!("get object tag response: {:#?}", res);
        assert!(res.is_ok());

        let ret = res.unwrap();
        assert!(ret.contains_key("tag-b"));
        assert_eq!("tag-value-b", ret.get("tag-b").unwrap());

        assert!(ret.contains_key("tag-c"));
        assert_eq!("tag-value-c", ret.get("tag-c").unwrap());

        let _ = client.delete_object_tags(bucket_name, &object_key, None).await;

        let res = client.get_object_tags(
            bucket_name,
            &object_key,
            Some(GetObjectTagOptions {version_id: Some(version_id.clone())})
        ).await;
        log::debug!("get object tag response: {:#?}", res);
        assert!(res.is_ok());
        let ret = res.unwrap();
        assert!(ret.is_empty());

        let _ = client.delete_object(bucket_name, &object_key, None).await;

    }
}
