use crate::object_common::ObjectAcl;
use crate::object_ext_common::{build_get_object_acl_request, build_put_object_acl_request, parse_objcect_acl_from_xml, GetObjectAclOptions};
use crate::Result;

pub trait ObjectExtOperations {
    /// Get and object's acl.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectacl>
    fn get_object_acl<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectAclOptions>) -> Result<ObjectAcl>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;

    /// Put and object's acl. If you want to restore the object's acl to follow bucket acl settings, pass acl as `ObjectAcl::Default`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobjectacl>
    fn put_object_acl<S1, S2>(&self, bucket_name: S1, object_key: S2, acl: ObjectAcl, options: Option<GetObjectAclOptions>) -> Result<()>
    where
        S1: AsRef<str>,
        S2: AsRef<str>;
}

impl ObjectExtOperations for crate::blocking::Client {
    /// Get and object's acl.
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/getobjectacl>
    fn get_object_acl<S1, S2>(&self, bucket_name: S1, object_key: S2, options: Option<GetObjectAclOptions>) -> Result<ObjectAcl>
    where
        S1: AsRef<str>,
        S2: AsRef<str>
    {
        let request = build_get_object_acl_request(bucket_name.as_ref(), object_key.as_ref(), &options)?;
        let (_, content) = self.do_request::<String>(request)?;
        parse_objcect_acl_from_xml(&content)
    }

    /// Put and object's acl. If you want to restore the object's acl to follow bucket acl settings, pass acl as `ObjectAcl::Default`
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/putobjectacl>
    fn put_object_acl<S1, S2>(&self, bucket_name: S1, object_key: S2, acl: ObjectAcl, options: Option<GetObjectAclOptions>) -> Result<()>
    where
        S1: AsRef<str>,
        S2: AsRef<str>
    {
        let request = build_put_object_acl_request(bucket_name.as_ref(), object_key.as_ref(), acl, &options)?;
        let _ = self.do_request::<()>(request)?;
        Ok(())
    }
}


#[cfg(all(test, feature="blocking"))]
mod test_object_ext_operations_blocking {
    use std::sync::Once;

    use reqwest::StatusCode;
    use uuid::Uuid;

    use crate::{blocking::{object::ObjectOperations, object_ext::ObjectExtOperations, Client},  object_common::ObjectAcl};

    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::dotenv().unwrap();
        });
    }

    #[test]
    fn test_object_acl_blocking_1() {
        log::debug!("test a private object update to public");
        setup();

        let client = Client::from_env();

        let bucket_name = "yuanyq";
        let object_key = format!("rust-sdk-test/{}.jpg", Uuid::new_v4());
        let file_path = "/home/yuanyq/Pictures/01-cover.jpg";

        let response = client.put_object_from_file(bucket_name, &object_key, file_path, None);

        assert!(response.is_ok());

        let response = client.put_object_acl(bucket_name, &object_key, ObjectAcl::PublicRead, None);
        assert!(response.is_ok());

        let response = client.get_object_acl(bucket_name, &object_key, None);
        assert!(response.is_ok());
        let ret = response.unwrap();
        assert_eq!(ObjectAcl::PublicRead, ret);

        let url = format!("https://{}.oss-cn-beijing.aliyuncs.com/{}", bucket_name, object_key);
        let status = reqwest::blocking::get(&url).unwrap().status();
        assert_eq!(StatusCode::OK, status);

        let response = client.put_object_acl(bucket_name, &object_key, ObjectAcl::Default, None);
        assert!(response.is_ok());

        let response = client.get_object_acl(bucket_name, &object_key, None);
        assert!(response.is_ok());
        let ret = response.unwrap();
        assert_eq!(ObjectAcl::Default, ret);

        let url = format!("https://{}.oss-cn-beijing.aliyuncs.com/{}", bucket_name, object_key);
        let status = reqwest::blocking::get(&url).unwrap().status();
        assert_eq!(StatusCode::FORBIDDEN, status);

        client.delete_object(bucket_name, &object_key, None).unwrap();
    }
}
