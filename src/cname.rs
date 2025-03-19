//! Bucket cname

use crate::{
    cname_common::{CnameInfo, ListCnameResult},
    error::Error,
    request::{OssRequest, RequestMethod},
    util::validate_bucket_name,
    Client, Result,
};
use async_trait::async_trait;

#[async_trait]
pub trait CnameOperations {
    /// List bucket cname
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listcname>
    async fn list_cname<S>(&self, bucket_name: S) -> Result<Vec<CnameInfo>>
    where
        S: AsRef<str> + Send;
}

#[async_trait]
impl CnameOperations for Client {
    // List bucket cname
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listcname>
    async fn list_cname<S>(&self, bucket_name: S) -> Result<Vec<CnameInfo>>
    where
        S: AsRef<str> + Send,
    {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("cname", "");

        let (_, xml) = self.do_request::<String>(request).await?;

        let ListCnameResult { cnames } = ListCnameResult::from_xml(&xml)?;

        Ok(cnames)
    }
}

#[cfg(test)]
pub mod test_cname_async {
    use std::sync::Once;

    use crate::{cname::CnameOperations, Client};

    static INIT: Once = Once::new();

    fn setup_comp() {
        INIT.call_once(|| {
            simple_logger::init_with_level(log::Level::Debug).unwrap();
            dotenvy::from_filename(".env.comp").unwrap();
        });
    }

    #[tokio::test]
    async fn test_cname_async_1() {
        setup_comp();

        let client = Client::from_env();

        let res = client.list_cname("mi-public").await;
        assert!(res.is_ok());

        let cnames = res.unwrap();
        assert_eq!(3, cnames.len());

        log::debug!("{:#?}", cnames);

        assert!(cnames.iter().any(|item| item.domain == "public-cdn-oss.mosoteach.cn"));
    }
}
