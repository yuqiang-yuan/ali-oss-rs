use crate::{
    cname_common::{CnameInfo, ListCnameResult},
    error::Error,
    request::{OssRequest, RequestMethod},
    util::validate_bucket_name,
    Result,
};

use super::Client;

pub trait CnameOperations {
    /// List bucket cname
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listcname>
    fn list_cname<S>(&self, bucket_name: S) -> Result<Vec<CnameInfo>>
    where
        S: AsRef<str>;
}

impl CnameOperations for Client {
    // List bucket cname
    ///
    /// Official document: <https://help.aliyun.com/zh/oss/developer-reference/listcname>
    fn list_cname<S>(&self, bucket_name: S) -> Result<Vec<CnameInfo>>
    where
        S: AsRef<str>,
    {
        let bucket_name = bucket_name.as_ref();

        if !validate_bucket_name(bucket_name) {
            return Err(Error::Other(format!("invalid bucket name: {}", bucket_name)));
        }

        let request = OssRequest::new().method(RequestMethod::Get).bucket(bucket_name).add_query("cname", "");

        let (_, xml) = self.do_request::<String>(request)?;

        let ListCnameResult { cnames } = ListCnameResult::from_xml(&xml)?;

        Ok(cnames)
    }
}
