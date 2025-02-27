# Aliyun OSS Rust SDK

[![Crates.io Version](https://img.shields.io/crates/v/ali-oss-rs?_ts_=202502262317)](https://crates.io/crates/ali-oss-rs)
![Crates.io MSRV](https://img.shields.io/crates/msrv/ali-oss-rs?_ts_=202502262317)
[![docs.rs](https://img.shields.io/docsrs/ali-oss-rs)](https://docs.rs/ali-oss-rs)
[![Crates.io License](https://img.shields.io/crates/l/ali-oss-rs?_ts_=202502262317)](https://github.com/yuqiang-yuan/ali-oss-rs?tab=License-1-ov-file)

[English](https://github.com/yuqiang-yuan/ali-oss-rs) | [中文](https://github.com/yuqiang-yuan/ali-oss-rs/blob/dev/README.zh-CN.md)

*This project is under active development. Any feedback and contribution would be greatly appreciated.*

Aliyun Object Storage Service (OSS) is a massive, secure, cost-effective, and highly reliable cloud storage service provided by Alibaba Cloud. Users can store and access any type of data at any time, from anywhere, using any internet device through simple REST interfaces. OSS provides SDKs in multiple programming languages to help developers quickly integrate with OSS services.

# Features

- Split operations to separated modules to reduce size of the final artifact.
- Uses asynchronous calls to Aliyun API by default.
- Supports blocking calls with `blocking` feature enabled.
- Supports serialization and deserialization of data with `serde-support` feature enabled.
- Supports field name "camelCase" while serializing/deserializing data with `serde-camelcase` feature enabled.
- Supports using rust tls with `rust-tls` feature enabled.
- Re-export `serde` and `serde_json` crate.

# Implemented Operations

- Buckets
  - Create bucket
  - List buckets
  - Delete bucket
  - Get bucket information
  - Get bucket statistics data
  - Get bucket location
  - List objects in bucket. (v2)
- Objects
  - Put object: upload local file, buffer, base64 string with callback support
  - Put object: create a folder
  - Get object: download to local file
  - Get object metadata
  - Head object: get detail metadata of an object
  - Copy object
  - Delete object, or delete multiple objects
  - Check if object exists
  - Append object: from file, buffer and base64 string
  - Restore object
  - Clean restored object
  - URL with signature for `GET` request
  - Multipart uploads: from file with range, buffer and base64 string.
  - Multipart uploads: list parts and abort multipart uploads
  - Abort multipart uploads
  - Multipart uploads copy
- Objects extension operations
  - Permissions control
    - Get object acl
    - Put object acl
  - Symlink
    - Put symlink
    - Get symlink
  - Tagging
    - Get tagging
    - Put tagging
    - Delete tagging


**Notice**: The `etag` in this library is sanitized by removing the leading and trailing double quotation marks (`"`). I don't understand why the ETag returned from the Aliyun API is wrapped in double quotation marks.


# Examples

You need add `dotenvy` crate to your project.

```rust
    dotenvy::dotenv().unwrap();

    // `.env` file should contain the following keys
    //
    // ALI_ACCESS_KEY_ID=your_access_key_id
    // ALI_ACCESS_KEY_SECRET=your_access_key_secret
    // ALI_OSS_REGION=cn-beijing
    // ALI_OSS_ENDPOINT=oss-cn-beijing.aliyuncs.com
    let client = ali_oss_rs::Client::from_env();
    let list_buckets_result = client.list_buckets(None).await?;

    list_buckets_result.buckets.iter().for_each(|b| println!("{}\t{}", b.name, b.storage_class));

    let objects = client
        .list_objects(
            "example-bucket",
            Some(
                ListObjectsOptionsBuilder::default()
                    .prefix("test/")
                    .delimiter('/')
                    .build()
            )
        ).await?;
    objects.contents.iter().for_each(|o| println!("{}\t{}", o.key, o.size));

    Ok(())
```
