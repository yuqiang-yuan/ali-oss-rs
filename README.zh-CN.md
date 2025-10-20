# Aliyun OSS Rust SDK

[![Crates.io Version](https://img.shields.io/crates/v/ali-oss-rs?_ts_=20251020)](https://crates.io/crates/ali-oss-rs)
![Crates.io MSRV](https://img.shields.io/crates/msrv/ali-oss-rs?_ts_=20251020)
[![docs.rs](https://img.shields.io/docsrs/ali-oss-rs)](https://docs.rs/ali-oss-rs)
[![Crates.io License](https://img.shields.io/crates/l/ali-oss-rs?_ts_=20251020)](https://github.com/yuqiang-yuan/ali-oss-rs?tab=License-1-ov-file)

[English](https://github.com/yuqiang-yuan/ali-oss-rs) | [中文](https://github.com/yuqiang-yuan/ali-oss-rs/blob/dev/README.zh-CN.md)

阿里云对象存储 OSS（Object Storage Service）是阿里云提供的一种海量、安全、低成本、高可靠的云存储服务。
用户可以通过简单的 REST 接口，在任何时间、任何地点，使用任何互联网设备存储和访问任意类型的数据。OSS 提供多种编程语言的 SDK，帮助开发者快速接入 OSS 服务。
本项目是阿里云 OSS 的 Rust SDK

# 特点

- 不同的操作拆分到不同的子模块，可以按需引入，减小最后构建输出的文件大小。
- 默认使用异步 API 请求网络。
- 启用 `blocking` 特性支持同步 API 请求。
- 启用 `serde-support` 特性可以使得本项目的一些暴露出来的类型支持序列化（使用 `serde` 类库）。
- 启用 `serde-camelcase` 特性支持序列化时采用小驼峰命名方式，如果需要将数据序列化成 JSON 数据，可以使用此特性。
- 启用 `rust-tls` 特性配置 `reqwest` 采用 Rust TLS。
- 重新导出了 `serde`, `serde_json` 库

# Implemented Operations

- Bucket
  - 创建 bucket
  - 列出 bucket
  - 删除 bucket
  - 获取 bucket 信息
  - 获取 bucket 统计数据
  - 获取 bucket 详细信息
  - 列出 bucket 中的文件
- Object
  - 创建 object。支持从本地文件、字节数据、Base64 字符串上传。支持回调
  - 创建目录
  - 下载 object 到本地文件
  - 下载 object 到内存
  - 获取 object 元数据
  - 获取 object 详细的元数据
  - 复制 object
  - 删除 object。 支持批量删除
  - 检查 object 是否存在
  - 向 object 追加内容。支持从本地文件、字节数据、Base64 字符串追加
  - 解冻归档 object
  - 清理解冻的归档 object
  - 分片上传：支持从文件、字节数据、Base64 字符串分片上传。支持回调
  - 分片上传：列出一个 bucket 中的未完成/未取消的碎片
  - 取消分片上传
  - 分片复制 object。如果要复制大于 1GB 的 object，需要使用分片复制
- Object 的更多操作
  - 权限控制（ACL）
    - 读取 object acl
    - 设置 object acl
  - 软链接（Symlink）
    - 获取软链接信息
    - 创建软链接
  - 标签（Tagging）
    - 设置或更新标签
    - 获取标签信息
    - 删除标签
- 其他
  - 预签名 `GET` 请求的 URL，适用于在浏览器中预览私有访问的 object
  - 预签名请求，返回 URL 和计算后的请求头，方便直接在其他语言或者框架中使用

**注意**: 本项目中，`etag` 标签的首尾双引号（`"`）都被清理了（实在搞不懂未和在 ETag 前后都带有双引号）。从 API 返回的 ETag 清理之后再提取；需要提交 ETag 的调用，也会自动补充首尾双引号。对使用者而言，不用关心 ETag 上双引号的问题。


# Examples

运行下面的示例代码，你需要在项目中引入 `dotenvy` crate。

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
