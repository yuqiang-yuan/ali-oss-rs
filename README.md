# Aliyun OSS Rust SDK

*This project is under active development*

Aliyun Object Storage Service (OSS) is a massive, secure, cost-effective, and highly reliable cloud storage service provided by Alibaba Cloud. Users can store and access any type of data at any time, from anywhere, using any internet device through simple REST interfaces. OSS provides SDKs in multiple programming languages to help developers quickly integrate with OSS services.

## Features

- Uses asynchronous calls to Aliyun API by default
- Supports blocking calls with `blocking` feature enabled
- Supports serialization and deserialization of data with `serde` feature enabled
- Supports field name "camelCase" while serializing/deserializing data with `serde_camelcase` feature enabled
- Supports using rust tls with `rust-tls` feature enabled

## Implemented Operations

- Bucket
  - Create bucket
  - List buckets
  - Delete bucket
  - Get bucket information
  - Get bucket stat
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
