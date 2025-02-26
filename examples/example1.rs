use ali_oss_rs::{bucket::BucketOperations, bucket_common::ListObjectsOptionsBuilder, Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().unwrap();
    // `.env` file should contain the following keys
    //
    // ALI_ACCESS_KEY_ID=your_access_key_id
    // ALI_ACCESS_KEY_SECRET=your_access_key_secret
    // ALI_OSS_REGION=cn-beijing
    // ALI_OSS_ENDPOINT=oss-cn-beijing.aliyuncs.com
    let client = Client::from_env();
    let list_buckets_result = client.list_buckets(None).await?;

    list_buckets_result.buckets.iter().for_each(|b| println!("{}\t{}", b.name, b.storage_class));

    let objects = client
        .list_objects("example-bucket", Some(ListObjectsOptionsBuilder::default().prefix("test").build()))
        .await?;
    objects.contents.iter().for_each(|o| println!("{}\t{}", o.key, o.size));

    Ok(())
}
