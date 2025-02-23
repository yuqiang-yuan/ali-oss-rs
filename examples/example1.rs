use ali_oss_rs::{bucket::BucketOperations, error::Result, Client};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().unwrap();

    let client = Client::from_env();
    let list_buckets_result = client.list_buckets(None).await?;

    list_buckets_result.buckets.iter().for_each(|b| println!("{}\t{}", b.name, b.storage_class));

    Ok(())
}
