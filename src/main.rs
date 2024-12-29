use lambda_runtime::{service_fn, LambdaEvent, Error};
use serde_json::Value;

mod mongo;
mod s3upload;
mod web_scraper;

use anyhow::Result;
use aws_config;
use mongodb::{bson, Client};
use std::env;
use tokio;
use web_scraper::process;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let handler = service_fn(func);
    lambda_runtime::run(handler).await?;
    Ok(())
}

async fn func(event: LambdaEvent<Value>) -> Result<(), Error> {
    let (_payload, _context) = event.into_parts();

    let leeds_url =
        "https://publicaccess.leeds.gov.uk/online-applications/search.do?action=weeklyList";

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&config);

    let uri = env::var("MONGODB_URI")?;
    let collection_name = env::var("MONGODB_COLLECTION")?;
    let database_name = env::var("MONGODB_DATABASE")?;
    let mongo_client = Client::with_uri_str(&uri).await?;
    let mongo_db = mongo_client.database(&database_name);
    let mongo_collection = mongo_db.collection::<bson::Document>(&collection_name);

    println!("Starting web scraper...");
    if let Err(e) = process(&leeds_url, &mongo_collection, &s3_client).await {
        eprintln!("Error in process: {}", e);
    }
    Ok(())
}
