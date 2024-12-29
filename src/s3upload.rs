use anyhow::Result;
use aws_sdk_s3::{operation::put_object, primitives::ByteStream, Client};
use mongodb::bson::{doc, Bson, Document};
use std::env;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;


/// Uploads a file from the given URL to S3, returning a document
/// which describes the uploaded file and its location in S3.
///
/// The returned document contains the fields:
///
/// - `type`: the type of the file (e.g. "pdf")
/// - `name`: the name of the file
/// - `size`: the size of the file in bytes
/// - `doc_type`: the type of document this file is associated with
/// - `s3`: a document containing details about the uploaded file's
///   location in S3, including the bucket name, key, and location.
///
/// The function will panic if the `AWS_BUCKET_NAME` or `AWS_REGION`
/// environment variables are not set.
pub async fn upload_file(
    file_type: &str,
    url: &str,
    client: &reqwest::Client,
    s3_client: &Client,
) -> Result<Document> {
    let response = client.get(url).send().await?;
    let contents = response.bytes().await?;
    let file_size = contents.len();

    // Generate a unique key for the S3 object
    
    let key = thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(16)
                    .map(char::from)
                    .collect::<String>();
                
    let bucket_name = env::var("AWS_BUCKET_NAME").unwrap();
    let region = env::var("AWS_REGION").unwrap();
    let file_url = format!(
        "https://{}.s3.{}.amazonaws.com/{}",
        bucket_name, region, key
    );

    let body = ByteStream::from(contents);

    println!("Uploading file to S3: {}", url);
    println!("File url: {}", file_url);

    // Prepare the S3 upload request
    let request = put_object::PutObjectInput::builder()
        .bucket(bucket_name.clone())
        .key(key.clone())
        .body(body) // send the file as bytes
        .set_content_type(Some("application/pdf".to_string()))
        .send_with(s3_client)
        .await;
    
    match request {
        Ok(res) => Ok(doc! {
            "type": "pdf",
            "name": key.clone(),
            "size": Bson::Int64(file_size as i64),
            "doc_type": file_type,
            "s3": {
                "Bucket": bucket_name,
                "key": key.clone(),
                "Key": key.clone(),
                "ETag": res.e_tag.unwrap_or_default(),
                "Location": file_url,
                "ServerSideEncryption": res.server_side_encryption.map(|e| e.to_string()).unwrap_or_default()
            }
        }),
        Err(e) => Err(anyhow::anyhow!("Error uploading file to S3: {:#?}", e)),
    }
}

// fn get_docs() -> Vec<dto::Docs> {
