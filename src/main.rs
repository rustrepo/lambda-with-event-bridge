use lambda_runtime::{service_fn, LambdaEvent, Error};
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let handler = service_fn(func);
    lambda_runtime::run(handler).await?;
    Ok(())
}

async fn func(event: LambdaEvent<Value>) -> Result<(), Error> {
    let (payload, _context) = event.into_parts();
    println!("Hello, World!");
    println!("Received event: {}", payload);
    Ok(())
}
