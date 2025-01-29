use mini_redis::{client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello, world!");

    let mut client = client::connect("127.0.0.1:6379").await?;

    client.set("hello", "world".into()).await?;

    let result = client.get("hello").await?;

    println!("Got value from the bin; result ={:?}", result);

    Ok(())
}
