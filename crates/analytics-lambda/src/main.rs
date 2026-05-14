mod runtime;

use lambda_runtime::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    runtime::run().await
}
