mod runtime;

#[cfg(test)]
mod runtime_tests;

use lambda_runtime::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    runtime::run().await
}
