#[tokio::main]
async fn main() -> Result<(), analytics_lambda::BootstrapError> {
    analytics_lambda::run_bootstrap().await
}
