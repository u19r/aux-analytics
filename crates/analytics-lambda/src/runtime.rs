use std::sync::Arc;

use analytics_lambda::AnalyticsLambdaHandler;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;

pub(crate) async fn run() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let handler = Arc::new(AnalyticsLambdaHandler::from_env()?);
    tracing::info!("starting analytics lambda handler");
    lambda_runtime::run(service_fn(move |event| {
        let handler = Arc::clone(&handler);
        async move { handle_lambda_event(event, handler).await }
    }))
    .await?;
    Ok(())
}

async fn handle_lambda_event(
    event: LambdaEvent<Value>,
    handler: Arc<AnalyticsLambdaHandler>,
) -> Result<Value, Error> {
    handler
        .handle_event(event.payload)
        .await
        .map_err(Error::from)
}
