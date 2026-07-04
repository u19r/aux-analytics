use std::sync::Arc;

use analytics_lambda::AnalyticsLambdaHandler;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;

pub(crate) async fn run() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let handler = Arc::new(
        AnalyticsLambdaHandler::from_env()
            .await
            .map_err(sanitized_lambda_error)?,
    );
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
        .map_err(sanitized_lambda_error)
}

fn sanitized_lambda_error(error: impl std::error::Error) -> Error {
    Error::from(sanitize_error_message(error.to_string().as_str()))
}

fn sanitize_error_message(message: &str) -> String {
    message
        .split_whitespace()
        .map(redact_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn redact_token(token: &str) -> &str {
    if token.contains("://") && token.contains('@') {
        return "<redacted-url>";
    }
    if token.starts_with("secret_") {
        return "<redacted-password>";
    }
    token
}

#[cfg(test)]
mod tests {
    use super::sanitize_error_message;

    #[test]
    fn sanitizes_database_urls_and_password_tokens() {
        let message = "failed to connect to db://user:secret_token@example.test:1234/catalog with \
                       secret_token";

        let sanitized = sanitize_error_message(message);

        assert!(!sanitized.contains("db://"));
        assert!(!sanitized.contains("secret_token"));
        assert_eq!(
            sanitized,
            "failed to connect to <redacted-url> with <redacted-password>"
        );
    }
}
