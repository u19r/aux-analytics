use std::error::Error as StdError;

use thiserror::Error;

use crate::error::{ApiError, ApiErrorKind};

#[derive(Debug, Error)]
pub(crate) enum BackgroundJobError {
    #[error("background source job configuration is invalid: {0}")]
    InvalidConfiguration(&'static str),
    #[error("background source job payload is invalid: {0}")]
    InvalidPayload(&'static str),
    #[error("background source job payload has no source scope")]
    MissingScope,
    #[error("background source job payload exceeds {maximum} bytes: {actual}")]
    PayloadTooLarge { actual: usize, maximum: usize },
    #[error("background source poller is not registered")]
    PollerNotRegistered,
    #[error("background source destination fence is unavailable")]
    DestinationFenceUnavailable,
    #[error("background source destination backend failed")]
    DestinationBackend,
    #[error("background source destination failed")]
    Destination,
    #[error("background retention job payload is invalid: {0}")]
    RetentionInvalidPayload(&'static str),
    #[error("background retention destination fence is unavailable")]
    RetentionDestinationFenceUnavailable,
    #[error("background retention destination backend failed")]
    RetentionDestinationBackend,
    #[error("background retention destination failed")]
    RetentionDestination,
}

pub(crate) fn api_config_error(error: impl StdError + Send + Sync + 'static) -> ApiError {
    ApiError::with_source(ApiErrorKind::Config, error)
}
