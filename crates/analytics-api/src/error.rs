use std::{fmt, net::AddrParseError};

use analytics_engine::AnalyticsEngineError;
use config::ConfigError;
use thiserror::Error;
use tracing_subscriber::util::TryInitError;

pub(crate) type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug, Error)]
#[error("{kind}{debug}")]
pub(crate) struct ApiError {
    kind: ApiErrorKind,
    debug: ApiErrorDebug,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl ApiError {
    pub(crate) fn with_source(
        kind: ApiErrorKind,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            debug: ApiErrorDebug::None,
            source: Some(Box::new(source)),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_debug(kind: ApiErrorKind, debug: ApiErrorDebug) -> Self {
        Self {
            kind,
            debug,
            source: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApiErrorKind {
    Io,
    Json,
    Config,
    ManifestValidation,
    Engine,
    AddressParse,
    TracingInit,
    Metrics,
    Storage,
}

impl fmt::Display for ApiErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => write!(f, "api I/O failed"),
            Self::Json => write!(f, "api JSON processing failed"),
            Self::Config => write!(f, "analytics configuration failed"),
            Self::ManifestValidation => write!(f, "analytics manifest validation failed"),
            Self::Engine => write!(f, "analytics engine operation failed"),
            Self::AddressParse => write!(f, "http bind address is invalid"),
            Self::TracingInit => write!(f, "tracing initialization failed"),
            Self::Metrics => write!(f, "metrics initialization failed"),
            Self::Storage => write!(f, "analytics storage operation failed"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ApiErrorDebug {
    None,
    Message(String),
}

impl fmt::Display for ApiErrorDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => Ok(()),
            Self::Message(message) => write!(f, ": {message}"),
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(source: std::io::Error) -> Self {
        Self::with_source(ApiErrorKind::Io, source)
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(source: serde_json::Error) -> Self {
        Self::with_source(ApiErrorKind::Json, source)
    }
}

impl From<ConfigError> for ApiError {
    fn from(source: ConfigError) -> Self {
        let message = source.to_string();
        Self {
            kind: ApiErrorKind::Config,
            debug: ApiErrorDebug::Message(message),
            source: Some(Box::new(source)),
        }
    }
}

impl From<analytics_contract::ManifestValidationError> for ApiError {
    fn from(source: analytics_contract::ManifestValidationError) -> Self {
        Self::with_source(ApiErrorKind::ManifestValidation, source)
    }
}

impl From<analytics_contract::ManifestLoadError> for ApiError {
    fn from(source: analytics_contract::ManifestLoadError) -> Self {
        let message = source.to_string();
        Self {
            kind: ApiErrorKind::ManifestValidation,
            debug: ApiErrorDebug::Message(message),
            source: Some(Box::new(source)),
        }
    }
}

impl From<AnalyticsEngineError> for ApiError {
    fn from(source: AnalyticsEngineError) -> Self {
        Self::with_source(ApiErrorKind::Engine, source)
    }
}

impl From<AddrParseError> for ApiError {
    fn from(source: AddrParseError) -> Self {
        Self::with_source(ApiErrorKind::AddressParse, source)
    }
}

impl From<TryInitError> for ApiError {
    fn from(source: TryInitError) -> Self {
        Self::with_source(ApiErrorKind::TracingInit, source)
    }
}

impl From<metrics_exporter_prometheus::BuildError> for ApiError {
    fn from(source: metrics_exporter_prometheus::BuildError) -> Self {
        Self::with_source(ApiErrorKind::Metrics, source)
    }
}

impl From<analytics_storage::AnalyticsStorageError> for ApiError {
    fn from(source: analytics_storage::AnalyticsStorageError) -> Self {
        Self::with_source(ApiErrorKind::Storage, source)
    }
}
