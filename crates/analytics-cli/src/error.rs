use std::fmt;

use analytics_engine::AnalyticsEngineError;
use config::ConfigError;
use thiserror::Error;

pub(crate) type CliResult<T> = Result<T, CliError>;

#[derive(Debug, Error)]
#[error("{kind}{debug}")]
pub(crate) struct CliError {
    kind: CliErrorKind,
    debug: CliErrorDebug,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl CliError {
    pub(crate) fn with_source(
        kind: CliErrorKind,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            debug: CliErrorDebug::None,
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn with_debug(kind: CliErrorKind, debug: CliErrorDebug) -> Self {
        Self {
            kind,
            debug,
            source: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliErrorKind {
    Io,
    Json,
    Config,
    ManifestValidation,
    StructuredQueryValidation,
    Engine,
    Storage,
}

impl fmt::Display for CliErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => write!(f, "cli I/O failed"),
            Self::Json => write!(f, "cli JSON processing failed"),
            Self::Config => write!(f, "analytics configuration failed"),
            Self::ManifestValidation => write!(f, "analytics manifest validation failed"),
            Self::StructuredQueryValidation => write!(f, "structured query validation failed"),
            Self::Engine => write!(f, "analytics engine operation failed"),
            Self::Storage => write!(f, "analytics storage operation failed"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CliErrorDebug {
    None,
    Message(String),
}

impl fmt::Display for CliErrorDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => Ok(()),
            Self::Message(message) => write!(f, ": {message}"),
        }
    }
}

impl From<std::io::Error> for CliError {
    fn from(source: std::io::Error) -> Self {
        Self::with_source(CliErrorKind::Io, source)
    }
}

impl From<serde_json::Error> for CliError {
    fn from(source: serde_json::Error) -> Self {
        Self::with_source(CliErrorKind::Json, source)
    }
}

impl From<ConfigError> for CliError {
    fn from(source: ConfigError) -> Self {
        let message = source.to_string();
        Self {
            kind: CliErrorKind::Config,
            debug: CliErrorDebug::Message(message),
            source: Some(Box::new(source)),
        }
    }
}

impl From<analytics_contract::ManifestValidationError> for CliError {
    fn from(source: analytics_contract::ManifestValidationError) -> Self {
        Self::with_source(CliErrorKind::ManifestValidation, source)
    }
}

impl From<analytics_contract::StructuredQueryValidationError> for CliError {
    fn from(source: analytics_contract::StructuredQueryValidationError) -> Self {
        Self::with_source(CliErrorKind::StructuredQueryValidation, source)
    }
}

impl From<analytics_contract::ManifestLoadError> for CliError {
    fn from(source: analytics_contract::ManifestLoadError) -> Self {
        let message = source.to_string();
        Self {
            kind: CliErrorKind::ManifestValidation,
            debug: CliErrorDebug::Message(message),
            source: Some(Box::new(source)),
        }
    }
}

impl From<AnalyticsEngineError> for CliError {
    fn from(source: AnalyticsEngineError) -> Self {
        Self::with_source(CliErrorKind::Engine, source)
    }
}

impl From<analytics_storage::AnalyticsStorageError> for CliError {
    fn from(source: analytics_storage::AnalyticsStorageError) -> Self {
        Self::with_source(CliErrorKind::Storage, source)
    }
}
