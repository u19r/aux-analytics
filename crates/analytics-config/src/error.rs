use std::{fmt, path::PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
#[error("{kind}{debug}")]
pub struct ConfigError {
    kind: ConfigErrorKind,
    debug: ConfigErrorDebug,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl ConfigError {
    #[must_use]
    pub fn kind(&self) -> ConfigErrorKind {
        self.kind
    }

    pub fn argument(message: impl Into<String>) -> Self {
        Self::with_debug(
            ConfigErrorKind::Argument,
            ConfigErrorDebug::Message(message.into()),
        )
    }

    pub(crate) fn io(path: PathBuf, source: std::io::Error) -> Self {
        Self::with_source(ConfigErrorKind::Io, ConfigErrorDebug::Path(path), source)
    }

    pub(crate) fn json(path: Option<PathBuf>, source: serde_json::Error) -> Self {
        Self::with_source(
            ConfigErrorKind::Json,
            path.map_or(ConfigErrorDebug::None, ConfigErrorDebug::Path),
            source,
        )
    }

    pub(crate) fn invalid_override_path(path: impl Into<String>) -> Self {
        Self::with_debug(
            ConfigErrorKind::InvalidOverridePath,
            ConfigErrorDebug::ConfigPath(path.into()),
        )
    }

    pub(crate) fn new(kind: ConfigErrorKind) -> Self {
        Self {
            kind,
            debug: ConfigErrorDebug::None,
            source: None,
        }
    }

    pub(crate) fn with_debug(kind: ConfigErrorKind, debug: ConfigErrorDebug) -> Self {
        Self {
            kind,
            debug,
            source: None,
        }
    }

    fn with_source(
        kind: ConfigErrorKind,
        debug: ConfigErrorDebug,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            debug,
            source: Some(Box::new(source)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigErrorKind {
    Argument,
    Io,
    Json,
    InvalidOverridePath,
    MissingBackend,
    MissingDucklakeDataPath,
    MissingCatalogBackend,
    MissingCatalogConnectionString,
    MissingDucklakeObjectStoragePath,
    MissingManifestPath,
    InvalidSourceTableStreamType,
    InvalidRetentionConfig,
}

impl fmt::Display for ConfigErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Argument => formatter.write_str("invalid config argument"),
            Self::Io => formatter.write_str("failed to read config"),
            Self::Json => formatter.write_str("invalid config JSON"),
            Self::InvalidOverridePath => formatter.write_str("invalid override path"),
            Self::MissingBackend => formatter.write_str(
                "one backend is required: --duckdb, --ducklake-sqlite-catalog, or \
                 --ducklake-postgres-catalog",
            ),
            Self::MissingDucklakeDataPath => {
                formatter.write_str("--ducklake-data-path is required for DuckLake backends")
            }
            Self::MissingCatalogBackend => formatter.write_str(
                "analytics catalog backend is required: pass a backend flag or set \
                 analytics.catalog.backend",
            ),
            Self::MissingCatalogConnectionString => formatter.write_str(
                "analytics catalog connection string is required: pass a backend flag or set \
                 analytics.catalog.connection_string",
            ),
            Self::MissingDucklakeObjectStoragePath => formatter
                .write_str("analytics.object_storage.path is required for DuckLake backends"),
            Self::MissingManifestPath => formatter.write_str(
                "analytics manifest path is required: pass --manifest or set \
                 analytics.manifest_path",
            ),
            Self::InvalidSourceTableStreamType => formatter.write_str(
                "analytics source table needs a stream_type or analytics.source.stream_type",
            ),
            Self::InvalidRetentionConfig => {
                formatter.write_str("analytics retention config is invalid")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigErrorDebug {
    None,
    Message(String),
    Path(PathBuf),
    ConfigPath(String),
    SourceTableName(String),
    RetentionTableName(String),
}

impl fmt::Display for ConfigErrorDebug {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => Ok(()),
            Self::Message(message) => write!(formatter, ": {message}"),
            Self::Path(path) => write!(formatter, ": path={}", path.display()),
            Self::ConfigPath(path) => write!(formatter, ": config_path={path}"),
            Self::SourceTableName(table_name) => write!(formatter, ": table_name={table_name}"),
            Self::RetentionTableName(table_name) => {
                write!(formatter, ": analytics_table_name={table_name}")
            }
        }
    }
}
