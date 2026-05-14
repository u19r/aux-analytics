use std::{fmt, path::Path};

use thiserror::Error;

use crate::AnalyticsManifest;

pub type ManifestLoadResult<T> = Result<T, ManifestLoadError>;

pub fn read_manifest(path: impl AsRef<Path>) -> ManifestLoadResult<AnalyticsManifest> {
    let path = path.as_ref();
    let contents = std::fs::read_to_string(path).map_err(|source| {
        ManifestLoadError::with_source(
            ManifestLoadErrorKind::Io,
            ManifestLoadErrorDebug::Path(path.display().to_string()),
            source,
        )
    })?;
    let value = serde_json::from_str::<serde_json::Value>(contents.as_str()).map_err(|source| {
        ManifestLoadError::with_source(
            ManifestLoadErrorKind::Json,
            ManifestLoadErrorDebug::Path(path.display().to_string()),
            source,
        )
    })?;
    validate_manifest_schema(&value)?;
    let manifest = serde_json::from_value::<AnalyticsManifest>(value).map_err(|source| {
        ManifestLoadError::with_source(
            ManifestLoadErrorKind::Json,
            ManifestLoadErrorDebug::None,
            source,
        )
    })?;
    manifest.validate().map_err(|source| {
        ManifestLoadError::with_source(
            ManifestLoadErrorKind::ManifestValidation,
            ManifestLoadErrorDebug::None,
            source,
        )
    })?;
    Ok(manifest)
}

fn validate_manifest_schema(value: &serde_json::Value) -> ManifestLoadResult<()> {
    let schema = schemars::schema_for!(AnalyticsManifest);
    let schema_value = serde_json::to_value(schema).map_err(|source| {
        ManifestLoadError::with_source(
            ManifestLoadErrorKind::Json,
            ManifestLoadErrorDebug::None,
            source,
        )
    })?;
    let validator = jsonschema::validator_for(&schema_value).map_err(|source| {
        ManifestLoadError::with_source(
            ManifestLoadErrorKind::SchemaValidation,
            ManifestLoadErrorDebug::None,
            source,
        )
    })?;
    let errors = validator
        .iter_errors(value)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ManifestLoadError::with_debug(
            ManifestLoadErrorKind::SchemaValidation,
            ManifestLoadErrorDebug::SchemaErrors(errors),
        ))
    }
}

#[derive(Debug, Error)]
#[error("{kind}{debug}")]
pub struct ManifestLoadError {
    kind: ManifestLoadErrorKind,
    debug: ManifestLoadErrorDebug,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl ManifestLoadError {
    #[must_use]
    pub fn kind(&self) -> ManifestLoadErrorKind {
        self.kind
    }

    fn with_debug(kind: ManifestLoadErrorKind, debug: ManifestLoadErrorDebug) -> Self {
        Self {
            kind,
            debug,
            source: None,
        }
    }

    fn with_source(
        kind: ManifestLoadErrorKind,
        debug: ManifestLoadErrorDebug,
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
pub enum ManifestLoadErrorKind {
    Io,
    Json,
    SchemaValidation,
    ManifestValidation,
}

impl fmt::Display for ManifestLoadErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => formatter.write_str("failed to read analytics manifest"),
            Self::Json => formatter.write_str("invalid analytics manifest JSON"),
            Self::SchemaValidation => {
                formatter.write_str("analytics manifest failed schema validation")
            }
            Self::ManifestValidation => formatter.write_str("analytics manifest is invalid"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestLoadErrorDebug {
    None,
    Path(String),
    SchemaErrors(Vec<String>),
}

impl fmt::Display for ManifestLoadErrorDebug {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => Ok(()),
            Self::Path(path) => write!(formatter, ": path={path}"),
            Self::SchemaErrors(errors) => write!(formatter, ": {}", errors.join("; ")),
        }
    }
}
