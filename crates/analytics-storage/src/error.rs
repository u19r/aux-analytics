use std::fmt;

use reqwest::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{kind}{debug}")]
pub struct AnalyticsStorageError {
    kind: AnalyticsStorageErrorKind,
    debug: AnalyticsStorageErrorDebug,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl AnalyticsStorageError {
    pub(crate) fn new(kind: AnalyticsStorageErrorKind) -> Self {
        Self {
            kind,
            debug: AnalyticsStorageErrorDebug::None,
            source: None,
        }
    }

    pub(crate) fn with_debug(
        kind: AnalyticsStorageErrorKind,
        debug: AnalyticsStorageErrorDebug,
    ) -> Self {
        Self {
            kind,
            debug,
            source: None,
        }
    }

    fn with_source(
        kind: AnalyticsStorageErrorKind,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            debug: AnalyticsStorageErrorDebug::None,
            source: Some(Box::new(source)),
        }
    }

    fn with_debug_source(
        kind: AnalyticsStorageErrorKind,
        debug: AnalyticsStorageErrorDebug,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            debug,
            source: Some(Box::new(source)),
        }
    }

    /// Returns a stable, redacted category for operational diagnostics.
    ///
    /// Response bodies and backend messages are intentionally excluded so
    /// callers can log the failure class without leaking source data.
    #[must_use]
    pub fn category(&self) -> &'static str {
        self.kind.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum AnalyticsStorageErrorKind {
    #[error("analytics source table is not registered in the manifest")]
    UnregisteredSourceTable,
    #[error("analytics source table needs stream_identifier for storage_stream polling")]
    MissingStreamIdentifier,
    #[error("analytics.source.endpoint_url is required for aux_storage polling")]
    MissingAuxStorageEndpoint,
    #[error("invalid analytics source endpoint url")]
    InvalidUrl,
    #[error("invalid analytics source header")]
    InvalidHeader,
    #[error("analytics source http request failed")]
    Http,
    #[error("analytics source http request returned an error status")]
    HttpStatus,
    #[error("analytics source json error")]
    Json,
    #[error("aws stream polling failed")]
    AwsSdk,
    #[error("aws stream record did not include a sequence number")]
    MissingSequenceNumber,
    #[error("aws stream record did not include keys")]
    MissingKeys,
    #[error("source table matches multiple analytics source routes")]
    AmbiguousSourceRoute,
    #[error("source poll response has an invalid system-stream boundary")]
    MissingResponseScope,
    #[error("source poll response has an invalid system-stream boundary")]
    ResponseScopeMismatch,
    #[error("source poll response has an invalid system-stream boundary")]
    MissingResponseCheckpoint,
    #[error("source poll response has an invalid system-stream boundary")]
    MissingRecordSourceTable,
    #[error("source poll response has an invalid system-stream boundary")]
    MissingRecordSystemPosition,
    #[error("source poll batch has an empty or invalid source scope")]
    InvalidSourceBatchScope,
    #[error("source poll batch contains multiple source scopes")]
    MixedSourceBatchScope,
    #[error("aws stream attribute value variant is unsupported")]
    UnsupportedAwsAttributeValue,
    #[error("analytics retention tenant_policy.endpoint_url is required")]
    MissingRetentionPolicyEndpoint,
    #[error("analytics retention policy lookup failed")]
    RetentionPolicyLookup,
    #[error("analytics retention policy value is invalid")]
    InvalidRetentionPolicyValue,
}

impl AnalyticsStorageErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::UnregisteredSourceTable => "unregistered_source_table",
            Self::MissingStreamIdentifier => "missing_stream_identifier",
            Self::MissingAuxStorageEndpoint => "missing_aux_storage_endpoint",
            Self::InvalidUrl => "invalid_url",
            Self::InvalidHeader => "invalid_header",
            Self::Http => "http",
            Self::HttpStatus => "http_status",
            Self::Json => "json",
            Self::AwsSdk => "aws_sdk",
            Self::MissingSequenceNumber => "missing_sequence_number",
            Self::MissingKeys => "missing_keys",
            Self::AmbiguousSourceRoute => "ambiguous_source_route",
            Self::MissingResponseScope => "missing_response_scope",
            Self::ResponseScopeMismatch => "response_scope_mismatch",
            Self::MissingResponseCheckpoint => "missing_response_checkpoint",
            Self::MissingRecordSourceTable => "missing_record_source_table",
            Self::MissingRecordSystemPosition => "missing_record_system_position",
            Self::InvalidSourceBatchScope => "invalid_source_batch_scope",
            Self::MixedSourceBatchScope => "mixed_source_batch_scope",
            Self::UnsupportedAwsAttributeValue => "unsupported_aws_attribute_value",
            Self::MissingRetentionPolicyEndpoint => "missing_retention_policy_endpoint",
            Self::RetentionPolicyLookup => "retention_policy_lookup",
            Self::InvalidRetentionPolicyValue => "invalid_retention_policy_value",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AnalyticsStorageErrorDebug {
    None,
    SourceTableName(String),
    HttpStatus { status: StatusCode, body: String },
    AwsSdk(String),
    Message(String),
}

impl fmt::Display for AnalyticsStorageErrorDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => Ok(()),
            Self::SourceTableName(table_name) => write!(f, ": table_name={table_name}"),
            Self::HttpStatus { status, body } => write!(f, ": status={status}, body={body}"),
            Self::AwsSdk(message) | Self::Message(message) => write!(f, ": {message}"),
        }
    }
}

impl From<url::ParseError> for AnalyticsStorageError {
    fn from(source: url::ParseError) -> Self {
        Self::with_source(AnalyticsStorageErrorKind::InvalidUrl, source)
    }
}

impl From<reqwest::header::InvalidHeaderValue> for AnalyticsStorageError {
    fn from(source: reqwest::header::InvalidHeaderValue) -> Self {
        Self::with_source(AnalyticsStorageErrorKind::InvalidHeader, source)
    }
}

impl From<reqwest::Error> for AnalyticsStorageError {
    fn from(source: reqwest::Error) -> Self {
        let message = source.to_string();
        Self::with_debug_source(
            AnalyticsStorageErrorKind::Http,
            AnalyticsStorageErrorDebug::Message(message),
            source,
        )
    }
}

impl From<http_request::HttpRequestError> for AnalyticsStorageError {
    fn from(source: http_request::HttpRequestError) -> Self {
        let message = source.to_string();
        Self::with_debug_source(
            AnalyticsStorageErrorKind::Http,
            AnalyticsStorageErrorDebug::Message(message),
            source,
        )
    }
}

impl From<serde_json::Error> for AnalyticsStorageError {
    fn from(source: serde_json::Error) -> Self {
        Self::with_source(AnalyticsStorageErrorKind::Json, source)
    }
}

pub type AnalyticsStorageResult<T> = Result<T, AnalyticsStorageError>;

#[derive(Debug, thiserror::Error)]
pub enum LiveDatasetError {
    #[error("dataset not found")]
    NotFound,
    #[error("dataset version conflict")]
    VersionConflict,
    #[error("invalid dataset or query")]
    InvalidQuery,
    #[error("source changed; repeat the query without a cursor")]
    SourceChanged,
    #[error("query exceeds the indexed work budget")]
    QueryTooBroad,
    #[error("source is unavailable or has an unsupported index")]
    SourceUnavailable,
    #[error("dataset storage is unavailable")]
    Storage,
}
