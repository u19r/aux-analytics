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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnalyticsStorageErrorKind {
    UnregisteredSourceTable,
    MissingStreamIdentifier,
    MissingAuxStorageEndpoint,
    InvalidUrl,
    InvalidHeader,
    Http,
    HttpStatus,
    Json,
    AwsSdk,
    MissingSequenceNumber,
    MissingKeys,
    UnsupportedAwsAttributeValue,
    MissingRetentionPolicyEndpoint,
    RetentionPolicyLookup,
    InvalidRetentionPolicyValue,
}

impl fmt::Display for AnalyticsStorageErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnregisteredSourceTable => {
                write!(
                    f,
                    "analytics source table is not registered in the manifest"
                )
            }
            Self::MissingStreamIdentifier => write!(
                f,
                "analytics source table needs stream_identifier for storage_stream polling"
            ),
            Self::MissingAuxStorageEndpoint => {
                write!(
                    f,
                    "analytics.source.endpoint_url is required for aux_storage polling"
                )
            }
            Self::InvalidUrl => write!(f, "invalid analytics source endpoint url"),
            Self::InvalidHeader => write!(f, "invalid analytics source header"),
            Self::Http => write!(f, "analytics source http request failed"),
            Self::HttpStatus => write!(f, "analytics source http request returned an error status"),
            Self::Json => write!(f, "analytics source json error"),
            Self::AwsSdk => write!(f, "aws stream polling failed"),
            Self::MissingSequenceNumber => {
                write!(f, "aws stream record did not include a sequence number")
            }
            Self::MissingKeys => write!(f, "aws stream record did not include keys"),
            Self::UnsupportedAwsAttributeValue => {
                write!(f, "aws stream attribute value variant is unsupported")
            }
            Self::MissingRetentionPolicyEndpoint => {
                write!(
                    f,
                    "analytics retention tenant_policy.endpoint_url is required"
                )
            }
            Self::RetentionPolicyLookup => write!(f, "analytics retention policy lookup failed"),
            Self::InvalidRetentionPolicyValue => {
                write!(f, "analytics retention policy value is invalid")
            }
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
        Self::with_source(AnalyticsStorageErrorKind::Http, source)
    }
}

impl From<serde_json::Error> for AnalyticsStorageError {
    fn from(source: serde_json::Error) -> Self {
        Self::with_source(AnalyticsStorageErrorKind::Json, source)
    }
}

pub type AnalyticsStorageResult<T> = Result<T, AnalyticsStorageError>;
