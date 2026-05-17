use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OperationTypeError {
    #[error("operation id cannot be empty")]
    EmptyOperationId,
    #[error("operation actor cannot be empty")]
    EmptyActor,
    #[error("operation cursor cannot move backwards from {current} to {next}")]
    CursorMovedBackwards { current: u64, next: u64 },
    #[error("invalid operation kind {0}")]
    InvalidKind(String),
    #[error("invalid operation phase {0}")]
    InvalidPhase(String),
    #[error("invalid operation status {0}")]
    InvalidStatus(String),
    #[error("invalid cancellation state {0}")]
    InvalidCancellationState(String),
    #[error("invalid operation event kind {0}")]
    InvalidEventKind(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperationId(String);

impl OperationId {
    pub fn new(value: impl Into<String>) -> Result<Self, OperationTypeError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(OperationTypeError::EmptyOperationId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<String> for OperationId {
    type Error = OperationTypeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<OperationId> for String {
    fn from(value: OperationId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperationActor(String);

impl OperationActor {
    pub fn new(value: impl Into<String>) -> Result<Self, OperationTypeError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(OperationTypeError::EmptyActor);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<String> for OperationActor {
    type Error = OperationTypeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<OperationActor> for String {
    fn from(value: OperationActor) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Backfill,
    Check,
    RawBackup,
    TableFix,
    PrivacyFix,
    Trim,
}

impl OperationKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Backfill => "backfill",
            Self::Check => "check",
            Self::RawBackup => "raw_backup",
            Self::TableFix => "table_fix",
            Self::PrivacyFix => "privacy_fix",
            Self::Trim => "trim",
        }
    }
}

impl fmt::Display for OperationKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for OperationKind {
    type Err = OperationTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "backfill" => Ok(Self::Backfill),
            "check" => Ok(Self::Check),
            "raw_backup" => Ok(Self::RawBackup),
            "table_fix" => Ok(Self::TableFix),
            "privacy_fix" => Ok(Self::PrivacyFix),
            "trim" => Ok(Self::Trim),
            other => Err(OperationTypeError::InvalidKind(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationPhase {
    Submitted,
    Planning,
    Executing,
    Validating,
    Completed,
}

impl OperationPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Planning => "planning",
            Self::Executing => "executing",
            Self::Validating => "validating",
            Self::Completed => "completed",
        }
    }
}

impl fmt::Display for OperationPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for OperationPhase {
    type Err = OperationTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "submitted" => Ok(Self::Submitted),
            "planning" => Ok(Self::Planning),
            "executing" => Ok(Self::Executing),
            "validating" => Ok(Self::Validating),
            "completed" => Ok(Self::Completed),
            other => Err(OperationTypeError::InvalidPhase(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Submitted,
    Planned,
    Running,
    Paused,
    Cancelling,
    Cancelled,
    Failed,
    Succeeded,
}

impl OperationStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Planned => "planned",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
            Self::Succeeded => "succeeded",
        }
    }
}

impl fmt::Display for OperationStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for OperationStatus {
    type Err = OperationTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "submitted" => Ok(Self::Submitted),
            "planned" => Ok(Self::Planned),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "cancelling" => Ok(Self::Cancelling),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            "succeeded" => Ok(Self::Succeeded),
            other => Err(OperationTypeError::InvalidStatus(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationState {
    NotRequested,
    Requested,
    Accepted,
}

impl CancellationState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotRequested => "not_requested",
            Self::Requested => "requested",
            Self::Accepted => "accepted",
        }
    }
}

impl fmt::Display for CancellationState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CancellationState {
    type Err = OperationTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "not_requested" => Ok(Self::NotRequested),
            "requested" => Ok(Self::Requested),
            "accepted" => Ok(Self::Accepted),
            other => Err(OperationTypeError::InvalidCancellationState(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationCursor {
    pub label: String,
    pub position: u64,
}

impl OperationCursor {
    #[must_use]
    pub fn initial(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            position: 0,
        }
    }

    pub fn advance_to(&mut self, next: u64) -> Result<(), OperationTypeError> {
        if next < self.position {
            return Err(OperationTypeError::CursorMovedBackwards {
                current: self.position,
                next,
            });
        }
        self.position = next;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitPolicy {
    #[serde(default = "default_max_rows_per_second")]
    pub max_rows_per_second: Option<u64>,
    #[serde(default = "default_max_bytes_per_second")]
    pub max_bytes_per_second: Option<u64>,
    #[serde(default = "default_max_source_requests_per_second")]
    pub max_source_requests_per_second: Option<u64>,
    #[serde(default = "default_max_destination_writes_per_second")]
    pub max_destination_writes_per_second: Option<u64>,
    #[serde(default = "default_max_parallel_chunks")]
    pub max_parallel_chunks: u16,
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
    #[serde(default = "default_pause_ms")]
    pub pause_ms: u64,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default)]
    pub maintenance_windows_utc: Vec<MaintenanceWindow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceWindow {
    pub start_minute_utc: u16,
    pub end_minute_utc: u16,
}

impl Default for RateLimitPolicy {
    fn default() -> Self {
        Self {
            max_rows_per_second: default_max_rows_per_second(),
            max_bytes_per_second: default_max_bytes_per_second(),
            max_source_requests_per_second: default_max_source_requests_per_second(),
            max_destination_writes_per_second: default_max_destination_writes_per_second(),
            max_parallel_chunks: default_max_parallel_chunks(),
            batch_size: default_batch_size(),
            pause_ms: default_pause_ms(),
            retry_backoff_ms: default_retry_backoff_ms(),
            maintenance_windows_utc: Vec::new(),
        }
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "serde default function must return the Option field type"
)]
const fn default_max_rows_per_second() -> Option<u64> {
    Some(100)
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "serde default function must return the Option field type"
)]
const fn default_max_bytes_per_second() -> Option<u64> {
    Some(1_000_000)
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "serde default function must return the Option field type"
)]
const fn default_max_source_requests_per_second() -> Option<u64> {
    Some(5)
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "serde default function must return the Option field type"
)]
const fn default_max_destination_writes_per_second() -> Option<u64> {
    Some(5)
}

const fn default_max_parallel_chunks() -> u16 {
    1
}

const fn default_batch_size() -> u64 {
    100
}

const fn default_pause_ms() -> u64 {
    100
}

const fn default_retry_backoff_ms() -> u64 {
    1_000
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRequest {
    pub operation_id: OperationId,
    pub kind: OperationKind,
    pub actor: OperationActor,
    pub target_tables: Vec<String>,
    pub dry_run: bool,
    pub rate_limit: RateLimitPolicy,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationResult {
    pub status: OperationStatus,
    pub rows_scanned: u64,
    pub rows_written: u64,
    pub rows_deleted: u64,
    pub error_class: Option<String>,
    pub summary: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationEventKind {
    Submitted,
    Planned,
    Started,
    Paused,
    Resumed,
    CursorAdvanced,
    CancellationRequested,
    CancellationAccepted,
    Failed,
    Succeeded,
}

impl OperationEventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Planned => "planned",
            Self::Started => "started",
            Self::Paused => "paused",
            Self::Resumed => "resumed",
            Self::CursorAdvanced => "cursor_advanced",
            Self::CancellationRequested => "cancellation_requested",
            Self::CancellationAccepted => "cancellation_accepted",
            Self::Failed => "failed",
            Self::Succeeded => "succeeded",
        }
    }
}

impl fmt::Display for OperationEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for OperationEventKind {
    type Err = OperationTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "submitted" => Ok(Self::Submitted),
            "planned" => Ok(Self::Planned),
            "started" => Ok(Self::Started),
            "paused" => Ok(Self::Paused),
            "resumed" => Ok(Self::Resumed),
            "cursor_advanced" => Ok(Self::CursorAdvanced),
            "cancellation_requested" => Ok(Self::CancellationRequested),
            "cancellation_accepted" => Ok(Self::CancellationAccepted),
            "failed" => Ok(Self::Failed),
            "succeeded" => Ok(Self::Succeeded),
            other => Err(OperationTypeError::InvalidEventKind(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationEvent {
    pub operation_id: OperationId,
    pub event_id: u64,
    pub occurred_at_ms: i64,
    pub kind: OperationEventKind,
    pub phase: OperationPhase,
    pub status: OperationStatus,
    pub cursor: Option<OperationCursor>,
    pub counts: serde_json::Value,
    pub message: Option<String>,
}
