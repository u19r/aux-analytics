use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use super::identifiers::OperationTypeError;

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
