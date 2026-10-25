use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use super::{OperationCursor, OperationId, OperationPhase, OperationStatus, OperationTypeError};

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
