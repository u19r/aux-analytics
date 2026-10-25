use std::{fmt, num::NonZeroU64};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

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
    #[error("invalid operation chunk key {0}")]
    InvalidOperationChunkKey(String),
    #[error("invalid operation event kind {0}")]
    InvalidEventKind(String),
    #[error("maintenance execution generation must be non-zero")]
    InvalidMaintenanceExecutionGeneration,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema, ToSchema,
)]
#[serde(try_from = "String", into = "String")]
pub struct OperationId(String);

impl OperationId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, OperationTypeError> {
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
        Self::try_new(value)
    }
}

impl From<OperationId> for String {
    fn from(value: OperationId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(try_from = "String", into = "String")]
pub struct OperationActor(String);

impl OperationActor {
    pub fn try_new(value: impl Into<String>) -> Result<Self, OperationTypeError> {
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
        Self::try_new(value)
    }
}

impl From<OperationActor> for String {
    fn from(value: OperationActor) -> Self {
        value.0
    }
}

/// Stable identity for one externally visible operation effect.
///
/// A retry must use the same key for the same logical chunk or repair. The key
/// is deliberately derived only from the immutable operation id and logical
/// position; it is never a random attempt id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperationChunkKey(String);

impl OperationChunkKey {
    #[must_use]
    pub fn for_operation(operation_id: &OperationId, label: &str, position: u64) -> Self {
        Self(format!("{}:{label}:{position}", operation_id.as_str()))
    }

    #[must_use]
    pub fn for_repair(operation_id: &OperationId, table_name: &str, record_key: &[u8]) -> Self {
        let mut encoded_key = String::with_capacity(record_key.len().saturating_mul(2));
        for byte in record_key {
            use std::fmt::Write as _;
            write!(&mut encoded_key, "{byte:02x}").expect("writing an effect key cannot fail");
        }
        Self(format!(
            "{}:repair:{table_name}:{encoded_key}",
            operation_id.as_str()
        ))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Destination authority for one maintenance execution attempt.
///
/// The operation id is the stable logical identity shared by retries. The
/// generation is the monotonic lease epoch allocated by `OperationStore`;
/// destination transactions must compare it before applying an effect.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaintenanceExecutionAuthority {
    execution_id: OperationId,
    generation: NonZeroU64,
}

impl MaintenanceExecutionAuthority {
    pub fn try_new(execution_id: OperationId, generation: u64) -> Result<Self, OperationTypeError> {
        let generation = NonZeroU64::new(generation)
            .ok_or(OperationTypeError::InvalidMaintenanceExecutionGeneration)?;
        Ok(Self {
            execution_id,
            generation,
        })
    }

    #[must_use]
    pub fn execution_id(&self) -> &OperationId {
        &self.execution_id
    }

    #[must_use]
    pub const fn generation(&self) -> NonZeroU64 {
        self.generation
    }

    #[must_use]
    pub const fn fence(&self) -> NonZeroU64 {
        self.generation
    }
}

impl TryFrom<String> for OperationChunkKey {
    type Error = OperationTypeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.trim().is_empty() {
            return Err(OperationTypeError::InvalidOperationChunkKey(value));
        }
        Ok(Self(value))
    }
}

impl From<OperationChunkKey> for String {
    fn from(value: OperationChunkKey) -> Self {
        value.0
    }
}
