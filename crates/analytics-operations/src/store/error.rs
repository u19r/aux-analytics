use super::*;

#[derive(Debug, Error)]
pub enum OperationStoreError {
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Type(#[from] OperationTypeError),
    #[error("operation {0} already exists")]
    DuplicateOperation(OperationId),
    #[error("operation {0} already exists with a different request")]
    ConflictingOperation(OperationId),
    #[error("operation {0} not found")]
    OperationNotFound(OperationId),
    #[error("operation {operation_id} cannot transition from {current_status} to {next_status}")]
    InvalidStatusTransition {
        operation_id: OperationId,
        current_status: OperationStatus,
        next_status: OperationStatus,
    },
    #[error("operation {operation_id} is not authorized for actor {actor:?}")]
    UnauthorizedActor {
        operation_id: OperationId,
        actor: OperationActor,
    },
    #[error("operation {operation_id} is already owned by an active execution attempt")]
    ExecutionAttemptInProgress { operation_id: OperationId },
    #[error(
        "operation {operation_id} durable state is stale for expected revision {expected_revision}"
    )]
    StaleOperationState {
        operation_id: OperationId,
        expected_revision: u64,
    },
    #[error("operation {operation_id} has an unfinished effect {effect_key}")]
    EffectInProgress {
        operation_id: OperationId,
        effect_key: String,
    },
    #[error(
        "operation {operation_id} cannot accept cancellation while effect {effect_key} is pending"
    )]
    PendingEffect {
        operation_id: OperationId,
        effect_key: String,
    },
    #[error("operation audit counts cannot include sensitive field {0}")]
    UnsafeAuditCountField(String),
}

pub type OperationStoreResult<T> = Result<T, OperationStoreError>;
