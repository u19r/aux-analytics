use thiserror::Error;

use crate::{
    OperationId, OperationKind, OperationStoreError, PrivacyFixError, RateLimitDenied,
    RawBackupError, TableFixError, TrimError,
};

#[derive(Debug, Error)]
pub enum MaintenanceError {
    #[error(transparent)]
    Store(#[from] OperationStoreError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error(transparent)]
    TableFix(#[from] TableFixError),
    #[error(transparent)]
    PrivacyFix(#[from] PrivacyFixError),
    #[error(transparent)]
    Trim(#[from] TrimError),
    #[error(transparent)]
    RawBackup(#[from] RawBackupError),
    #[error(transparent)]
    RateLimit(#[from] RateLimitDenied),
    #[error("{kind} maintenance executor cannot handle the submitted request")]
    UnsupportedExecutor { kind: OperationKind },
    #[error("operation {operation_id} already has a different maintenance request")]
    ConflictingOperation { operation_id: OperationId },
    #[error("operation {operation_id} is already running")]
    OperationInProgress { operation_id: OperationId },
    #[error("maintenance destination initialization failed: {message}")]
    DestinationInitialization { message: String },
    #[error(
        "maintenance executor failed for {operation_id}: {executor_error}; recording terminal \
         failure also failed: {terminal_error}"
    )]
    ExecutionTerminalUpdateFailed {
        operation_id: OperationId,
        executor_error: String,
        terminal_error: String,
    },
}

pub type MaintenanceResult<T> = Result<T, MaintenanceError>;
