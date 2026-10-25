use super::*;

#[derive(Debug, Error)]
pub enum CheckError {
    #[error(transparent)]
    Store(#[from] OperationStoreError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("check request must target at least one table")]
    EmptyTargetTables,
    #[error("check sample limit must be at least 1")]
    InvalidSampleLimit,
    #[error("source row reader failed: {0}")]
    SourceRead(String),
    #[error("destination row reader failed: {0}")]
    DestinationRead(String),
    #[error(
        "check failed: {operation_error}; recording terminal failure also failed: \
         {persistence_error}"
    )]
    FailurePersistence {
        operation_error: String,
        persistence_error: String,
    },
    #[error(transparent)]
    RateLimit(#[from] RateLimitDenied),
    #[error("{backend:?} check backend is unsupported: {reason}")]
    UnsupportedBackend {
        backend: CheckBackendKind,
        reason: String,
    },
    #[error("{backend:?} check backend is only partially supported: {reason}")]
    PartiallySupportedBackend {
        backend: CheckBackendKind,
        reason: String,
    },
    #[error("check backend configuration is invalid: {0}")]
    InvalidBackendConfiguration(String),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error("cannot start DuckDB check scan worker: {0}")]
    ScanWorkerStart(#[source] std::io::Error),
    #[error("DuckDB check scan worker panicked")]
    ScanWorkerPanicked,
}

pub type CheckResult<T> = Result<T, CheckError>;
