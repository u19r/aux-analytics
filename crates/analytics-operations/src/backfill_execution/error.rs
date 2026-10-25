use thiserror::Error;

use crate::{OperationStoreError, RateLimitDenied};

#[derive(Debug, Error)]
pub enum BackfillExecutionError {
    #[error(transparent)]
    Store(#[from] OperationStoreError),
    #[error("backfill execution requires at least one chunk")]
    EmptyFixture,
    #[error("backfill execution must target at least one table")]
    EmptyTargetTables,
    #[error("backfill chunk parallelism must be at least 1")]
    InvalidParallelism,
    #[error("snapshot chunk source failed: {0}")]
    SnapshotSource(String),
    #[error("stream catch-up source failed: {0}")]
    StreamSource(String),
    #[error("backfill destination write failed: {0}")]
    DestinationWrite(String),
    #[error("backfill batch encoding failed: {0}")]
    BatchEncoding(#[from] serde_json::Error),
    #[error(
        "backfill destination failure recovery failed: {destination_error}; durable recovery: \
         {recovery_error}"
    )]
    EffectRecoveryFailed {
        destination_error: String,
        recovery_error: String,
    },
    #[error(
        "backfill operation error {operation_error}; durable lifecycle update failed: \
         {persistence_error}"
    )]
    LifecyclePersistenceFailed {
        operation_error: String,
        persistence_error: String,
    },
    #[error(transparent)]
    RateLimit(#[from] RateLimitDenied),
    #[error("backfill operation was cancelled before the next batch")]
    Cancelled,
    #[error(transparent)]
    PrivacyPolicy(#[from] analytics_contract::PrivacyPolicyError),
}

pub type BackfillExecutionResult<T> = Result<T, BackfillExecutionError>;
