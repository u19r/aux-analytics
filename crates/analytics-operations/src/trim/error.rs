use thiserror::Error;

use crate::{OperationStoreError, OperationTypeError};

#[derive(Debug, Error)]
pub enum TrimError {
    #[error(transparent)]
    OperationType(#[from] OperationTypeError),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error("trim table name cannot be empty")]
    EmptyTableName,
    #[error("trim table name is not a safe DuckDB identifier: {0}")]
    InvalidTableIdentifier(String),
    #[error("trim row key cannot be empty")]
    EmptyRowKey,
    #[error("trim row predicate cannot be empty")]
    EmptyPredicate,
    #[error("trim row predicate is not safe for bounded row deletion")]
    UnsafePredicate,
    #[error("trim horizon must be greater than zero")]
    EmptyHorizon,
    #[error("trim request requires at least one selected row key")]
    EmptyRowSelection,
    #[error("destructive trim execution requires a confirmation token")]
    MissingConfirmationToken,
    #[error("trim target is not supported by this destination")]
    UnsupportedTarget,
    #[error("trim candidate source cannot provide a revisioned membership snapshot")]
    MissingCandidateSnapshot,
    #[error("trim candidate revision or membership changed before commit")]
    StaleCandidateSnapshot,
    #[error("trim destination authority does not match the execution attempt")]
    DestinationAuthorityMismatch,
    #[error("trim destination authority is stale")]
    StaleDestinationAuthority,
    #[error("trim destination authority is ambiguous")]
    AmbiguousDestinationAuthority,
    #[error("trim destination receipt conflicts with the requested effect")]
    ConflictingDestinationReceipt,
    #[error(transparent)]
    RateLimit(#[from] crate::RateLimitDenied),
    #[error(transparent)]
    OperationStore(#[from] OperationStoreError),
    #[error(
        "trim destination exceeded the candidate bound {candidate_rows}; committed rows: \
         {deleted_rows}"
    )]
    DeleteCountExceededDryRun {
        candidate_rows: u64,
        deleted_rows: u64,
    },
}

pub type TrimResult<T> = Result<T, TrimError>;
