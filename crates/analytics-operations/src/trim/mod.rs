use analytics_contract::INTERNAL_EXPIRY_COLUMN;
use sha2::{Digest, Sha256};

use crate::{
    MaintenanceExecutionAuthority, OperationChunkKey, OperationRateLimiter, RateLimitClock,
    RateLimitUsage, enforce_rate_limit_with_limiter,
};

mod destination;
mod execution;

pub use destination::DuckDbTrimRows;
pub(crate) use destination::preflight_rate_limit;
pub(crate) use execution::TrimExecution;

mod candidate;
mod error;
mod predicate;
mod report;
mod request;

pub use candidate::{TrimCandidateSource, TrimDestination};
pub use error::{TrimError, TrimResult};
pub use predicate::{TrimPredicate, TrimPredicateValue};
pub use report::{TrimCandidateSnapshot, TrimDeletionOutcome, TrimReport};
pub use request::{TrimRequest, TrimTarget};

#[cfg(test)]
mod predicate_tests;
