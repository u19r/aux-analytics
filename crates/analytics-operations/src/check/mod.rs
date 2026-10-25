#[cfg(test)]
use std::cell::Cell;
use std::{cmp::Ordering, collections::HashSet, vec};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    CheckBackendKind, CheckBackendSupport, CheckRowSource, ExecutionAttempt, OperationActor,
    OperationEventKind, OperationId, OperationKind, OperationPhase, OperationRateLimiter,
    OperationRequest, OperationStatus, OperationStore, OperationStoreError, RateLimitClock,
    RateLimitDenied, RateLimitPolicy, RateLimitUsage, StoredOperation, SystemRateLimitClock,
    check_position::CheckSourcePosition, enforce_rate_limit_with_limiter,
};

mod error;
pub use error::{CheckError, CheckResult};

mod readers;
mod report;
mod request;
mod row_stream;
mod rows;

pub use readers::{DestinationRows, SourceTruthRows};
pub use report::{CheckMetrics, CheckOutcome, CheckReport, CheckSamples, LocalCheckExecutor};
pub use request::{CheckComparisonStrategy, CheckOutputFormat, CheckRequest};
pub use row_stream::{CheckRowStream, CheckRowStreamItem, CheckRowStreamSource};
pub(crate) use rows::filter_target_tables;
pub use rows::{CheckRow, LocalCheckDataset};
#[cfg(test)]
pub(crate) use rows::{
    check_row_clone_count, filter_target_tables_baseline, reset_check_row_clone_count,
};

mod comparison;
#[cfg(test)]
mod comparison_tests;
mod execution;

pub(crate) use comparison::{
    BackendMappedCheckRowStream, compare_row_streams, map_backend_read_error, row_sort_key,
};
