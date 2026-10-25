use analytics_contract::{PrivacyPolicy, StorageStreamRecord};

use crate::{
    OperationCursor, OperationEventKind, OperationKind, OperationPhase, OperationRateLimiter,
    OperationRequest, OperationStatus, OperationStore, OperationStoreResult, RateLimitClock,
    RateLimitUsage, StoredOperation, SystemRateLimitClock,
};

mod batch;
mod entry;
mod execution_loop;
mod support;

pub use entry::{BackfillExecutionInput, LocalBackfillExecutor};
pub(crate) use support::{
    PrivacyBackfillMetrics, create_or_resume_operation, effective_rows_per_second,
    ensure_operation_can_write, fail_after_error, filter_chunks, filter_stream_updates,
    metrics_json, pause_for_retry, recover_destination_failure, release_stale_effect_for_retry,
};

mod accounting;
mod error;
mod fixture;
mod report;
mod request;
mod source;
mod state;

use accounting::BackfillBatchAccounting;
#[cfg(test)]
pub(crate) use accounting::serialized_len;
pub use error::{BackfillExecutionError, BackfillExecutionResult};
pub use fixture::{LocalBackfillChunk, LocalBackfillFixture, LocalStreamUpdate};
pub use report::{BackfillExecutionMetrics, BackfillExecutionProgress, BackfillExecutionReport};
pub use request::BackfillExecutionRequest;
pub use source::{
    BackfillDestinationWriter, CountingBackfillDestinationWriter, SnapshotChunkSource,
    StreamCatchupSource,
};
pub use state::BackfillExecutionState;
