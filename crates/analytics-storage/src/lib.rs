mod aux_storage_client;
mod aws_stream;
mod current_row;
mod error;
mod execution;
mod facade;
mod ingest_hashed_range;
pub(crate) mod planning;
mod poller;
mod poller_config;
mod retention;
mod types;

pub use aux_storage_client::{
    AuxStorageCoordinationClient, AuxStorageLeaseClient, AuxStorageLeaseOutcome, BootstrapLease,
    ChangeIndexMarker, IngestRateRollup, ListChangeIndexMarkersRequest,
    ListChangeIndexMarkersResponse, ProcessorHeartbeat, ProcessorState, SlotLease,
    SlotLeaseOutcome, SourceProgress,
};
pub use current_row::{
    AuxStorageCurrentRowClient, AuxStorageCurrentRowPageReader, AuxStorageCurrentRowScanRequest,
    DynamoDbCurrentRowClient, DynamoDbCurrentRowPageReader, DynamoDbCurrentRowScanRequest,
    DynamoDbCurrentRowScanResponse,
};
pub use error::{AnalyticsStorageError, AnalyticsStorageResult};
pub use execution::{BackfillExecutionInputs, SnapshotChunkSource, StreamCatchupSource};
pub use ingest_hashed_range::{
    ChangeVersionstamp, LeaseToken, MarkerScanDecision, ProcessorGeneration, ProcessorId,
    ProcessorMember, ProcessorMode, ProgressDecision, SlotId, SlotLeaseAction, SlotLeasePlan,
    SlotLeaseState, TrimDecision, marker_scan_decision, owned_slots, plan_slot_leases,
    progress_decision, rendezvous_owner, trim_decision,
};
pub use planning::{SourceTablePlan, table_plans};
pub use poller::SourcePoller;
pub use poller_config::{PollRequest, PollerConfig};
pub use retention::RetentionPolicyLookup;
pub use types::{
    PollBatch, PolledRecord, SnapshotChunk, SnapshotChunkCheckpoint, SourceCheckpoint,
};

#[cfg(test)]
mod aux_storage_client_tests;
#[cfg(test)]
mod aws_stream_tests;
#[cfg(test)]
mod current_row_aux_storage_tests;
#[cfg(test)]
mod current_row_dynamodb_tests;
#[cfg(test)]
mod current_row_test_support;
#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod poller_tests;
#[cfg(test)]
mod quint_ingest_hashed_range_tests;
#[cfg(test)]
mod quint_polling_restart_resume_tests;
#[cfg(test)]
mod quint_polling_storage_tests;
#[cfg(test)]
mod storage_tests;
