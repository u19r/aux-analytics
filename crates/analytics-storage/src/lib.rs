mod aux_storage_client;
mod aws_stream;
mod current_row;
mod error;
mod execution;
mod facade;
pub(crate) mod planning;
mod poller;
mod poller_config;
mod retention;
mod source_routes;
mod types;

pub use aux_storage_client::{AuxStorageLeaseClient, AuxStorageLeaseOutcome};
pub use current_row::{
    AuxStorageCurrentRowClient, AuxStorageCurrentRowPageReader, AuxStorageCurrentRowScanRequest,
    DynamoDbCurrentRowClient, DynamoDbCurrentRowPageReader, DynamoDbCurrentRowScanRequest,
    DynamoDbCurrentRowScanResponse,
};
pub use error::{AnalyticsStorageError, AnalyticsStorageResult};
pub use execution::{BackfillExecutionInputs, SnapshotChunkSource, StreamCatchupSource};
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
mod quint_polling_restart_resume_tests;
#[cfg(test)]
mod quint_polling_storage_tests;
#[cfg(test)]
mod source_routes_tests;
#[cfg(test)]
mod storage_tests;
