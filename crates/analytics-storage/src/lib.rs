mod aux_storage_client;
mod aux_storage_http;
mod aws_client;
mod aws_stream;
mod constants;
mod current_row;
mod error;
mod execution;
mod planning;
mod poller;
mod poller_config;
mod retention;
mod source_batch;
mod source_routes;
mod stream_conversion;

pub use current_row::{
    AuxStorageCurrentRowClient, AuxStorageCurrentRowPageReader, AuxStorageCurrentRowScanRequest,
    DynamoDbCurrentRowClient, DynamoDbCurrentRowPageReader, DynamoDbCurrentRowScanRequest,
    DynamoDbCurrentRowScanResponse,
};
pub use error::{AnalyticsStorageError, AnalyticsStorageResult, LiveDatasetError};
pub use execution::{BackfillExecutionInputs, SnapshotChunkSource, StreamCatchupSource};
pub use planning::{SourceTablePlan, SourceTablePlanView};
pub use poller::SourcePoller;
pub use poller_config::{PollRequest, PollerConfig};
pub use retention::RetentionPolicyLookup;
pub use source_batch::{
    PollBatch, PolledRecord, SnapshotChunk, SnapshotChunkCheckpoint, SourceBatchScope,
    SourceCheckpoint, SystemStreamBatch, SystemStreamResume,
};

#[cfg(test)]
mod aux_storage_client_tests;
#[cfg(test)]
mod aws_stream_tests;
#[cfg(test)]
mod conversion_performance_tests;
#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod logical_record_key_performance_tests;
#[cfg(test)]
mod source_routes_tests;
#[cfg(test)]
mod storage_tests;

mod live_dataset;
pub use live_dataset::{
    ApplicationMutation, ApplicationRecord, ApplicationSource, LiveDatasetManager,
};
