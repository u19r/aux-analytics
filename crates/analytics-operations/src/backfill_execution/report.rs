use serde::{Deserialize, Serialize};

use crate::OperationId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillExecutionReport {
    pub operation_id: OperationId,
    pub metrics: BackfillExecutionMetrics,
    pub completed_chunks: Vec<u64>,
    pub checkpoint: u64,
    pub resumed_from_chunk: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BackfillExecutionMetrics {
    pub chunks_total: u64,
    pub chunks_completed: u64,
    pub chunks_retried: u64,
    pub rows_scanned: u64,
    pub rows_written: u64,
    pub stream_rows_applied: u64,
    pub validation_runs: u64,
    pub audit_events_emitted: u64,
    pub max_parallel_chunks: u16,
    pub effective_parallel_chunks: u16,
    pub throttle_pause_ms: u64,
    pub retry_delay_ms: u64,
    pub effective_rows_per_second: u64,
    pub configured_max_rows_per_second: Option<u64>,
    pub configured_max_bytes_per_second: Option<u64>,
    pub configured_max_source_requests_per_second: Option<u64>,
    pub configured_max_destination_writes_per_second: Option<u64>,
    pub privacy_policy_version: Option<String>,
    pub privacy_dropped_fields: u64,
    pub privacy_dropped_records: u64,
}

/// Durable execution progress. Metrics remain cumulative across retryable
/// pauses; the retryable marker describes the lifecycle state and never
/// replaces the metrics document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BackfillExecutionProgress {
    #[serde(flatten)]
    pub metrics: BackfillExecutionMetrics,
    pub retryable: bool,
}
