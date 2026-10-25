//! Durable snapshot and catch-up progress across retries.

use std::collections::BTreeSet;

use super::{
    BackfillExecutionMetrics, BackfillExecutionProgress, BackfillExecutionRequest,
    LocalBackfillChunk, LocalStreamUpdate,
};
use crate::{OperationId, OperationStore, OperationStoreResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillExecutionState {
    pub(super) completed_chunks: BTreeSet<u64>,
    pub(super) checkpoint: u64,
    pub(super) stream_checkpoint: u64,
    pub(super) metrics: BackfillExecutionMetrics,
}

impl BackfillExecutionState {
    pub(super) fn from_store(
        store: &OperationStore,
        operation_id: &OperationId,
        request: &BackfillExecutionRequest,
        chunks: &[LocalBackfillChunk],
    ) -> OperationStoreResult<Self> {
        let operation = store.show_operation(operation_id)?;
        let (cursor_label, completed_until) =
            operation.cursor.as_ref().map_or(("chunk", 0), |cursor| {
                (cursor.label.as_str(), cursor.position)
            });
        let completed_chunks = chunks
            .iter()
            .filter(|chunk| cursor_label == "stream" || chunk.chunk_id <= completed_until)
            .map(|chunk| chunk.chunk_id)
            .collect::<BTreeSet<_>>();
        let completed_chunk_count = completed_chunks.len() as u64;
        let mut metrics =
            serde_json::from_value::<BackfillExecutionProgress>(operation.progress)?.metrics;
        metrics.chunks_total = chunks.len() as u64;
        metrics.chunks_completed = metrics.chunks_completed.max(completed_chunk_count);
        metrics.throttle_pause_ms = metrics.throttle_pause_ms.max(request.rate_limit.pause_ms);
        metrics.max_parallel_chunks = request.rate_limit.max_parallel_chunks;
        metrics.effective_parallel_chunks = request.rate_limit.max_parallel_chunks.max(1);
        metrics.configured_max_rows_per_second = request.rate_limit.max_rows_per_second;
        metrics.configured_max_bytes_per_second = request.rate_limit.max_bytes_per_second;
        metrics.configured_max_source_requests_per_second =
            request.rate_limit.max_source_requests_per_second;
        metrics.configured_max_destination_writes_per_second =
            request.rate_limit.max_destination_writes_per_second;
        Ok(Self {
            completed_chunks,
            checkpoint: completed_until,
            stream_checkpoint: if cursor_label == "stream" {
                completed_until
            } else {
                0
            },
            metrics,
        })
    }

    pub(super) fn mark_chunk_completed(&mut self, chunk: &LocalBackfillChunk, rows_written: u64) {
        if self.completed_chunks.insert(chunk.chunk_id) {
            self.metrics.chunks_completed += 1;
            self.metrics.rows_scanned += chunk.row_count();
            self.metrics.rows_written += rows_written;
            self.checkpoint = self.checkpoint.max(chunk.chunk_id);
        }
    }

    pub(super) fn mark_chunk_retried(&mut self) {
        self.metrics.chunks_retried += 1;
    }

    pub(super) fn apply_stream_update(&mut self, update: &LocalStreamUpdate, rows_written: u64) {
        self.metrics.stream_rows_applied += update.row_count();
        self.metrics.rows_written += rows_written;
        self.checkpoint = self.checkpoint.max(update.cursor);
        self.stream_checkpoint = self.stream_checkpoint.max(update.cursor);
    }
}
