use std::collections::BTreeSet;

use analytics_contract::{PrivacyPolicy, StorageStreamRecord};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    OperationActor, OperationCursor, OperationEventKind, OperationId, OperationKind,
    OperationPhase, OperationRateLimiter, OperationRequest, OperationStatus, OperationStore,
    OperationStoreError, OperationStoreResult, RateLimitPolicy, RateLimitUsage,
};

#[derive(Debug, Error)]
pub enum BackfillExecutionError {
    #[error(transparent)]
    Store(#[from] OperationStoreError),
    #[error("backfill execution requires at least one chunk")]
    EmptyFixture,
    #[error("backfill chunk parallelism must be at least 1")]
    InvalidParallelism,
    #[error("snapshot chunk source failed: {0}")]
    SnapshotSource(String),
    #[error("stream catch-up source failed: {0}")]
    StreamSource(String),
    #[error("backfill destination write failed: {0}")]
    DestinationWrite(String),
    #[error(transparent)]
    PrivacyPolicy(#[from] analytics_contract::PrivacyPolicyError),
}

pub type BackfillExecutionResult<T> = Result<T, BackfillExecutionError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillExecutionRequest {
    pub operation_id: OperationId,
    pub actor: OperationActor,
    pub target_tables: Vec<String>,
    pub rate_limit: RateLimitPolicy,
    pub fail_once_at_chunk: Option<u64>,
}

pub trait SnapshotChunkSource {
    fn snapshot_chunks(&self) -> BackfillExecutionResult<Vec<LocalBackfillChunk>>;
}

pub trait StreamCatchupSource {
    fn stream_updates_after_snapshot(&self) -> BackfillExecutionResult<Vec<LocalStreamUpdate>>;
}

pub trait BackfillDestinationWriter {
    fn write_snapshot_chunk(
        &mut self,
        chunk: &LocalBackfillChunk,
    ) -> BackfillExecutionResult<BackfillWriteOutcome>;

    fn write_stream_update(
        &mut self,
        update: &LocalStreamUpdate,
    ) -> BackfillExecutionResult<BackfillWriteOutcome>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillWriteOutcome {
    pub rows_written: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CountingBackfillDestinationWriter;

impl BackfillDestinationWriter for CountingBackfillDestinationWriter {
    fn write_snapshot_chunk(
        &mut self,
        chunk: &LocalBackfillChunk,
    ) -> BackfillExecutionResult<BackfillWriteOutcome> {
        Ok(BackfillWriteOutcome {
            rows_written: chunk.rows,
        })
    }

    fn write_stream_update(
        &mut self,
        update: &LocalStreamUpdate,
    ) -> BackfillExecutionResult<BackfillWriteOutcome> {
        Ok(BackfillWriteOutcome {
            rows_written: update.rows,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalBackfillFixture {
    pub chunks: Vec<LocalBackfillChunk>,
    pub stream_updates: Vec<LocalStreamUpdate>,
}

impl LocalBackfillFixture {
    #[must_use]
    pub fn single_table(chunk_rows: &[u64], stream_updates: &[u64]) -> Self {
        Self {
            chunks: chunk_rows
                .iter()
                .enumerate()
                .map(|(index, rows)| LocalBackfillChunk {
                    chunk_id: u64::try_from(index + 1).unwrap_or(u64::MAX),
                    rows: *rows,
                    records: Vec::new(),
                })
                .collect(),
            stream_updates: stream_updates
                .iter()
                .copied()
                .map(|cursor| LocalStreamUpdate {
                    cursor,
                    rows: 1,
                    records: Vec::new(),
                })
                .collect(),
        }
    }
}

impl SnapshotChunkSource for LocalBackfillFixture {
    fn snapshot_chunks(&self) -> BackfillExecutionResult<Vec<LocalBackfillChunk>> {
        Ok(self.chunks.clone())
    }
}

impl StreamCatchupSource for LocalBackfillFixture {
    fn stream_updates_after_snapshot(&self) -> BackfillExecutionResult<Vec<LocalStreamUpdate>> {
        Ok(self.stream_updates.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalBackfillChunk {
    pub chunk_id: u64,
    pub rows: u64,
    #[serde(default)]
    pub records: Vec<StorageStreamRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalStreamUpdate {
    pub cursor: u64,
    pub rows: u64,
    #[serde(default)]
    pub records: Vec<StorageStreamRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillExecutionReport {
    pub operation_id: OperationId,
    pub metrics: BackfillExecutionMetrics,
    pub completed_chunks: Vec<u64>,
    pub checkpoint: u64,
    pub resumed_from_chunk: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillExecutionState {
    completed_chunks: BTreeSet<u64>,
    checkpoint: u64,
    metrics: BackfillExecutionMetrics,
}

impl BackfillExecutionState {
    fn from_store(
        store: &OperationStore,
        operation_id: &OperationId,
        request: &BackfillExecutionRequest,
        chunks: &[LocalBackfillChunk],
    ) -> OperationStoreResult<Self> {
        let operation = store.show_operation(operation_id)?;
        let completed_until = operation.cursor.map_or(0, |cursor| cursor.position);
        let completed_chunks = chunks
            .iter()
            .filter(|chunk| chunk.chunk_id <= completed_until)
            .map(|chunk| chunk.chunk_id)
            .collect::<BTreeSet<_>>();
        Ok(Self {
            completed_chunks,
            checkpoint: completed_until,
            metrics: BackfillExecutionMetrics {
                chunks_total: chunks.len() as u64,
                chunks_completed: completed_until,
                max_parallel_chunks: request.rate_limit.max_parallel_chunks,
                effective_parallel_chunks: request.rate_limit.max_parallel_chunks.max(1),
                throttle_pause_ms: request.rate_limit.pause_ms,
                configured_max_rows_per_second: request.rate_limit.max_rows_per_second,
                configured_max_bytes_per_second: request.rate_limit.max_bytes_per_second,
                configured_max_source_requests_per_second: request
                    .rate_limit
                    .max_source_requests_per_second,
                configured_max_destination_writes_per_second: request
                    .rate_limit
                    .max_destination_writes_per_second,
                ..BackfillExecutionMetrics::default()
            },
        })
    }

    fn mark_chunk_completed(&mut self, chunk: &LocalBackfillChunk, rows_written: u64) {
        if self.completed_chunks.insert(chunk.chunk_id) {
            self.metrics.chunks_completed += 1;
            self.metrics.rows_scanned += chunk.rows;
            self.metrics.rows_written += rows_written;
            self.checkpoint = self.checkpoint.max(chunk.chunk_id);
        }
    }

    fn mark_chunk_retried(&mut self) {
        self.metrics.chunks_retried += 1;
    }

    fn apply_stream_update(&mut self, update: &LocalStreamUpdate, rows_written: u64) {
        self.metrics.stream_rows_applied += update.rows;
        self.metrics.rows_written += rows_written;
        self.checkpoint = self.checkpoint.max(update.cursor);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalBackfillExecutor;

impl LocalBackfillExecutor {
    pub fn execute_with_privacy_policy<Snapshots, Stream>(
        &self,
        store: &OperationStore,
        request: &BackfillExecutionRequest,
        snapshot_source: &Snapshots,
        stream_source: &Stream,
        policy: &PrivacyPolicy,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
    {
        let mut destination = CountingBackfillDestinationWriter;
        self.execute_with_privacy_policy_to_destination(
            store,
            request,
            snapshot_source,
            stream_source,
            &mut destination,
            policy,
        )
    }

    pub fn execute_with_privacy_policy_to_destination<Snapshots, Stream, Destination>(
        &self,
        store: &OperationStore,
        request: &BackfillExecutionRequest,
        snapshot_source: &Snapshots,
        stream_source: &Stream,
        destination: &mut Destination,
        policy: &PrivacyPolicy,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
        Destination: BackfillDestinationWriter,
    {
        let (chunks, chunk_drops) = filter_chunks(snapshot_source.snapshot_chunks()?, policy)?;
        let (stream_updates, stream_drops) =
            filter_stream_updates(stream_source.stream_updates_after_snapshot()?, policy)?;
        let privacy_dropped_fields = chunk_drops.dropped_fields + stream_drops.dropped_fields;
        let privacy_dropped_records = chunk_drops.dropped_records + stream_drops.dropped_records;
        Self::execute_prepared(
            store,
            request,
            &chunks,
            &stream_updates,
            destination,
            Some(policy.version.clone()),
            PrivacyBackfillMetrics {
                dropped_fields: privacy_dropped_fields,
                dropped_records: privacy_dropped_records,
            },
        )
    }

    pub fn execute<Snapshots, Stream>(
        &self,
        store: &OperationStore,
        request: &BackfillExecutionRequest,
        snapshot_source: &Snapshots,
        stream_source: &Stream,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
    {
        let mut destination = CountingBackfillDestinationWriter;
        self.execute_to_destination(
            store,
            request,
            snapshot_source,
            stream_source,
            &mut destination,
        )
    }

    pub fn execute_to_destination<Snapshots, Stream, Destination>(
        &self,
        store: &OperationStore,
        request: &BackfillExecutionRequest,
        snapshot_source: &Snapshots,
        stream_source: &Stream,
        destination: &mut Destination,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
        Destination: BackfillDestinationWriter,
    {
        let chunks = snapshot_source.snapshot_chunks()?;
        let stream_updates = stream_source.stream_updates_after_snapshot()?;
        Self::execute_prepared(
            store,
            request,
            &chunks,
            &stream_updates,
            destination,
            None,
            PrivacyBackfillMetrics::default(),
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "backfill execution keeps durable cursor transitions in one ordered flow"
    )]
    fn execute_prepared(
        store: &OperationStore,
        request: &BackfillExecutionRequest,
        chunks: &[LocalBackfillChunk],
        stream_updates: &[LocalStreamUpdate],
        destination: &mut impl BackfillDestinationWriter,
        privacy_policy_version: Option<String>,
        privacy_metrics: PrivacyBackfillMetrics,
    ) -> BackfillExecutionResult<BackfillExecutionReport> {
        if chunks.is_empty() {
            return Err(BackfillExecutionError::EmptyFixture);
        }
        if request.rate_limit.max_parallel_chunks == 0 {
            return Err(BackfillExecutionError::InvalidParallelism);
        }
        create_or_resume_operation(store, request, chunks.len())?;
        let mut state =
            BackfillExecutionState::from_store(store, &request.operation_id, request, chunks)?;
        state.metrics.privacy_policy_version = privacy_policy_version;
        state.metrics.privacy_dropped_fields = privacy_metrics.dropped_fields;
        state.metrics.privacy_dropped_records = privacy_metrics.dropped_records;
        let limiter = OperationRateLimiter::new(request.rate_limit.clone());
        let resumed_from_chunk = state.checkpoint;
        store.transition_with_counts(
            &request.operation_id,
            OperationPhase::Executing,
            OperationStatus::Running,
            Some(OperationCursor {
                label: "chunk".to_string(),
                position: state.checkpoint,
            }),
            OperationEventKind::Started,
            metrics_json(&state.metrics),
            Some("backfill execution started"),
        )?;
        state.metrics.audit_events_emitted += 1;

        for chunk in chunks {
            if state.completed_chunks.contains(&chunk.chunk_id) {
                continue;
            }
            if request.fail_once_at_chunk == Some(chunk.chunk_id)
                && state.metrics.chunks_retried == 0
            {
                state.mark_chunk_retried();
                let retry_delay_ms = limiter.retry_backoff_ms(state.metrics.chunks_retried);
                state.metrics.retry_delay_ms =
                    state.metrics.retry_delay_ms.saturating_add(retry_delay_ms);
                state.metrics.throttle_pause_ms = state
                    .metrics
                    .throttle_pause_ms
                    .saturating_add(retry_delay_ms);
                store.transition_with_counts(
                    &request.operation_id,
                    OperationPhase::Executing,
                    OperationStatus::Running,
                    Some(OperationCursor {
                        label: "chunk".to_string(),
                        position: state.checkpoint,
                    }),
                    OperationEventKind::Paused,
                    metrics_json(&state.metrics),
                    Some("chunk failed before destination commit; retrying"),
                )?;
                state.metrics.audit_events_emitted += 1;
            }
            let decision = limiter.reserve(RateLimitUsage {
                rows: chunk.rows,
                destination_writes: chunk.rows,
                ..RateLimitUsage::default()
            });
            state.metrics.throttle_pause_ms = state
                .metrics
                .throttle_pause_ms
                .saturating_add(decision.wait_ms);
            let write = destination.write_snapshot_chunk(chunk)?;
            state.mark_chunk_completed(chunk, write.rows_written);
            store.transition_with_counts(
                &request.operation_id,
                OperationPhase::Executing,
                OperationStatus::Running,
                Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: chunk.chunk_id,
                }),
                OperationEventKind::CursorAdvanced,
                metrics_json(&state.metrics),
                Some("backfill chunk committed"),
            )?;
            state.metrics.audit_events_emitted += 1;
        }

        for update in stream_updates {
            let write = destination.write_stream_update(update)?;
            state.apply_stream_update(update, write.rows_written);
        }
        state.metrics.effective_rows_per_second =
            effective_rows_per_second(state.metrics.rows_written, state.metrics.throttle_pause_ms);
        state.metrics.validation_runs += 1;
        store.transition_with_counts(
            &request.operation_id,
            OperationPhase::Validating,
            OperationStatus::Running,
            Some(OperationCursor {
                label: "stream".to_string(),
                position: state.checkpoint,
            }),
            OperationEventKind::CursorAdvanced,
            metrics_json(&state.metrics),
            Some("stream catch-up applied and validation started"),
        )?;
        state.metrics.audit_events_emitted += 1;
        store.transition_with_counts(
            &request.operation_id,
            OperationPhase::Completed,
            OperationStatus::Succeeded,
            Some(OperationCursor {
                label: "stream".to_string(),
                position: state.checkpoint,
            }),
            OperationEventKind::Succeeded,
            metrics_json(&state.metrics),
            Some("backfill execution completed"),
        )?;
        state.metrics.audit_events_emitted += 1;

        Ok(BackfillExecutionReport {
            operation_id: request.operation_id.clone(),
            metrics: state.metrics,
            completed_chunks: state.completed_chunks.into_iter().collect(),
            checkpoint: state.checkpoint,
            resumed_from_chunk,
        })
    }
}

fn create_or_resume_operation(
    store: &OperationStore,
    request: &BackfillExecutionRequest,
    chunk_count: usize,
) -> OperationStoreResult<()> {
    let operation_request = OperationRequest {
        operation_id: request.operation_id.clone(),
        kind: OperationKind::Backfill,
        actor: request.actor.clone(),
        target_tables: request.target_tables.clone(),
        dry_run: false,
        rate_limit: request.rate_limit.clone(),
        payload: serde_json::json!({
            "mode": "local_backfill_execution",
            "chunk_count": chunk_count,
        }),
    };
    match store.create_operation(&operation_request) {
        Ok(()) | Err(OperationStoreError::DuplicateOperation(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

fn metrics_json(metrics: &BackfillExecutionMetrics) -> serde_json::Value {
    serde_json::to_value(metrics).unwrap_or_else(|_| serde_json::json!({}))
}

fn effective_rows_per_second(rows_written: u64, throttle_pause_ms: u64) -> u64 {
    if throttle_pause_ms == 0 {
        return rows_written;
    }
    rows_written.saturating_mul(1_000) / throttle_pause_ms
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PrivacyBackfillMetrics {
    dropped_fields: u64,
    dropped_records: u64,
}

fn filter_chunks(
    chunks: Vec<LocalBackfillChunk>,
    policy: &PrivacyPolicy,
) -> BackfillExecutionResult<(Vec<LocalBackfillChunk>, PrivacyBackfillMetrics)> {
    let mut metrics = PrivacyBackfillMetrics::default();
    let mut filtered_chunks = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let (records, drops) = filter_records(chunk.records, policy)?;
        metrics.dropped_fields += drops.dropped_fields;
        metrics.dropped_records += drops.dropped_records;
        filtered_chunks.push(LocalBackfillChunk {
            chunk_id: chunk.chunk_id,
            rows: if records.is_empty() {
                chunk.rows.saturating_sub(drops.dropped_records)
            } else {
                records.len() as u64
            },
            records,
        });
    }
    Ok((filtered_chunks, metrics))
}

fn filter_stream_updates(
    updates: Vec<LocalStreamUpdate>,
    policy: &PrivacyPolicy,
) -> BackfillExecutionResult<(Vec<LocalStreamUpdate>, PrivacyBackfillMetrics)> {
    let mut metrics = PrivacyBackfillMetrics::default();
    let mut filtered_updates = Vec::with_capacity(updates.len());
    for update in updates {
        let (records, drops) = filter_records(update.records, policy)?;
        metrics.dropped_fields += drops.dropped_fields;
        metrics.dropped_records += drops.dropped_records;
        filtered_updates.push(LocalStreamUpdate {
            cursor: update.cursor,
            rows: if records.is_empty() {
                update.rows.saturating_sub(drops.dropped_records)
            } else {
                records.len() as u64
            },
            records,
        });
    }
    Ok((filtered_updates, metrics))
}

fn filter_records(
    records: Vec<StorageStreamRecord>,
    policy: &PrivacyPolicy,
) -> BackfillExecutionResult<(Vec<StorageStreamRecord>, PrivacyBackfillMetrics)> {
    let mut metrics = PrivacyBackfillMetrics::default();
    let mut filtered_records = Vec::with_capacity(records.len());
    for mut record in records {
        let had_keys = !record.keys.is_empty();
        let keys = policy.filter_item(&record.keys)?;
        metrics.dropped_fields += keys.dropped_fields;
        record.keys = keys.item;
        if let Some(old_image) = &record.old_image {
            let filtered = policy.filter_item(old_image)?;
            metrics.dropped_fields += filtered.dropped_fields;
            record.old_image = Some(filtered.item);
        }
        if let Some(new_image) = &record.new_image {
            let filtered = policy.filter_item(new_image)?;
            metrics.dropped_fields += filtered.dropped_fields;
            record.new_image = Some(filtered.item);
        }
        if had_keys && record.keys.is_empty() {
            metrics.dropped_records += 1;
            continue;
        }
        filtered_records.push(record);
    }
    Ok((filtered_records, metrics))
}
