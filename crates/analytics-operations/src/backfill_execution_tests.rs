use std::{
    cell::Cell,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};

use super::performance_measurement::{
    ATTRIBUTE_COUNT, KEY_BYTES, PAYLOAD_BYTES, Probe, RECORD_COUNT, assert_realistic_fixture,
    digest_bytes, measure_candidate, workload,
};
use crate::{
    BackfillDestinationWriter, BackfillExecutionError, BackfillExecutionProgress,
    BackfillExecutionRequest, LocalBackfillChunk, LocalBackfillExecutor, LocalBackfillFixture,
    LocalStreamUpdate, MaintenanceWindow, OperationActor, OperationCursor, OperationEventKind,
    OperationId, OperationPhase, OperationRequest, OperationStatus, OperationStore,
    OperationStoreError, RateLimitClock, RateLimitDenied, RateLimitPolicy, SnapshotChunkSource,
    StreamCatchupSource, store_test_support::OperationStoreTestFixture,
};

#[test]
#[ignore = "debug-only backfill JSON byte-accounting allocation regression"]
fn given_backfill_records_when_json_byte_accounting_is_measured_then_bytes_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let (_rows, raw_records) = workload();
    assert_realistic_fixture(&raw_records);
    let records = raw_records
        .into_iter()
        .map(|record| record.record)
        .collect::<Vec<_>>();
    println!(
        "backfill-json-byte-accounting-fixture | records={} | attributes={} | key_bytes={} | \
         payload_bytes={}",
        RECORD_COUNT, ATTRIBUTE_COUNT, KEY_BYTES, PAYLOAD_BYTES,
    );

    let result = measure_candidate(
        "backfill JSON byte accounting",
        "backfill_execution::batch_accounting",
        || backfill_json_bytes_probe(&records, false),
        || backfill_json_bytes_probe(&records, true),
    );
    let baseline_counts = result
        .baseline_allocations
        .iter()
        .map(|sample| sample.count)
        .collect::<Vec<_>>();
    let candidate_counts = result
        .candidate_allocations
        .iter()
        .map(|sample| sample.count)
        .collect::<Vec<_>>();
    let baseline_bytes = result
        .baseline_allocations
        .iter()
        .map(|sample| sample.bytes)
        .collect::<Vec<_>>();
    let candidate_bytes = result
        .candidate_allocations
        .iter()
        .map(|sample| sample.bytes)
        .collect::<Vec<_>>();
    println!(
        "backfill-json-byte-accounting | baseline_ns={:?} | candidate_ns={:?} | \
         pair_delta_percent={:?} | baseline_allocation_counts={baseline_counts:?} | \
         candidate_allocation_counts={candidate_counts:?} | \
         baseline_allocated_bytes={baseline_bytes:?} | \
         candidate_allocated_bytes={candidate_bytes:?} | semantic_equality={} | kept={}",
        result
            .baseline_samples
            .iter()
            .map(|sample| sample.as_nanos())
            .collect::<Vec<_>>(),
        result
            .candidate_samples
            .iter()
            .map(|sample| sample.as_nanos())
            .collect::<Vec<_>>(),
        result.pair_deltas,
        result.semantically_equal,
        result.kept,
    );
    assert!(result.semantically_equal);
    assert!(result.kept);
}

fn backfill_json_bytes_probe(records: &[StorageStreamRecord], streaming: bool) -> Probe {
    let encoded_bytes = if streaming {
        crate::backfill_execution::serialized_len(records)
            .expect("streaming records are serializable")
    } else {
        serde_json::to_vec(records)
            .expect("buffered records are serializable")
            .len()
    };
    let encoded_bytes = u64::try_from(encoded_bytes).unwrap_or(u64::MAX);
    Probe {
        digest: digest_bytes(0, &encoded_bytes.to_le_bytes()),
        bytes: encoded_bytes,
    }
}

fn request(id: &str) -> BackfillExecutionRequest {
    BackfillExecutionRequest {
        operation_id: OperationId::try_new(id).unwrap(),
        actor: OperationActor::try_new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        rate_limit: RateLimitPolicy::default(),
        fail_once_at_chunk: None,
    }
}

#[derive(Debug)]
struct TestSnapshotSource {
    chunks: Vec<LocalBackfillChunk>,
}

impl SnapshotChunkSource for TestSnapshotSource {
    fn snapshot_chunks(&self) -> crate::BackfillExecutionResult<Vec<LocalBackfillChunk>> {
        Ok(self.chunks.clone())
    }
}

#[derive(Debug)]
struct TestStreamSource {
    updates: Vec<LocalStreamUpdate>,
}

impl StreamCatchupSource for TestStreamSource {
    fn stream_updates_after_snapshot(
        &self,
    ) -> crate::BackfillExecutionResult<Vec<LocalStreamUpdate>> {
        Ok(self.updates.clone())
    }
}

#[derive(Debug, Default)]
struct RecordingDestinationWriter {
    snapshot_chunks: Vec<(u64, u64, usize)>,
    stream_updates: Vec<(u64, u64, usize)>,
}

#[derive(Debug)]
struct CountingSnapshotSource {
    calls: Cell<u32>,
    chunks: Vec<LocalBackfillChunk>,
}

impl SnapshotChunkSource for CountingSnapshotSource {
    fn snapshot_chunks(&self) -> crate::BackfillExecutionResult<Vec<LocalBackfillChunk>> {
        self.calls.set(self.calls.get() + 1);
        Ok(self.chunks.clone())
    }
}

impl BackfillDestinationWriter for RecordingDestinationWriter {
    fn write_snapshot_chunk(
        &mut self,
        _key: &crate::OperationChunkKey,
        chunk: &LocalBackfillChunk,
    ) -> crate::BackfillExecutionResult<()> {
        self.snapshot_chunks
            .push((chunk.chunk_id, chunk.row_count(), chunk.records.len()));
        Ok(())
    }

    fn write_stream_update(
        &mut self,
        _key: &crate::OperationChunkKey,
        update: &LocalStreamUpdate,
    ) -> crate::BackfillExecutionResult<()> {
        self.stream_updates
            .push((update.cursor, update.row_count(), update.records.len()));
        Ok(())
    }
}

#[test]
fn given_local_fixture_when_backfill_executes_then_operation_succeeds_with_audit_metrics() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture::single_table(&[2, 3], &[10]);
    let executor = LocalBackfillExecutor;

    let report = executor
        .execute(&store, &request("backfill_exec_1"), &fixture, &fixture)
        .unwrap();

    assert_eq!(report.metrics.chunks_total, 2);
    assert_eq!(report.metrics.chunks_completed, 2);
    assert_eq!(report.metrics.rows_scanned, 5);
    assert_eq!(report.metrics.rows_written, 6);
    assert_eq!(report.metrics.stream_rows_applied, 1);
    assert_eq!(report.metrics.validation_runs, 1);
    assert_eq!(report.checkpoint, 10);
    let stored = store
        .show_operation(&OperationId::try_new("backfill_exec_1").unwrap())
        .unwrap();
    assert_eq!(stored.status, OperationStatus::Succeeded);
    let events = store.audit_events(&stored.operation_id).unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.kind == OperationEventKind::Succeeded)
    );
    assert!(
        events
            .iter()
            .any(|event| event.counts["rows_written"].as_u64() == Some(6))
    );
}

#[test]
fn given_succeeded_backfill_when_execution_is_repeated_then_persisted_report_is_reused() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture::single_table(&[2, 3], &[10]);
    let executor = LocalBackfillExecutor;
    let request = request("backfill_idempotent_1");
    let mut first_destination = RecordingDestinationWriter::default();

    let first = crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| executor.execute_to_destination(&input, &mut first_destination))
        .unwrap();
    let mut second_destination = RecordingDestinationWriter::default();
    let second = crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| executor.execute_to_destination(&input, &mut second_destination))
        .unwrap();

    assert_eq!(second, first);
    assert_eq!(first_destination.snapshot_chunks.len(), 2);
    assert_eq!(first_destination.stream_updates.len(), 1);
    assert!(second_destination.snapshot_chunks.is_empty());
    assert!(second_destination.stream_updates.is_empty());
}

#[test]
fn given_separate_snapshot_and_stream_sources_when_backfill_executes_then_both_inputs_are_used() {
    let store = OperationStore::connect_in_memory().unwrap();
    let snapshot_source = TestSnapshotSource {
        chunks: vec![
            LocalBackfillChunk::from_row_count(1, 4),
            LocalBackfillChunk::from_row_count(2, 6),
        ],
    };
    let stream_source = TestStreamSource {
        updates: vec![LocalStreamUpdate::from_row_count(8, 3)],
    };
    let executor = LocalBackfillExecutor;

    let report = executor
        .execute(
            &store,
            &request("backfill_split_inputs_1"),
            &snapshot_source,
            &stream_source,
        )
        .unwrap();

    assert_eq!(report.metrics.rows_scanned, 10);
    assert_eq!(report.metrics.stream_rows_applied, 3);
    assert_eq!(report.metrics.rows_written, 13);
    assert_eq!(report.checkpoint, 8);
}

#[test]
fn given_destination_writer_when_backfill_executes_then_whole_chunks_are_written() {
    let store = OperationStore::connect_in_memory().unwrap();
    let snapshot_source = TestSnapshotSource {
        chunks: vec![
            LocalBackfillChunk::new(
                1,
                vec![
                    stream_record("1", "one@example.test"),
                    stream_record("2", "two@example.test"),
                ],
            ),
            LocalBackfillChunk::new(2, vec![stream_record("3", "three@example.test")]),
        ],
    };
    let stream_source = TestStreamSource {
        updates: vec![LocalStreamUpdate::new(
            9,
            vec![stream_record("9", "stream@example.test")],
        )],
    };
    let executor = LocalBackfillExecutor;
    let mut destination = RecordingDestinationWriter::default();

    let report = crate::BackfillExecutionInput::try_new(
        &store,
        &request("backfill_destination_writer_1"),
        &snapshot_source,
        &stream_source,
    )
    .and_then(|input| executor.execute_to_destination(&input, &mut destination))
    .unwrap();

    assert_eq!(destination.snapshot_chunks, vec![(1, 2, 2), (2, 1, 1)]);
    assert_eq!(destination.stream_updates, vec![(9, 1, 1)]);
    assert_eq!(report.metrics.rows_scanned, 3);
    assert_eq!(report.metrics.rows_written, 4);
    assert_eq!(report.checkpoint, 9);
}

#[test]
fn given_record_fixture_and_privacy_policy_when_backfill_executes_then_denied_fields_are_counted() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture {
        chunks: vec![LocalBackfillChunk::new(
            1,
            vec![stream_record("1", "snapshot@example.test")],
        )],
        stream_updates: vec![LocalStreamUpdate::new(
            2,
            vec![stream_record("2", "stream@example.test")],
        )],
    };
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .unwrap()
        .with_denied_key_name("email");
    let executor = LocalBackfillExecutor;

    let report = crate::BackfillExecutionInput::try_new(
        &store,
        &request("backfill_privacy_1"),
        &fixture,
        &fixture,
    )
    .and_then(|input| executor.execute_with_privacy_policy(&input, &policy))
    .unwrap();

    assert_eq!(
        report.metrics.privacy_policy_version.as_deref(),
        Some("privacy-v1")
    );
    assert_eq!(report.metrics.privacy_dropped_fields, 2);
    assert_eq!(report.metrics.privacy_dropped_records, 0);
    assert_eq!(report.metrics.rows_scanned, 1);
    assert_eq!(report.metrics.stream_rows_applied, 1);
    assert_eq!(report.metrics.rows_written, 2);
    let events = store.audit_events(&report.operation_id).unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.counts["privacy_dropped_fields"].as_u64() == Some(2))
    );
}

#[test]
fn given_privacy_policy_drops_all_record_keys_when_backfill_executes_then_record_drop_is_counted() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture {
        chunks: vec![LocalBackfillChunk::new(
            1,
            vec![keyed_stream_record("1", "snapshot@example.test")],
        )],
        stream_updates: Vec::new(),
    };
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .unwrap()
        .with_denied_key_name("pk");
    let executor = LocalBackfillExecutor;

    let report = crate::BackfillExecutionInput::try_new(
        &store,
        &request("backfill_privacy_drop_record_1"),
        &fixture,
        &fixture,
    )
    .and_then(|input| executor.execute_with_privacy_policy(&input, &policy))
    .unwrap();

    assert_eq!(report.metrics.privacy_dropped_fields, 1);
    assert_eq!(report.metrics.privacy_dropped_records, 1);
    assert_eq!(report.metrics.rows_scanned, 0);
    assert_eq!(report.metrics.rows_written, 0);
    let events = store.audit_events(&report.operation_id).unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.counts["privacy_dropped_records"].as_u64() == Some(1))
    );
}

#[test]
fn given_existing_cursor_when_backfill_resumes_then_completed_chunks_are_not_rewritten() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation_id = OperationId::try_new("backfill_resume_1").unwrap();
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: crate::OperationKind::Backfill,
            actor: OperationActor::try_new("operator").unwrap(),
            target_tables: vec!["users".to_string()],
            dry_run: false,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::json!({"mode": "local_backfill_execution"}),
        })
        .unwrap();
    let store_fixture = OperationStoreTestFixture::new();
    store_fixture
        .prepare(
            &store,
            &operation_id,
            &OperationActor::try_new("operator").unwrap(),
        )
        .unwrap();
    store_fixture
        .transition(
            &store,
            &operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Paused,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 1,
                }),
                event_kind: OperationEventKind::Paused,
                counts: serde_json::json!({}),
                message: Some("first chunk already committed; waiting for resume"),
            },
        )
        .unwrap();
    let fixture = LocalBackfillFixture::single_table(&[2, 3], &[]);
    let executor = LocalBackfillExecutor;

    let report = executor
        .execute(&store, &request("backfill_resume_1"), &fixture, &fixture)
        .unwrap();

    assert_eq!(report.resumed_from_chunk, 1);
    assert_eq!(report.metrics.rows_scanned, 3);
    assert_eq!(report.metrics.rows_written, 3);
    assert_eq!(report.completed_chunks, vec![1, 2]);
}

#[test]
fn given_stream_cursor_when_backfill_resumes_then_committed_stream_updates_are_not_replayed() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation_id = OperationId::try_new("backfill_stream_resume_1").unwrap();
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: crate::OperationKind::Backfill,
            actor: OperationActor::try_new("operator").unwrap(),
            target_tables: vec!["users".to_string()],
            dry_run: false,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::json!({"mode": "local_backfill_execution"}),
        })
        .unwrap();
    let store_fixture = OperationStoreTestFixture::new();
    store_fixture
        .prepare(
            &store,
            &operation_id,
            &OperationActor::try_new("operator").unwrap(),
        )
        .unwrap();
    store_fixture
        .transition(
            &store,
            &operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Paused,
                cursor: Some(OperationCursor {
                    label: "stream".to_string(),
                    position: 10,
                }),
                event_kind: OperationEventKind::Paused,
                counts: serde_json::json!({}),
                message: Some("stream update already committed; waiting for resume"),
            },
        )
        .unwrap();
    let fixture = LocalBackfillFixture {
        chunks: vec![
            LocalBackfillChunk::from_row_count(1, 2),
            LocalBackfillChunk::from_row_count(2, 3),
        ],
        stream_updates: vec![
            LocalStreamUpdate::from_row_count(10, 2),
            LocalStreamUpdate::from_row_count(11, 1),
        ],
    };
    let mut destination = RecordingDestinationWriter::default();

    let report = crate::BackfillExecutionInput::try_new(
        &store,
        &request("backfill_stream_resume_1"),
        &fixture,
        &fixture,
    )
    .and_then(|input| LocalBackfillExecutor.execute_to_destination(&input, &mut destination))
    .unwrap();

    assert!(destination.snapshot_chunks.is_empty());
    assert_eq!(destination.stream_updates, vec![(11, 1, 1)]);
    assert_eq!(report.metrics.stream_rows_applied, 1);
    assert_eq!(report.checkpoint, 11);
}

#[test]
fn given_chunk_failure_when_backfill_runs_then_chunk_is_retried_before_cursor_advances() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture::single_table(&[2, 3], &[]);
    let mut request = request("backfill_retry_1");
    request.fail_once_at_chunk = Some(2);
    request.rate_limit.pause_ms = 0;
    let executor = LocalBackfillExecutor;

    let report = executor
        .execute(&store, &request, &fixture, &fixture)
        .unwrap();

    assert_eq!(report.metrics.chunks_retried, 1);
    assert_eq!(report.metrics.rows_scanned, 5);
    assert_eq!(report.metrics.rows_written, 5);
    let events = store.audit_events(&request.operation_id).unwrap();
    let retry_event = events
        .iter()
        .find(|event| {
            event.message.as_deref() == Some("chunk failed before destination commit; retrying")
        })
        .expect("retry audit event");
    assert_eq!(
        retry_event.cursor.as_ref().map(|cursor| cursor.position),
        Some(1)
    );
    assert_eq!(
        report.metrics.throttle_pause_ms,
        request.rate_limit.retry_backoff_ms
    );
    assert_eq!(
        report.metrics.retry_delay_ms,
        request.rate_limit.retry_backoff_ms
    );
}

#[test]
fn given_low_rows_per_second_limit_when_backfill_runs_then_destination_batch_is_denied() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture::single_table(&[250], &[]);
    let mut request = request("backfill_rate_limit_1");
    request.rate_limit.max_rows_per_second = Some(100);
    request.rate_limit.max_destination_writes_per_second = None;
    request.rate_limit.pause_ms = 0;
    let executor = LocalBackfillExecutor;
    let mut destination = RecordingDestinationWriter::default();

    let error = crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| executor.execute_to_destination(&input, &mut destination))
        .unwrap_err();

    assert!(matches!(
        error,
        BackfillExecutionError::RateLimit(RateLimitDenied { wait_ms: 2_500 })
    ));
    assert!(destination.snapshot_chunks.is_empty());
    assert!(destination.stream_updates.is_empty());
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Paused
    );
}

#[test]
fn given_record_count_exceeds_row_budget_when_backfill_runs_then_snapshot_is_denied() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture {
        chunks: vec![LocalBackfillChunk::new(
            1,
            vec![
                stream_record("1", "one@example.test"),
                stream_record("2", "two@example.test"),
            ],
        )],
        stream_updates: Vec::new(),
    };
    let mut request = request("backfill_exact_row_budget_1");
    request.rate_limit.max_rows_per_second = Some(1);
    request.rate_limit.max_destination_writes_per_second = None;
    request.rate_limit.pause_ms = 0;
    let mut destination = RecordingDestinationWriter::default();

    let error = crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| LocalBackfillExecutor.execute_to_destination(&input, &mut destination))
        .unwrap_err();

    assert!(matches!(error, BackfillExecutionError::RateLimit(_)));
    assert_eq!(fixture.chunks[0].row_count(), 2);
    assert!(destination.snapshot_chunks.is_empty());
    assert!(destination.stream_updates.is_empty());
}

#[test]
fn given_backfill_batch_without_records_when_deserialized_then_it_is_rejected() {
    assert!(
        serde_json::from_value::<LocalBackfillChunk>(serde_json::json!({
            "chunk_id": 1
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<LocalStreamUpdate>(serde_json::json!({
            "cursor": 1
        }))
        .is_err()
    );
}

#[test]
fn given_oversized_snapshot_record_when_backfill_runs_then_encoded_byte_budget_is_enforced() {
    let store = OperationStore::connect_in_memory().unwrap();
    let oversized_email = "x".repeat(4_096);
    let fixture = LocalBackfillFixture {
        chunks: vec![LocalBackfillChunk::new(
            1,
            vec![stream_record("1", &oversized_email)],
        )],
        stream_updates: Vec::new(),
    };
    let mut request = request("backfill_exact_snapshot_bytes_1");
    request.rate_limit.max_bytes_per_second = Some(1);
    request.rate_limit.max_destination_writes_per_second = None;
    request.rate_limit.pause_ms = 0;
    let mut destination = RecordingDestinationWriter::default();

    let error = crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| LocalBackfillExecutor.execute_to_destination(&input, &mut destination))
        .unwrap_err();

    assert!(matches!(
        error,
        BackfillExecutionError::RateLimit(RateLimitDenied { wait_ms }) if wait_ms > 1
    ));
    assert!(destination.snapshot_chunks.is_empty());
}

#[test]
fn given_snapshot_batch_when_byte_budget_matches_json_length_then_batch_is_written() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture {
        chunks: vec![LocalBackfillChunk::new(
            1,
            vec![stream_record("1", "ada@example.test")],
        )],
        stream_updates: Vec::new(),
    };
    let encoded_bytes = serde_json::to_vec(&fixture.chunks[0].records)
        .expect("snapshot records are serializable")
        .len() as u64;
    let mut request = request("backfill_exact_snapshot_bytes_2");
    request.rate_limit.max_bytes_per_second = Some(encoded_bytes);
    request.rate_limit.max_destination_writes_per_second = None;
    request.rate_limit.pause_ms = 0;
    let mut destination = RecordingDestinationWriter::default();

    crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| LocalBackfillExecutor.execute_to_destination(&input, &mut destination))
        .expect("exactly sized snapshot batch should be admitted");

    assert_eq!(destination.snapshot_chunks, vec![(1, 1, 1)]);
}

#[test]
fn given_oversized_stream_record_when_backfill_runs_then_stream_byte_budget_is_enforced() {
    let store = OperationStore::connect_in_memory().unwrap();
    let oversized_email = "x".repeat(4_096);
    let fixture = LocalBackfillFixture {
        chunks: vec![LocalBackfillChunk::new(1, Vec::new())],
        stream_updates: vec![LocalStreamUpdate::new(
            2,
            vec![stream_record("2", &oversized_email)],
        )],
    };
    let mut request = request("backfill_exact_stream_bytes_1");
    request.rate_limit.max_bytes_per_second = Some(1);
    request.rate_limit.max_destination_writes_per_second = None;
    request.rate_limit.pause_ms = 0;
    let mut destination = RecordingDestinationWriter::default();

    let error = crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| LocalBackfillExecutor.execute_to_destination(&input, &mut destination))
        .unwrap_err();

    assert!(matches!(error, BackfillExecutionError::RateLimit(_)));
    assert!(destination.snapshot_chunks.is_empty());
    assert!(destination.stream_updates.is_empty());
}

#[test]
fn given_maintenance_window_denial_when_executing_backfill_then_resume_preserves_cumulative_backfill_progress()
 {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture::single_table(&[1, 2], &[]);
    let mut request = request("backfill_window_resume_1");
    request.rate_limit.maintenance_windows_utc = vec![MaintenanceWindow {
        start_minute_utc: 0,
        end_minute_utc: 1,
    }];
    request.rate_limit.pause_ms = 0;
    let executor = LocalBackfillExecutor;
    let first_clock = SteppingRateLimitClock::new(3);
    let mut first_destination = RecordingDestinationWriter::default();

    let error = crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| {
            executor.execute_to_destination_at(&input, &mut first_destination, &first_clock)
        })
        .unwrap_err();

    assert!(matches!(error, BackfillExecutionError::RateLimit(_)));
    assert_eq!(first_destination.snapshot_chunks, vec![(1, 1, 1)]);
    let paused = store.show_operation(&request.operation_id).unwrap();
    assert_eq!(paused.status, OperationStatus::Paused);
    assert_eq!(
        paused.cursor.as_ref().map(|cursor| cursor.position),
        Some(1)
    );
    let progress: BackfillExecutionProgress =
        serde_json::from_value(paused.progress).expect("typed paused progress");
    assert!(progress.retryable);
    assert_eq!(progress.metrics.rows_scanned, 1);
    assert_eq!(progress.metrics.rows_written, 1);
    assert_eq!(progress.metrics.chunks_completed, 1);

    let mut resumed_destination = RecordingDestinationWriter::default();
    let report = crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| {
            executor.execute_to_destination_at(
                &input,
                &mut resumed_destination,
                &AlwaysOpenRateLimitClock,
            )
        })
        .expect("resume after maintenance window");

    assert_eq!(resumed_destination.snapshot_chunks, vec![(2, 2, 2)]);
    assert_eq!(report.metrics.rows_scanned, 3);
    assert_eq!(report.metrics.rows_written, 3);
    assert_eq!(report.metrics.chunks_completed, 2);
    assert_eq!(report.checkpoint, 2);
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Succeeded
    );
}

#[test]
fn given_progress_interleaves_after_backfill_write_when_executing_backfill_then_pending_effect_is_released_for_retry()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("operations.duckdb");
    let owner_store = OperationStore::connect_duckdb(database_path.as_path()).unwrap();
    let progress_store = OperationStore::connect_duckdb(database_path.as_path()).unwrap();
    let fixture = LocalBackfillFixture::single_table(&[1], &[]);
    let request = request("backfill_stale_effect_1");
    let mut destination = ProgressAdvancingBackfillDestination {
        store: &progress_store,
        actor: request.actor.clone(),
        operation_id: request.operation_id.clone(),
        advanced: Cell::new(false),
        writes: 0,
    };

    let error = crate::BackfillExecutionInput::try_new(&owner_store, &request, &fixture, &fixture)
        .and_then(|input| LocalBackfillExecutor.execute_to_destination(&input, &mut destination))
        .unwrap_err();

    assert!(matches!(
        error,
        BackfillExecutionError::Store(OperationStoreError::StaleOperationState { .. })
    ));
    assert_eq!(destination.writes, 1);
    assert_eq!(
        owner_store
            .show_operation(&request.operation_id)
            .unwrap()
            .status,
        OperationStatus::Paused
    );

    let report = crate::BackfillExecutionInput::try_new(&owner_store, &request, &fixture, &fixture)
        .and_then(|input| LocalBackfillExecutor.execute_to_destination(&input, &mut destination))
        .expect("retry should reclaim the released idempotency key");
    assert_eq!(destination.writes, 2);
    assert_eq!(report.metrics.rows_written, 1);
    assert_eq!(
        owner_store
            .show_operation(&request.operation_id)
            .unwrap()
            .status,
        OperationStatus::Succeeded
    );
}

#[test]
fn given_source_budget_denial_when_backfill_starts_then_sources_are_not_read() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture::single_table(&[1], &[]);
    let mut request = request("backfill_source_budget_1");
    request.rate_limit.max_source_requests_per_second = Some(0);
    let source = CountingSnapshotSource {
        calls: Cell::new(0),
        chunks: fixture.chunks.clone(),
    };

    let error = LocalBackfillExecutor
        .execute(&store, &request, &source, &fixture)
        .unwrap_err();

    assert!(matches!(error, BackfillExecutionError::RateLimit(_)));
    assert_eq!(source.calls.get(), 0);
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Paused
    );
}

#[test]
fn given_zero_parallelism_when_backfill_runs_then_it_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture::single_table(&[1], &[]);
    let mut request = request("backfill_parallelism_1");
    request.rate_limit.max_parallel_chunks = 0;
    let executor = LocalBackfillExecutor;

    let error = executor
        .execute(&store, &request, &fixture, &fixture)
        .unwrap_err();

    assert!(matches!(error, BackfillExecutionError::InvalidParallelism));
}

#[test]
fn given_empty_snapshot_fixture_after_operation_start_when_executing_backfill_then_failure_is_terminal_and_persisted()
 {
    let store = OperationStore::connect_in_memory().unwrap();
    let request = request("backfill_empty_fixture_1");
    let empty_snapshot = TestSnapshotSource { chunks: Vec::new() };
    let stream = TestStreamSource {
        updates: Vec::new(),
    };

    let error = LocalBackfillExecutor
        .execute(&store, &request, &empty_snapshot, &stream)
        .unwrap_err();

    assert!(matches!(error, BackfillExecutionError::EmptyFixture));
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Failed
    );
}

#[test]
fn given_changed_privacy_policy_on_backfill_retry_when_executing_backfill_then_operation_conflict_is_rejected()
 {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture::single_table(&[1], &[]);
    let request = request("backfill_privacy_conflict_1");
    let first_policy = PrivacyPolicy::try_new("privacy-v1").unwrap();
    crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| LocalBackfillExecutor.execute_with_privacy_policy(&input, &first_policy))
        .unwrap();

    let second_policy = PrivacyPolicy::try_new("privacy-v2").unwrap();
    let error = crate::BackfillExecutionInput::try_new(&store, &request, &fixture, &fixture)
        .and_then(|input| LocalBackfillExecutor.execute_with_privacy_policy(&input, &second_policy))
        .unwrap_err();

    assert!(matches!(
        error,
        BackfillExecutionError::Store(OperationStoreError::ConflictingOperation(_))
    ));
}

fn stream_record(sequence_number: &str, email: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: StorageItem::new(),
        sequence_number: sequence_number.to_string(),
        old_image: None,
        new_image: Some(StorageItem::from_iter([(
            "email".to_string(),
            StorageValue::S(email.to_string()),
        )])),
    }
}

#[derive(Debug, Clone)]
struct SteppingRateLimitClock {
    calls: Arc<AtomicUsize>,
    open_calls: usize,
}

impl SteppingRateLimitClock {
    fn new(open_calls: usize) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            open_calls,
        }
    }
}

impl RateLimitClock for SteppingRateLimitClock {
    fn minute_utc(&self) -> u16 {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.open_calls {
            0
        } else {
            1
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AlwaysOpenRateLimitClock;

impl RateLimitClock for AlwaysOpenRateLimitClock {
    fn minute_utc(&self) -> u16 {
        0
    }
}

struct ProgressAdvancingBackfillDestination<'a> {
    store: &'a OperationStore,
    actor: OperationActor,
    operation_id: OperationId,
    advanced: Cell<bool>,
    writes: u32,
}

impl BackfillDestinationWriter for ProgressAdvancingBackfillDestination<'_> {
    fn write_snapshot_chunk(
        &mut self,
        _key: &crate::OperationChunkKey,
        _chunk: &LocalBackfillChunk,
    ) -> crate::BackfillExecutionResult<()> {
        self.writes += 1;
        if !self.advanced.replace(true) {
            let operation = self.store.show_operation(&self.operation_id).unwrap();
            self.store
                .transition_with_counts_as(
                    &operation,
                    &self.actor,
                    crate::OperationTransition {
                        phase: OperationPhase::Executing,
                        status: OperationStatus::Running,
                        cursor: operation.cursor.clone(),
                        event_kind: OperationEventKind::CursorAdvanced,
                        counts: serde_json::json!({"destination_progress": 1}),
                        message: Some("test progress interleaving"),
                    },
                )
                .unwrap();
        }
        Ok(())
    }

    fn write_stream_update(
        &mut self,
        _key: &crate::OperationChunkKey,
        _update: &LocalStreamUpdate,
    ) -> crate::BackfillExecutionResult<()> {
        Ok(())
    }
}

fn keyed_stream_record(sequence_number: &str, email: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: StorageItem::from_iter([(
            "pk".to_string(),
            StorageValue::S(sequence_number.to_string()),
        )]),
        sequence_number: sequence_number.to_string(),
        old_image: None,
        new_image: Some(StorageItem::from_iter([(
            "email".to_string(),
            StorageValue::S(email.to_string()),
        )])),
    }
}
