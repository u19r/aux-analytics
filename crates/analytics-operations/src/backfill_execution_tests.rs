use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};

use crate::{
    BackfillDestinationWriter, BackfillExecutionError, BackfillExecutionRequest,
    BackfillWriteOutcome, LocalBackfillChunk, LocalBackfillExecutor, LocalBackfillFixture,
    LocalStreamUpdate, OperationActor, OperationCursor, OperationEventKind, OperationId,
    OperationPhase, OperationRequest, OperationStatus, OperationStore, RateLimitPolicy,
    SnapshotChunkSource, StreamCatchupSource,
};

fn request(id: &str) -> BackfillExecutionRequest {
    BackfillExecutionRequest {
        operation_id: OperationId::new(id).unwrap(),
        actor: OperationActor::new("operator").unwrap(),
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

impl BackfillDestinationWriter for RecordingDestinationWriter {
    fn write_snapshot_chunk(
        &mut self,
        chunk: &LocalBackfillChunk,
    ) -> crate::BackfillExecutionResult<BackfillWriteOutcome> {
        self.snapshot_chunks
            .push((chunk.chunk_id, chunk.rows, chunk.records.len()));
        Ok(BackfillWriteOutcome {
            rows_written: chunk.rows,
        })
    }

    fn write_stream_update(
        &mut self,
        update: &LocalStreamUpdate,
    ) -> crate::BackfillExecutionResult<BackfillWriteOutcome> {
        self.stream_updates
            .push((update.cursor, update.rows, update.records.len()));
        Ok(BackfillWriteOutcome {
            rows_written: update.rows,
        })
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
        .show_operation(&OperationId::new("backfill_exec_1").unwrap())
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
fn given_separate_snapshot_and_stream_sources_when_backfill_executes_then_both_inputs_are_used() {
    let store = OperationStore::connect_in_memory().unwrap();
    let snapshot_source = TestSnapshotSource {
        chunks: vec![
            LocalBackfillChunk {
                chunk_id: 1,
                rows: 4,
                records: Vec::new(),
            },
            LocalBackfillChunk {
                chunk_id: 2,
                rows: 6,
                records: Vec::new(),
            },
        ],
    };
    let stream_source = TestStreamSource {
        updates: vec![LocalStreamUpdate {
            cursor: 8,
            rows: 3,
            records: Vec::new(),
        }],
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
            LocalBackfillChunk {
                chunk_id: 1,
                rows: 2,
                records: vec![
                    stream_record("1", "one@example.test"),
                    stream_record("2", "two@example.test"),
                ],
            },
            LocalBackfillChunk {
                chunk_id: 2,
                rows: 1,
                records: vec![stream_record("3", "three@example.test")],
            },
        ],
    };
    let stream_source = TestStreamSource {
        updates: vec![LocalStreamUpdate {
            cursor: 9,
            rows: 1,
            records: vec![stream_record("9", "stream@example.test")],
        }],
    };
    let executor = LocalBackfillExecutor;
    let mut destination = RecordingDestinationWriter::default();

    let report = executor
        .execute_to_destination(
            &store,
            &request("backfill_destination_writer_1"),
            &snapshot_source,
            &stream_source,
            &mut destination,
        )
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
        chunks: vec![LocalBackfillChunk {
            chunk_id: 1,
            rows: 0,
            records: vec![stream_record("1", "snapshot@example.test")],
        }],
        stream_updates: vec![LocalStreamUpdate {
            cursor: 2,
            rows: 0,
            records: vec![stream_record("2", "stream@example.test")],
        }],
    };
    let policy = PrivacyPolicy::new("privacy-v1")
        .unwrap()
        .with_denied_key_name("email");
    let executor = LocalBackfillExecutor;

    let report = executor
        .execute_with_privacy_policy(
            &store,
            &request("backfill_privacy_1"),
            &fixture,
            &fixture,
            &policy,
        )
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
        chunks: vec![LocalBackfillChunk {
            chunk_id: 1,
            rows: 1,
            records: vec![keyed_stream_record("1", "snapshot@example.test")],
        }],
        stream_updates: Vec::new(),
    };
    let policy = PrivacyPolicy::new("privacy-v1")
        .unwrap()
        .with_denied_key_name("pk");
    let executor = LocalBackfillExecutor;

    let report = executor
        .execute_with_privacy_policy(
            &store,
            &request("backfill_privacy_drop_record_1"),
            &fixture,
            &fixture,
            &policy,
        )
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
    let operation_id = OperationId::new("backfill_resume_1").unwrap();
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: crate::OperationKind::Backfill,
            actor: OperationActor::new("operator").unwrap(),
            target_tables: vec!["users".to_string()],
            dry_run: false,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::json!({"mode": "resume-test"}),
        })
        .unwrap();
    store
        .transition(
            &operation_id,
            OperationPhase::Executing,
            OperationStatus::Running,
            Some(OperationCursor {
                label: "chunk".to_string(),
                position: 1,
            }),
            OperationEventKind::CursorAdvanced,
            Some("first chunk already committed"),
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
fn given_low_rows_per_second_limit_when_backfill_runs_then_throttle_time_is_reported() {
    let store = OperationStore::connect_in_memory().unwrap();
    let fixture = LocalBackfillFixture::single_table(&[250], &[]);
    let mut request = request("backfill_rate_limit_1");
    request.rate_limit.max_rows_per_second = Some(100);
    request.rate_limit.max_destination_writes_per_second = None;
    request.rate_limit.pause_ms = 0;
    let executor = LocalBackfillExecutor;

    let report = executor
        .execute(&store, &request, &fixture, &fixture)
        .unwrap();

    assert_eq!(report.metrics.throttle_pause_ms, 2_500);
    assert_eq!(report.metrics.effective_rows_per_second, 100);
    assert_eq!(report.metrics.configured_max_rows_per_second, Some(100));
    assert_eq!(
        report.metrics.configured_max_destination_writes_per_second,
        None
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

fn stream_record(sequence_number: &str, email: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: StorageItem::new(),
        sequence_number: sequence_number.to_string(),
        old_image: None,
        new_image: Some(StorageItem::from([(
            "email".to_string(),
            StorageValue::S(email.to_string()),
        )])),
    }
}

fn keyed_stream_record(sequence_number: &str, email: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: StorageItem::from([(
            "pk".to_string(),
            StorageValue::S(sequence_number.to_string()),
        )]),
        sequence_number: sequence_number.to_string(),
        old_image: None,
        new_image: Some(StorageItem::from([(
            "email".to_string(),
            StorageValue::S(email.to_string()),
        )])),
    }
}
