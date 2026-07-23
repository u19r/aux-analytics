use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use analytics_contract::{AnalyticsManifest, StorageStreamRecord, TableRegistration};
use config::AnalyticsSourceConfig;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    PollBatch, PolledRecord, SnapshotChunk, SnapshotChunkCheckpoint, SourceCheckpoint,
    SourcePoller, SourceTablePlan,
    aux_storage_client::AuxStorageStreamClient,
    aws_stream::AwsShardIteratorState,
    execution::{BackfillExecutionInputs, SnapshotChunkSource, StreamCatchupSource},
    poller::{
        AUX_STORAGE_SHARD_ID, AuxStorageGlobalPoller, GLOBAL_SOURCE_NAME,
        apply_aws_iterator_checkpoint, aux_storage_initial_cursor, aws_stream_response_batch,
        checkpoint_position, expand_records, iterator_checkpoint_shard_id, next_aux_storage_cursor,
        table_index_from_request_shard, table_request_names_from,
    },
    source_routes::CompiledSourceRoutes,
};

#[test]
fn given_one_source_record_when_multiple_analytics_tables_use_it_then_polling_fans_out_record() {
    let record = StorageStreamRecord {
        sequence_number: "record-1".to_string(),
        keys: HashMap::default(),
        old_image: None,
        new_image: None,
    };

    let expanded = expand_records(
        "source_users",
        &[
            "analytics_users".to_string(),
            "analytics_accounts".to_string(),
        ],
        [record],
    );

    assert_eq!(expanded.len(), 2);
    assert_eq!(expanded[0].analytics_table_name, "analytics_users");
    assert_eq!(expanded[0].record_key, "record-1");
    assert_eq!(expanded[1].analytics_table_name, "analytics_accounts");
    assert_eq!(expanded[1].record.sequence_number, "record-1");
}

#[test]
fn given_aux_storage_checkpoint_for_same_table_when_committed_then_cursor_advances() {
    let mut poller = AuxStorageGlobalPoller {
        client: AuxStorageStreamClient::new("http://127.0.0.1:1", Duration::from_secs(1)).unwrap(),
        routes: test_routes(),
        last_evaluated_key: Some("old".to_string()),
        max_records_per_response: 1_000,
    };

    poller.commit(&SourceCheckpoint {
        source_table_name: GLOBAL_SOURCE_NAME.to_string(),
        shard_id: AUX_STORAGE_SHARD_ID.to_string(),
        position: "new".to_string(),
    });

    assert_eq!(poller.last_evaluated_key.as_deref(), Some("new"));
}

#[test]
fn given_aux_storage_checkpoint_for_other_shard_when_committed_then_cursor_is_unchanged() {
    let mut poller = AuxStorageGlobalPoller {
        client: AuxStorageStreamClient::new("http://127.0.0.1:1", Duration::from_secs(1)).unwrap(),
        routes: test_routes(),
        last_evaluated_key: Some("old".to_string()),
        max_records_per_response: 1_000,
    };

    poller.commit(&SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: "other-shard".to_string(),
        position: "new".to_string(),
    });

    assert_eq!(poller.last_evaluated_key.as_deref(), Some("old"));
}

#[test]
fn given_checkpoints_when_matching_table_and_shard_exists_then_position_is_reused() {
    let checkpoints = vec![
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "aux-storage".to_string(),
            position: "cursor-users".to_string(),
        },
        SourceCheckpoint {
            source_table_name: "source_accounts".to_string(),
            shard_id: "aux-storage".to_string(),
            position: "cursor-accounts".to_string(),
        },
    ];

    assert_eq!(
        checkpoint_position(&checkpoints, "source_users", "aux-storage").as_deref(),
        Some("cursor-users")
    );
    assert_eq!(
        checkpoint_position(&checkpoints, "source_users", "other-shard"),
        None
    );
}

#[test]
fn given_global_checkpoint_when_aux_storage_poller_starts_then_cursor_is_reused() {
    let checkpoints = vec![SourceCheckpoint {
        source_table_name: GLOBAL_SOURCE_NAME.to_string(),
        shard_id: AUX_STORAGE_SHARD_ID.to_string(),
        position: "global-cursor".to_string(),
    }];

    let cursor = aux_storage_initial_cursor(
        &checkpoints,
        "nsystem",
        &["analytics_registered_tables".to_string()],
    );

    assert_eq!(cursor.as_deref(), Some("global-cursor"));
}

#[test]
fn given_pollable_tables_when_requests_are_planned_then_table_indexes_are_used_as_shard_ids() {
    assert_eq!(table_request_names_from(3, 0), vec!["0", "1", "2"]);
    assert_eq!(table_index_from_request_shard("2"), Some(2));
    assert_eq!(table_index_from_request_shard("not-a-table-index"), None);
}

#[test]
fn given_poll_budget_below_table_count_when_requests_are_planned_then_start_index_rotates() {
    assert_eq!(
        table_request_names_from(5, 0),
        vec!["0", "1", "2", "3", "4"]
    );
    assert_eq!(
        table_request_names_from(5, 4),
        vec!["4", "0", "1", "2", "3"]
    );
    assert_eq!(
        table_request_names_from(5, 8),
        vec!["3", "4", "0", "1", "2"]
    );
}

#[test]
fn given_aux_storage_response_has_resume_key_when_cursor_is_advanced_then_resume_key_wins() {
    let cursor = next_aux_storage_cursor(Some("next-page".to_string()), Some("seq-2".to_string()));

    assert_eq!(cursor.as_deref(), Some("next-page"));
}

#[test]
fn given_terminal_aux_storage_response_has_records_when_cursor_is_advanced_then_last_sequence_is_checkpointed()
 {
    let cursor = next_aux_storage_cursor(None, Some("seq-2".to_string()));

    assert_eq!(cursor.as_deref(), Some("seq-2"));
}

#[test]
fn given_empty_terminal_aux_storage_response_when_cursor_is_advanced_then_polling_stops() {
    let cursor = next_aux_storage_cursor(None, None);

    assert_eq!(cursor, None);
}

#[test]
fn given_aws_iterator_checkpoint_for_matching_table_and_shard_when_committed_then_iterator_advances()
 {
    let mut states = vec![AwsShardIteratorState {
        shard_id: "shard-0001".to_string(),
        iterator: "old-iterator".to_string(),
    }];
    let checkpoint = SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: "__iterator:shard-0001".to_string(),
        position: "new-iterator".to_string(),
    };

    let applied = apply_aws_iterator_checkpoint("source_users", &mut states, &checkpoint);

    assert!(applied);
    assert_eq!(states[0].iterator, "new-iterator");
}

#[test]
fn given_aws_iterator_checkpoint_for_other_table_when_committed_then_iterator_is_unchanged() {
    let mut states = vec![AwsShardIteratorState {
        shard_id: "shard-0001".to_string(),
        iterator: "old-iterator".to_string(),
    }];
    let checkpoint = SourceCheckpoint {
        source_table_name: "source_accounts".to_string(),
        shard_id: "__iterator:shard-0001".to_string(),
        position: "new-iterator".to_string(),
    };

    let applied = apply_aws_iterator_checkpoint("source_users", &mut states, &checkpoint);

    assert!(!applied);
    assert_eq!(states[0].iterator, "old-iterator");
}

#[test]
fn given_persisted_record_checkpoint_when_committed_to_aws_stream_then_iterator_is_unchanged() {
    let checkpoint = SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: "shard-0001".to_string(),
        position: "42".to_string(),
    };

    assert_eq!(iterator_checkpoint_shard_id(&checkpoint), None);
}

#[test]
fn given_aws_stream_records_and_next_iterator_when_response_is_batched_then_record_and_iterator_checkpoints_are_emitted()
 {
    let batch = aws_stream_response_batch(
        "source_users",
        &["users".to_string(), "accounts".to_string()],
        "shard-0001",
        vec![stream_record("seq-1"), stream_record("seq-2")],
        Some("next-iterator".to_string()),
    );

    assert_eq!(batch.records.len(), 4);
    assert_eq!(batch.records[0].analytics_table_name, "users");
    assert_eq!(batch.records[1].analytics_table_name, "accounts");
    assert_eq!(batch.checkpoints.len(), 2);
    assert_eq!(batch.checkpoints[0].shard_id, "shard-0001");
    assert_eq!(batch.checkpoints[0].position, "seq-2");
    assert_eq!(batch.checkpoints[1].shard_id, "__iterator:shard-0001");
    assert_eq!(batch.checkpoints[1].position, "next-iterator");
}

#[test]
fn given_aws_stream_response_without_records_when_batched_then_only_iterator_checkpoint_is_emitted()
{
    let batch = aws_stream_response_batch(
        "source_users",
        &["users".to_string()],
        "shard-0001",
        Vec::new(),
        Some("next-iterator".to_string()),
    );

    assert!(batch.records.is_empty());
    assert_eq!(batch.checkpoints.len(), 1);
    assert_eq!(batch.checkpoints[0].shard_id, "__iterator:shard-0001");
}

#[tokio::test]
async fn given_terminal_aux_storage_page_when_polled_then_last_record_sequence_is_checkpointed() {
    let base_url = serve_once(
        r#"{"Records":[{"SourceTableName":"source_users","Keys":{"pk":{"S":"USER#1"}},"SequenceNumber":"seq-1"},{"SourceTableName":"source_users","Keys":{"pk":{"S":"USER#2"}},"SequenceNumber":"seq-2"}]}"#,
    )
    .await;
    let mut poller = AuxStorageGlobalPoller {
        client: AuxStorageStreamClient::new(&base_url, Duration::from_secs(1)).unwrap(),
        routes: test_routes(),
        last_evaluated_key: None,
        max_records_per_response: 1_000,
    };

    let batch = poller.poll_once(1).await.expect("poll aux-storage page");

    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.records[0].record_key, "seq-1");
    assert_eq!(batch.checkpoints.len(), 1);
    assert_eq!(batch.checkpoints[0].position, "seq-2");
}

#[tokio::test]
async fn unmatched_global_record_advances_checkpoint_without_emitting_work() {
    let base_url = serve_once(
        r#"{"Records":[{"SourceTableName":"n00042","Keys":{"pk":{"S":"UNKNOWN#1"}},"NewImage":{"et":{"S":"unknown"}},"SequenceNumber":"seq-1"}]}"#,
    )
    .await;
    let registration: TableRegistration = serde_json::from_value(json!({
        "source_table_name": "n",
        "source_table_name_prefix": "n",
        "analytics_table_name": "users",
        "condition_expression": "#entity_type = :entity_type",
        "expression_attribute_names": {"#entity_type": "entity_type"},
        "expression_attribute_values": {":entity_type": {"S": "user"}}
    }))
    .expect("registration");
    let mut poller = AuxStorageGlobalPoller {
        client: AuxStorageStreamClient::new(&base_url, Duration::from_secs(1)).unwrap(),
        routes: CompiledSourceRoutes::from_manifest(&AnalyticsManifest::new(vec![registration]))
            .expect("compile routes"),
        last_evaluated_key: None,
        max_records_per_response: 1_000,
    };

    let batch = poller.poll_once(1).await.expect("poll aux-storage page");

    assert!(batch.records.is_empty());
    assert_eq!(batch.checkpoints.len(), 1);
    assert_eq!(batch.checkpoints[0].position, "seq-1");
}

#[tokio::test]
async fn given_registered_aux_storage_table_when_polled_by_name_then_only_that_table_is_polled() {
    let base_url = serve_once(
        r#"{"Records":[{"SourceTableName":"source_accounts","Keys":{"pk":{"S":"ACCOUNT#1"}},"SequenceNumber":"seq-account-1"}]}"#,
    )
    .await;
    let mut poller = SourcePoller::from_plans(
        &source_config(&base_url),
        vec![
            SourceTablePlan::aux_storage("source_users".to_string(), vec!["users".to_string()]),
            SourceTablePlan::aux_storage(
                "source_accounts".to_string(),
                vec!["accounts".to_string()],
            ),
        ],
        &[],
    )
    .await
    .expect("source poller");

    let batch = poller
        .poll_aux_storage_table("source_accounts")
        .await
        .expect("poll changed table");

    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].analytics_table_name, "accounts");
    assert_eq!(batch.records[0].record_key, "seq-account-1");
    assert_eq!(batch.checkpoints[0].source_table_name, GLOBAL_SOURCE_NAME);
}

#[tokio::test]
async fn given_unregistered_aux_storage_table_when_polled_by_name_then_empty_batch_is_returned() {
    let mut poller = SourcePoller::from_plans(
        &source_config("http://127.0.0.1:1"),
        vec![SourceTablePlan::aux_storage(
            "source_users".to_string(),
            vec!["users".to_string()],
        )],
        &[],
    )
    .await
    .expect("source poller");

    let batch = poller
        .poll_aux_storage_table("source_accounts")
        .await
        .expect("poll unknown table");

    assert!(batch.records.is_empty());
    assert!(batch.checkpoints.is_empty());
}

#[tokio::test]
async fn given_local_fixture_when_used_as_execution_inputs_then_snapshot_and_catchup_sources_are_split()
 {
    let snapshot_checkpoint = SnapshotChunkCheckpoint {
        source_table_name: "source_users".to_string(),
        chunk_id: "chunk-0001".to_string(),
    };
    let catchup_checkpoint = SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: AUX_STORAGE_SHARD_ID.to_string(),
        position: "seq-2".to_string(),
    };
    let snapshot_fixture = LocalExecutionSource::with_snapshot_chunks([SnapshotChunk {
        checkpoint: snapshot_checkpoint.clone(),
        records: vec![polled_record("users", "snapshot-1")],
    }]);
    let catchup_fixture = LocalExecutionSource::with_stream_batches([PollBatch {
        records: vec![polled_record("users", "seq-2")],
        checkpoints: vec![catchup_checkpoint.clone()],
    }]);
    let mut inputs = BackfillExecutionInputs::new(snapshot_fixture, catchup_fixture);

    let snapshot = inputs
        .snapshot_chunks
        .next_snapshot_chunk()
        .await
        .expect("read snapshot chunk")
        .expect("snapshot chunk");
    inputs
        .snapshot_chunks
        .commit_snapshot_chunk(&snapshot.checkpoint);
    let catchup = inputs
        .stream_catchup
        .poll_stream_catchup()
        .await
        .expect("poll catch-up stream");
    inputs
        .stream_catchup
        .commit_stream_catchup(&catchup.checkpoints);

    assert_eq!(snapshot.records[0].record_key, "snapshot-1");
    assert_eq!(catchup.records[0].record_key, "seq-2");
    assert_eq!(
        inputs.snapshot_chunks.committed_snapshot_chunks,
        vec![snapshot_checkpoint]
    );
    assert_eq!(
        inputs.stream_catchup.committed_stream_checkpoints,
        vec![catchup_checkpoint]
    );
}

async fn serve_once(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = Arc::new(body.to_string());
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0_u8; 4096];
        let _ = socket.read(&mut buffer).await.unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    format!("http://{addr}")
}

fn source_config(endpoint_url: &str) -> AnalyticsSourceConfig {
    AnalyticsSourceConfig {
        endpoint_url: Some(endpoint_url.to_string()),
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    }
}

fn test_routes() -> CompiledSourceRoutes {
    CompiledSourceRoutes::from_plans(&[SourceTablePlan::aux_storage(
        "source_users".to_string(),
        vec!["users".to_string()],
    )])
    .expect("compile routes")
}

fn stream_record(sequence_number: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: HashMap::default(),
        old_image: None,
        new_image: None,
    }
}

fn polled_record(analytics_table_name: &str, record_key: &str) -> PolledRecord {
    PolledRecord {
        source_table_name: "source_users".to_string(),
        analytics_table_name: analytics_table_name.to_string(),
        record_key: record_key.to_string(),
        record: stream_record(record_key),
    }
}

#[derive(Debug, Default)]
struct LocalExecutionSource {
    snapshot_chunks: VecDeque<SnapshotChunk>,
    stream_batches: VecDeque<PollBatch>,
    committed_snapshot_chunks: Vec<SnapshotChunkCheckpoint>,
    committed_stream_checkpoints: Vec<SourceCheckpoint>,
}

impl LocalExecutionSource {
    fn with_snapshot_chunks(chunks: impl IntoIterator<Item = SnapshotChunk>) -> Self {
        Self {
            snapshot_chunks: chunks.into_iter().collect(),
            ..Self::default()
        }
    }

    fn with_stream_batches(batches: impl IntoIterator<Item = PollBatch>) -> Self {
        Self {
            stream_batches: batches.into_iter().collect(),
            ..Self::default()
        }
    }
}

impl SnapshotChunkSource for LocalExecutionSource {
    async fn next_snapshot_chunk(
        &mut self,
    ) -> crate::AnalyticsStorageResult<Option<SnapshotChunk>> {
        Ok(self.snapshot_chunks.pop_front())
    }

    fn commit_snapshot_chunk(&mut self, checkpoint: &SnapshotChunkCheckpoint) {
        self.committed_snapshot_chunks.push(checkpoint.clone());
    }
}

impl StreamCatchupSource for LocalExecutionSource {
    async fn poll_stream_catchup(&mut self) -> crate::AnalyticsStorageResult<PollBatch> {
        Ok(self.stream_batches.pop_front().unwrap_or(PollBatch {
            records: Vec::new(),
            checkpoints: Vec::new(),
        }))
    }

    fn commit_stream_catchup(&mut self, checkpoints: &[SourceCheckpoint]) {
        self.committed_stream_checkpoints
            .extend(checkpoints.iter().cloned());
    }
}
