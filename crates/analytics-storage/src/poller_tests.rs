use std::{collections::HashMap, sync::Arc, time::Duration};

use analytics_contract::StorageStreamRecord;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    SourceCheckpoint,
    aux_storage_client::AuxStorageStreamClient,
    aws_stream::AwsShardIteratorState,
    poller::{
        AUX_STORAGE_SHARD_ID, AuxStorageTablePoller, apply_aws_iterator_checkpoint,
        aws_stream_response_batch, checkpoint_position, expand_records,
        iterator_checkpoint_shard_id, next_aux_storage_cursor, table_index_from_request_shard,
        table_request_names,
    },
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
    let mut poller = AuxStorageTablePoller {
        client: AuxStorageStreamClient::new("http://127.0.0.1:1", Duration::from_secs(1)).unwrap(),
        source_table_name: "source_users".to_string(),
        analytics_table_names: vec!["users".to_string()],
        last_evaluated_key: Some("old".to_string()),
    };

    poller.commit(&SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: AUX_STORAGE_SHARD_ID.to_string(),
        position: "new".to_string(),
    });

    assert_eq!(poller.last_evaluated_key.as_deref(), Some("new"));
}

#[test]
fn given_aux_storage_checkpoint_for_other_shard_when_committed_then_cursor_is_unchanged() {
    let mut poller = AuxStorageTablePoller {
        client: AuxStorageStreamClient::new("http://127.0.0.1:1", Duration::from_secs(1)).unwrap(),
        source_table_name: "source_users".to_string(),
        analytics_table_names: vec!["users".to_string()],
        last_evaluated_key: Some("old".to_string()),
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
fn given_pollable_tables_when_requests_are_planned_then_table_indexes_are_used_as_shard_ids() {
    assert_eq!(table_request_names(3), vec!["0", "1", "2"]);
    assert_eq!(table_index_from_request_shard("2"), Some(2));
    assert_eq!(table_index_from_request_shard("not-a-table-index"), None);
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
        r#"{"TableName":"source_users","Records":[{"Keys":{"pk":{"S":"USER#1"}},"SequenceNumber":"seq-1"},{"Keys":{"pk":{"S":"USER#2"}},"SequenceNumber":"seq-2"}]}"#,
    )
    .await;
    let mut poller = AuxStorageTablePoller {
        client: AuxStorageStreamClient::new(&base_url, Duration::from_secs(1)).unwrap(),
        source_table_name: "source_users".to_string(),
        analytics_table_names: vec!["users".to_string()],
        last_evaluated_key: None,
    };

    let batch = poller.poll_once(1).await.expect("poll aux-storage page");

    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.records[0].record_key, "seq-1");
    assert_eq!(batch.checkpoints.len(), 1);
    assert_eq!(batch.checkpoints[0].position, "seq-2");
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

fn stream_record(sequence_number: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: HashMap::default(),
        old_image: None,
        new_image: None,
    }
}
