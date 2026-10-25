use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use config::{
    AnalyticsSourceConfig, AnalyticsStreamType, RemoteCredentialsConfig,
    RemoteStaticCredentialsConfig,
};
use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};
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
        AuxStorageGlobalPoller, AwsStreamResponseBatchInput, apply_aws_iterator_checkpoint,
        aws_stream_response_batch, checkpoint_position, iterator_checkpoint_shard_id,
        route_records, test_support::serve_sequence_with_capture,
    },
    source_routes::CompiledSourceRoutes,
};

#[test]
fn given_nested_records_when_fanned_out_then_zero_one_many_destinations_preserve_images_and_order()
{
    let records = fanout_records();
    for count in [0, 1, 4, 16] {
        let tables = (0..count)
            .map(|index| format!("destination_{index}"))
            .collect::<Vec<_>>();
        let expanded = route_records("source", &tables, "shard", records.clone());
        assert_eq!(expanded.len(), if count == 0 { 0 } else { records.len() });
        for (output, expected) in expanded.iter().zip(&records) {
            assert_eq!(output.source_table_name, "source");
            assert_eq!(output.shard_id, "shard");
            assert_eq!(output.analytics_table_names, tables);
            assert_eq!(output.record_key, expected.sequence_number);
            assert_eq!(&output.record, expected);
            assert_eq!(output.system_position, None);
        }
    }
}

fn fanout_records() -> Vec<StorageStreamRecord> {
    let image = StorageItem::from_iter([
        (
            "nested".to_owned(),
            StorageValue::M(StorageItem::from_iter([(
                "list".to_owned(),
                StorageValue::L(vec![
                    StorageValue::S("payload".repeat(128)),
                    StorageValue::B("AAE=".to_owned()),
                ]),
            )])),
        ),
        (
            "strings".to_owned(),
            StorageValue::SS(vec!["a".to_owned(), "b".to_owned()]),
        ),
        (
            "numbers".to_owned(),
            StorageValue::NS(vec!["1".to_owned(), "2".to_owned()]),
        ),
        (
            "binaries".to_owned(),
            StorageValue::BS(vec!["AAE=".to_owned(), "AgM=".to_owned()]),
        ),
    ]);
    vec![
        StorageStreamRecord {
            sequence_number: "first".to_owned(),
            keys: StorageItem::new(),
            old_image: Some(image.clone()),
            new_image: Some(image.clone()),
        },
        StorageStreamRecord {
            sequence_number: "second".to_owned(),
            keys: image,
            old_image: None,
            new_image: None,
        },
    ]
}

#[tokio::test]
async fn given_system_stream_batch_when_committed_then_opaque_checkpoint_is_sent_on_next_poll() {
    let token = system_checkpoint();
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
        r#"{"Records":[{"SourceTableName":"source_users","Keys":{},"SequenceNumber":"seq-1","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-a","Offset":"1"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
        r#"{"Records":[],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
    ])
    .await;
    let source = source_config(&base_url);
    let mut poller = SourcePoller::from_plans(
        &source,
        vec![SourceTablePlan::aux_storage(
            "source_users".to_string(),
            vec!["users".to_string()],
        )],
        &[],
    )
    .await
    .expect("source poller");

    let first = poller
        .poll_aux_storage_table("source_users")
        .await
        .expect("selected system stream page");
    assert_eq!(first.records.len(), 1);
    assert_eq!(
        first.system_stream.as_ref().map(|batch| &batch.checkpoint),
        Some(&token)
    );
    poller.commit_batch(&first);
    poller
        .poll_aux_storage_table("source_users")
        .await
        .expect("poll committed system stream");

    let requests = requests.lock().expect("requests");
    let request = requests.get(2).expect("get request after commit");
    assert!(request.contains("\"SystemStream\":true"));
    assert!(request.contains(&format!("\"Checkpoint\":\"{}\"", token.as_str())));
    assert!(!request.contains("LastEvaluatedKey"));
}

#[tokio::test]
async fn given_system_stream_page_with_multiple_logical_tables_when_polled_then_each_lane_keeps_response_order()
 {
    let records_response = r#"{
            "Records": [
                {"SourceTableName":"source_users","Keys":{},"SequenceNumber":"seq-user-1","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-users","Offset":"1"}},
                {"SourceTableName":"source_accounts","Keys":{},"SequenceNumber":"seq-account-1","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-accounts","Offset":"7"}},
                {"SourceTableName":"source_users","Keys":{},"SequenceNumber":"seq-user-2","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-users","Offset":"2"}},
                {"SourceTableName":"source_accounts","Keys":{},"SequenceNumber":"seq-account-2","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-accounts","Offset":"8"}}
            ],
            "SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},
            "Checkpoint":"v1.0123456789abcdef0123456789abcdef"
        }"#;
    let expected_source_encoded_bytes = {
        let response: storage_types::GetStreamRecordsResponse =
            serde_json::from_str(records_response).expect("stream response");
        serde_json::to_vec(response.records())
            .expect("records serialization")
            .len()
    };
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
        records_response,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..Default::default()
    };
    let mut poller = SourcePoller::from_plans(
        &source,
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
        .poll_aux_storage_table("source_users")
        .await
        .expect("system stream page");

    assert_eq!(batch.source_record_count, 4);
    assert_eq!(batch.source_encoded_bytes, expected_source_encoded_bytes);
    assert_eq!(batch.records.len(), 4);
    assert_eq!(
        batch
            .records
            .iter()
            .flat_map(|record| record.analytics_table_names.iter().map(String::as_str))
            .collect::<Vec<_>>(),
        ["users", "accounts", "users", "accounts"]
    );
    for (analytics_table_name, expected_offsets) in
        [("users", ["1", "2"]), ("accounts", ["7", "8"])]
    {
        let offsets = batch
            .records
            .iter()
            .filter(|record| {
                record
                    .analytics_table_names
                    .iter()
                    .any(|name| name == analytics_table_name)
            })
            .map(|record| {
                record
                    .system_position
                    .as_ref()
                    .expect("logical position")
                    .offset()
                    .as_str()
            })
            .collect::<Vec<_>>();
        assert_eq!(offsets, expected_offsets);
    }
    assert_eq!(requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn given_unregistered_system_records_when_polled_then_selected_records_and_checkpoint_are_preserved()
 {
    for (response, expected_records) in [
        (
            r#"{"Records":[{"SourceTableName":"job","Keys":{},"SequenceNumber":"seq-job","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-job","Offset":"1"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
            0,
        ),
        (
            r#"{"Records":[{"SourceTableName":"job","Keys":{},"SequenceNumber":"seq-job","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-job","Offset":"1"}},{"SourceTableName":"source_users","Keys":{},"SequenceNumber":"seq-user","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-users","Offset":"2"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
            1,
        ),
    ] {
        let (base_url, requests) = serve_sequence_with_capture(vec![
            r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
            response,
            r#"{"Records":[],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
        ])
        .await;
        let mut poller = SourcePoller::from_plans(
            &source_config(&base_url),
            vec![SourceTablePlan::aux_storage(
                "source_users".to_string(),
                vec!["users".to_string()],
            )],
            &[],
        )
        .await
        .expect("source poller");

        let batch = poller.poll_once().await.expect("filtered page");
        assert_eq!(batch.source_record_count, 1 + expected_records);
        assert_eq!(batch.records.len(), expected_records);
        assert!(batch.records.iter().all(|record| {
            record.source_table_name == "source_users"
                && record.record.sequence_number == "seq-user"
        }));
        assert_eq!(
            batch
                .system_stream
                .as_ref()
                .map(|stream| stream.checkpoint.clone()),
            (expected_records != 0).then(system_checkpoint)
        );
        poller.commit_batch(&batch);
        poller.poll_once().await.expect("poll after filtered page");

        let requests = requests.lock().unwrap();
        assert!(requests[2].contains(&format!(
            "\"Checkpoint\":\"{}\"",
            system_checkpoint().as_str()
        )));
    }
}

#[tokio::test]
async fn given_system_stream_record_without_logical_metadata_when_polled_then_no_batch_is_returned()
{
    for response in [
        r#"{"Records":[{"SourceTableName":"source_users","Keys":{},"SequenceNumber":"seq-1"}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
        r#"{"Records":[{"Keys":{},"SequenceNumber":"seq-1","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-a","Offset":"1"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
        r#"{"Records":[{"SourceTableName":"job","Keys":{},"SequenceNumber":"seq-1"}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
    ] {
        let (base_url, requests) = serve_sequence_with_capture(vec![
            r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
            response,
        ])
        .await;
        let source = source_config(&base_url);
        let mut poller = SourcePoller::from_plans(
            &source,
            vec![SourceTablePlan::aux_storage(
                "source_users".to_string(),
                vec!["users".to_string()],
            )],
            &[],
        )
        .await
        .expect("source poller");

        assert!(poller.poll_aux_storage_table("source_users").await.is_err());
        assert_eq!(requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn given_resumed_system_stream_with_wrong_scope_when_constructed_then_get_is_not_attempted() {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
    ])
    .await;
    let source = source_config(&base_url);
    let resume = crate::SystemStreamResume::new(
        SystemStreamScope::try_new("other-system", 7).expect("resume scope"),
        system_checkpoint(),
    );

    let error = SourcePoller::from_plans_with_system_resume(
        &source,
        vec![SourceTablePlan::aux_storage(
            "source_users".to_string(),
            vec!["users".to_string()],
        )],
        &[],
        Some(resume),
    )
    .await
    .expect_err("mismatched system scope must fail closed");
    assert_eq!(error.category(), "invalid_source_batch_scope");
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(requests.lock().unwrap()[0].contains("DescribeSystemStream"));
}

#[tokio::test]
async fn given_reloaded_system_stream_scope_and_checkpoint_when_constructed_then_exact_checkpoint_is_resumed()
 {
    let token = system_checkpoint();
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
        r#"{"Records":[],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
    ])
    .await;
    let source = source_config(&base_url);
    let mut poller = SourcePoller::from_plans_with_system_resume(
        &source,
        vec![SourceTablePlan::aux_storage(
            "source_users".to_string(),
            vec!["users".to_string()],
        )],
        &[],
        Some(crate::SystemStreamResume::new(
            system_scope(),
            token.clone(),
        )),
    )
    .await
    .expect("source poller resumes after re-describe");

    let batch = poller
        .poll_aux_storage_table("source_users")
        .await
        .expect("resumed system stream page");
    assert!(
        batch.system_stream.is_none(),
        "idle resume must not create a durable job"
    );
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("DescribeSystemStream"));
    assert!(requests[1].contains("GetStreamRecords"));
    assert!(requests[1].contains("\"SystemStream\":true"));
    assert!(requests[1].contains(&format!("\"Checkpoint\":\"{}\"", token.as_str())));
    assert!(!requests[1].contains("LastEvaluatedKey"));
}

#[test]
fn given_checkpoints_when_matching_table_and_shard_exists_then_position_is_reused() {
    let checkpoints = vec![
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "cursor-users".to_string(),
        },
        SourceCheckpoint {
            source_table_name: "source_accounts".to_string(),
            shard_id: "shard-0002".to_string(),
            position: "cursor-accounts".to_string(),
        },
    ];

    assert_eq!(
        checkpoint_position(&checkpoints, "source_users", "shard-0001").as_deref(),
        Some("cursor-users")
    );
    assert_eq!(
        checkpoint_position(&checkpoints, "source_users", "other-shard"),
        None
    );
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
        AwsStreamResponseBatchInput {
            source_table_name: "source_users",
            analytics_table_names: &["users".to_string(), "accounts".to_string()],
            shard_id: "shard-0001",
            source_predecessor: None,
            contract_records: vec![stream_record("seq-1"), stream_record("seq-2")],
            next_iterator: Some("next-iterator".to_string()),
        },
        &mut Vec::new(),
    )
    .expect("batch stream response");

    assert_eq!(batch.source_response_count, 1);
    assert_eq!(batch.source_nonempty_response_count, 1);
    assert_eq!(batch.source_record_count, 2);
    assert!(batch.source_encoded_bytes > 0);
    assert_eq!(batch.records.len(), 2);
    for record in &batch.records {
        assert_eq!(record.analytics_table_names, ["users", "accounts"]);
    }
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
        AwsStreamResponseBatchInput {
            source_table_name: "source_users",
            analytics_table_names: &["users".to_string()],
            shard_id: "shard-0001",
            source_predecessor: None,
            contract_records: Vec::new(),
            next_iterator: Some("next-iterator".to_string()),
        },
        &mut Vec::new(),
    )
    .expect("batch empty stream response");

    assert_eq!(batch.source_response_count, 1);
    assert_eq!(batch.source_nonempty_response_count, 0);
    assert_eq!(batch.source_record_count, 0);
    assert_eq!(batch.source_encoded_bytes, 2);
    assert!(batch.records.is_empty());
    assert_eq!(batch.checkpoints.len(), 1);
    assert_eq!(batch.checkpoints[0].shard_id, "__iterator:shard-0001");
}

#[test]
fn given_reused_aws_serialization_buffer_when_pages_change_size_then_bytes_and_effects_match() {
    let mut buffer = Vec::new();
    let tables = ["users".to_string(), "accounts".to_string()];
    for page in 0..16 {
        let records: Vec<_> = (0..page % 4)
            .map(|record| stream_record(&format!("{page}-{record}-{}", "x".repeat(page * 31))))
            .collect();
        let expected_bytes = serde_json::to_vec(&records).unwrap().len();
        let iterator = format!("next-{page}");
        let batch = aws_stream_response_batch(
            AwsStreamResponseBatchInput {
                source_table_name: "source_users",
                analytics_table_names: &tables,
                shard_id: "shard-0001",
                source_predecessor: None,
                contract_records: records.clone(),
                next_iterator: Some(iterator.clone()),
            },
            &mut buffer,
        )
        .unwrap();
        assert_eq!(batch.source_encoded_bytes, expected_bytes);
        assert_eq!(buffer.len(), expected_bytes);
        assert_eq!(batch.source_record_count, records.len());
        assert_eq!(batch.records.len(), records.len());
        for (record, expected) in batch.records.iter().zip(&records) {
            assert_eq!(record.analytics_table_names, tables);
            assert_eq!(record.source_table_name, "source_users");
            assert_eq!(record.shard_id, "shard-0001");
            assert_eq!(record.record_key, expected.sequence_number);
            assert_eq!(
                serde_json::to_vec(&record.record).unwrap(),
                serde_json::to_vec(expected).unwrap()
            );
        }
        let mut checkpoints = Vec::new();
        if let Some(last) = records.last() {
            checkpoints.push(SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-0001".to_string(),
                position: last.sequence_number.clone(),
            });
        }
        checkpoints.push(SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "__iterator:shard-0001".to_string(),
            position: iterator,
        });
        assert_eq!(batch.checkpoints, checkpoints);
    }
}

#[tokio::test]
async fn given_system_stream_response_without_scope_when_polling_repeatedly_then_polling_fails_closed_without_advancing()
 {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"Records":[]}"#,
        r#"{"Records":[]}"#,
        r#"{"Records":[]}"#,
    ])
    .await;
    let mut poller = AuxStorageGlobalPoller {
        client: AuxStorageStreamClient::try_new(&base_url, Duration::from_secs(1)).unwrap(),
        routes: test_routes(),
        source_scope: system_scope(),
        checkpoint: None,
        scan_checkpoint: None,
        max_records_per_response: 1_000,
        serialization_buffer: Vec::new(),
    };

    for _ in 0..3 {
        let error = poller
            .poll_once(1)
            .await
            .expect_err("global AuxStorage polling must fail closed");
        assert_eq!(error.category(), "missing_response_scope");
        assert_eq!(poller.checkpoint, None);
    }

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("\"LastEvaluatedKey\":"))
    );
}

#[tokio::test]
async fn given_nonempty_system_stream_page_without_checkpoint_when_polling_then_polling_fails_closed()
 {
    let (base_url, _) = serve_sequence_with_capture(vec![
        r#"{"Records":[{"SourceTableName":"source_users","Keys":{},"SequenceNumber":"seq-1","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-a","Offset":"1"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
    ])
    .await;
    let mut poller = AuxStorageGlobalPoller {
        client: AuxStorageStreamClient::try_new(&base_url, Duration::from_secs(1)).unwrap(),
        routes: test_routes(),
        source_scope: system_scope(),
        checkpoint: None,
        scan_checkpoint: None,
        max_records_per_response: 8_192,
        serialization_buffer: Vec::new(),
    };

    let error = poller
        .poll_once(1)
        .await
        .expect_err("nonempty pages without a checkpoint must fail closed");

    assert_eq!(error.category(), "missing_response_checkpoint");
    assert_eq!(poller.checkpoint, None);
}

#[tokio::test]
async fn given_system_stream_provider_fault_when_polled_then_same_opaque_predecessor_is_retried() {
    let cases = [
        (
            "provider status failure",
            503,
            Some("ServiceUnavailable"),
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ServiceUnavailable"}"#,
        ),
        ("malformed JSON", 200, None, "{malformed-json"),
    ];

    for (label, status, error_type, first_body) in cases {
        let token = system_checkpoint();
        let valid_body = r#"{"Records":[{"SourceTableName":"source_users","Keys":{},"SequenceNumber":"seq-1","SystemPosition":{"ShardId":"shard-a","LineageId":"lineage-a","Offset":"1"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#;
        let (base_url, requests) = serve_status_sequence_with_capture(vec![
            (status, error_type, first_body),
            (200, None, valid_body),
        ])
        .await;
        let mut poller = AuxStorageGlobalPoller {
            client: AuxStorageStreamClient::try_new(&base_url, Duration::from_secs(1))
                .expect("AuxStorage client"),
            routes: test_routes(),
            source_scope: system_scope(),
            checkpoint: Some(token.clone()),
            scan_checkpoint: Some(token.clone()),
            max_records_per_response: 1_000,
            serialization_buffer: Vec::new(),
        };

        let error = poller
            .poll_once(1)
            .await
            .expect_err("provider failure must preserve the frontier");
        assert_eq!(
            error.category(),
            if status == 200 { "json" } else { "http_status" },
            "{label} error category"
        );
        assert_eq!(poller.checkpoint, Some(token.clone()));
        let retry = poller
            .poll_once(1)
            .await
            .expect("retry uses the unchanged predecessor");
        assert_eq!(
            retry
                .system_stream
                .as_ref()
                .and_then(|batch| batch.previous_checkpoint.clone()),
            Some(token.clone())
        );
        assert_eq!(poller.checkpoint, Some(token.clone()));

        let requests = requests.lock().expect("requests");
        assert_eq!(requests.len(), 2, "{label} must be retried once");
        assert!(
            requests
                .iter()
                .all(|request| request.contains(&format!("\"Checkpoint\":\"{}\"", token.as_str())))
        );
    }
}

#[tokio::test]
async fn given_empty_initial_aws_shard_discovery_when_shard_appears_then_poll_rediscovers_with_checkpoint()
 {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[]}}"#,
        r#"{"StreamDescription":{"Shards":[]}}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-1"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-2"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 1,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let checkpoints = vec![SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: "shard-0001".to_string(),
        position: "record-41".to_string(),
    }];
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &checkpoints)
        .await
        .expect("source poller");

    let base_time = Instant::now();
    let initial_batch = poller
        .poll_once_at(base_time)
        .await
        .expect("initial empty poll");
    assert_eq!(initial_batch.source_response_count, 0);

    let rediscovered_batch = poller
        .poll_once_at(base_time + Duration::from_secs(1))
        .await
        .expect("rediscovered poll");

    assert_eq!(rediscovered_batch.source_response_count, 1);
    assert_eq!(rediscovered_batch.checkpoints.len(), 1);
    assert_eq!(
        rediscovered_batch.checkpoints[0].shard_id,
        "__iterator:shard-0001"
    );
    assert!(requests.lock().unwrap().iter().any(|request| {
        request.contains("\"ShardIteratorType\":\"AFTER_SEQUENCE_NUMBER\"")
            && request.contains("\"SequenceNumber\":\"record-41\"")
    }));
}

#[tokio::test]
async fn given_closed_aws_shard_when_new_shard_appears_then_poll_rediscovers_without_duplicates() {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1","EndingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-1"}"#,
        r#"{"Records":[]}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1","EndingSequenceNumber":"1"}},{"ShardId":"shard-0002","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        r#"{"ShardIterator":"iterator-2"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-3"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let checkpoints = vec![SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: "shard-0002".to_string(),
        position: "record-22".to_string(),
    }];
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &checkpoints)
        .await
        .expect("source poller");

    let closed_batch = poller.poll_once().await.expect("closed shard poll");
    assert_eq!(closed_batch.source_response_count, 1);
    assert!(closed_batch.records.is_empty());

    let rediscovered_batch = poller.poll_once().await.expect("rediscovered shard poll");

    assert_eq!(rediscovered_batch.source_response_count, 1);
    assert_eq!(rediscovered_batch.checkpoints.len(), 1);
    assert_eq!(
        rediscovered_batch.checkpoints[0].shard_id,
        "__iterator:shard-0002"
    );
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.GetShardIterator"))
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-0001\"")
            })
            .count(),
        1
    );
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.GetShardIterator")
            && request.contains("\"ShardId\":\"shard-0002\"")
            && request.contains("\"ShardIteratorType\":\"AFTER_SEQUENCE_NUMBER\"")
            && request.contains("\"SequenceNumber\":\"record-22\"")
    }));
}

#[tokio::test]
async fn given_closed_aws_parent_when_live_sibling_remains_then_new_child_is_serviced_fairly() {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-a","SequenceNumberRange":{"StartingSequenceNumber":"1","EndingSequenceNumber":"1"}},{"ShardId":"shard-b","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        r#"{"ShardIterator":"iterator-a"}"#,
        r#"{"ShardIterator":"iterator-b"}"#,
        r#"{"Records":[]}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-c","SequenceNumberRange":{"StartingSequenceNumber":"3"}}]}}"#,
        r#"{"ShardIterator":"iterator-c"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-b-next"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-c-next"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    let closed_batch = poller.poll_once().await.expect("closed parent poll");
    assert_eq!(closed_batch.source_response_count, 1);
    let sibling_batch = poller.poll_once().await.expect("live sibling poll");
    assert_eq!(sibling_batch.source_response_count, 1);
    let child_batch = poller.poll_once().await.expect("new child poll");
    assert_eq!(child_batch.source_response_count, 1);

    let requests = requests.lock().unwrap();
    let child_iterator_requests = requests
        .iter()
        .filter(|request| {
            request.contains("DynamoDBStreams_20120810.GetShardIterator")
                && request.contains("\"ShardId\":\"shard-c\"")
        })
        .count();
    assert_eq!(child_iterator_requests, 1);
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-b\"")
            })
            .count(),
        1
    );
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.DescribeStream")
            && request.contains("\"ExclusiveStartShardId\":\"shard-b\"")
    }));
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.GetRecords")
            && request.contains("\"ShardIterator\":\"iterator-c\"")
    }));
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.GetRecords")
            && request.contains("\"ShardIterator\":\"iterator-b\"")
    }));
}

#[tokio::test]
async fn given_live_aws_shard_when_polled_repeatedly_then_discovery_stays_quiet() {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-1"}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]},"Records":[],"NextShardIterator":"iterator-1-next"}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]},"Records":[],"NextShardIterator":"iterator-1-next-2"}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]},"Records":[],"NextShardIterator":"iterator-1-next-3"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    for _ in 0..3 {
        let batch = poller.poll_once().await.expect("poll live shard");
        assert_eq!(batch.source_response_count, 1);
        poller.commit(&batch.checkpoints);
    }

    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        1,
        "steady-state polling must not rediscover live shards"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.GetRecords"))
            .count(),
        3
    );
}

#[tokio::test]
async fn given_older_aws_page_after_newer_commit_when_committing_replayed_page_then_progress_does_not_rewind()
 {
    let (base_url, _requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-a"}"#,
        r#"{"Records":[{"dynamodb":{"Keys":{"pk":{"S":"key-a"}},"SequenceNumber":"record-a"}}],"NextShardIterator":"iterator-b"}"#,
        r#"{"Records":[{"dynamodb":{"Keys":{"pk":{"S":"key-b"}},"SequenceNumber":"record-b"}}],"NextShardIterator":"iterator-c"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..Default::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    let first = poller.poll_once().await.expect("first page");
    let second = poller.poll_once().await.expect("second page");
    assert_eq!(first.source_predecessor, None);
    assert_eq!(second.source_predecessor, None);

    poller.commit_batch(&second);
    poller.commit_batch(&first);

    assert_eq!(
        poller
            .aws_record_checkpoint("source_users", "shard-0001")
            .map(|checkpoint| checkpoint.position),
        Some("record-b".to_string())
    );
}

#[tokio::test]
async fn given_empty_aws_stream_when_polled_at_high_frequency_then_discovery_waits_for_deadline() {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[]}}"#,
        r#"{"StreamDescription":{"Shards":[]}}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-1"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-1-next"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    let base_time = Instant::now();
    for _ in 0..100 {
        let batch = poller
            .poll_once_at(base_time)
            .await
            .expect("poll empty stream");
        assert_eq!(batch.source_response_count, 0);
    }

    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        2,
        "high-frequency empty polling must not rediscover before its deadline"
    );

    let before_deadline = poller
        .poll_once_at(base_time + Duration::from_millis(999))
        .await
        .expect("poll before deadline");
    assert_eq!(before_deadline.source_response_count, 0);
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        2
    );

    let due_batch = poller
        .poll_once_at(base_time + Duration::from_secs(1))
        .await
        .expect("poll at deadline");
    assert_eq!(due_batch.source_response_count, 1);

    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        3,
        "the first due retry should discover the delayed shard"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-0001\"")
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn given_persistent_empty_aws_stream_when_polling_before_and_at_discovery_deadlines_then_rediscovery_uses_exponential_wall_clock_backoff()
 {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[]}}"#,
        r#"{"StreamDescription":{"Shards":[]}}"#,
        r#"{"StreamDescription":{"Shards":[]}}"#,
        r#"{"StreamDescription":{"Shards":[]}}"#,
        r#"{"StreamDescription":{"Shards":[]}}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    let base_time = Instant::now();
    for timestamp in [
        base_time,
        base_time + Duration::from_millis(999),
        base_time + Duration::from_secs(1),
        base_time + Duration::from_millis(2_999),
        base_time + Duration::from_secs(3),
        base_time + Duration::from_millis(6_999),
        base_time + Duration::from_secs(7),
    ] {
        let batch = poller
            .poll_once_at(timestamp)
            .await
            .expect("poll empty stream");
        assert_eq!(batch.source_response_count, 0);
    }

    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        5,
        "empty-stream discovery should run at 0s, 1s, 3s, and 7s after startup"
    );
}

#[tokio::test]
async fn given_due_discovery_error_when_polling_before_and_at_retry_deadline_then_retry_waits_for_wall_clock_deadline()
 {
    let (base_url, requests) = serve_status_sequence_with_capture(vec![
        (200, None, r#"{"StreamDescription":{"Shards":[]}}"#),
        (200, None, r#"{"StreamDescription":{"Shards":[]}}"#),
        (
            400,
            Some("ValidationException"),
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ValidationException"}"#,
        ),
        (200, None, r#"{"StreamDescription":{"Shards":[]}}"#),
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    let base_time = Instant::now();
    assert_eq!(
        poller
            .poll_once_at(base_time)
            .await
            .expect("initial empty poll")
            .source_response_count,
        0
    );
    let due_result = poller
        .poll_once_at(base_time + Duration::from_secs(1))
        .await;
    assert!(due_result.is_err());

    for _ in 0..100 {
        let batch = poller
            .poll_once_at(base_time + Duration::from_secs(1))
            .await
            .expect("poll before retry deadline");
        assert_eq!(batch.source_response_count, 0);
    }
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        3,
        "failed due discovery must not be retried at poll frequency"
    );

    let retry_batch = poller
        .poll_once_at(base_time + Duration::from_secs(3))
        .await
        .expect("retry at backoff deadline");
    assert_eq!(retry_batch.source_response_count, 0);
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        4,
        "the retry should run once when its wall-clock deadline arrives"
    );
}

#[tokio::test]
async fn given_pending_shard_iterator_error_when_polling_before_and_at_retry_deadline_then_failed_shard_is_retried_before_later_shards()
 {
    let (base_url, requests) = serve_status_sequence_with_capture(vec![
        (
            200,
            None,
            r#"{"StreamDescription":{"Shards":[]}}"#,
        ),
        (
            200,
            None,
            r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-a","SequenceNumberRange":{"StartingSequenceNumber":"1"}},{"ShardId":"shard-b","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        ),
        (
            400,
            Some("ValidationException"),
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ValidationException"}"#,
        ),
        (200, None, r#"{"ShardIterator":"iterator-a"}"#),
        (200, None, r#"{"ShardIterator":"iterator-b"}"#),
        (
            200,
            None,
            r#"{"Records":[],"NextShardIterator":"iterator-a-next"}"#,
        ),
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    let base_time = Instant::now();
    assert!(poller.poll_once_at(base_time).await.is_err());
    assert_eq!(poller.aws_pending_shard_count(), 2);
    for _ in 0..100 {
        let batch = poller
            .poll_once_at(base_time)
            .await
            .expect("poll before failed shard retry deadline");
        assert_eq!(batch.source_response_count, 0);
    }

    let retry_batch = poller
        .poll_once_at(base_time + Duration::from_secs(1))
        .await
        .expect("retry failed shard at deadline");
    assert_eq!(retry_batch.source_response_count, 1);
    assert_eq!(poller.aws_pending_shard_count(), 0);

    let requests = requests.lock().unwrap();
    let describe_count = requests
        .iter()
        .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
        .count();
    assert_eq!(
        describe_count, 2,
        "pending iterator retry must not rediscover"
    );
    let shard_a_iterator_requests = requests
        .iter()
        .filter(|request| {
            request.contains("DynamoDBStreams_20120810.GetShardIterator")
                && request.contains("\"ShardId\":\"shard-a\"")
        })
        .count();
    assert_eq!(shard_a_iterator_requests, 2);
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-b\"")
            })
            .count(),
        1
    );
    let iterator_request_order = requests
        .iter()
        .filter(|request| request.contains("DynamoDBStreams_20120810.GetShardIterator"))
        .map(|request| {
            if request.contains("\"ShardId\":\"shard-a\"") {
                "a"
            } else if request.contains("\"ShardId\":\"shard-b\"") {
                "b"
            } else {
                "unknown"
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(iterator_request_order, ["a", "a", "b"]);
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.GetRecords")
            && request.contains("\"ShardIterator\":\"iterator-a\"")
    }));
}

#[tokio::test]
async fn given_partial_discovery_success_then_iterator_failure_when_polled_at_same_time_then_retry_waits_for_deadline()
 {
    let (base_url, requests) = serve_status_sequence_with_capture(vec![
        (
            200,
            None,
            r#"{"StreamDescription":{"Shards":[]}}"#,
        ),
        (
            200,
            None,
            r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-a","SequenceNumberRange":{"StartingSequenceNumber":"1"}},{"ShardId":"shard-b","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        ),
        (200, None, r#"{"ShardIterator":"iterator-a"}"#),
        (
            400,
            Some("ValidationException"),
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ValidationException"}"#,
        ),
        (200, None, r#"{"ShardIterator":"iterator-b"}"#),
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        // Keep this scheduler trace focused on discovery and iterator calls.
        poll_max_responses_per_interval: 0,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    let base_time = Instant::now();
    assert!(poller.poll_once_at(base_time).await.is_err());
    assert_eq!(poller.aws_shard_iterator_count(), 1);
    assert_eq!(poller.aws_pending_shard_count(), 1);

    for _ in 0..100 {
        let batch = poller
            .poll_once_at(base_time)
            .await
            .expect("poll before failed iterator retry deadline");
        assert_eq!(batch.source_response_count, 0);
    }
    let (describe_count_before_deadline, iterator_count_before_deadline) = {
        let requests_before_deadline = requests.lock().unwrap();
        (
            requests_before_deadline
                .iter()
                .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
                .count(),
            requests_before_deadline
                .iter()
                .filter(|request| request.contains("DynamoDBStreams_20120810.GetShardIterator"))
                .count(),
        )
    };
    assert_eq!(
        describe_count_before_deadline, 2,
        "partial discovery failure must not trigger another DescribeStream"
    );
    assert_eq!(
        iterator_count_before_deadline, 2,
        "partial discovery failure must not retry B before its deadline"
    );

    let retry_batch = poller
        .poll_once_at(base_time + Duration::from_secs(1))
        .await
        .expect("retry failed iterator at deadline");
    assert_eq!(retry_batch.source_response_count, 0);
    assert_eq!(poller.aws_shard_iterator_count(), 2);
    assert_eq!(poller.aws_pending_shard_count(), 0);

    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-a\"")
            })
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-b\"")
            })
            .count(),
        2,
        "the failed shard must be the one retried at the deadline"
    );
}

#[tokio::test]
async fn given_delayed_aws_child_after_empty_rediscovery_when_polling_sibling_and_rediscovering_then_child_is_eventually_serviced()
 {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-a","SequenceNumberRange":{"StartingSequenceNumber":"1","EndingSequenceNumber":"1"}},{"ShardId":"shard-b","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        r#"{"ShardIterator":"iterator-a"}"#,
        r#"{"ShardIterator":"iterator-b"}"#,
        r#"{"Records":[]}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-b","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-b-next"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-b-next-2"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-b-next-3"}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-c","SequenceNumberRange":{"StartingSequenceNumber":"3"}}]}}"#,
        r#"{"ShardIterator":"iterator-c"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-b-next-4"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-c-next"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    let base_time = Instant::now();
    let closed_batch = poller
        .poll_once_at(base_time)
        .await
        .expect("closed parent poll");
    assert_eq!(closed_batch.source_response_count, 1);

    let rediscovery_batch = poller
        .poll_once_at(base_time)
        .await
        .expect("empty child rediscovery");
    assert_eq!(rediscovery_batch.source_response_count, 1);

    let sibling_batch = poller
        .poll_once_at(base_time)
        .await
        .expect("live sibling poll");
    assert_eq!(sibling_batch.source_response_count, 1);
    let sibling_batch = poller
        .poll_once_at(base_time)
        .await
        .expect("live sibling poll");
    assert_eq!(sibling_batch.source_response_count, 1);

    let child_discovery_batch = poller
        .poll_once_at(base_time + Duration::from_secs(1))
        .await
        .expect("delayed child discovery");
    assert_eq!(child_discovery_batch.source_response_count, 1);

    let child_batch = poller
        .poll_once_at(base_time + Duration::from_secs(1))
        .await
        .expect("new child poll");
    assert_eq!(child_batch.source_response_count, 1);

    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        3
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-c\"")
            })
            .count(),
        1
    );
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.GetRecords")
            && request.contains("\"ShardIterator\":\"iterator-c\"")
    }));
}

#[tokio::test]
async fn given_expired_aws_iterator_when_polled_then_only_that_shard_rebuilds_after_checkpoint() {
    let (base_url, requests) = serve_status_sequence_with_capture(vec![
        (200, None, r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-a","SequenceNumberRange":{"StartingSequenceNumber":"1"}},{"ShardId":"shard-b","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#),
        (200, None, r#"{"ShardIterator":"iterator-a-old"}"#),
        (200, None, r#"{"ShardIterator":"iterator-b"}"#),
        (400, Some("ExpiredIteratorException"), r#"{"__type":"com.amazonaws.dynamodb.v20120810#ExpiredIteratorException","message":"expired"}"#),
        (200, None, r#"{"ShardIterator":"iterator-a-new"}"#),
        (200, None, r#"{"Records":[],"NextShardIterator":"iterator-b-next"}"#),
        (200, None, r#"{"Records":[],"NextShardIterator":"iterator-a-next"}"#),
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(
        &source,
        vec![plan],
        &[
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-a".to_string(),
                position: "record-a-41".to_string(),
            },
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-b".to_string(),
                position: "record-b-22".to_string(),
            },
        ],
    )
    .await
    .expect("source poller");

    let expired_batch = poller.poll_once().await.expect("expired iterator recovery");
    assert_eq!(expired_batch.source_response_count, 0);
    let sibling_batch = poller.poll_once().await.expect("sibling poll");
    assert_eq!(sibling_batch.source_response_count, 1);
    let rebuilt_batch = poller.poll_once().await.expect("rebuilt shard poll");
    assert_eq!(rebuilt_batch.source_response_count, 1);

    let requests = requests.lock().unwrap();
    let shard_a_iterator_requests = requests
        .iter()
        .filter(|request| {
            request.contains("DynamoDBStreams_20120810.GetShardIterator")
                && request.contains("\"ShardId\":\"shard-a\"")
        })
        .collect::<Vec<_>>();
    let shard_b_iterator_requests = requests
        .iter()
        .filter(|request| {
            request.contains("DynamoDBStreams_20120810.GetShardIterator")
                && request.contains("\"ShardId\":\"shard-b\"")
        })
        .collect::<Vec<_>>();
    assert_eq!(shard_a_iterator_requests.len(), 2);
    assert_eq!(shard_b_iterator_requests.len(), 1);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        1
    );
    assert!(shard_a_iterator_requests.iter().any(|request| {
        request.contains("\"ShardIteratorType\":\"AFTER_SEQUENCE_NUMBER\"")
            && request.contains("\"SequenceNumber\":\"record-a-41\"")
    }));
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.GetRecords")
            && request.contains("\"ShardIterator\":\"iterator-a-new\"")
    }));
    assert!(!requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.GetShardIterator")
            && request.contains("\"ShardId\":\"shard-b\"")
            && request.contains("record-a-41")
    }));
}

#[tokio::test]
async fn given_expired_iterator_rebuild_error_when_polling_sibling_before_and_at_retry_deadline_then_retry_waits_for_wall_clock_deadline()
 {
    let expired_response = (
        400,
        Some("ExpiredIteratorException"),
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ExpiredIteratorException","message":"expired"}"#,
    );
    let sibling_response = (
        200,
        None,
        r#"{"Records":[],"NextShardIterator":"iterator-b-next"}"#,
    );
    let mut responses = vec![
        (
            200,
            None,
            r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-a","SequenceNumberRange":{"StartingSequenceNumber":"1"}},{"ShardId":"shard-b","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        ),
        (200, None, r#"{"ShardIterator":"iterator-a-old"}"#),
        (200, None, r#"{"ShardIterator":"iterator-b"}"#),
        expired_response,
        (
            400,
            Some("ValidationException"),
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ValidationException"}"#,
        ),
    ];
    responses.extend((0..100).map(|_| sibling_response));
    responses.push((200, None, r#"{"ShardIterator":"iterator-a-new"}"#));
    responses.push(sibling_response);
    responses.push((
        200,
        None,
        r#"{"Records":[],"NextShardIterator":"iterator-a-new-next"}"#,
    ));
    let (base_url, requests) = serve_status_sequence_with_capture(responses).await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(
        &source,
        vec![plan],
        &[
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-a".to_string(),
                position: "record-a-41".to_string(),
            },
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-b".to_string(),
                position: "record-b-22".to_string(),
            },
        ],
    )
    .await
    .expect("source poller");

    let base_time = Instant::now();
    assert!(poller.poll_once_at(base_time).await.is_err());
    assert_eq!(poller.aws_shard_iterator_count(), 1);
    assert_eq!(poller.aws_pending_shard_count(), 0);
    for _ in 0..100 {
        let batch = poller
            .poll_once_at(base_time)
            .await
            .expect("poll live sibling while expired shard is suspended");
        assert_eq!(batch.source_response_count, 1);
    }

    let (describe_count, shard_a_iterator_count, shard_b_iterator_count, shard_a_record_count) = {
        let requests = requests.lock().unwrap();
        (
            requests
                .iter()
                .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
                .count(),
            requests
                .iter()
                .filter(|request| {
                    request.contains("DynamoDBStreams_20120810.GetShardIterator")
                        && request.contains("\"ShardId\":\"shard-a\"")
                })
                .count(),
            requests
                .iter()
                .filter(|request| {
                    request.contains("DynamoDBStreams_20120810.GetShardIterator")
                        && request.contains("\"ShardId\":\"shard-b\"")
                })
                .count(),
            requests
                .iter()
                .filter(|request| {
                    request.contains("DynamoDBStreams_20120810.GetRecords")
                        && request.contains("\"ShardIterator\":\"iterator-a-old\"")
                })
                .count(),
        )
    };
    assert_eq!(describe_count, 1);
    assert_eq!(
        shard_a_iterator_count, 2,
        "rebuild failure must not retry early"
    );
    assert_eq!(shard_b_iterator_count, 1);
    assert_eq!(
        shard_a_record_count, 1,
        "expired A must be suspended after one call"
    );

    let retry_batch = poller
        .poll_once_at(base_time + Duration::from_secs(1))
        .await
        .expect("expired iterator rebuild at deadline");
    assert_eq!(retry_batch.source_response_count, 1);
    let resumed_batch = poller
        .poll_once_at(base_time + Duration::from_secs(1))
        .await
        .expect("poll rebuilt shard");
    assert_eq!(resumed_batch.source_response_count, 1);
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.GetShardIterator"))
            .count(),
        4,
        "expired iterator rebuild should retry once at its deadline"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetRecords")
                    && request.contains("\"ShardIterator\":\"iterator-a-old\"")
            })
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetRecords")
                    && request.contains("\"ShardIterator\":\"iterator-b\"")
            })
            .count(),
        101
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetRecords")
                    && request.contains("\"ShardIterator\":\"iterator-a-new\"")
            })
            .count(),
        1,
        "the rebuilt shard should resume only after its replacement succeeds"
    );
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.GetShardIterator")
            && request.contains("\"ShardId\":\"shard-a\"")
            && request.contains("\"ShardIteratorType\":\"AFTER_SEQUENCE_NUMBER\"")
            && request.contains("\"SequenceNumber\":\"record-a-41\"")
    }));
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.GetRecords"))
            .count(),
        103
    );
}

#[tokio::test]
async fn given_two_expired_shards_when_first_rebuild_stays_empty_then_later_shard_is_retried_fairly()
 {
    let expired_response = (
        400,
        Some("ExpiredIteratorException"),
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ExpiredIteratorException","message":"expired"}"#,
    );
    let mut responses = vec![
        (
            200,
            None,
            r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-a","SequenceNumberRange":{"StartingSequenceNumber":"1"}},{"ShardId":"shard-b","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        ),
        (200, None, r#"{"ShardIterator":"iterator-a-old"}"#),
        (200, None, r#"{"ShardIterator":"iterator-b-old"}"#),
        expired_response,
        (200, None, r#"{}"#),
        expired_response,
        (200, None, r#"{}"#),
        (200, None, r#"{"ShardIterator":"iterator-b-new"}"#),
        (
            200,
            None,
            r#"{"Records":[],"NextShardIterator":"iterator-b-new-next"}"#,
        ),
        (
            200,
            None,
            r#"{"Records":[],"NextShardIterator":"iterator-b-new-next-2"}"#,
        ),
        (200, None, r#"{}"#),
        (
            200,
            None,
            r#"{"Records":[],"NextShardIterator":"iterator-b-new-next-3"}"#,
        ),
    ];
    let (base_url, requests) =
        serve_status_sequence_with_capture(std::mem::take(&mut responses)).await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(
        &source,
        vec![plan],
        &[
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-a".to_string(),
                position: "record-a-41".to_string(),
            },
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-b".to_string(),
                position: "record-b-22".to_string(),
            },
        ],
    )
    .await
    .expect("source poller");

    let base_time = Instant::now();
    assert_eq!(
        poller
            .poll_once_at(base_time)
            .await
            .expect("suspend A after expired iterator")
            .source_response_count,
        0
    );
    assert_eq!(poller.aws_shard_iterator_count(), 1);
    assert_eq!(poller.aws_pending_expired_shard_ids(), ["shard-a"]);

    assert_eq!(
        poller
            .poll_once_at(base_time)
            .await
            .expect("suspend B after expired iterator")
            .source_response_count,
        0
    );
    assert_eq!(poller.aws_shard_iterator_count(), 0);
    assert_eq!(
        poller.aws_pending_expired_shard_ids(),
        ["shard-a", "shard-b"]
    );

    assert_eq!(
        poller
            .poll_once_at(base_time + Duration::from_secs(1))
            .await
            .expect("retry both expired shards once at deadline")
            .source_response_count,
        1
    );
    assert_eq!(poller.aws_shard_iterator_count(), 1);
    assert_eq!(poller.aws_pending_expired_shard_ids(), ["shard-a"]);
    assert_eq!(
        poller
            .poll_once_at(base_time + Duration::from_secs(1))
            .await
            .expect("continue polling B while A is backed off")
            .source_response_count,
        1
    );
    assert_eq!(poller.aws_pending_expired_shard_ids(), ["shard-a"]);

    assert_eq!(
        poller
            .poll_once_at(base_time + Duration::from_secs(3))
            .await
            .expect("retry A at its next backoff deadline")
            .source_response_count,
        1
    );
    assert_eq!(poller.aws_pending_expired_shard_ids(), ["shard-a"]);

    let requests = requests.lock().unwrap();
    let iterator_request_order = requests
        .iter()
        .filter(|request| request.contains("DynamoDBStreams_20120810.GetShardIterator"))
        .map(|request| {
            if request.contains("\"ShardId\":\"shard-a\"") {
                "a"
            } else if request.contains("\"ShardId\":\"shard-b\"") {
                "b"
            } else {
                "unknown"
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(iterator_request_order, ["a", "b", "a", "a", "b", "a"]);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        1,
        "expired rebuilds must not rediscover the stream"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-a\"")
            })
            .count(),
        4,
        "A is retried once per deadline without duplicate queue entries"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-b\"")
            })
            .count(),
        2,
        "B must not be starved behind A"
    );
    assert!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains("\"ShardId\":\"shard-a\"")
            })
            .all(|request| {
                request.contains("\"ShardIteratorType\":\"AFTER_SEQUENCE_NUMBER\"")
                    && request.contains("\"SequenceNumber\":\"record-a-41\"")
            })
    );
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.GetShardIterator")
            && request.contains("\"ShardId\":\"shard-b\"")
            && request.contains("\"ShardIteratorType\":\"AFTER_SEQUENCE_NUMBER\"")
            && request.contains("\"SequenceNumber\":\"record-b-22\"")
    }));
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetRecords")
                    && request.contains("\"ShardIterator\":\"iterator-a-old\"")
            })
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetRecords")
                    && request.contains("\"ShardIterator\":\"iterator-b-old\"")
            })
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.contains("DynamoDBStreams_20120810.GetRecords")
                    && request.contains("\"ShardIterator\":\"iterator-b-new\"")
            })
            .count(),
        3,
        "B continues across A's later failed deadline"
    );
}

#[tokio::test]
async fn given_multiple_aws_shard_rotations_when_polled_then_iterator_state_stays_bounded() {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1","EndingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-1"}"#,
        r#"{"Records":[]}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1","EndingSequenceNumber":"1"}},{"ShardId":"shard-0002","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        r#"{"ShardIterator":"iterator-2"}"#,
        r#"{"Records":[]}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1","EndingSequenceNumber":"1"}},{"ShardId":"shard-0002","SequenceNumberRange":{"StartingSequenceNumber":"2","EndingSequenceNumber":"2"}},{"ShardId":"shard-0003","SequenceNumberRange":{"StartingSequenceNumber":"3"}}]}}"#,
        r#"{"ShardIterator":"iterator-3"}"#,
        r#"{"Records":[]}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1","EndingSequenceNumber":"1"}},{"ShardId":"shard-0002","SequenceNumberRange":{"StartingSequenceNumber":"2","EndingSequenceNumber":"2"}},{"ShardId":"shard-0003","SequenceNumberRange":{"StartingSequenceNumber":"3","EndingSequenceNumber":"3"}},{"ShardId":"shard-0004","SequenceNumberRange":{"StartingSequenceNumber":"4"}}]}}"#,
        r#"{"ShardIterator":"iterator-4"}"#,
        r#"{"Records":[]}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 1,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let checkpoints = vec![
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0002".to_string(),
            position: "record-2".to_string(),
        },
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0003".to_string(),
            position: "record-3".to_string(),
        },
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0004".to_string(),
            position: "record-4".to_string(),
        },
    ];
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &checkpoints)
        .await
        .expect("source poller");

    for rotation in 1..=4 {
        let batch = poller.poll_once().await.expect("poll rotated shard");
        assert_eq!(batch.source_response_count, 1, "rotation {rotation}");
        assert_eq!(poller.aws_shard_iterator_count(), 0, "rotation {rotation}");
        assert_eq!(
            poller.aws_pending_terminal_count(),
            0,
            "rotation {rotation}"
        );
    }

    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.GetShardIterator"))
            .count(),
        4
    );
    for (shard_id, position) in [
        ("shard-0001", None),
        ("shard-0002", Some("record-2")),
        ("shard-0003", Some("record-3")),
        ("shard-0004", Some("record-4")),
    ] {
        let shard_requests = requests.iter().filter(|request| {
            request.contains("DynamoDBStreams_20120810.GetShardIterator")
                && request.contains(&format!("\"ShardId\":\"{shard_id}\""))
        });
        assert_eq!(shard_requests.count(), 1, "shard {shard_id}");
        if let Some(position) = position {
            assert!(requests.iter().any(|request| {
                request.contains("DynamoDBStreams_20120810.GetShardIterator")
                    && request.contains(&format!("\"ShardId\":\"{shard_id}\""))
                    && request.contains("\"ShardIteratorType\":\"AFTER_SEQUENCE_NUMBER\"")
                    && request.contains(&format!("\"SequenceNumber\":\"{position}\""))
            }));
        }
    }
}

#[tokio::test]
async fn given_more_active_aws_shards_than_response_budget_when_polling_and_committing_each_response_then_service_rotates_by_shard_id()
 {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}},{"ShardId":"shard-0002","SequenceNumberRange":{"StartingSequenceNumber":"2"}},{"ShardId":"shard-0003","SequenceNumberRange":{"StartingSequenceNumber":"3"}}]}}"#,
        r#"{"ShardIterator":"iterator-1"}"#,
        r#"{"ShardIterator":"iterator-2"}"#,
        r#"{"ShardIterator":"iterator-3"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-1-next"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-2-next"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-3-next"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-1-next-2"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 3,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[])
        .await
        .expect("source poller");

    for expected_iterator in ["iterator-1", "iterator-2", "iterator-3", "iterator-1-next"] {
        let batch = poller.poll_once().await.expect("poll active shard");
        assert_eq!(batch.source_response_count, 1);
        poller.commit(&batch.checkpoints);
        let requests = requests.lock().unwrap();
        let request = requests
            .iter()
            .rev()
            .find(|request| request.contains("DynamoDBStreams_20120810.GetRecords"))
            .expect("get records request");
        assert!(
            request.contains(&format!("\"ShardIterator\":\"{expected_iterator}\"")),
            "expected {expected_iterator} in {request}"
        );
    }
}

#[tokio::test]
async fn given_invalid_aws_record_response_when_polling_failed_shard_again_then_round_robin_cursor_and_iterator_are_retried()
 {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}},{"ShardId":"shard-0002","SequenceNumberRange":{"StartingSequenceNumber":"2"}}]}}"#,
        r#"{"ShardIterator":"iterator-1"}"#,
        r#"{"ShardIterator":"iterator-2"}"#,
        r#"{"Records":[{"dynamodb":{"SequenceNumber":"record-1"}}]}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-1-next"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let durable_checkpoint = SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: "shard-0001".to_string(),
        position: "checkpoint-before".to_string(),
    };
    let mut poller = SourcePoller::from_plans(
        &source,
        vec![plan],
        std::slice::from_ref(&durable_checkpoint),
    )
    .await
    .expect("source poller");

    assert_eq!(
        poller.aws_record_checkpoint("source_users", "shard-0001"),
        Some(durable_checkpoint.clone())
    );
    assert!(poller.poll_once().await.is_err());
    assert_eq!(
        poller.aws_record_checkpoint("source_users", "shard-0001"),
        Some(durable_checkpoint.clone()),
        "failed record decoding must not advance the durable checkpoint"
    );
    let retry_batch = poller.poll_once().await.expect("retry failed shard");
    assert_eq!(retry_batch.source_response_count, 1);
    assert!(retry_batch.records.is_empty());
    assert_eq!(
        poller.aws_record_checkpoint("source_users", "shard-0001"),
        Some(durable_checkpoint),
        "retry remains uncommitted until the destination effect settles"
    );

    let records_requests = requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.contains("DynamoDBStreams_20120810.GetRecords"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(records_requests.len(), 2);
    assert!(
        records_requests
            .iter()
            .all(|request| request.contains("\"ShardIterator\":\"iterator-1\""))
    );
}

#[tokio::test]
async fn given_aws_lease_reacquisition_when_checkpoints_reload_then_iterator_restarts_after_durable_position()
 {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-old"}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-new"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-new-next"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 1,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let initial_checkpoint = SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: "shard-0001".to_string(),
        position: "record-41".to_string(),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[initial_checkpoint])
        .await
        .expect("source poller");

    poller
        .reload_checkpoints(&[SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "record-99".to_string(),
        }])
        .await
        .expect("reload checkpoints");
    let batch = poller.poll_once().await.expect("poll reloaded iterator");
    assert_eq!(batch.source_response_count, 1);

    let requests = requests.lock().unwrap();
    let iterator_requests = requests
        .iter()
        .filter(|request| request.contains("DynamoDBStreams_20120810.GetShardIterator"))
        .collect::<Vec<_>>();
    assert_eq!(iterator_requests.len(), 2);
    assert!(iterator_requests[0].contains("\"SequenceNumber\":\"record-41\""));
    assert!(iterator_requests[1].contains("\"SequenceNumber\":\"record-99\""));
    let records_request = requests
        .iter()
        .find(|request| request.contains("DynamoDBStreams_20120810.GetRecords"))
        .expect("get records request");
    assert!(records_request.contains("\"ShardIterator\":\"iterator-new\""));
}

#[tokio::test]
async fn given_paginated_closed_startup_shards_beyond_capacity_when_polling_to_exhaustion_then_each_is_visited_once()
 {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1","EndingSequenceNumber":"1"}}],"LastEvaluatedShardId":"shard-0001"}}"#,
        r#"{"ShardIterator":"iterator-1"}"#,
        r#"{"Records":[]}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0002","SequenceNumberRange":{"StartingSequenceNumber":"2","EndingSequenceNumber":"2"}}],"LastEvaluatedShardId":"shard-0002"}}"#,
        r#"{"ShardIterator":"iterator-2"}"#,
        r#"{"Records":[]}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0003","SequenceNumberRange":{"StartingSequenceNumber":"3","EndingSequenceNumber":"3"}}]}}"#,
        r#"{"ShardIterator":"iterator-3"}"#,
        r#"{"Records":[]}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 1,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let checkpoints = vec![
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "record-1".to_string(),
        },
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0002".to_string(),
            position: "record-2".to_string(),
        },
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0003".to_string(),
            position: "record-3".to_string(),
        },
    ];
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &checkpoints)
        .await
        .expect("source poller");

    assert_eq!(poller.aws_shard_iterator_count(), 1);
    assert_eq!(poller.aws_pending_shard_count(), 0);
    assert!(
        poller.aws_shard_iterator_count() + poller.aws_pending_shard_count() <= 1,
        "startup scheduler exceeded max_shards"
    );
    for expected_pending in [0, 0, 0] {
        let batch = poller.poll_once().await.expect("poll startup shard");
        assert_eq!(batch.source_response_count, 1);
        assert_eq!(poller.aws_shard_iterator_count(), 0);
        assert_eq!(poller.aws_pending_shard_count(), expected_pending);
        assert!(
            poller.aws_shard_iterator_count() + poller.aws_pending_shard_count() <= 1,
            "startup scheduler exceeded max_shards"
        );
    }

    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.DescribeStream"))
            .count(),
        3
    );
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.DescribeStream")
            && request.contains("\"ExclusiveStartShardId\":\"shard-0001\"")
    }));
    assert!(requests.iter().any(|request| {
        request.contains("DynamoDBStreams_20120810.DescribeStream")
            && request.contains("\"ExclusiveStartShardId\":\"shard-0002\"")
    }));
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("DynamoDBStreams_20120810.GetShardIterator"))
            .count(),
        3
    );
    for (shard_id, position) in [
        ("shard-0001", "record-1"),
        ("shard-0002", "record-2"),
        ("shard-0003", "record-3"),
    ] {
        let shard_requests = requests.iter().filter(|request| {
            request.contains("DynamoDBStreams_20120810.GetShardIterator")
                && request.contains(&format!("\"ShardId\":\"{shard_id}\""))
        });
        assert_eq!(shard_requests.count(), 1, "shard {shard_id}");
        assert!(requests.iter().any(|request| {
            request.contains("DynamoDBStreams_20120810.GetShardIterator")
                && request.contains(&format!("\"ShardId\":\"{shard_id}\""))
                && request.contains("\"ShardIteratorType\":\"AFTER_SEQUENCE_NUMBER\"")
                && request.contains(&format!("\"SequenceNumber\":\"{position}\""))
        }));
    }
}

#[tokio::test]
async fn given_registered_aux_storage_table_when_polled_by_name_then_scope_missing_fails_closed() {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
        r#"{"Records":[{"SourceTableName":"source_accounts","Keys":{"pk":{"S":"ACCOUNT#1"}},"SequenceNumber":"seq-account-1"}]}"#,
    ])
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

    let error = poller
        .poll_aux_storage_table("source_accounts")
        .await
        .expect_err("AuxStorage polling must fail closed without source scope");

    assert_eq!(error.category(), "missing_response_scope");
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(!requests[1].contains("LastEvaluatedKey"));
}

#[tokio::test]
async fn given_unregistered_aux_storage_table_when_polled_by_name_then_empty_batch_is_returned() {
    let (base_url, _requests) = serve_sequence_with_capture(vec![
        r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
    ])
    .await;
    let mut poller = SourcePoller::from_plans(
        &source_config(&base_url),
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
        shard_id: "shard-0001".to_string(),
        position: "seq-2".to_string(),
    };
    let snapshot_fixture = LocalExecutionSource::with_snapshot_chunks([SnapshotChunk {
        checkpoint: snapshot_checkpoint.clone(),
        records: vec![polled_record("users", "snapshot-1")],
    }]);
    let catchup_fixture = LocalExecutionSource::with_stream_batches([PollBatch {
        source_response_count: 1,
        source_nonempty_response_count: 1,
        source_record_count: 1,
        source_encoded_bytes: 1,
        records: vec![polled_record("users", "seq-2")],
        checkpoints: vec![catchup_checkpoint.clone()],
        source_predecessor: None,
        system_stream: None,
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

async fn serve_status_sequence_with_capture(
    responses: Vec<(u16, Option<&'static str>, &'static str)>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured_requests = Arc::clone(&requests);
    tokio::spawn(async move {
        for (status, error_type, body) in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 4096];
            let size = socket.read(&mut buffer).await.unwrap();
            captured_requests
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&buffer[..size]).into_owned());
            let error_header = error_type
                .map(|value| format!("x-amzn-ErrorType: {value}\r\n"))
                .unwrap_or_default();
            let response = format!(
                "HTTP/1.1 {status} TEST\r\nContent-Type: \
                 application/json\r\n{error_header}Content-Length: {}\r\nConnection: \
                 close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (format!("http://{addr}"), requests)
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

fn system_scope() -> SystemStreamScope {
    SystemStreamScope::try_new("aux-system", 7).expect("system scope")
}

fn system_checkpoint() -> SystemStreamCheckpointToken {
    SystemStreamCheckpointToken::try_from("v1.0123456789abcdef0123456789abcdef")
        .expect("system checkpoint")
}

fn stream_record(sequence_number: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: StorageItem::new(),
        old_image: None,
        new_image: None,
    }
}

fn polled_record(analytics_table_name: &str, record_key: &str) -> PolledRecord {
    PolledRecord {
        source_table_name: "source_users".to_string(),
        shard_id: "shard-0001".to_string(),
        analytics_table_names: vec![analytics_table_name.to_string()],
        record_key: record_key.to_string(),
        record: stream_record(record_key),
        system_position: None,
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
            source_response_count: 0,
            source_nonempty_response_count: 0,
            source_record_count: 0,
            source_encoded_bytes: 0,
            records: Vec::new(),
            checkpoints: Vec::new(),
            source_predecessor: None,
            system_stream: None,
        }))
    }

    fn commit_stream_catchup(&mut self, checkpoints: &[SourceCheckpoint]) {
        self.committed_stream_checkpoints
            .extend(checkpoints.iter().cloned());
    }
}

#[tokio::test]
async fn given_oversized_aws_page_when_limit_is_reduced_then_retry_uses_durable_iterator_and_smaller_limit()
 {
    let (base_url, requests) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-old"}"#,
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-0001","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-new"}"#,
        r#"{"Records":[],"NextShardIterator":"iterator-new-next"}"#,
    ])
    .await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_records_per_response: 100,
        poll_max_shards: 1,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan {
        source_table_name: "source_users".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["users".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/users/stream/1".to_string(),
        ),
    };
    let initial_checkpoint = SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: "shard-0001".to_string(),
        position: "record-41".to_string(),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &[initial_checkpoint])
        .await
        .expect("source poller");

    assert!(
        poller
            .reduce_page_limit()
            .await
            .expect("reduce page and rewind")
    );
    let batch = poller.poll_once().await.expect("poll reloaded iterator");
    assert_eq!(batch.source_response_count, 1);

    let requests = requests.lock().unwrap();
    let iterator_requests = requests
        .iter()
        .filter(|request| request.contains("DynamoDBStreams_20120810.GetShardIterator"))
        .collect::<Vec<_>>();
    assert_eq!(iterator_requests.len(), 2);
    assert!(iterator_requests[0].contains("\"SequenceNumber\":\"record-41\""));
    assert!(iterator_requests[1].contains("\"SequenceNumber\":\"record-41\""));
    let records_request = requests
        .iter()
        .find(|request| request.contains("DynamoDBStreams_20120810.GetRecords"))
        .expect("get records request");
    assert!(records_request.contains("\"ShardIterator\":\"iterator-new\""));
    assert!(records_request.contains("\"Limit\":50"));
}

#[tokio::test]
async fn given_terminal_and_malformed_shard_responses_when_polled_together_then_topology_changes_without_checkpoint_advancement()
 {
    let (base_url, _) = serve_sequence_with_capture(vec![
        r#"{"StreamDescription":{"Shards":[{"ShardId":"shard-a","SequenceNumberRange":{"StartingSequenceNumber":"1"}},{"ShardId":"shard-b","SequenceNumberRange":{"StartingSequenceNumber":"1"}}]}}"#,
        r#"{"ShardIterator":"iterator-a"}"#,
        r#"{"ShardIterator":"iterator-b"}"#,
        r#"{"Records":[{"dynamodb":{"SequenceNumber":"malformed"}}]}"#,
        r#"{"Records":[]}"#,
    ]).await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(base_url),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "test-access".to_string(),
                secret_key: "test-secret".to_string(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        poll_max_shards: 2,
        poll_max_responses_per_interval: 2,
        ..AnalyticsSourceConfig::default()
    };
    let checkpoints = ["shard-a", "shard-b"].map(|shard_id| SourceCheckpoint {
        source_table_name: "source_events".to_string(),
        shard_id: shard_id.to_string(),
        position: "before".to_string(),
    });
    let plan = SourceTablePlan {
        source_table_name: "source_events".to_string(),
        source_table_name_prefix: None,
        analytics_table_names: vec!["events".to_string()],
        registrations: Vec::new(),
        stream_type: AnalyticsStreamType::StorageStream,
        stream_identifier: Some(
            "arn:aws:dynamodb:us-east-1:123456789012:table/events/stream/1".to_string(),
        ),
    };
    let mut poller = SourcePoller::from_plans(&source, vec![plan], &checkpoints)
        .await
        .unwrap();
    assert!(poller.poll_once().await.is_err());
    assert_eq!(poller.aws_shard_iterator_count(), 1);
    assert_eq!(poller.aws_pending_terminal_count(), 0);
    for checkpoint in checkpoints {
        assert_eq!(
            poller.aws_record_checkpoint("source_events", &checkpoint.shard_id),
            Some(checkpoint)
        );
    }
}
