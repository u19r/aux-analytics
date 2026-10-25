use std::{collections::BTreeMap, io::Read, sync::Arc};

use analytics_contract::{
    AnalyticsManifest, PrivacyPolicy, QueryExpression, QueryPredicate, QuerySelect, StructuredQuery,
};
use analytics_engine::{AnalyticsEngineError, IngestOutcome, StorageBackend};
use analytics_fixtures::{user_item, users_manifest};
use analytics_operations::OperationStore;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use config::{AnalyticsCatalogBackend, AnalyticsLambdaResponseCompression, LambdaEnvironment};
use flate2::read::GzDecoder;
use serde_json::json;

use super::lambda::*;
use crate::AnalyticsLambdaError;

#[tokio::test]
async fn given_lambda_handler_when_rows_ingested_and_queried_then_rows_are_visible() {
    let handler = test_handler();

    let ingest_response = handler
        .handle_event(json!({
            "operation": "ingest",
            "analytics_table_name": "users",
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", "a@example.com", "org-a"),
            }
        }))
        .await
        .expect("ingest");
    assert_eq!(ingest_response["outcome"], "inserted");

    let query_response = handler
        .handle_event(json!({
            "operation": "unscoped_structured_query",
            "query": email_query(None),
        }))
        .await
        .expect("query");
    assert_eq!(query_response["rows"][0]["email"], "a@example.com");
}

#[tokio::test]
async fn given_expanded_registration_when_lambda_ingests_compact_list_then_rows_are_queryable() {
    let manifest = expanded_lambda_manifest();
    let handler = AnalyticsLambdaHandler::try_new(
        manifest,
        &StorageBackend::DuckDb {
            path: ":memory:".to_string(),
        },
        crate::LambdaHandlerOptions::default(),
    )
    .expect("handler");

    let ingest_response = handler
        .handle_event(json!({
            "operation": "ingest",
            "analytics_table_name": "metric_points",
            "record_key": "batch-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": {
                    "tenant_id": {"S": "tenant-a"},
                    "batch_id": {"S": "batch-1"},
                    "payload": {"L": [
                        {"N": "2"},
                        {"L": [
                            {"L": [{"S": "requests"}, {"S": "event-1"}, {"N": "7"}]},
                            {"L": [{"S": "latency"}, {"S": "event-2"}, {"N": "11"}]}
                        ]}
                    ]}
                }
            }
        }))
        .await
        .expect("ingest");
    assert_eq!(ingest_response["outcome"], "inserted");

    let query_response = handler
        .handle_event(json!({
            "operation": "unscoped_structured_query",
            "query": StructuredQuery {
                analytics_table_name: "metric_points".to_string(),
                table_alias: None,
                joins: Vec::new(),
                select: vec![
                    QuerySelect::Column {
                        table_alias: None,
                        column_name: "event_id".to_string(),
                        alias: None,
                    },
                    QuerySelect::Column {
                        table_alias: None,
                        column_name: "series_name".to_string(),
                        alias: None,
                    },
                ],
                filters: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            },
        }))
        .await
        .expect("query");

    assert_eq!(
        query_response["rows"],
        json!([
            {"event_id": "event-1", "series_name": "requests"},
            {"event_id": "event-2", "series_name": "latency"}
        ])
    );
}

#[tokio::test]
async fn given_lambda_privacy_policy_when_ingested_then_denied_fields_are_filtered() {
    let handler = test_handler_with_privacy_policy(
        PrivacyPolicy::try_new("privacy-v1")
            .expect("policy")
            .with_denied_key_name("email"),
    );

    handler
        .handle_event(json!({
            "operation": "ingest",
            "analytics_table_name": "users",
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", "private@example.com", "org-a"),
            }
        }))
        .await
        .expect("ingest");

    let query_response = handler
        .handle_event(json!({
            "operation": "unscoped_structured_query",
            "query": email_query(None),
        }))
        .await
        .expect("query");
    assert!(query_response["rows"][0]["email"].is_null());
}

#[tokio::test]
async fn given_lambda_rows_when_structured_query_executed_then_rows_are_returned() {
    let handler = test_handler();

    handler
        .handle_event(json!({
            "operation": "ingest",
            "analytics_table_name": "users",
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", "structured@example.com", "org-a"),
            }
        }))
        .await
        .expect("ingest");

    let query_response = handler
        .handle_event(json!({
            "operation": "unscoped_structured_query",
            "query": StructuredQuery {
                analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
                select: vec![QuerySelect::Column {
                    table_alias: None,
                    column_name: "email".to_string(),
                    alias: None,
                }],
                filters: vec![QueryPredicate::Eq {
                    expression: QueryExpression::Column {
                        table_alias: None,
                        column_name: "org_id".to_string(),
                    },
                    value: json!("org-a"),
                }],
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: Some(1),
                offset: None,
            }
        }))
        .await
        .expect("structured query");

    assert_eq!(query_response["rows"][0]["email"], "structured@example.com");
    assert_eq!(query_response["execution"]["row_count"], 1);
    assert_eq!(query_response["execution"]["tables"], json!(["users"]));
    assert_eq!(query_response["columns"][0]["name"], "email");
    assert!(query_response["source_watermark"].is_object());
}

#[tokio::test]
async fn given_lambda_tenant_rows_when_queried_then_only_target_tenant_rows_are_returned() {
    let handler = test_handler();

    for (record_key, email, org_id) in [
        ("user-1", "tenant-a@example.com", "org-a"),
        ("user-2", "tenant-b@example.com", "org-b"),
    ] {
        handler
            .handle_event(json!({
                "operation": "ingest",
                "analytics_table_name": "users",
                "record_key": record_key,
                "record": {
                    "Keys": {},
                    "SequenceNumber": record_key,
                    "NewImage": user_item(record_key, email, org_id),
                }
            }))
            .await
            .expect("ingest");
    }

    let query_response = handler
        .handle_event(json!({
            "operation": "tenant_query",
            "target_tenant_id": "tenant_01",
            "query": StructuredQuery {
                analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
                select: vec![QuerySelect::Column {
                    table_alias: None,
                    column_name: "email".to_string(),
                    alias: None,
                }],
                filters: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }
        }))
        .await
        .expect("tenant query");

    let rows = query_response["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|row| row["email"].as_str() == Some("tenant-a@example.com"))
    );

    let query_response = handler
        .handle_event(json!({
            "operation": "tenant_query",
            "target_tenant_id": "tenant_02",
            "query": StructuredQuery {
                analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
                select: vec![QuerySelect::Column {
                    table_alias: None,
                    column_name: "email".to_string(),
                    alias: None,
                }],
                filters: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }
        }))
        .await
        .expect("other tenant query");

    let rows = query_response["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn given_lambda_named_queries_when_batch_executed_then_named_results_are_returned() {
    let handler = test_handler();
    handler
        .handle_event(json!({
            "operation": "ingest",
            "analytics_table_name": "users",
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", "tenant-a@example.com", "org-a"),
            }
        }))
        .await
        .expect("ingest");

    let query_response = handler
        .handle_event(json!({
            "operation": "tenant_query_batch",
            "target_tenant_id": "tenant_01",
            "queries": [
                {
                    "name": "total_users",
                    "query": StructuredQuery {
                        analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
                        select: vec![QuerySelect::Count {
                            alias: "count".to_string(),
                        }],
                        filters: Vec::new(),
                        group_by: Vec::new(),
                        order_by: Vec::new(),
                        limit: Some(1),
                        offset: None,
                    },
                },
                {
                    "name": "matching_users",
                    "query": StructuredQuery {
                        analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
                        select: vec![QuerySelect::Column {
                            table_alias: None,
                            column_name: "email".to_string(),
                            alias: None,
                        }],
                        filters: Vec::new(),
                        group_by: Vec::new(),
                        order_by: Vec::new(),
                        limit: Some(1),
                        offset: None,
                    },
                },
            ],
        }))
        .await
        .expect("tenant query batch");

    assert_eq!(query_response["results"][0]["name"], "total_users");
    assert_eq!(query_response["results"][0]["rows"][0]["count"], 1);
    assert_eq!(query_response["results"][0]["execution"]["row_count"], 1);
    assert_eq!(query_response["results"][0]["columns"][0]["name"], "count");
    assert_eq!(query_response["results"][1]["name"], "matching_users");
    assert_eq!(
        query_response["results"][1]["rows"][0]["email"],
        "tenant-a@example.com"
    );
}

#[tokio::test]
async fn given_lambda_when_requested_then_query_responses_that_exceed_configured_size_rejected() {
    let handler = test_handler_with_response_config(LambdaResponseConfig {
        max_response_size_kb: Some(1),
        response_compression: AnalyticsLambdaResponseCompression::None,
        query_only: false,
        disable_query_deadline: false,
    });
    let large_email = format!("{}@example.com", "large-response-payload".repeat(128));
    handler
        .handle_event(json!({
            "operation": "ingest",
            "analytics_table_name": "users",
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", large_email.as_str(), "org-a"),
            }
        }))
        .await
        .expect("ingest");

    let error = handler
        .handle_event(json!({
            "operation": "tenant_query",
            "target_tenant_id": "tenant_01",
            "query": email_query(Some(1)),
        }))
        .await
        .expect_err("response too large");

    assert!(error.to_string().contains("analytics_response_too_large"));
}

#[tokio::test]
async fn given_oversized_raw_json_when_lambda_query_compressed_then_response_fits_limit() {
    let handler = test_handler_with_response_config(LambdaResponseConfig {
        max_response_size_kb: Some(1),
        response_compression: AnalyticsLambdaResponseCompression::GzipBase64,
        query_only: false,
        disable_query_deadline: false,
    });
    let large_email = format!("{}@example.com", "compressible".repeat(128));
    handler
        .handle_event(json!({
            "operation": "ingest",
            "analytics_table_name": "users",
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", large_email.as_str(), "org-a"),
            }
        }))
        .await
        .expect("ingest");

    let response = handler
        .handle_event(json!({
            "operation": "tenant_query",
            "target_tenant_id": "tenant_01",
            "query": email_query(Some(1)),
        }))
        .await
        .expect("compressed response");

    assert_eq!(response["encoding"], "gzip+base64");
    let decoded = BASE64
        .decode(response["payload"].as_str().expect("payload"))
        .expect("base64");
    let mut decoder = GzDecoder::new(decoded.as_slice());
    let mut json_payload = String::new();
    decoder
        .read_to_string(&mut json_payload)
        .expect("gzip payload");
    let decompressed: serde_json::Value = serde_json::from_str(&json_payload).expect("json");
    assert_eq!(decompressed["rows"][0]["email"], large_email);
    assert_eq!(decompressed["execution"]["row_count"], 1);
}

#[tokio::test]
async fn given_lambda_query_only_mode_when_requested_then_ingest_operations_rejected() {
    let handler = test_handler_with_response_config(LambdaResponseConfig {
        max_response_size_kb: None,
        response_compression: AnalyticsLambdaResponseCompression::None,
        query_only: true,
        disable_query_deadline: false,
    });

    let error = handler
        .handle_event(json!({
            "operation": "ingest",
            "analytics_table_name": "users",
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", "a@example.com", "org-a"),
            }
        }))
        .await
        .expect_err("query-only ingest");

    assert!(
        error
            .to_string()
            .contains("analytics.query.lambda_query_only")
    );
}

#[tokio::test]
async fn given_query_only_lambda_when_maintenance_submitted_then_rejected_without_persistence() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let handler = test_handler_with_response_config(LambdaResponseConfig {
        max_response_size_kb: None,
        response_compression: AnalyticsLambdaResponseCompression::None,
        query_only: true,
        disable_query_deadline: false,
    })
    .with_operation_store_path(&operation_store_path);

    let error = handler
        .handle_event(json!({
            "operation": "maintenance_submit",
            "actor": "lambda-test",
            "request": {
                "kind": "trim",
                "request": {
                    "operation_id": "lambda-query-only-trim",
                    "table_name": "users",
                    "dry_run": true,
                    "target": {
                        "type": "row_keys",
                        "keys": [[117, 115, 101, 114, 45, 49]]
                    },
                    "confirmation_token": null
                }
            }
        }))
        .await
        .expect_err("query-only maintenance");

    assert!(
        error
            .to_string()
            .contains("mutation operations are disabled when analytics.query.lambda_query_only")
    );
    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    assert!(store.list_operations().expect("operations").is_empty());
}

#[tokio::test]
async fn given_lambda_when_requested_then_tenant_facing_raw_sql_events_rejected() {
    let handler = test_handler();

    let err = handler
        .handle_event(json!({
            "operation": "unscoped_sql_query",
            "sql": "select 1",
        }))
        .await
        .expect_err("raw SQL event");

    assert!(err.to_string().contains("schema validation"));
}

#[tokio::test]
async fn given_canonical_maintenance_request_when_lambda_submitted_twice_then_identity_is_stable() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let handler = test_handler().with_operation_store_path(&operation_store_path);
    let event = json!({
        "operation": "maintenance_submit",
        "actor": "lambda-test",
        "request": {
            "kind": "trim",
            "request": {
                "operation_id": "lambda-trim-1",
                "table_name": "users",
                "dry_run": true,
                "target": {
                    "type": "row_keys",
                    "keys": [[117, 115, 101, 114, 45, 49]]
                },
                "confirmation_token": null
            }
        }
    });

    let first = handler
        .handle_event(event.clone())
        .await
        .expect("first maintenance submission");
    let second = handler
        .handle_event(event)
        .await
        .expect("idempotent maintenance submission");

    assert_eq!(first["operation_id"], "lambda-trim-1");
    assert_eq!(first["outcome"], "created");
    assert_eq!(second["operation_id"], "lambda-trim-1");
    assert_eq!(second["outcome"], "existing");
    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    assert_eq!(store.list_operations().expect("operations").len(), 1);
}

#[tokio::test]
async fn given_unfenced_maintenance_request_when_submitted_to_lambda_then_nothing_is_persisted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let handler = test_handler().with_operation_store_path(&operation_store_path);
    let source = tempdir.path().join("source").to_string_lossy().into_owned();
    let clean = tempdir.path().join("clean").to_string_lossy().into_owned();
    let quarantine = tempdir
        .path()
        .join("quarantine")
        .to_string_lossy()
        .into_owned();

    let raw_backup = json!({
        "operation": "maintenance_submit",
        "actor": "lambda-test",
        "request": {
            "kind": "raw_backup",
            "request": {
                "operation_id": "lambda-raw-backup-unfenced",
                "object_id": "object-1",
                "source_identity": "source-users",
                "privacy_policy_version": null,
                "policy_dropped_records": 0,
                "layout": {
                    "schema_version": 1,
                    "compression": "none",
                    "checksum_algorithm": "fnv1a64",
                    "encryption": {"type": "none"}
                },
                "compacted_from_object_ids": [],
                "records": [{
                    "table_name": "users",
                    "record_key": "user-1",
                    "source_position": null,
                    "record": {"Keys": {}, "SequenceNumber": "1"}
                }]
            }
        }
    });
    let error = handler
        .handle_event(raw_backup)
        .await
        .expect_err("raw-backup submission");
    assert!(
        error
            .to_string()
            .contains("does not support atomic maintenance fencing")
    );

    for (operation_id, dry_run) in [
        ("lambda-privacy-dry-run", true),
        ("lambda-privacy-apply", false),
    ] {
        let privacy_fix = json!({
            "operation": "maintenance_submit",
            "actor": "lambda-test",
            "request": {
                "kind": "privacy_fix",
                "request": {
                    "operation_id": operation_id,
                    "source_backup_prefix": source.clone(),
                    "clean_target_prefix": clean.clone(),
                    "quarantine_target_prefix": quarantine.clone(),
                    "raw_backup_object_ids": ["object-1"],
                    "tables": ["users"],
                    "policy": {"version": "privacy-v1"},
                    "dry_run": dry_run,
                    "destructive_action": "none",
                    "confirmation_token": null
                }
            }
        });
        let error = handler
            .handle_event(privacy_fix)
            .await
            .expect_err("privacy-fix submission");
        assert!(
            error
                .to_string()
                .contains("does not support atomic maintenance fencing")
        );
    }

    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    assert!(store.list_operations().expect("operations").is_empty());
}

#[tokio::test]
async fn given_lambda_when_requested_then_unknown_maintenance_fields_without_persisting_rejected() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let handler = test_handler().with_operation_store_path(&operation_store_path);
    let event = json!({
        "operation": "maintenance_submit",
        "actor": "lambda-test",
        "request": {
            "kind": "trim",
            "request": {
                "operation_id": "lambda-trim-unknown",
                "table_name": "users",
                "dry_run": true,
                "target": {
                    "type": "row_keys",
                    "keys": [[117, 115, 101, 114, 45, 49]]
                },
                "confirmation_token": null
            }
        }
    });

    let mut top_level = event.clone();
    top_level["legacy_mode"] = json!(true);
    let error = handler
        .handle_event(top_level)
        .await
        .expect_err("unknown event field");
    assert!(error.to_string().contains("schema validation"));

    let mut nested = event.clone();
    nested["request"]["request"]["target"]["legacy_mode"] = json!(true);
    let error = handler
        .handle_event(nested)
        .await
        .expect_err("unknown maintenance field");
    assert!(error.to_string().contains("schema validation"));

    let mut privacy_policy = event.clone();
    privacy_policy["request"]["request"]["kind"] = json!("privacy_fix");
    privacy_policy["request"]["request"]["request"] = json!({
        "operation_id": "lambda-privacy-unknown",
            "source_backup_prefix": "source",
            "clean_target_prefix": "clean",
            "quarantine_target_prefix": "quarantine",
            "raw_backup_object_ids": ["object-1"],
        "tables": ["users"],
        "policy": {
            "version": "privacy-v1",
            "legacy_mode": true
        },
        "dry_run": true,
        "destructive_action": "none",
        "confirmation_token": null
    });
    let error = handler
        .handle_event(privacy_policy)
        .await
        .expect_err("unknown privacy policy field");
    assert!(error.to_string().contains("schema validation"));

    let mut raw_backup_record = event.clone();
    raw_backup_record["request"]["request"]["kind"] = json!("raw_backup");
    raw_backup_record["request"]["request"]["request"] = json!({
        "operation_id": "lambda-record-unknown",
        "object_id": "object-1",
        "source_identity": "source-users",
        "privacy_policy_version": null,
        "policy_dropped_records": 0,
        "layout": {
            "schema_version": 1,
            "compression": "none",
            "checksum_algorithm": "fnv1a64",
            "encryption": {"type": "none"}
        },
        "compacted_from_object_ids": [],
        "records": [{
            "table_name": "users",
            "record_key": "user-1",
            "source_position": null,
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "legacy_mode": true
            }
        }]
    });
    let error = handler
        .handle_event(raw_backup_record)
        .await
        .expect_err("unknown stream record field");
    assert!(error.to_string().contains("schema validation"));

    let mut raw_backup_position = event;
    raw_backup_position["request"]["request"]["kind"] = json!("raw_backup");
    raw_backup_position["request"]["request"]["request"] = json!({
        "operation_id": "lambda-position-unknown",
        "object_id": "object-1",
        "source_identity": "source-users",
        "privacy_policy_version": null,
        "policy_dropped_records": 0,
        "layout": {
            "schema_version": 1,
            "compression": "none",
            "checksum_algorithm": "fnv1a64",
            "encryption": {"type": "none"}
        },
        "compacted_from_object_ids": [],
        "records": [{
            "table_name": "users",
            "record_key": "user-1",
            "source_position": {
                "type": "dynamo_db_stream_sequence",
                "stream_arn": "stream",
                "shard_id": "shard-1",
                "sequence_number": "1",
                "legacy_mode": true
            },
            "record": {
                "Keys": {},
                "SequenceNumber": "1"
            }
        }]
    });
    let error = handler
        .handle_event(raw_backup_position)
        .await
        .expect_err("unknown source position field");
    assert!(error.to_string().contains("schema validation"));

    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    assert!(store.list_operations().expect("operations").is_empty());
}

#[tokio::test]
async fn given_lambda_when_requested_then_maintenance_submission_without_actor_or_operation_store_rejected()
 {
    let handler = test_handler();
    let event = json!({
        "operation": "maintenance_submit",
        "request": {
            "kind": "trim",
            "request": {
                "operation_id": "lambda-trim-missing-actor",
                "table_name": "users",
                "dry_run": true,
                "target": {
                    "type": "row_keys",
                    "keys": [[117]]
                },
                "confirmation_token": null
            }
        }
    });

    let error = handler
        .handle_event(event)
        .await
        .expect_err("missing actor");
    assert!(error.to_string().contains("schema validation"));

    let error = handler
        .handle_event(json!({
            "operation": "maintenance_submit",
            "actor": "lambda-test",
            "request": {
                "kind": "trim",
                "request": {
                    "operation_id": "lambda-trim-no-store",
                    "table_name": "users",
                    "dry_run": true,
                    "target": {"type": "row_keys", "keys": [[117]]},
                    "confirmation_token": null
                }
            }
        }))
        .await
        .expect_err("unconfigured operation store");
    assert!(
        error
            .to_string()
            .contains("operation store is not configured")
    );
}

#[tokio::test]
async fn given_lambda_record_batch_when_ingested_then_all_outcomes_are_returned() {
    let handler = test_handler();

    let response = handler
        .handle_event(json!({
            "operation": "ingest_batch",
            "analytics_table_name": "users",
            "records": [
                {
                    "record_key": "user-1",
                    "record": {
                        "Keys": {},
                        "SequenceNumber": "1",
                        "NewImage": user_item("user-1", "a@example.com", "org-a"),
                    }
                },
                {
                    "record_key": "user-2",
                    "record": {
                        "eventID": "event-2",
                        "eventName": "INSERT",
                        "dynamodb": {
                            "Keys": {},
                            "SequenceNumber": "2",
                            "NewImage": user_item("user-2", "b@example.com", "org-b"),
                        }
                    }
                }
            ],
        }))
        .await
        .expect("batch ingest");

    assert_eq!(response["processed"], 2);
    assert_eq!(response["outcomes"], json!(["inserted", "inserted"]));
}

fn test_handler() -> AnalyticsLambdaHandler {
    AnalyticsLambdaHandler::try_new(
        users_manifest(),
        &StorageBackend::DuckDb {
            path: ":memory:".to_string(),
        },
        crate::LambdaHandlerOptions::default(),
    )
    .expect("handler")
}

fn expanded_lambda_manifest() -> AnalyticsManifest {
    serde_json::from_value(json!({
        "version": 1,
        "tables": [{
            "source_table_name": "nsystem",
            "analytics_table_name": "metric_points",
            "tenant_selector": {
                "kind": "attribute",
                "attribute_name": "tenant_id"
            },
            "row_identity": {
                "kind": "attributes",
                "attribute_names": ["batch_id", "row_ordinal"]
            },
            "row_expansion": {
                "list_path": [
                    {"kind": "attribute", "name": "payload"},
                    {"kind": "index", "index": 1}
                ],
                "source_fields": [
                    {
                        "output_attribute_name": "tenant_id",
                        "path": [{"kind": "attribute", "name": "tenant_id"}]
                    },
                    {
                        "output_attribute_name": "batch_id",
                        "path": [{"kind": "attribute", "name": "batch_id"}]
                    }
                ],
                "element_fields": [
                    {
                        "output_attribute_name": "series_name",
                        "path": [{"kind": "index", "index": 0}]
                    },
                    {
                        "output_attribute_name": "event_id",
                        "path": [{"kind": "index", "index": 1}]
                    }
                ],
                "ordinal_attribute_name": "row_ordinal"
            },
            "document_column": null,
            "skip_delete": true,
            "projection_attribute_names": [
                "series_name",
                "event_id",
                "batch_id",
                "row_ordinal"
            ]
        }]
    }))
    .expect("expanded manifest")
}

fn test_handler_with_privacy_policy(policy: PrivacyPolicy) -> AnalyticsLambdaHandler {
    AnalyticsLambdaHandler::try_new(
        users_manifest(),
        &StorageBackend::DuckDb {
            path: ":memory:".to_string(),
        },
        crate::LambdaHandlerOptions {
            privacy_policy: Some(Arc::new(policy)),
            ..Default::default()
        },
    )
    .expect("handler")
}

fn test_handler_with_response_config(
    response_config: LambdaResponseConfig,
) -> AnalyticsLambdaHandler {
    AnalyticsLambdaHandler::try_new(
        users_manifest(),
        &StorageBackend::DuckDb {
            path: ":memory:".to_string(),
        },
        crate::LambdaHandlerOptions {
            privacy_policy: None,
            max_read_connections: config::DEFAULT_QUERY_MAX_READ_CONNECTIONS,
            response_config,
            ..Default::default()
        },
    )
    .expect("handler")
}

fn email_query(limit: Option<u32>) -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::Column {
            table_alias: None,
            column_name: "email".to_string(),
            alias: None,
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit,
        offset: None,
    }
}

#[test]
fn given_lambda_manifest_env_when_config_file_also_has_manifest_then_env_wins() {
    let manifest = manifest_file();
    let environment = lambda_environment([
        (
            "AUX_ANALYTICS_MANIFEST",
            manifest.path().to_string_lossy().into_owned(),
        ),
        ("AUX_ANALYTICS_DUCKDB", ":memory:".to_string()),
    ]);

    let resolved = config::resolve_config(config::ConfigRequest::lambda(environment))
        .expect("resolved config");

    assert_eq!(
        resolved.manifest.source,
        config::ManifestSource::ExplicitArgument
    );
}

#[test]
fn given_lambda_without_manifest_path_when_resolved_then_manifest_requirement_is_reported() {
    let environment = lambda_environment([("AUX_ANALYTICS_DUCKDB", ":memory:".to_string())]);

    let error = config::resolve_config(config::ConfigRequest::lambda(environment))
        .expect_err("missing manifest");

    assert_eq!(error.kind(), config::ConfigErrorKind::MissingManifestPath);
}

#[test]
fn given_aws_lambda_env_when_resolved_then_query_and_ducklake_config_are_overridden() {
    let manifest = manifest_file();
    let environment = lambda_environment([
        (
            "AUX_ANALYTICS_MANIFEST",
            manifest.path().to_string_lossy().into_owned(),
        ),
        (
            "AUX_ANALYTICS_CATALOG_BACKEND",
            "ducklake_aux_catalog".to_string(),
        ),
        (
            "AUX_ANALYTICS_DUCKLAKE_AUX_CATALOG",
            "/tmp/aux-ducklake/metadata.duckdb".to_string(),
        ),
        (
            "AUX_ANALYTICS_DUCKLAKE_DATA_PATH",
            "s3://analytics-bucket/warehouse".to_string(),
        ),
        ("AUX_ANALYTICS_OBJECT_REGION", "us-east-1".to_string()),
        ("AUX_ANALYTICS_CATALOG_DUCKDB_THREADS", "1".to_string()),
        ("AUX_ANALYTICS_QUERY_MAX_READ_CONNECTIONS", "1".to_string()),
        (
            "AUX_ANALYTICS_QUERY_MAX_RESPONSE_SIZE_KB",
            "5800".to_string(),
        ),
        (
            "AUX_ANALYTICS_QUERY_LAMBDA_RESPONSE_COMPRESSION",
            "gzip_base64".to_string(),
        ),
        ("AUX_ANALYTICS_QUERY_LAMBDA_QUERY_ONLY", "true".to_string()),
        (
            "AUX_ANALYTICS_QUERY_DISABLE_QUERY_DEADLINE",
            "true".to_string(),
        ),
    ]);

    let resolved = config::resolve_config(config::ConfigRequest::lambda(environment))
        .expect("resolved config");

    assert_eq!(
        resolved.root.analytics.catalog.backend,
        Some(AnalyticsCatalogBackend::DucklakeAuxCatalog)
    );
    assert_eq!(
        resolved.root.analytics.object_storage.bucket.as_deref(),
        Some("analytics-bucket")
    );
    assert_eq!(
        resolved.root.analytics.object_storage.path.as_deref(),
        Some("warehouse")
    );
    assert_eq!(
        resolved.root.analytics.object_storage.region.as_deref(),
        Some("us-east-1")
    );
    assert_eq!(
        resolved
            .root
            .analytics
            .object_storage
            .endpoint_url
            .as_deref(),
        Some("s3.us-east-1.amazonaws.com")
    );
    assert_eq!(resolved.root.analytics.catalog.duckdb_threads, Some(1));
    assert_eq!(resolved.root.analytics.query.max_read_connections, 1);
    assert_eq!(
        resolved.root.analytics.query.max_response_size_kb,
        Some(5800)
    );
    assert_eq!(
        resolved.root.analytics.query.lambda_response_compression,
        AnalyticsLambdaResponseCompression::GzipBase64
    );
    assert!(resolved.root.analytics.query.lambda_query_only);
    assert!(resolved.root.analytics.query.disable_query_deadline);
    assert!(!resolved.routes.ingest_enabled);
    assert!(resolved.routes.query_enabled);
}

#[test]
fn given_direct_ducklake_env_when_backend_is_resolved_then_object_storage_config_is_preserved() {
    let manifest = manifest_file();
    let mut root = config::RootConfig::default();
    root.analytics.object_storage.bucket = Some("analytics-bucket".to_string());
    root.analytics.object_storage.path = Some("warehouse".to_string());
    root.analytics.object_storage.region = Some("us-east-1".to_string());
    root.analytics.object_storage.credentials = Some(config::RemoteCredentialsConfig {
        instance_keys: Some(true),
        ..config::RemoteCredentialsConfig::default()
    });
    let config_file = tempfile::NamedTempFile::new().expect("config file");
    std::fs::write(
        config_file.path(),
        serde_json::to_vec(&root).expect("config json"),
    )
    .expect("config contents");
    let environment = lambda_environment([
        (
            "AUX_ANALYTICS_CONFIG",
            config_file.path().to_string_lossy().into_owned(),
        ),
        (
            "AUX_ANALYTICS_MANIFEST",
            manifest.path().to_string_lossy().into_owned(),
        ),
        (
            "AUX_ANALYTICS_DUCKLAKE_AUX_CATALOG",
            "/tmp/aux-ducklake/metadata.duckdb".to_string(),
        ),
        (
            "AUX_ANALYTICS_DUCKLAKE_DATA_PATH",
            "s3://analytics-bucket/warehouse".to_string(),
        ),
    ]);

    let resolved = config::resolve_config(config::ConfigRequest::lambda(environment))
        .expect("resolved config");
    let StorageBackend::DuckLake {
        catalog,
        catalog_path,
        data_path,
        object_storage,
        ..
    } = resolved.storage_backend.expect("backend")
    else {
        panic!("expected DuckLake backend");
    };
    assert_eq!(catalog, analytics_engine::CatalogType::AuxCatalog);
    assert_eq!(catalog_path, "/tmp/aux-ducklake/metadata.duckdb");
    assert_eq!(data_path, "s3://analytics-bucket/warehouse");
    assert_eq!(
        object_storage
            .as_ref()
            .and_then(|storage| storage.credentials.as_ref())
            .and_then(|credentials| credentials.instance_keys),
        Some(true)
    );
}

#[test]
fn given_invalid_lambda_query_env_when_resolved_then_validation_error_names_variable() {
    let manifest = manifest_file();
    let environment = lambda_environment([
        (
            "AUX_ANALYTICS_MANIFEST",
            manifest.path().to_string_lossy().into_owned(),
        ),
        ("AUX_ANALYTICS_DUCKDB", ":memory:".to_string()),
        (
            "AUX_ANALYTICS_QUERY_MAX_RESPONSE_SIZE_KB",
            "many".to_string(),
        ),
    ]);

    let error = config::resolve_config(config::ConfigRequest::lambda(environment))
        .expect_err("invalid env");

    assert!(
        error
            .to_string()
            .contains("AUX_ANALYTICS_QUERY_MAX_RESPONSE_SIZE_KB")
    );
}

#[test]
fn given_duckdb_catalog_config_when_lambda_backend_is_resolved_then_embedded_duckdb_is_used() {
    let manifest = manifest_file();
    let environment = lambda_environment([
        (
            "AUX_ANALYTICS_MANIFEST",
            manifest.path().to_string_lossy().into_owned(),
        ),
        ("AUX_ANALYTICS_DUCKDB", ":memory:".to_string()),
    ]);

    let resolved = config::resolve_config(config::ConfigRequest::lambda(environment))
        .expect("resolved config");

    assert!(matches!(
        resolved.storage_backend,
        Some(StorageBackend::DuckDb { path }) if path == ":memory:"
    ));
}

fn lambda_environment<const N: usize>(values: [(&str, String); N]) -> LambdaEnvironment {
    let values = values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect::<BTreeMap<_, _>>();
    LambdaEnvironment::from_lookup(|key| values.get(key).cloned())
}

fn manifest_file() -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("manifest file");
    let manifest = users_manifest();
    std::fs::write(
        file.path(),
        serde_json::to_vec(&manifest).expect("manifest json"),
    )
    .expect("manifest contents");
    file
}

#[test]
fn given_ducklake_object_storage_bucket_when_config_is_resolved_then_uri_is_normalized() {
    let manifest = manifest_file();
    let environment = lambda_environment([
        (
            "AUX_ANALYTICS_MANIFEST",
            manifest.path().to_string_lossy().into_owned(),
        ),
        (
            "AUX_ANALYTICS_CATALOG_BACKEND",
            "ducklake_sqlite".to_string(),
        ),
        (
            "AUX_ANALYTICS_DUCKLAKE_SQLITE_CATALOG",
            "/tmp/aux-ducklake/metadata.duckdb".to_string(),
        ),
        (
            "AUX_ANALYTICS_DUCKLAKE_DATA_PATH",
            "s3://analytics-bucket/warehouse".to_string(),
        ),
    ]);

    let resolved = config::resolve_config(config::ConfigRequest::lambda(environment))
        .expect("resolved config");
    let StorageBackend::DuckLake { data_path, .. } = resolved.storage_backend.expect("backend")
    else {
        panic!("expected DuckLake backend");
    };

    assert_eq!(data_path, "s3://analytics-bucket/warehouse");
}

#[test]
fn given_ducklake_backend_without_object_storage_path_when_resolved_then_path_requirement_is_reported()
 {
    let manifest = manifest_file();
    let config_file = tempfile::NamedTempFile::new().expect("config file");
    let mut root = config::RootConfig::default();
    root.analytics.catalog.backend = Some(AnalyticsCatalogBackend::DucklakeSqlite);
    root.analytics.catalog.connection_string = Some("metadata.duckdb".to_string());
    std::fs::write(
        config_file.path(),
        serde_json::to_vec(&root).expect("config json"),
    )
    .expect("config contents");
    let environment = lambda_environment([
        (
            "AUX_ANALYTICS_CONFIG",
            config_file.path().to_string_lossy().into_owned(),
        ),
        (
            "AUX_ANALYTICS_MANIFEST",
            manifest.path().to_string_lossy().into_owned(),
        ),
        (
            "AUX_ANALYTICS_CATALOG_BACKEND",
            "ducklake_sqlite".to_string(),
        ),
    ]);

    let error = config::resolve_config(config::ConfigRequest::lambda(environment))
        .expect_err("missing object storage path");

    assert_eq!(
        error.kind(),
        config::ConfigErrorKind::MissingDucklakeObjectStoragePath
    );
}

#[test]
fn given_lambda_ingest_outcome_when_serialized_then_external_status_names_are_stable() {
    assert_eq!(ingest_outcome_name(IngestOutcome::Inserted), "inserted");
    assert_eq!(ingest_outcome_name(IngestOutcome::Updated), "updated");
    assert_eq!(ingest_outcome_name(IngestOutcome::Deleted), "deleted");
    assert_eq!(ingest_outcome_name(IngestOutcome::Skipped), "skipped");
}

#[test]
fn given_missing_record_attribute_when_classified_then_lambda_exposes_record_contract_code() {
    let error = AnalyticsLambdaError::ingest(AnalyticsEngineError::MissingAttribute(
        "tenant_id".to_string(),
    ));

    assert_eq!(
        error.to_string(),
        "analytics_record_contract: record is missing required attribute tenant_id"
    );
}

#[test]
fn given_runtime_engine_failure_when_classified_then_lambda_keeps_retryable_error() {
    let error = AnalyticsLambdaError::ingest(AnalyticsEngineError::InvalidConditionExpression(
        "invalid".to_string(),
    ));

    assert_eq!(
        error.to_string(),
        "analytics engine error: condition expression is invalid: invalid"
    );
}
