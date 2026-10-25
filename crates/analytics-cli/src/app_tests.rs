use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, QueryExpression,
    QueryPredicate, QuerySelect, RowIdentity, StorageItem, StorageStreamRecord, StorageValue,
    StructuredQuery, TableRegistration, TenantSelector,
};
use analytics_engine::AnalyticsEngine;
use analytics_operations::{
    BackfillExecutionRequest, FilesystemRawBackupStore, LocalBackfillChunk, LocalBackfillFixture,
    LocalStreamUpdate, OperationActor, OperationId, OperationKind, OperationRequest,
    OperationStore, RateLimitPolicy, RawBackupObjectId, RawBackupRecord, RawBackupReplaySummary,
    RawBackupWriteRequest,
};
use clap::Parser;
use config::{AnalyticsSourceConfig, AnalyticsStreamType, RootConfig};

use crate::{app::run, cli::Cli};

#[tokio::test]
async fn given_schema_command_when_run_then_manifest_schema_json_is_returned() {
    let cli = Cli::parse_from(["aux-analytics", "schema"]);

    let output = run(cli)
        .await
        .expect("schema command should run")
        .expect("output");

    let schema: serde_json::Value = serde_json::from_str(&output).expect("schema json");
    assert_eq!(schema["title"], "AnalyticsManifest");
}

#[tokio::test]
async fn given_config_schema_command_when_run_then_config_schema_json_is_returned() {
    let cli = Cli::parse_from(["aux-analytics", "config-schema"]);

    let output = run(cli)
        .await
        .expect("config schema command should run")
        .expect("output");

    let schema: serde_json::Value = serde_json::from_str(&output).expect("schema json");
    assert_eq!(schema["title"], "RootConfig");
}

#[tokio::test]
async fn given_openapi_command_when_run_then_openapi_json_is_returned() {
    let cli = Cli::parse_from(["aux-analytics", "openapi"]);

    let output = run(cli)
        .await
        .expect("openapi command should run")
        .expect("output");

    let document: serde_json::Value = serde_json::from_str(&output).expect("openapi json");
    assert_eq!(document["openapi"], "3.1.0");
}

#[tokio::test]
async fn given_completions_command_when_run_for_zsh_then_completion_script_is_returned() {
    let cli = Cli::parse_from(["aux-analytics", "completions", "zsh"]);

    let output = run(cli)
        .await
        .expect("completions command should run")
        .expect("output");

    assert!(output.contains("#compdef aux-analytics"), "{output}");
    assert!(output.contains("_aux-analytics"), "{output}");
    assert!(output.contains("privacy-fix"), "{output}");
    assert!(output.contains("completions"), "{output}");
}

#[tokio::test]
async fn given_tenant_query_command_when_run_then_only_target_tenant_rows_are_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let db_path = tempdir.path().join("analytics.duckdb");
    let manifest_path = tempdir.path().join("manifest.json");
    let query_path = tempdir.path().join("query.json");
    let manifest = tenant_attribute_manifest();

    std::fs::write(
        manifest_path.as_path(),
        serde_json::to_string(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
    std::fs::write(
        query_path.as_path(),
        serde_json::to_string(&email_query()).expect("query json"),
    )
    .expect("write query");

    let engine =
        AnalyticsEngine::connect_duckdb(db_path.to_str().expect("db path")).expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    for (tenant_id, user_id, email) in [
        ("tenant-a", "user-a", "a@example.com"),
        ("tenant-b", "user-b", "b@example.com"),
    ] {
        engine
            .ingest_stream_record(
                &manifest,
                "users",
                user_id.as_bytes(),
                StorageStreamRecord {
                    sequence_number: user_id.to_string(),
                    keys: StorageItem::new(),
                    old_image: None,
                    new_image: Some(StorageItem::from_iter([
                        (
                            "tenant_id".to_string(),
                            StorageValue::S(tenant_id.to_string()),
                        ),
                        ("user_id".to_string(), StorageValue::S(user_id.to_string())),
                        (
                            "profile".to_string(),
                            StorageValue::M(StorageItem::from_iter([(
                                "email".to_string(),
                                StorageValue::S(email.to_string()),
                            )])),
                        ),
                    ])),
                },
            )
            .expect("ingest");
    }
    drop(engine);

    let cli = Cli::parse_from([
        "aux-analytics",
        "tenant-query",
        "--target-tenant-id",
        "tenant-a",
        "--manifest",
        manifest_path.to_str().expect("manifest path"),
        "--query",
        query_path.to_str().expect("query path"),
        "--duckdb",
        db_path.to_str().expect("db path"),
    ]);

    let output = run(cli)
        .await
        .expect("tenant query should run")
        .expect("output");
    let response: serde_json::Value = serde_json::from_str(&output).expect("query response");

    assert_eq!(response["rows"].as_array().map(Vec::len), Some(1));
    assert_eq!(response["rows"][0]["email"], "a@example.com");
    assert_eq!(response["columns"][0]["name"], "email");
    assert_eq!(response["execution"]["row_count"], 1);
}

#[tokio::test]
async fn given_typed_structured_query_when_run_then_canonical_response_metadata_is_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let db_path = tempdir.path().join("analytics.duckdb");
    let manifest_path = tempdir.path().join("manifest.json");
    let query_path = tempdir.path().join("query.json");
    let manifest = typed_query_manifest();
    let query = typed_metadata_query();

    write_json(manifest_path.as_path(), &manifest);
    write_json(query_path.as_path(), &query);

    let engine =
        AnalyticsEngine::connect_duckdb(db_path.to_str().expect("db path")).expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    drop(engine);

    let connection = duckdb::Connection::open(db_path.as_path()).expect("open database");
    connection
        .execute_batch(
            "insert into points
                (table_name, tenant_id, __id, unsigned_value, decimal_value, category)
             select 'source_points', 'tenant-a', 'point-' || cast(index as varchar),
                    cast(index as ubigint), cast(1.25 as decimal(38, 18)), 'active'
             from range(1000000) as rows(index)",
        )
        .expect("seed typed rows");
    drop(connection);

    let cli = Cli::parse_from([
        "aux-analytics",
        "unscoped-structured-query",
        "--manifest",
        manifest_path.to_str().expect("manifest path"),
        "--query",
        query_path.to_str().expect("query path"),
        "--duckdb",
        db_path.to_str().expect("db path"),
    ]);

    let output = run(cli)
        .await
        .expect("unscoped query should run")
        .expect("output");
    let response: serde_json::Value = serde_json::from_str(&output).expect("query response");
    let columns = response["columns"].as_array().expect("columns");
    let value_type = |name: &str| {
        columns
            .iter()
            .find(|column| column["name"] == name)
            .and_then(|column| column["value_type"].as_str())
    };

    let mut mismatches = Vec::new();
    if !response["execution"]["plan_shape"].is_object() {
        mismatches.push("execution.plan_shape is missing".to_string());
    } else {
        if response["execution"]["plan_shape"]["filter_count"] != 1 {
            mismatches.push("execution.plan_shape.filter_count is not 1".to_string());
        }
        if response["execution"]["plan_shape"]["aggregate_count"] != 3 {
            mismatches.push("execution.plan_shape.aggregate_count is not 3".to_string());
        }
    }
    if !response["execution"]["elapsed_ms"]
        .as_u64()
        .is_some_and(|elapsed_ms| elapsed_ms > 0)
    {
        mismatches.push(format!(
            "execution.elapsed_ms is {:?}, expected a non-zero value",
            response["execution"]["elapsed_ms"]
        ));
    }
    for (name, expected) in [
        ("unsigned_total", "u64"),
        ("decimal_total", "decimal"),
        ("event_at_ms", "timestamp_ms"),
    ] {
        if value_type(name) != Some(expected) {
            mismatches.push(format!(
                "columns.{name}.value_type is {:?}, expected {expected:?}",
                value_type(name)
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "canonical response mismatches: {mismatches:?}"
    );
}

#[tokio::test]
async fn given_manifest_backfill_plan_when_run_then_json_plan_is_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config_path = write_backfill_config(tempdir.path(), AnalyticsStreamType::AuxStorage);

    let cli = Cli::parse_from([
        "aux-analytics",
        "backfill",
        "plan",
        "--config",
        config_path.to_str().expect("config path"),
        "--estimated-rows",
        "25",
        "--chunk-target-rows",
        "10",
    ]);

    let output = run(cli)
        .await
        .expect("backfill plan should run")
        .expect("output");
    let plan: serde_json::Value = serde_json::from_str(&output).expect("plan json");

    assert_eq!(plan["snapshot_method"], "aux_storage_enumeration");
    assert_eq!(plan["chunk_count"], 3);
}

#[tokio::test]
async fn given_manifest_backfill_plan_when_text_output_requested_then_summary_is_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config_path = write_backfill_config(tempdir.path(), AnalyticsStreamType::AuxStorage);

    let cli = Cli::parse_from([
        "aux-analytics",
        "backfill",
        "plan",
        "--config",
        config_path.to_str().expect("config path"),
        "--estimated-rows",
        "25",
        "--chunk-target-rows",
        "10",
        "--output",
        "text",
    ]);

    let output = run(cli)
        .await
        .expect("backfill plan should run")
        .expect("output");

    assert!(output.contains("Backfill plan"));
    assert!(output.contains("snapshot method: AuxStorageEnumeration"));
    assert!(output.contains("chunks: 3"));
    assert!(output.contains("validation steps:"));
    assert!(output.contains("required permissions:"));
    assert!(output.contains("aux-storage metadata read access"));
}

#[tokio::test]
async fn given_local_backfill_run_when_cli_executes_then_operation_report_is_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let request_path = tempdir.path().join("backfill-execution.json");
    let fixture_path = tempdir.path().join("backfill-fixture.json");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    write_json(
        request_path.as_path(),
        &BackfillExecutionRequest {
            operation_id: OperationId::try_new("backfill_cli_run_1").unwrap(),
            actor: OperationActor::try_new("cli-test").unwrap(),
            target_tables: vec!["users".to_string()],
            rate_limit: RateLimitPolicy::default(),
            fail_once_at_chunk: None,
        },
    );
    write_json(
        fixture_path.as_path(),
        &LocalBackfillFixture {
            chunks: vec![LocalBackfillChunk::from_row_count(1, 2)],
            stream_updates: vec![LocalStreamUpdate::from_row_count(2, 1)],
        },
    );

    let cli = Cli::parse_from([
        "aux-analytics",
        "backfill",
        "run",
        "--request",
        request_path.to_str().expect("request path"),
        "--fixture",
        fixture_path.to_str().expect("fixture path"),
        "--operation-store-duckdb",
        operation_store_path.to_str().expect("operation store path"),
    ]);

    let output = run(cli)
        .await
        .expect("backfill run should execute")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");

    assert_eq!(report["operation_id"], "backfill_cli_run_1");
    assert_eq!(report["metrics"]["rows_written"], 3);
    assert_eq!(report["checkpoint"], 2);
}

#[tokio::test]
async fn given_privacy_fix_raw_backup_cli_when_dry_run_then_report_is_persisted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let source_root = tempdir.path().join("source");
    let clean_root = tempdir.path().join("clean");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("privacy-fix-request.json");
    write_raw_backup_fixture(
        source_root.as_path(),
        "backup_privacy_fix_cli",
        "users_privacy_fix_cli",
        vec![raw_backup_record_json("user-privacy-fix")],
    );
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "privacy_fix",
            "request": {
                "operation_id": "privacy_fix_cli_1",
                "source_backup_prefix": source_root,
                "clean_target_prefix": clean_root,
                "quarantine_target_prefix": tempdir.path().join("quarantine"),
                "raw_backup_object_ids": ["users_privacy_fix_cli"],
                "tables": ["users"],
                "policy": {
                    "version": "privacy-v1",
                    "denied_key_names": ["email"]
                },
                "dry_run": true,
                "destructive_action": "none",
                "confirmation_token": null
            }
        })
        .to_string(),
    )
    .expect("write request");

    let privacy_fix_cli = Cli::parse_from([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--request",
        request_path.to_str().expect("request path"),
    ]);
    let output = run(privacy_fix_cli)
        .await
        .expect("privacy fix should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");
    let store = OperationStore::connect_duckdb(store_path.as_path()).expect("store");
    let persisted = store
        .privacy_fix_report(&OperationId::try_new("privacy_fix_cli_1").expect("operation id"))
        .expect("persisted report");

    assert_eq!(report["dry_run"], true);
    assert_eq!(report["tainted_objects"], 1);
    assert_eq!(persisted.privacy_dropped_fields, 1);
}

#[tokio::test]
async fn given_table_fix_request_file_when_dry_run_then_report_is_persisted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let backup_root = tempdir.path().join("backup");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("table-fix-request.json");
    write_raw_backup_fixture(
        backup_root.as_path(),
        "backup_table_fix",
        "users_table_fix",
        vec![raw_backup_record_json("user-privacy-fix")],
    );
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "table_fix",
            "request": {
                "operation_id": "table_fix_cli_1",
                "table_name": "users",
                "dry_run": true,
                "selection": {
                    "type": "row_keys",
                    "keys": [b"user-privacy-fix".to_vec()]
                },
                "source": {
                    "type": "raw_backup",
                    "object_ids": ["users_table_fix"]
                },
                "validation": {
                    "require_post_fix_check": true,
                    "max_rows": 1000
                }
            }
        })
        .to_string(),
    )
    .expect("write request");

    let fix_cli = Cli::parse_from([
        "aux-analytics",
        "fix",
        "table",
        "--request",
        request_path.to_str().expect("request path"),
        "--raw-backup-root",
        backup_root.to_str().expect("backup root"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
    ]);
    let output = run(fix_cli)
        .await
        .expect("table fix should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");
    let store = OperationStore::connect_duckdb(store_path.as_path()).expect("store");
    let persisted = store
        .table_fix_report(&OperationId::try_new("table_fix_cli_1").expect("operation id"))
        .expect("persisted report");

    assert_eq!(report["dry_run"], true);
    assert_eq!(report["repaired_rows"], 0);
    assert_eq!(persisted.repaired_rows, 0);
}

#[tokio::test]
async fn given_trim_plan_request_file_when_dry_run_then_report_is_persisted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let store_path = tempdir.path().join("operations.duckdb");
    let duckdb_path = tempdir.path().join("analytics.duckdb");
    let request_path = tempdir.path().join("trim-request.json");
    write_duckdb_analytical_rows(
        duckdb_path.as_path(),
        &[("757365722d31", "one@example.com")],
    );
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "trim",
            "request": {
                "operation_id": "trim_cli_1",
                "table_name": "users",
                "dry_run": true,
                "target": {
                    "type": "row_keys",
                    "keys": [b"user-1".to_vec()]
                },
                "confirmation_token": null
            }
        })
        .to_string(),
    )
    .expect("write trim request");

    let trim_cli = Cli::parse_from([
        "aux-analytics",
        "trim",
        "plan",
        "--request",
        request_path.to_str().expect("request path"),
        "--destination-duckdb",
        duckdb_path.to_str().expect("duckdb path"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
    ]);
    let output = run(trim_cli)
        .await
        .expect("trim plan should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");
    let store = OperationStore::connect_duckdb(store_path.as_path()).expect("store");
    let persisted = store
        .trim_report(&OperationId::try_new("trim_cli_1").expect("operation id"))
        .expect("persisted trim report");

    assert_eq!(report["dry_run"], true);
    assert_eq!(report["candidate_rows"], 1);
    assert_eq!(persisted.candidate_rows, 1);
}

#[tokio::test]
async fn given_trim_apply_request_with_duckdb_destination_when_cli_runs_then_rows_are_deleted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let store_path = tempdir.path().join("operations.duckdb");
    let duckdb_path = tempdir.path().join("analytics.duckdb");
    let request_path = tempdir.path().join("trim-request.json");
    write_duckdb_analytical_rows(
        duckdb_path.as_path(),
        &[
            ("757365722d31", "one@example.com"),
            ("757365722d32", "two@example.com"),
        ],
    );
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "trim",
            "request": {
                "operation_id": "trim_cli_apply_duckdb",
                "table_name": "users",
                "dry_run": false,
                "target": {
                    "type": "row_keys",
                    "keys": [b"user-1".to_vec()]
                },
                "confirmation_token": "trim-confirmed"
            }
        })
        .to_string(),
    )
    .expect("write trim request");

    let trim_cli = Cli::parse_from([
        "aux-analytics",
        "trim",
        "plan",
        "--request",
        request_path.to_str().expect("request path"),
        "--destination-duckdb",
        duckdb_path.to_str().expect("duckdb path"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
    ]);
    let output = run(trim_cli)
        .await
        .expect("trim apply should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");

    assert_eq!(report["dry_run"], false);
    assert_eq!(report["candidate_rows"], 1);
    assert_eq!(report["rows_deleted"], 1);
    assert_eq!(duckdb_analytical_row_count(duckdb_path.as_path()), 1);
}

#[tokio::test]
async fn given_table_fix_apply_request_when_cli_has_no_destination_then_request_fails_closed() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let backup_root = tempdir.path().join("backup");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("table-fix-request.json");
    write_raw_backup_fixture(
        backup_root.as_path(),
        "backup_table_fix_apply",
        "users_table_fix_apply",
        vec![raw_backup_record_json("user-privacy-fix")],
    );
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "table_fix",
            "request": {
                "operation_id": "table_fix_cli_apply",
                "table_name": "users",
                "dry_run": false,
                "selection": {
                    "type": "row_keys",
                    "keys": [b"user-privacy-fix".to_vec()]
                },
                "source": {
                    "type": "raw_backup",
                    "object_ids": ["users_table_fix_apply"]
                },
                "validation": {
                    "require_post_fix_check": true,
                    "max_rows": 1000,
                    "confirmation": "table-fix-confirmed"
                }
            }
        })
        .to_string(),
    )
    .expect("write request");

    let fix_cli = Cli::parse_from([
        "aux-analytics",
        "fix",
        "table",
        "--request",
        request_path.to_str().expect("request path"),
        "--raw-backup-root",
        backup_root.to_str().expect("backup root"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
    ]);
    let error = run(fix_cli)
        .await
        .expect_err("table fix apply should fail closed");

    assert!(error.to_string().contains("--manifest"));
}

#[tokio::test]
async fn given_table_fix_source_read_fails_when_running_cli_then_destination_is_not_opened_before_submission()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let backup_root = tempdir.path().join("backup");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("table-fix-request.json");
    let manifest_path = tempdir.path().join("missing-manifest.json");
    let destination_path = tempdir.path().join("destination.duckdb");
    std::fs::create_dir_all(backup_root.as_path()).expect("create backup root");
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "table_fix",
            "request": {
                "operation_id": "table_fix_cli_lazy_destination",
                "table_name": "users",
                "dry_run": false,
                "selection": {
                    "type": "row_keys",
                    "keys": [b"user-missing-index".to_vec()]
                },
                "source": {
                    "type": "raw_backup",
                    "object_ids": ["missing-index"]
                },
                "validation": {
                    "require_post_fix_check": true,
                    "max_rows": 1000,
                    "confirmation": "table-fix-confirmed"
                }
            }
        })
        .to_string(),
    )
    .expect("write request");

    let fix_cli = Cli::parse_from([
        "aux-analytics",
        "fix",
        "table",
        "--request",
        request_path.to_str().expect("request path"),
        "--raw-backup-root",
        backup_root.to_str().expect("backup root"),
        "--manifest",
        manifest_path.to_str().expect("manifest path"),
        "--destination-duckdb",
        destination_path.to_str().expect("destination path"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
    ]);
    let error = run(fix_cli)
        .await
        .expect_err("table fix source read should fail");

    assert!(!error.to_string().contains("manifest"), "{error}");
    assert!(!manifest_path.exists());
    assert!(!destination_path.exists());
    let store = OperationStore::connect_duckdb(store_path.as_path()).expect("operation store");
    assert_eq!(
        store
            .show_operation(&OperationId::try_new("table_fix_cli_lazy_destination").expect("id"))
            .expect("submitted operation")
            .status,
        analytics_operations::OperationStatus::Paused
    );
}

#[tokio::test]
async fn given_invalid_table_fix_submission_when_running_cli_then_manifest_and_destination_are_not_opened()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let request_path = tempdir.path().join("table-fix-request.json");
    let store_path = tempdir.path().join("operations.duckdb");
    let manifest_path = tempdir.path().join("missing-manifest.json");
    let destination_path = tempdir.path().join("destination.duckdb");
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "table_fix",
            "request": {
                "operation_id": "table_fix_cli_invalid_submission",
                "table_name": "users",
                "dry_run": false,
                "selection": {
                    "type": "row_keys",
                    "keys": [b"user-invalid".to_vec()]
                },
                "source": {
                    "type": "raw_backup",
                    "object_ids": ["backup-invalid"]
                },
                "validation": {
                    "require_post_fix_check": true,
                    "max_rows": 1000,
                    "confirmation": null
                }
            }
        })
        .to_string(),
    )
    .expect("write request");

    let fix_cli = Cli::parse_from([
        "aux-analytics",
        "fix",
        "table",
        "--request",
        request_path.to_str().expect("request path"),
        "--raw-backup-root",
        tempdir.path().join("backup").to_str().expect("backup root"),
        "--manifest",
        manifest_path.to_str().expect("manifest path"),
        "--destination-duckdb",
        destination_path.to_str().expect("destination path"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
    ]);
    let error = run(fix_cli)
        .await
        .expect_err("invalid table fix submission should fail");

    assert!(error.to_string().contains("confirmation token"), "{error}");
    assert!(!manifest_path.exists());
    assert!(!destination_path.exists());
    assert!(!store_path.exists());
}

#[tokio::test]
async fn given_table_fix_apply_request_with_duckdb_destination_when_running_cli_then_row_is_repaired()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let backup_root = tempdir.path().join("backup");
    let store_path = tempdir.path().join("operations.duckdb");
    let duckdb_path = tempdir.path().join("analytics.duckdb");
    let manifest_path = tempdir.path().join("manifest.json");
    let request_path = tempdir.path().join("table-fix-request.json");
    let manifest = tenant_attribute_manifest();
    std::fs::write(
        manifest_path.as_path(),
        serde_json::to_string(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
    write_raw_backup_fixture(
        backup_root.as_path(),
        "backup_table_fix_apply_duckdb",
        "users_table_fix_apply_duckdb",
        vec![manifest_raw_backup_record_with_identity_json(
            "stream-event-1",
            "user-apply",
        )],
    );
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "table_fix",
            "request": {
                "operation_id": "table_fix_cli_apply_duckdb",
                "table_name": "users",
                "dry_run": false,
                "selection": {
                    "type": "row_keys",
                    "keys": [b"stream-event-1".to_vec()]
                },
                "source": {
                    "type": "raw_backup",
                    "object_ids": ["users_table_fix_apply_duckdb"]
                },
                "validation": {
                    "require_post_fix_check": true,
                    "max_rows": 1000,
                    "confirmation": "table-fix-confirmed"
                }
            }
        })
        .to_string(),
    )
    .expect("write request");

    let output = run(Cli::parse_from([
        "aux-analytics",
        "fix",
        "table",
        "--request",
        request_path.to_str().expect("request path"),
        "--raw-backup-root",
        backup_root.to_str().expect("backup root"),
        "--manifest",
        manifest_path.to_str().expect("manifest path"),
        "--destination-duckdb",
        duckdb_path.to_str().expect("duckdb path"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
    ]))
    .await
    .expect("table fix apply should run")
    .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");

    assert_eq!(report["dry_run"], false);
    assert_eq!(report["repaired_rows"], 1);
    assert_eq!(duckdb_analytical_row_count(duckdb_path.as_path()), 1);
    let engine = AnalyticsEngine::connect_duckdb(duckdb_path.as_path()).expect("open destination");
    let rows = engine
        .query_unscoped_sql_json("SELECT __id, tenant_id, email, item FROM users")
        .expect("query repaired destination");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["__id"], "757365722d6170706c79");
    assert_eq!(rows[0]["tenant_id"], "tenant-a");
    assert_eq!(rows[0]["email"], "apply@example.test");
    assert_eq!(rows[0]["item"]["user_id"], "user-apply");
    assert_eq!(rows[0]["item"]["profile"]["email"], "apply@example.test");
}

#[tokio::test]
async fn given_privacy_fix_delete_cli_with_confirmation_when_apply_then_request_fails_closed() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let source_root = tempdir.path().join("source");
    let clean_root = tempdir.path().join("clean");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("privacy-fix-request.json");
    write_raw_backup_fixture(
        source_root.as_path(),
        "backup_privacy_fix_delete",
        "users_privacy_fix_delete",
        vec![raw_backup_record_json("user-privacy-fix")],
    );
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "privacy_fix",
            "request": {
                "operation_id": "privacy_fix_cli_delete",
                "source_backup_prefix": source_root,
                "clean_target_prefix": clean_root,
                "quarantine_target_prefix": tempdir.path().join("quarantine"),
                "raw_backup_object_ids": ["users_privacy_fix_delete"],
                "tables": ["users"],
                "policy": {
                    "version": "privacy-v1",
                    "denied_key_names": ["email"]
                },
                "dry_run": false,
                "destructive_action": "delete_tainted",
                "confirmation_token": "delete-tainted"
            }
        })
        .to_string(),
    )
    .expect("write request");

    let privacy_fix_cli = Cli::parse_from([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--request",
        request_path.to_str().expect("request path"),
    ]);
    let source_object_path = source_root.join("users_privacy_fix_delete.jsonl");
    let source_index_path = source_root.join("users_privacy_fix_delete.index.json");
    let before_source_object = std::fs::read(source_object_path.as_path()).expect("source object");
    let before_source_index = std::fs::read(source_index_path.as_path()).expect("source index");
    let error = run(privacy_fix_cli)
        .await
        .expect_err("privacy fix apply should fail closed");

    assert_eq!(
        error.to_string(),
        "analytics operation control failed: raw backup destination filesystem does not support \
         atomic maintenance fencing"
    );
    assert_eq!(
        std::fs::read(source_object_path.as_path()).expect("source object"),
        before_source_object
    );
    assert_eq!(
        std::fs::read(source_index_path.as_path()).expect("source index"),
        before_source_index
    );
    assert!(!clean_root.exists());
    let store = OperationStore::connect_duckdb(store_path.as_path()).expect("store");
    assert!(store.list_operations().expect("list operations").is_empty());
}

#[tokio::test]
async fn given_privacy_fix_delete_cli_without_confirmation_when_apply_then_request_is_rejected() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let source_root = tempdir.path().join("source");
    let clean_root = tempdir.path().join("clean");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("privacy-fix-request.json");
    write_raw_backup_fixture(
        source_root.as_path(),
        "backup_privacy_fix_reject",
        "users_privacy_fix_reject",
        vec![raw_backup_record_json("user-privacy-fix")],
    );
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "privacy_fix",
            "request": {
                "operation_id": "privacy_fix_cli_reject",
                "source_backup_prefix": source_root,
                "clean_target_prefix": clean_root,
                "quarantine_target_prefix": tempdir.path().join("quarantine"),
                "raw_backup_object_ids": ["users_privacy_fix_reject"],
                "tables": ["users"],
                "policy": {
                    "version": "privacy-v1",
                    "denied_key_names": ["email"]
                },
                "dry_run": false,
                "destructive_action": "delete_tainted",
                "confirmation_token": null
            }
        })
        .to_string(),
    )
    .expect("write request");

    let privacy_fix_cli = Cli::parse_from([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--request",
        request_path.to_str().expect("request path"),
    ]);
    let error = run(privacy_fix_cli)
        .await
        .expect_err("privacy fix should require confirmation");

    assert!(error.to_string().contains("confirmation token"));
}

#[tokio::test]
async fn given_privacy_fix_request_file_when_dry_run_then_report_is_persisted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let source_root = tempdir.path().join("source");
    let clean_root = tempdir.path().join("clean");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("privacy-fix-request.json");
    write_raw_backup_fixture(
        source_root.as_path(),
        "backup_privacy_fix_request",
        "users_privacy_fix_request",
        vec![raw_backup_record_json("user-privacy-fix")],
    );
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "privacy_fix",
            "request": {
                "operation_id": "privacy_fix_cli_request",
                "source_backup_prefix": source_root,
                "clean_target_prefix": clean_root,
                "quarantine_target_prefix": tempdir.path().join("quarantine"),
                "raw_backup_object_ids": ["users_privacy_fix_request"],
                "tables": ["users"],
                "policy": {
                    "version": "privacy-v1",
                    "denied_key_names": ["email"]
                },
                "dry_run": true,
                "destructive_action": "none",
                "confirmation_token": null
            }
        })
        .to_string(),
    )
    .expect("write request");

    let privacy_fix_cli = Cli::parse_from([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--request",
        request_path.to_str().expect("request path"),
    ]);
    let output = run(privacy_fix_cli)
        .await
        .expect("privacy fix request should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");

    assert_eq!(report["dry_run"], true);
    assert_eq!(report["tainted_objects"], 1);
}

#[tokio::test]
async fn given_privacy_fix_request_file_without_kind_when_running_cli_then_cli_rejects_malformed_request()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("privacy-fix-request.json");
    std::fs::write(request_path.as_path(), "{}").expect("write request");

    let privacy_fix_cli = Cli::parse_from([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--request",
        request_path.to_str().expect("request path"),
    ]);
    let error = run(privacy_fix_cli)
        .await
        .expect_err("malformed request must be rejected");

    assert!(error.to_string().contains("JSON"), "{error}");
}

#[tokio::test]
async fn given_nested_privacy_policy_field_when_cli_reads_request_then_no_operation_is_created() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let request_path = tempdir.path().join("request.json");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let privacy_policy = serde_json::json!({
        "kind": "privacy_fix",
        "request": {
            "operation_id": "cli-privacy-unknown",
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
        }
    });
    std::fs::write(request_path.as_path(), privacy_policy.to_string()).expect("write request");
    let cli = Cli::parse_from([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--operation-store-duckdb",
        operation_store_path.to_str().expect("operation store path"),
        "--request",
        request_path.to_str().expect("request path"),
    ]);
    let error = run(cli)
        .await
        .expect_err("unknown nested privacy field must be rejected");
    assert!(format!("{error:?}").contains("unknown field"));

    assert!(
        OperationStore::connect_duckdb(operation_store_path.as_path())
            .expect("operation store")
            .list_operations()
            .expect("list operations")
            .is_empty()
    );
}

#[tokio::test]
async fn given_raw_backup_object_when_replay_dry_run_then_record_count_is_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let replay_request_path = tempdir.path().join("replay-request.json");
    write_raw_backup_fixture(
        tempdir.path(),
        "backup_cli_2",
        "users_0002",
        vec![raw_backup_record_json("user-1")],
    );
    std::fs::write(
        replay_request_path.as_path(),
        raw_backup_replay_request_json("users_0002").to_string(),
    )
    .expect("write replay request");

    let replay_cli = Cli::parse_from([
        "aux-analytics",
        "backup",
        "replay-dry-run",
        "--backup-root",
        tempdir.path().to_str().expect("backup root"),
        "--request",
        replay_request_path.to_str().expect("replay request path"),
    ]);
    let output = run(replay_cli)
        .await
        .expect("backup replay dry run should run")
        .expect("output");
    let summary: RawBackupReplaySummary = serde_json::from_str(&output).expect("summary json");

    assert_eq!(summary.object_id.as_str(), "users_0002");
    assert!(summary.dry_run);
    assert_eq!(summary.record_count, 1);
    assert!(summary.verification.checksum_valid);
    assert_eq!(summary.verification.metrics.checksum_failures, 0);
}

#[tokio::test]
async fn given_dynamodb_without_pitr_when_backfill_planned_then_parallel_scan_is_selected() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config_path = write_backfill_config(tempdir.path(), AnalyticsStreamType::StorageStream);

    let cli = Cli::parse_from([
        "aux-analytics",
        "backfill",
        "plan",
        "--config",
        config_path.to_str().expect("config path"),
        "--dynamodb-pitr-enabled=false",
        "--dynamodb-scan-enabled=true",
    ]);

    let output = run(cli)
        .await
        .expect("backfill plan should run")
        .expect("output");
    let plan: serde_json::Value = serde_json::from_str(&output).expect("plan json");

    assert_eq!(plan["snapshot_method"], "dynamo_db_parallel_scan");
}

#[tokio::test]
async fn given_manifest_without_target_row_identity_when_backfill_planned_then_rejected() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config_path = write_backfill_config(tempdir.path(), AnalyticsStreamType::AuxStorage);

    let cli = Cli::parse_from([
        "aux-analytics",
        "backfill",
        "plan",
        "--config",
        config_path.to_str().expect("config path"),
        "--target-table",
        "missing_table",
    ]);

    let error = run(cli)
        .await
        .expect_err("missing target table row identity should reject planning");

    assert!(
        error
            .to_string()
            .contains("stable row identity is required"),
        "{error}"
    );
}

#[tokio::test]
async fn given_operations_status_when_text_output_requested_then_summary_is_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let db_path = tempdir.path().join("operations.duckdb");
    let operation_id = OperationId::try_new("op_cli_1").expect("operation id");
    let store = OperationStore::connect_duckdb(db_path.as_path()).expect("store");
    store
        .create_operation(&OperationRequest {
            operation_id,
            kind: OperationKind::Backfill,
            actor: OperationActor::try_new("operator").expect("actor"),
            target_tables: vec!["users".to_string()],
            dry_run: true,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::json!({"mode": "test"}),
        })
        .expect("operation");
    drop(store);

    let cli = Cli::parse_from([
        "aux-analytics",
        "operations",
        "status",
        "--duckdb",
        db_path.to_str().expect("db path"),
        "--operation-id",
        "op_cli_1",
        "--output",
        "text",
    ]);

    let output = run(cli)
        .await
        .expect("operation status should run")
        .expect("output");

    assert!(output.contains("operation: op_cli_1"));
    assert!(output.contains("status: submitted"));
    assert!(output.contains("tables: users"));
}

#[tokio::test]
async fn given_operations_submit_when_canonical_request_file_is_used_then_result_is_idempotent() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let request_path = tempdir.path().join("maintenance-request.json");
    let store_path = tempdir.path().join("operations.duckdb");
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "kind": "trim",
            "request": {
                "operation_id": "cli-submit-trim",
                "table_name": "users",
                "dry_run": true,
                "target": {
                    "type": "horizon_trim",
                    "older_than_ms": 18446744073709551615_u64
                },
                "confirmation_token": null
            }
        })
        .to_string(),
    )
    .expect("write request");

    let command = || {
        Cli::parse_from([
            "aux-analytics",
            "operations",
            "submit",
            "--request",
            request_path.to_str().expect("request path"),
            "--duckdb",
            store_path.to_str().expect("store path"),
        ])
    };
    let first: serde_json::Value = serde_json::from_str(
        &run(command())
            .await
            .expect("first submission")
            .expect("first output"),
    )
    .expect("first result json");
    let second: serde_json::Value = serde_json::from_str(
        &run(command())
            .await
            .expect("second submission")
            .expect("second output"),
    )
    .expect("second result json");

    assert_eq!(first["operation_id"], "cli-submit-trim");
    assert_eq!(first["outcome"], "created");
    assert_eq!(second["operation_id"], "cli-submit-trim");
    assert_eq!(second["outcome"], "existing");
}

#[tokio::test]
async fn given_check_run_when_json_output_requested_then_report_is_persisted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let store_path = tempdir.path().join("operations.duckdb");
    let source_path = tempdir.path().join("source.json");
    let destination_path = tempdir.path().join("destination.json");
    write_check_rows(
        source_path.as_path(),
        &[("user-a", 11, 5), ("user-b", 22, 6)],
    );
    write_check_rows(
        destination_path.as_path(),
        &[("user-a", 11, 5), ("user-b", 99, 6)],
    );

    let cli = Cli::parse_from([
        "aux-analytics",
        "check",
        "run",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--operation-id",
        "check_cli_1",
        "--target-table",
        "users",
        "--source-rows",
        source_path.to_str().expect("source path"),
        "--destination-rows",
        destination_path.to_str().expect("destination path"),
        "--sample-limit",
        "1",
    ]);

    let output = run(cli)
        .await
        .expect("check command should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");

    assert_eq!(report["outcome"], "mismatched");
    assert_eq!(report["metrics"]["mismatched_rows"], 1);
    assert_eq!(
        report["samples"]["mismatched_keys"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    let store = OperationStore::connect_duckdb(store_path.as_path()).expect("store");
    let persisted = store
        .check_report(&OperationId::try_new("check_cli_1").expect("operation id"))
        .expect("persisted report");
    assert_eq!(persisted.metrics.mismatched_rows, 1);
}

#[tokio::test]
async fn given_check_run_when_text_output_requested_then_summary_is_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let store_path = tempdir.path().join("operations.duckdb");
    let source_path = tempdir.path().join("source.json");
    let destination_path = tempdir.path().join("destination.json");
    write_check_rows(source_path.as_path(), &[("user-a", 11, 5)]);
    write_check_rows(destination_path.as_path(), &[("user-a", 11, 5)]);

    let cli = Cli::parse_from([
        "aux-analytics",
        "check",
        "run",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--operation-id",
        "check_cli_2",
        "--target-table",
        "users",
        "--source-rows",
        source_path.to_str().expect("source path"),
        "--destination-rows",
        destination_path.to_str().expect("destination path"),
        "--output",
        "text",
    ]);

    let output = run(cli)
        .await
        .expect("check command should run")
        .expect("output");

    assert!(output.contains("check report: check_cli_2"));
    assert!(output.contains("outcome: Complete"));
    assert!(output.contains("checked rows: 1"));
}

#[tokio::test]
async fn given_check_run_with_duckdb_check_rows_when_json_output_requested_then_report_is_persisted()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let store_path = tempdir.path().join("operations.duckdb");
    let source_db_path = tempdir.path().join("source.duckdb");
    let destination_db_path = tempdir.path().join("destination.duckdb");
    write_duckdb_check_rows(source_db_path.as_path(), &[("user-a", 11, 5)]);
    write_duckdb_check_rows(destination_db_path.as_path(), &[("user-a", 99, 5)]);

    let cli = Cli::parse_from([
        "aux-analytics",
        "check",
        "run",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--operation-id",
        "check_cli_duckdb_1",
        "--target-table",
        "users",
        "--source-duckdb",
        source_db_path.to_str().expect("source db path"),
        "--source-duckdb-check-rows-table",
        "check_rows",
        "--destination-duckdb",
        destination_db_path.to_str().expect("destination db path"),
        "--destination-duckdb-check-rows-table",
        "check_rows",
    ]);

    let output = run(cli)
        .await
        .expect("check command should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");

    assert_eq!(report["outcome"], "mismatched");
    assert_eq!(report["metrics"]["mismatched_rows"], 1);
}

#[tokio::test]
async fn given_check_run_with_manifest_backed_duckdb_analytical_rows_when_running_cli_then_report_uses_manifest_projection()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let store_path = tempdir.path().join("operations.duckdb");
    let manifest_path = tempdir.path().join("manifest.json");
    let source_db_path = tempdir.path().join("source-analytics.duckdb");
    let destination_db_path = tempdir.path().join("destination-analytics.duckdb");
    std::fs::write(
        manifest_path.as_path(),
        serde_json::to_string(&tenant_attribute_manifest()).expect("manifest json"),
    )
    .expect("write manifest");
    write_duckdb_analytical_rows(source_db_path.as_path(), &[("user-a", "a@example.test")]);
    write_duckdb_analytical_rows(
        destination_db_path.as_path(),
        &[("user-a", "b@example.test")],
    );

    let cli = Cli::parse_from([
        "aux-analytics",
        "check",
        "run",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--operation-id",
        "check_cli_analytical_1",
        "--target-table",
        "users",
        "--manifest",
        manifest_path.to_str().expect("manifest path"),
        "--source-duckdb-analytical",
        source_db_path.to_str().expect("source db path"),
        "--destination-duckdb-analytical",
        destination_db_path.to_str().expect("destination db path"),
    ]);

    let output = run(cli)
        .await
        .expect("check command should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");

    assert_eq!(report["outcome"], "mismatched");
    assert_eq!(report["metrics"]["mismatched_rows"], 1);
}

fn tenant_attribute_manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "shared_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
        tenant_id: None,
        tenant_selector: TenantSelector::Attribute {
            attribute_name: "tenant_id".to_string(),
        },
        row_identity: RowIdentity::Attribute {
            attribute_name: "user_id".to_string(),
        },
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(vec![ProjectionColumn {
            column_name: "email".to_string(),
            attribute_path: "profile.email".to_string(),
            column_type: Some(AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::VarChar,
            }),
        }]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }])
}

fn typed_query_manifest() -> AnalyticsManifest {
    serde_json::from_value(serde_json::json!({
        "version": 1,
        "tables": [{
            "source_table_name": "source_points",
            "analytics_table_name": "points",
            "tenant_id": "tenant-a",
            "document_column": null,
            "columns": [
                {
                    "column_name": "unsigned_value",
                    "column_type": {
                        "kind": "primitive",
                        "primitive": "unsigned_big_int"
                    }
                },
                {
                    "column_name": "decimal_value",
                    "column_type": {
                        "kind": "primitive",
                        "primitive": "decimal"
                    }
                },
                {
                    "column_name": "category",
                    "column_type": {
                        "kind": "primitive",
                        "primitive": "var_char"
                    }
                }
            ]
        }]
    }))
    .expect("typed query manifest")
}

fn typed_metadata_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "points".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![
            QuerySelect::Sum {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "unsigned_value".to_string(),
                },
                alias: "unsigned_total".to_string(),
            },
            QuerySelect::Sum {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "decimal_value".to_string(),
                },
                alias: "decimal_total".to_string(),
            },
            QuerySelect::Count {
                alias: "event_at_ms".to_string(),
            },
        ],
        filters: vec![QueryPredicate::Eq {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "category".to_string(),
            },
            value: serde_json::json!("active"),
        }],
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(1),
        offset: None,
    }
}

fn write_backfill_config(
    directory: &std::path::Path,
    stream_type: AnalyticsStreamType,
) -> std::path::PathBuf {
    let manifest_path = directory.join("manifest.json");
    let config_path = directory.join("config.json");
    let manifest = tenant_attribute_manifest();
    std::fs::write(
        manifest_path.as_path(),
        serde_json::to_string(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
    let mut config = RootConfig::default();
    config.analytics.manifest_path = Some(manifest_path.to_string_lossy().to_string());
    config.analytics.source = AnalyticsSourceConfig {
        stream_type: Some(stream_type),
        ..AnalyticsSourceConfig::default()
    };
    std::fs::write(
        config_path.as_path(),
        serde_json::to_string(&config).expect("config json"),
    )
    .expect("write config");
    config_path
}

fn write_check_rows(path: &std::path::Path, rows: &[(&str, u64, u64)]) {
    let rows = rows
        .iter()
        .map(|(key, row_hash, source_position)| {
            serde_json::json!({
                "table": "users",
                "key": key,
                "row_hash": row_hash,
                "source_position": backup_position(*source_position),
                "contains_private_data": false
            })
        })
        .collect::<Vec<_>>();
    std::fs::write(path, serde_json::to_string(&rows).expect("rows json")).expect("write rows");
}

fn write_duckdb_check_rows(path: &std::path::Path, rows: &[(&str, u64, u64)]) {
    let connection = duckdb::Connection::open(path).expect("duckdb");
    connection
        .execute_batch(
            "create table check_rows (
                table_name varchar not null,
                row_key varchar not null,
                row_hash bigint not null,
                source_position json,
                contains_private_data boolean not null
            );",
        )
        .expect("create check rows table");
    let mut statement = connection
        .prepare("insert into check_rows values ('users', ?, ?, ?, false)")
        .expect("prepare insert");
    for (key, row_hash, source_position) in rows {
        statement
            .execute(duckdb::params![
                key,
                i64::try_from(*row_hash).expect("row hash fits i64"),
                serde_json::to_string(&backup_position(*source_position))
                    .expect("source position JSON")
            ])
            .expect("insert check row");
    }
}

fn write_duckdb_analytical_rows(path: &std::path::Path, rows: &[(&str, &str)]) {
    let connection = duckdb::Connection::open(path).expect("duckdb");
    connection
        .execute_batch(
            "create table users (
                __id varchar not null,
                email varchar not null,
                __source_position json
            );",
        )
        .expect("create users table");
    let mut statement = connection
        .prepare("insert into users values (?, ?, NULL)")
        .expect("prepare insert");
    for (key, email) in rows {
        statement
            .execute(duckdb::params![key, email])
            .expect("insert analytical row");
    }
}

fn duckdb_analytical_row_count(path: &std::path::Path) -> u64 {
    let connection = duckdb::Connection::open(path).expect("duckdb");
    let count: i64 = connection
        .query_row("select count(*) from users", [], |row| row.get(0))
        .expect("row count");
    u64::try_from(count).expect("row count fits u64")
}

fn raw_backup_record_json(record_key: &str) -> serde_json::Value {
    serde_json::json!({
        "table_name": "users",
        "record_key": record_key,
        "source_position": null,
        "record": {
            "Keys": {
                "pk": { "S": record_key }
            },
            "SequenceNumber": record_key,
            "NewImage": {
                "pk": { "S": record_key },
                "email": { "S": "ada@example.test" }
            }
        }
    })
}

fn raw_backup_replay_request_json(object_id: &str) -> serde_json::Value {
    serde_json::json!({"object_id": object_id})
}

fn manifest_raw_backup_record_with_identity_json(
    record_key: &str,
    user_id: &str,
) -> serde_json::Value {
    serde_json::json!({
        "table_name": "users",
        "record_key": record_key,
        "source_position": null,
        "record": {
            "Keys": {
                "user_id": { "S": user_id }
            },
            "SequenceNumber": record_key,
            "NewImage": {
                "tenant_id": { "S": "tenant-a" },
                "user_id": { "S": user_id },
                "profile": {
                    "M": {
                        "email": { "S": "apply@example.test" }
                    }
                }
            }
        }
    })
}

fn write_json<T>(path: &std::path::Path, value: &T)
where T: serde::Serialize {
    std::fs::write(path, serde_json::to_string(value).expect("json")).expect("write json");
}

fn write_raw_backup_fixture(
    backup_root: &std::path::Path,
    operation_id: &str,
    object_id: &str,
    records: Vec<serde_json::Value>,
) {
    let records = records
        .into_iter()
        .map(|record| serde_json::from_value::<RawBackupRecord>(record).expect("raw backup record"))
        .collect();
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new(operation_id).expect("operation id"),
        RawBackupObjectId::try_new(object_id).expect("object id"),
        "source_users",
        None,
        records,
    )
    .expect("raw backup request");
    FilesystemRawBackupStore::new(backup_root)
        .write_fixture_object(&request)
        .expect("write raw backup fixture");
}

fn email_query() -> StructuredQuery {
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
        limit: None,
        offset: None,
    }
}

fn backup_position(offset: u64) -> analytics_contract::SourcePosition {
    analytics_contract::SourcePosition::RawBackupObjectOffset {
        object_id: "check-fixture".to_string(),
        offset,
    }
}
