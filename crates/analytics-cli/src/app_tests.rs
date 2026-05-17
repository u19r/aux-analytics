use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, QuerySelect,
    RowIdentity, StorageStreamRecord, StorageValue, StructuredQuery, TableRegistration,
    TenantSelector,
};
use analytics_engine::AnalyticsEngine;
use analytics_operations::{
    BackfillExecutionRequest, LocalBackfillChunk, LocalBackfillFixture, LocalStreamUpdate,
    OperationActor, OperationId, OperationKind, OperationRequest, OperationStore, RateLimitPolicy,
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
                    keys: std::collections::HashMap::new(),
                    old_image: None,
                    new_image: Some(std::collections::HashMap::from([
                        (
                            "tenant_id".to_string(),
                            StorageValue::S(tenant_id.to_string()),
                        ),
                        ("user_id".to_string(), StorageValue::S(user_id.to_string())),
                        (
                            "profile".to_string(),
                            StorageValue::M(std::collections::HashMap::from([(
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
    let rows: serde_json::Value = serde_json::from_str(&output).expect("query rows");

    assert_eq!(rows.as_array().map(Vec::len), Some(1));
    assert_eq!(rows[0]["email"], "a@example.com");
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
            operation_id: OperationId::new("backfill_cli_run_1").unwrap(),
            actor: OperationActor::new("cli-test").unwrap(),
            target_tables: vec!["users".to_string()],
            rate_limit: RateLimitPolicy::default(),
            fail_once_at_chunk: None,
        },
    );
    write_json(
        fixture_path.as_path(),
        &LocalBackfillFixture {
            chunks: vec![LocalBackfillChunk {
                chunk_id: 1,
                rows: 2,
                records: Vec::new(),
            }],
            stream_updates: vec![LocalStreamUpdate {
                cursor: 2,
                rows: 1,
                records: Vec::new(),
            }],
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
async fn given_raw_backup_write_dry_run_when_run_then_json_summary_is_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let records_path = tempdir.path().join("records.json");
    std::fs::write(
        records_path.as_path(),
        serde_json::to_string(&vec![raw_backup_record_json("user-1")]).expect("records json"),
    )
    .expect("write records");

    let cli = Cli::parse_from([
        "aux-analytics",
        "backup",
        "write",
        "--backup-root",
        tempdir.path().to_str().expect("backup root"),
        "--operation-id",
        "backup_cli_1",
        "--object-id",
        "users_0001",
        "--source-identity",
        "source_users",
        "--records",
        records_path.to_str().expect("records path"),
        "--dry-run",
    ]);

    let output = run(cli)
        .await
        .expect("backup dry run should run")
        .expect("output");
    let summary: serde_json::Value = serde_json::from_str(&output).expect("summary json");

    assert_eq!(summary["dry_run"], true);
    assert_eq!(summary["record_count"], 1);
}

#[tokio::test]
async fn given_raw_backup_privacy_policy_when_write_dry_run_then_policy_drops_are_reported() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let records_path = tempdir.path().join("records.json");
    let policy_path = tempdir.path().join("privacy.json");
    std::fs::write(
        records_path.as_path(),
        serde_json::to_string(&vec![raw_backup_record_json("user-1")]).expect("records json"),
    )
    .expect("write records");
    write_email_privacy_policy(policy_path.as_path());

    let cli = Cli::parse_from([
        "aux-analytics",
        "backup",
        "write",
        "--backup-root",
        tempdir.path().to_str().expect("backup root"),
        "--operation-id",
        "backup_cli_privacy",
        "--object-id",
        "users_privacy",
        "--source-identity",
        "source_users",
        "--records",
        records_path.to_str().expect("records path"),
        "--privacy-policy",
        policy_path.to_str().expect("privacy policy path"),
        "--dry-run",
    ]);

    let output = run(cli)
        .await
        .expect("backup privacy dry run should run")
        .expect("output");
    let summary: serde_json::Value = serde_json::from_str(&output).expect("summary json");

    assert_eq!(summary["privacy_policy_version"], "privacy-v1");
    assert_eq!(summary["policy_drops"], 1);
}

#[tokio::test]
async fn given_privacy_fix_raw_backup_cli_when_dry_run_then_report_is_persisted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let source_root = tempdir.path().join("source");
    let clean_root = tempdir.path().join("clean");
    let store_path = tempdir.path().join("operations.duckdb");
    let records_path = tempdir.path().join("records.json");
    let policy_path = tempdir.path().join("privacy.json");
    std::fs::write(
        records_path.as_path(),
        serde_json::to_string(&vec![raw_backup_record_json("user-privacy-fix")])
            .expect("records json"),
    )
    .expect("write records");
    write_email_privacy_policy(policy_path.as_path());

    let write_cli = Cli::parse_from([
        "aux-analytics",
        "backup",
        "write",
        "--backup-root",
        source_root.to_str().expect("source root"),
        "--operation-id",
        "backup_privacy_fix_cli",
        "--object-id",
        "users_privacy_fix_cli",
        "--source-identity",
        "source_users",
        "--records",
        records_path.to_str().expect("records path"),
    ]);
    run(write_cli)
        .await
        .expect("backup write should run")
        .expect("output");

    let privacy_fix_cli = Cli::parse_from([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--source-backup-root",
        source_root.to_str().expect("source root"),
        "--clean-target-root",
        clean_root.to_str().expect("clean root"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--operation-id",
        "privacy_fix_cli_1",
        "--object-id",
        "users_privacy_fix_cli",
        "--privacy-policy",
        policy_path.to_str().expect("policy path"),
        "--table",
        "users",
    ]);
    let output = run(privacy_fix_cli)
        .await
        .expect("privacy fix should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");
    let store = OperationStore::connect_duckdb(store_path.as_path()).expect("store");
    let persisted = store
        .privacy_fix_report(&OperationId::new("privacy_fix_cli_1").expect("operation id"))
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
    write_cli_raw_backup_object(backup_root.as_path(), "users_table_fix").await;
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
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
        .table_fix_report(&OperationId::new("table_fix_cli_1").expect("operation id"))
        .expect("persisted report");

    assert_eq!(report["dry_run"], true);
    assert_eq!(report["repaired_rows"], 1);
    assert_eq!(persisted.repaired_rows, 1);
}

#[tokio::test]
async fn given_trim_plan_request_file_when_dry_run_then_report_is_persisted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("trim-request.json");
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "operation_id": "trim_cli_1",
            "table_name": "users",
            "dry_run": true,
            "target": {
                "type": "row_keys",
                "keys": [b"user-1".to_vec()]
            },
            "confirmation_token": null
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
        "--candidate-rows",
        "1",
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
        .trim_report(&OperationId::new("trim_cli_1").expect("operation id"))
        .expect("persisted trim report");

    assert_eq!(report["dry_run"], true);
    assert_eq!(report["candidate_rows"], 1);
    assert_eq!(persisted.candidate_rows, 1);
}

#[tokio::test]
async fn given_trim_apply_request_when_cli_runs_then_request_fails_closed() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let store_path = tempdir.path().join("operations.duckdb");
    let request_path = tempdir.path().join("trim-request.json");
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "operation_id": "trim_cli_apply",
            "table_name": "users",
            "dry_run": false,
            "target": {
                "type": "row_keys",
                "keys": [b"user-1".to_vec()]
            },
            "confirmation_token": "trim-confirmed"
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
        "--candidate-rows",
        "1",
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
    ]);
    let error = run(trim_cli)
        .await
        .expect_err("trim apply should fail closed");

    assert!(error.to_string().contains("bounded deletion"));
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
            "operation_id": "trim_cli_apply_duckdb",
            "table_name": "users",
            "dry_run": false,
            "target": {
                "type": "row_keys",
                "keys": [b"user-1".to_vec()]
            },
            "confirmation_token": "trim-confirmed"
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
    write_cli_raw_backup_object(backup_root.as_path(), "users_table_fix_apply").await;
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
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
                "max_rows": 1000
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
async fn given_table_fix_apply_request_with_duckdb_destination_then_row_is_repaired() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let backup_root = tempdir.path().join("backup");
    let store_path = tempdir.path().join("operations.duckdb");
    let duckdb_path = tempdir.path().join("analytics.duckdb");
    let manifest_path = tempdir.path().join("manifest.json");
    let request_path = tempdir.path().join("table-fix-request.json");
    let records_path = tempdir.path().join("records.json");
    let manifest = tenant_attribute_manifest();
    std::fs::write(
        manifest_path.as_path(),
        serde_json::to_string(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
    std::fs::write(
        records_path.as_path(),
        serde_json::to_string(&vec![manifest_raw_backup_record_json("user-apply")])
            .expect("records json"),
    )
    .expect("write records");
    run(Cli::parse_from([
        "aux-analytics",
        "backup",
        "write",
        "--backup-root",
        backup_root.to_str().expect("backup root"),
        "--operation-id",
        "backup_table_fix_apply_duckdb",
        "--object-id",
        "users_table_fix_apply_duckdb",
        "--source-identity",
        "source_users",
        "--records",
        records_path.to_str().expect("records path"),
    ]))
    .await
    .expect("backup write should run")
    .expect("output");
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "operation_id": "table_fix_cli_apply_duckdb",
            "table_name": "users",
            "dry_run": false,
            "selection": {
                "type": "row_keys",
                "keys": [b"user-apply".to_vec()]
            },
            "source": {
                "type": "raw_backup",
                "object_ids": ["users_table_fix_apply_duckdb"]
            },
            "validation": {
                "require_post_fix_check": true,
                "max_rows": 1000
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
}

#[tokio::test]
async fn given_privacy_fix_delete_cli_with_confirmation_when_apply_then_tainted_source_is_removed()
{
    let tempdir = tempfile::tempdir().expect("tempdir");
    let source_root = tempdir.path().join("source");
    let clean_root = tempdir.path().join("clean");
    let store_path = tempdir.path().join("operations.duckdb");
    let policy_path = tempdir.path().join("privacy.json");
    write_email_privacy_policy(policy_path.as_path());
    write_cli_raw_backup_object(source_root.as_path(), "users_privacy_fix_delete").await;

    let privacy_fix_cli = Cli::parse_from([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--source-backup-root",
        source_root.to_str().expect("source root"),
        "--clean-target-root",
        clean_root.to_str().expect("clean root"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--operation-id",
        "privacy_fix_cli_delete",
        "--object-id",
        "users_privacy_fix_delete",
        "--privacy-policy",
        policy_path.to_str().expect("policy path"),
        "--table",
        "users",
        "--apply",
        "--destructive-action",
        "delete-tainted",
        "--confirmation-token",
        "delete-tainted",
    ]);
    let output = run(privacy_fix_cli)
        .await
        .expect("privacy fix apply should run")
        .expect("output");
    let report: serde_json::Value = serde_json::from_str(&output).expect("report json");

    assert_eq!(report["dry_run"], false);
    assert_eq!(report["deleted_tainted_objects"], 1);
    assert!(
        !source_root
            .join("users_privacy_fix_delete.index.json")
            .exists()
    );
    assert!(
        clean_root
            .join("users_privacy_fix_delete.index.json")
            .exists()
    );
}

#[tokio::test]
async fn given_privacy_fix_delete_cli_without_confirmation_when_apply_then_request_is_rejected() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let source_root = tempdir.path().join("source");
    let clean_root = tempdir.path().join("clean");
    let store_path = tempdir.path().join("operations.duckdb");
    let policy_path = tempdir.path().join("privacy.json");
    write_email_privacy_policy(policy_path.as_path());
    write_cli_raw_backup_object(source_root.as_path(), "users_privacy_fix_reject").await;

    let privacy_fix_cli = Cli::parse_from([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--source-backup-root",
        source_root.to_str().expect("source root"),
        "--clean-target-root",
        clean_root.to_str().expect("clean root"),
        "--operation-store-duckdb",
        store_path.to_str().expect("store path"),
        "--operation-id",
        "privacy_fix_cli_reject",
        "--object-id",
        "users_privacy_fix_reject",
        "--privacy-policy",
        policy_path.to_str().expect("policy path"),
        "--table",
        "users",
        "--apply",
        "--destructive-action",
        "delete-tainted",
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
    write_cli_raw_backup_object(source_root.as_path(), "users_privacy_fix_request").await;
    std::fs::write(
        request_path.as_path(),
        serde_json::json!({
            "operation_id": "privacy_fix_cli_request",
            "source_backup_prefix": source_root,
            "clean_target_prefix": clean_root,
            "raw_backup_object_ids": ["users_privacy_fix_request"],
            "tables": ["users"],
            "policy": {
                "version": "privacy-v1",
                "denied_key_names": ["email"]
            },
            "dry_run": true,
            "destructive_action": "none",
            "confirmation_token": null
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
async fn given_privacy_fix_request_file_with_field_overrides_then_cli_rejects_ambiguous_request() {
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
        "--object-id",
        "ambiguous",
    ]);
    let error = run(privacy_fix_cli)
        .await
        .expect_err("request file cannot be mixed with overrides");

    assert!(error.to_string().contains("cannot be mixed"));
}

#[tokio::test]
async fn given_raw_backup_object_when_replay_dry_run_then_record_count_is_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let records_path = tempdir.path().join("records.json");
    std::fs::write(
        records_path.as_path(),
        serde_json::to_string(&vec![raw_backup_record_json("user-1")]).expect("records json"),
    )
    .expect("write records");

    let write_cli = Cli::parse_from([
        "aux-analytics",
        "backup",
        "write",
        "--backup-root",
        tempdir.path().to_str().expect("backup root"),
        "--operation-id",
        "backup_cli_2",
        "--object-id",
        "users_0002",
        "--source-identity",
        "source_users",
        "--records",
        records_path.to_str().expect("records path"),
    ]);
    run(write_cli)
        .await
        .expect("backup write should run")
        .expect("output");

    let replay_cli = Cli::parse_from([
        "aux-analytics",
        "backup",
        "replay-dry-run",
        "--backup-root",
        tempdir.path().to_str().expect("backup root"),
        "--object-id",
        "users_0002",
    ]);
    let output = run(replay_cli)
        .await
        .expect("backup replay dry run should run")
        .expect("output");
    let summary: serde_json::Value = serde_json::from_str(&output).expect("summary json");

    assert_eq!(summary["dry_run"], true);
    assert_eq!(summary["record_count"], 1);
    assert_eq!(summary["verification"]["checksum_valid"], true);
    assert_eq!(summary["verification"]["metrics"]["checksum_failures"], 0);
}

#[tokio::test]
async fn given_raw_backup_privacy_policy_when_replay_dry_run_then_policy_drops_are_reported() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let records_path = tempdir.path().join("records.json");
    let policy_path = tempdir.path().join("privacy.json");
    std::fs::write(
        records_path.as_path(),
        serde_json::to_string(&vec![raw_backup_record_json("user-1")]).expect("records json"),
    )
    .expect("write records");
    std::fs::write(
        policy_path.as_path(),
        serde_json::json!({
            "version": "privacy-v1",
            "denied_key_names": ["email"]
        })
        .to_string(),
    )
    .expect("write policy");

    let write_cli = Cli::parse_from([
        "aux-analytics",
        "backup",
        "write",
        "--backup-root",
        tempdir.path().to_str().expect("backup root"),
        "--operation-id",
        "backup_cli_4",
        "--object-id",
        "users_0004",
        "--source-identity",
        "source_users",
        "--records",
        records_path.to_str().expect("records path"),
    ]);
    run(write_cli)
        .await
        .expect("backup write should run")
        .expect("output");

    let replay_cli = Cli::parse_from([
        "aux-analytics",
        "backup",
        "replay-dry-run",
        "--backup-root",
        tempdir.path().to_str().expect("backup root"),
        "--object-id",
        "users_0004",
        "--privacy-policy",
        policy_path.to_str().expect("policy path"),
    ]);
    let output = run(replay_cli)
        .await
        .expect("backup replay dry run should run")
        .expect("output");
    let summary: serde_json::Value = serde_json::from_str(&output).expect("summary json");

    assert_eq!(summary["record_count"], 1);
    assert_eq!(summary["metrics"]["policy_drops"], 1);
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
    let operation_id = OperationId::new("op_cli_1").expect("operation id");
    let store = OperationStore::connect_duckdb(db_path.as_path()).expect("store");
    store
        .create_operation(&OperationRequest {
            operation_id,
            kind: OperationKind::Backfill,
            actor: OperationActor::new("operator").expect("actor"),
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
        .check_report(&OperationId::new("check_cli_1").expect("operation id"))
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
async fn given_check_run_with_manifest_backed_duckdb_analytical_rows_then_report_uses_manifest_projection()
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
    }])
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
                "source_position": source_position,
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
                source_position bigint not null,
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
                i64::try_from(*source_position).expect("source position fits i64")
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

fn manifest_raw_backup_record_json(record_key: &str) -> serde_json::Value {
    serde_json::json!({
        "table_name": "users",
        "record_key": record_key,
        "source_position": null,
        "record": {
            "Keys": {
                "user_id": { "S": record_key }
            },
            "SequenceNumber": record_key,
            "NewImage": {
                "tenant_id": { "S": "tenant-a" },
                "user_id": { "S": record_key },
                "profile": {
                    "M": {
                        "email": { "S": "apply@example.test" }
                    }
                }
            }
        }
    })
}

fn write_email_privacy_policy(path: &std::path::Path) {
    std::fs::write(
        path,
        serde_json::json!({
            "version": "privacy-v1",
            "denied_key_names": ["email"],
            "denied_key_prefixes": [],
            "denied_key_suffixes": [],
            "denied_exact_paths": [],
            "denied_path_regexes": [],
            "denied_value_regexes": []
        })
        .to_string(),
    )
    .expect("write privacy policy");
}

fn write_json<T>(path: &std::path::Path, value: &T)
where T: serde::Serialize {
    std::fs::write(path, serde_json::to_string(value).expect("json")).expect("write json");
}

async fn write_cli_raw_backup_object(backup_root: &std::path::Path, object_id: &str) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let records_path = tempdir.path().join("records.json");
    std::fs::write(
        records_path.as_path(),
        serde_json::to_string(&vec![raw_backup_record_json("user-privacy-fix")])
            .expect("records json"),
    )
    .expect("write records");
    let write_cli = Cli::parse_from([
        "aux-analytics",
        "backup",
        "write",
        "--backup-root",
        backup_root.to_str().expect("backup root"),
        "--operation-id",
        "backup_privacy_fix_cli",
        "--object-id",
        object_id,
        "--source-identity",
        "source_users",
        "--records",
        records_path.to_str().expect("records path"),
    ]);
    run(write_cli)
        .await
        .expect("backup write should run")
        .expect("output");
}

fn email_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "users".to_string(),
        select: vec![QuerySelect::Column {
            column_name: "email".to_string(),
            alias: None,
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
    }
}
