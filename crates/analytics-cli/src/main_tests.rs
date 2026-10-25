use clap::CommandFactory as _;
use config::{
    AnalyticsCatalogBackend, AnalyticsCatalogConfig, AnalyticsConfig, AnalyticsObjectStorageConfig,
    AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType, RootConfig,
};

use crate::{backend_args::BackendArgs, cli::Cli};

#[test]
fn given_object_storage_config_when_resolved_then_ducklake_backend_uses_configured_path() {
    let root = RootConfig {
        analytics: AnalyticsConfig {
            manifest_path: Some("manifest.json".to_string()),
            source: AnalyticsSourceConfig {
                stream_type: Some(AnalyticsStreamType::StorageStream),
                tables: Vec::new(),
                ..AnalyticsSourceConfig::default()
            },
            catalog: AnalyticsCatalogConfig {
                backend: Some(AnalyticsCatalogBackend::DucklakeAuxCatalog),
                connection_string: Some("/var/lib/aux-analytics/metadata.duckdb".to_string()),
                ..Default::default()
            },
            object_storage: AnalyticsObjectStorageConfig {
                bucket: Some("analytics-lake".to_string()),
                path: Some("tenant-data/prod".to_string()),
                ..AnalyticsObjectStorageConfig::default()
            },
            ..AnalyticsConfig::default()
        },
        ..RootConfig::default()
    };

    let backend =
        config::resolve_storage_backend(&(&empty_backend_args()).into(), &root).expect("backend");

    match backend {
        analytics_engine::StorageBackend::DuckLake {
            catalog,
            catalog_path,
            data_path,
            ..
        } => {
            assert_eq!(catalog, analytics_engine::CatalogType::AuxCatalog);
            assert_eq!(catalog_path, "/var/lib/aux-analytics/metadata.duckdb");
            assert_eq!(data_path, "s3://analytics-lake/tenant-data/prod");
        }
        other @ analytics_engine::StorageBackend::DuckDb { .. } => {
            panic!("unexpected backend: {other:?}");
        }
    }
}

#[test]
fn given_cli_backend_override_when_config_resolved_then_cli_backend_wins() {
    let mut root = RootConfig::default();
    root.analytics.catalog = AnalyticsCatalogConfig {
        backend: Some(AnalyticsCatalogBackend::DucklakeSqlite),
        connection_string: Some("metadata.ducklake".to_string()),
        ..Default::default()
    };
    root.analytics.object_storage.path = Some("lake-data".to_string());
    let backend_args = BackendArgs {
        duckdb: Some("local.duckdb".to_string()),
        ducklake_sqlite_catalog: None,
        ducklake_aux_catalog: None,
        ducklake_data_path: None,
    };

    let backend = config::resolve_storage_backend(&(&backend_args).into(), &root).expect("backend");

    match backend {
        analytics_engine::StorageBackend::DuckDb { path } => {
            assert_eq!(path, "local.duckdb");
        }
        other @ analytics_engine::StorageBackend::DuckLake { .. } => {
            panic!("unexpected backend: {other:?}");
        }
    }
}

#[test]
fn given_source_table_without_stream_type_when_configured_then_global_or_table_stream_type_is_required()
 {
    let source = AnalyticsSourceConfig {
        stream_type: None,
        tables: vec![AnalyticsSourceTableConfig {
            table_name: "tenant_entities".to_string(),
            stream_type: None,
            stream_identifier: None,
        }],
        ..AnalyticsSourceConfig::default()
    };

    let error = config::validate_source_config(&source).expect_err("missing stream type");

    assert!(error.to_string().contains("tenant_entities"));
}

#[test]
fn given_root_command_when_help_requested_then_entrypoint_workflows_are_documented() {
    let mut command = Cli::command();
    let mut help = Vec::new();
    command.write_long_help(&mut help).expect("help");
    let help = String::from_utf8(help).expect("utf8 help");

    for expected in [
        "Common workflows:",
        "Backend selection:",
        "Safety model:",
        "aux-analytics backfill plan",
        "aux-analytics privacy-fix raw-backup",
        "aux-analytics fix table",
        "aux-analytics trim plan",
        "aux-analytics completions zsh",
        "aux-analytics operations status",
        "Initialize analytical tables from a manifest",
        "Plan and run backfill workflows",
        "Generate a shell completion script",
    ] {
        assert!(help.contains(expected), "missing {expected:?} in:\n{help}");
    }
    assert!(!help.contains("aux-analytics backup write"), "{help}");
}

#[test]
fn given_completion_command_when_help_requested_then_local_installation_is_documented() {
    let help = command_help(["aux-analytics", "completions", "--help"]);

    for expected in [
        "Generate a shell completion script",
        "mkdir -p ~/.zfunc",
        "aux-analytics completions zsh > ~/.zfunc/_aux-analytics",
        "Shell to generate completions for",
    ] {
        assert!(help.contains(expected), "missing {expected:?} in:\n{help}");
    }
}

#[test]
fn given_backfill_and_operation_commands_when_help_requested_then_flags_are_documented() {
    let backfill_help = command_help(["aux-analytics", "backfill", "plan", "--help"]);
    for expected in [
        "Build a dry-run plan",
        "Read a complete BackfillRequest JSON file",
        "Whether DynamoDB point-in-time export is available",
        "Target analytics table to backfill",
    ] {
        assert!(
            backfill_help.contains(expected),
            "missing {expected:?} in:\n{backfill_help}"
        );
    }

    let backfill_run_help = command_help(["aux-analytics", "backfill", "run", "--help"]);
    for expected in [
        "Execute a BackfillExecutionRequest JSON file",
        "LocalBackfillFixture JSON file",
        "Operation store DuckDB path",
    ] {
        assert!(
            backfill_run_help.contains(expected),
            "missing {expected:?} in:\n{backfill_run_help}"
        );
    }

    let backfill_resume_help = command_help(["aux-analytics", "backfill", "resume", "--help"]);
    for expected in [
        "Resume a BackfillExecutionRequest",
        "LocalBackfillFixture JSON file",
        "Operation store DuckDB path",
    ] {
        assert!(
            backfill_resume_help.contains(expected),
            "missing {expected:?} in:\n{backfill_resume_help}"
        );
    }

    let operations_help = command_help(["aux-analytics", "operations", "status", "--help"]);
    for expected in [
        "Show one operation status",
        "Operation store DuckDB path",
        "Operation id to inspect",
    ] {
        assert!(
            operations_help.contains(expected),
            "missing {expected:?} in:\n{operations_help}"
        );
    }

    let privacy_fix_help = command_help(["aux-analytics", "privacy-fix", "raw-backup", "--help"]);
    for expected in [
        "Scan local raw backup objects, apply a PrivacyPolicy",
        "complete typed maintenance contract",
        "Apply requests",
        "fail closed",
        "Complete MaintenanceRequest JSON file",
        "Operation store DuckDB path",
    ] {
        assert!(
            privacy_fix_help.contains(expected),
            "missing {expected:?} in:\n{privacy_fix_help}"
        );
    }

    let backup_help = command_help(["aux-analytics", "backup", "--help"]);
    for expected in [
        "Validate local raw backup objects",
        "replay-dry-run",
        "Production object stores and raw-backup publication",
    ] {
        assert!(
            backup_help.contains(expected),
            "missing {expected:?} in:\n{backup_help}"
        );
    }
    assert!(!backup_help.contains("write"), "{backup_help}");

    let fix_help = command_help(["aux-analytics", "fix", "table", "--help"]);
    for expected in [
        "Execute a MaintenanceRequest JSON file with kind=table_fix",
        "Complete MaintenanceRequest JSON file with kind=table_fix",
        "Filesystem raw backup root",
        "required for apply mode",
        "Local DuckDB analytics database",
        "Operation store DuckDB path",
    ] {
        assert!(
            fix_help.contains(expected),
            "missing {expected:?} in:\n{fix_help}"
        );
    }
    let fix_parent_help = command_help(["aux-analytics", "fix", "--help"]);
    assert!(
        fix_parent_help.contains("support dry-run"),
        "{fix_parent_help}"
    );
    assert!(
        fix_parent_help.contains("confirmed apply mode"),
        "{fix_parent_help}"
    );
    assert!(
        !fix_parent_help.contains("apply mode fails closed"),
        "{fix_parent_help}"
    );

    let trim_help = command_help(["aux-analytics", "trim", "plan", "--help"]);
    for expected in [
        "Execute a MaintenanceRequest JSON file with kind=trim",
        "Complete MaintenanceRequest JSON file with kind=trim",
        "bounded local DuckDB source and destination",
        "Operation store DuckDB path",
    ] {
        assert!(
            trim_help.contains(expected),
            "missing {expected:?} in:\n{trim_help}"
        );
    }
    assert!(
        trim_help.contains("Run a destructive trim request"),
        "{trim_help}"
    );
    assert!(
        !trim_help.contains("Dry-run a destructive trim request"),
        "{trim_help}"
    );
    assert!(
        trim_help.contains("requires a request confirmation token"),
        "{trim_help}"
    );
    let trim_parent_help = command_help(["aux-analytics", "trim", "--help"]);
    assert!(
        trim_parent_help.contains("requires a typed confirmation token"),
        "{trim_parent_help}"
    );
    assert!(
        !trim_parent_help.contains("dry-run only"),
        "{trim_parent_help}"
    );
    assert!(
        !trim_parent_help.contains("apply mode fails closed"),
        "{trim_parent_help}"
    );
}

fn command_help<const N: usize>(args: [&str; N]) -> String {
    let mut command = Cli::command();
    let error = command
        .try_get_matches_from_mut(args)
        .expect_err("help exits");
    error
        .to_string()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn empty_backend_args() -> BackendArgs {
    BackendArgs {
        duckdb: None,
        ducklake_sqlite_catalog: None,
        ducklake_aux_catalog: None,
        ducklake_data_path: None,
    }
}

#[test]
fn given_checked_in_manifest_schema_when_generated_then_contract_matches() {
    let expected =
        serde_json::to_value(schemars::schema_for!(analytics_contract::AnalyticsManifest))
            .expect("manifest schema");
    let actual: serde_json::Value =
        serde_json::from_str(include_str!("../../../schemas/manifest.schema.json"))
            .expect("checked-in manifest schema");

    assert_eq!(actual, expected);
}

#[test]
fn given_checked_in_config_schema_when_generated_then_contract_matches() {
    let expected =
        serde_json::to_value(schemars::schema_for!(config::RootConfig)).expect("config schema");
    let actual: serde_json::Value =
        serde_json::from_str(include_str!("../../../schemas/config.schema.json"))
            .expect("checked-in config schema");

    assert_eq!(actual, expected);
}
