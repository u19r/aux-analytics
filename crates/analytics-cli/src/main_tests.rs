use clap::CommandFactory as _;
use config::{
    AnalyticsCatalogBackend, AnalyticsCatalogConfig, AnalyticsConfig, AnalyticsObjectStorageConfig,
    AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType, RootConfig,
};

use crate::{backend_args::BackendArgs, cli::Cli, runtime_config::validate_source_config};

#[test]
fn config_resolves_ducklake_backend_with_object_storage_path() {
    let root = RootConfig {
        analytics: AnalyticsConfig {
            manifest_path: Some("manifest.json".to_string()),
            source: AnalyticsSourceConfig {
                stream_type: Some(AnalyticsStreamType::StorageStream),
                tables: Vec::new(),
                ..AnalyticsSourceConfig::default()
            },
            catalog: AnalyticsCatalogConfig {
                backend: Some(AnalyticsCatalogBackend::DucklakePostgres),
                connection_string: Some("dbname=ducklake_catalog host=localhost".to_string()),
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
            assert_eq!(catalog, analytics_engine::CatalogType::Postgres);
            assert_eq!(catalog_path, "dbname=ducklake_catalog host=localhost");
            assert_eq!(data_path, "s3://analytics-lake/tenant-data/prod");
        }
        other @ analytics_engine::StorageBackend::DuckDb { .. } => {
            panic!("unexpected backend: {other:?}");
        }
    }
}

#[test]
fn cli_backend_args_override_config_backend() {
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
        ducklake_postgres_catalog: None,
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
fn source_tables_need_global_or_table_stream_type() {
    let source = AnalyticsSourceConfig {
        stream_type: None,
        tables: vec![AnalyticsSourceTableConfig {
            table_name: "tenant_entities".to_string(),
            stream_type: None,
            stream_identifier: None,
        }],
        ..AnalyticsSourceConfig::default()
    };

    let error = validate_source_config(&source).expect_err("missing stream type");

    assert!(error.to_string().contains("tenant_entities"));
}

#[test]
fn root_help_reads_like_entrypoint_documentation() {
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
}

#[test]
fn completion_help_explains_local_installation() {
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
fn command_help_explains_backfill_and_operation_flags() {
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
        "Source filesystem raw backup root",
        "Clean target filesystem raw backup root",
        "Cleanup action for tainted source objects after clean verification",
        "Required when destructive cleanup is requested",
    ] {
        assert!(
            privacy_fix_help.contains(expected),
            "missing {expected:?} in:\n{privacy_fix_help}"
        );
    }

    let fix_help = command_help(["aux-analytics", "fix", "table", "--help"]);
    for expected in [
        "Execute a TableFixRequest JSON file",
        "TableFixRequest JSON file",
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

    let trim_help = command_help(["aux-analytics", "trim", "plan", "--help"]);
    for expected in [
        "Execute a TrimRequest JSON file",
        "TrimRequest JSON file",
        "Candidate row count",
        "Local DuckDB analytics database",
        "Operation store DuckDB path",
    ] {
        assert!(
            trim_help.contains(expected),
            "missing {expected:?} in:\n{trim_help}"
        );
    }
}

fn command_help<const N: usize>(args: [&str; N]) -> String {
    let mut command = Cli::command();
    let error = command
        .try_get_matches_from_mut(args)
        .expect_err("help exits");
    error.to_string()
}

fn empty_backend_args() -> BackendArgs {
    BackendArgs {
        duckdb: None,
        ducklake_sqlite_catalog: None,
        ducklake_postgres_catalog: None,
        ducklake_data_path: None,
    }
}

#[test]
fn checked_in_manifest_schema_matches_contract_types() {
    let expected =
        serde_json::to_value(schemars::schema_for!(analytics_contract::AnalyticsManifest))
            .expect("manifest schema");
    let actual: serde_json::Value =
        serde_json::from_str(include_str!("../../../schemas/manifest.schema.json"))
            .expect("checked-in manifest schema");

    assert_eq!(actual, expected);
}

#[test]
fn checked_in_config_schema_matches_analytics_config_types() {
    let expected =
        serde_json::to_value(schemars::schema_for!(config::RootConfig)).expect("config schema");
    let actual: serde_json::Value =
        serde_json::from_str(include_str!("../../../schemas/config.schema.json"))
            .expect("checked-in config schema");

    assert_eq!(actual, expected);
}
