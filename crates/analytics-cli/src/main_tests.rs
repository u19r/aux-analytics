use config::{
    AnalyticsCatalogBackend, AnalyticsCatalogConfig, AnalyticsConfig, AnalyticsObjectStorageConfig,
    AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType, RootConfig,
};

use crate::{cli::BackendArgs, runtime_config::validate_source_config};

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
