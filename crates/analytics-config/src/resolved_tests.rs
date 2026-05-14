use crate::{
    AnalyticsCatalogBackend, AnalyticsCatalogConfig, AnalyticsConfig, AnalyticsObjectStorageConfig,
    AnalyticsRetentionConfig, AnalyticsRetentionTableConfig, AnalyticsSourceConfig,
    AnalyticsSourceTableConfig, BackendOverride, CatalogType, ConfigErrorKind,
    RetentionDurationSelector, RetentionTimestampConfig, RootConfig, StorageBackend,
    TenantRetentionPolicyConfig, TenantRetentionPolicyRequest, TenantRetentionPolicySource,
    TenantRetentionQueryTableRequest, parse_override_args, resolve_manifest_path,
    resolve_storage_backend, validate_retention_config, validate_source_config,
};

#[test]
fn resolves_ducklake_backend_from_config_with_bucket_path() {
    let root = RootConfig {
        analytics: AnalyticsConfig {
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

    let backend = resolve_storage_backend(&BackendOverride::default(), &root).expect("backend");

    match backend {
        StorageBackend::DuckLake {
            catalog,
            catalog_path,
            data_path,
            ..
        } => {
            assert_eq!(catalog, CatalogType::Postgres);
            assert_eq!(catalog_path, "dbname=ducklake_catalog host=localhost");
            assert_eq!(data_path, "s3://analytics-lake/tenant-data/prod");
        }
        StorageBackend::DuckDb { .. } => panic!("expected ducklake backend"),
    }
}

#[test]
fn backend_override_wins_over_config_backend() {
    let mut root = RootConfig::default();
    root.analytics.catalog = AnalyticsCatalogConfig {
        backend: Some(AnalyticsCatalogBackend::DucklakeSqlite),
        connection_string: Some("metadata.ducklake".to_string()),
    };
    root.analytics.object_storage.path = Some("lake-data".to_string());

    let backend = resolve_storage_backend(
        &BackendOverride {
            duckdb: Some("local.duckdb".to_string()),
            ..BackendOverride::default()
        },
        &root,
    )
    .expect("backend");

    match backend {
        StorageBackend::DuckDb { path } => assert_eq!(path, "local.duckdb"),
        StorageBackend::DuckLake { .. } => panic!("expected duckdb backend"),
    }
}

#[test]
fn source_validation_rejects_tables_without_stream_type() {
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

    assert_eq!(error.kind(), ConfigErrorKind::InvalidSourceTableStreamType);
    assert!(error.to_string().contains("tenant_entities"));
}

#[test]
fn retention_validation_accepts_dynamodb_query_with_limit_one() {
    let retention = AnalyticsRetentionConfig {
        enabled: true,
        tables: vec![retention_table(1)],
        ..AnalyticsRetentionConfig::default()
    };

    validate_retention_config(&retention).expect("valid retention config");
}

#[test]
fn retention_validation_rejects_query_limit_other_than_one() {
    let retention = AnalyticsRetentionConfig {
        enabled: true,
        tables: vec![retention_table(2)],
        ..AnalyticsRetentionConfig::default()
    };

    let error = validate_retention_config(&retention).expect_err("invalid query limit");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidRetentionConfig);
    assert!(error.to_string().contains("limit must equal 1"));
}

#[test]
fn override_args_require_path_value_syntax() {
    let error = parse_override_args(&["http.bind_addr".to_string()]).expect_err("invalid arg");

    assert_eq!(error.kind(), ConfigErrorKind::Argument);
    assert!(error.to_string().contains("PATH=VALUE"));
}

#[test]
fn manifest_path_prefers_explicit_argument() {
    let root = RootConfig {
        analytics: AnalyticsConfig {
            manifest_path: Some("config-manifest.json".to_string()),
            ..AnalyticsConfig::default()
        },
        ..RootConfig::default()
    };

    let path = resolve_manifest_path(Some("arg-manifest.json"), &root).expect("manifest path");

    assert_eq!(path, "arg-manifest.json");
}

#[test]
fn manifest_path_falls_back_to_config() {
    let root = RootConfig {
        analytics: AnalyticsConfig {
            manifest_path: Some("config-manifest.json".to_string()),
            ..AnalyticsConfig::default()
        },
        ..RootConfig::default()
    };

    let path = resolve_manifest_path(None, &root).expect("manifest path");

    assert_eq!(path, "config-manifest.json");
}

fn retention_table(limit: u32) -> AnalyticsRetentionTableConfig {
    AnalyticsRetentionTableConfig {
        analytics_table_name: "audit_events".to_string(),
        default_period_ms: 1_000,
        strict: true,
        timestamp: RetentionTimestampConfig::IngestedAt,
        tenant_policy: TenantRetentionPolicyConfig {
            source: TenantRetentionPolicySource::DynamoDb,
            endpoint_url: None,
            region: Some("us-east-1".to_string()),
            credentials: None,
            request: TenantRetentionPolicyRequest::QueryTable(TenantRetentionQueryTableRequest {
                table_name: "tenant_retention".to_string(),
                index_name: Some("tenant_id-index".to_string()),
                key_condition_expression: "tenant_id = :tenant_id".to_string(),
                filter_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: std::collections::BTreeMap::from([(
                    ":tenant_id".to_string(),
                    serde_json::json!({"S": "${tenant_id}"}),
                )]),
                scan_index_forward: None,
                limit,
            }),
            duration_selector: RetentionDurationSelector {
                attribute_path: "analytics.retention_ms".to_string(),
            },
            cache_ttl_ms: 1_000,
        },
    }
}
