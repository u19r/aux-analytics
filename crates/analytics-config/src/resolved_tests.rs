use storage_types::{AttributeValue, VecMap};

use crate::{
    AnalyticsCatalogBackend, AnalyticsCatalogConfig, AnalyticsConfig, AnalyticsIngestConfig,
    AnalyticsObjectStorageConfig, AnalyticsRetentionConfig, AnalyticsRetentionTableConfig,
    AnalyticsSourceConfig, AnalyticsSourceTableConfig, BackendOverride, CatalogType,
    ConfigErrorKind, RetentionDurationSelector, RetentionTimestampConfig, RootConfig,
    StorageBackend, TenantRetentionPolicyConfig, TenantRetentionPolicyRequest,
    TenantRetentionPolicySource, TenantRetentionQueryTableRequest,
    constants::MAX_RETENTION_DELETE_BATCH_SIZE, parse_override_args, resolve_storage_backend,
    resolved::resolve_storage_backend_with_root, validate_retention_config, validate_source_config,
};

#[test]
fn given_ducklake_catalog_and_bucket_prefix_when_backend_is_resolved_then_catalog_and_s3_path_are_preserved()
 {
    let root = RootConfig {
        analytics: AnalyticsConfig {
            catalog: AnalyticsCatalogConfig {
                backend: Some(AnalyticsCatalogBackend::DucklakeAuxCatalog),
                connection_string: Some("/var/lib/aux-analytics/metadata.duckdb".to_string()),
                ..AnalyticsCatalogConfig::default()
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
            assert_eq!(catalog, CatalogType::AuxCatalog);
            assert_eq!(catalog_path, "/var/lib/aux-analytics/metadata.duckdb");
            assert_eq!(data_path, "s3://analytics-lake/tenant-data/prod");
        }
        StorageBackend::DuckDb { .. } => panic!("expected ducklake backend"),
    }
}

#[test]
fn given_ducklake_config_and_duckdb_override_when_backend_is_resolved_then_duckdb_override_wins() {
    let mut root = RootConfig::default();
    root.analytics.catalog = AnalyticsCatalogConfig {
        backend: Some(AnalyticsCatalogBackend::DucklakeSqlite),
        connection_string: Some("metadata.ducklake".to_string()),
        ..AnalyticsCatalogConfig::default()
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
fn given_root_storage_and_catalog_settings_when_ducklake_backend_override_then_preserves_root_storage_and_catalog_settings()
 {
    let mut root = RootConfig::default();
    root.analytics.catalog.duckdb_threads = Some(4);
    root.analytics.object_storage = AnalyticsObjectStorageConfig {
        bucket: Some("analytics-bucket".to_string()),
        path: Some("warehouse".to_string()),
        credentials: Some(crate::RemoteCredentialsConfig {
            instance_keys: Some(true),
            ..crate::RemoteCredentialsConfig::default()
        }),
        ..AnalyticsObjectStorageConfig::default()
    };

    let backend = resolve_storage_backend_with_root(
        &BackendOverride {
            ducklake_aux_catalog: Some("metadata.duckdb".to_string()),
            ducklake_data_path: Some("s3://analytics-bucket/warehouse".to_string()),
            ..BackendOverride::default()
        },
        &root,
    )
    .expect("backend");

    let StorageBackend::DuckLake {
        object_storage,
        catalog_settings,
        ..
    } = backend
    else {
        panic!("expected ducklake backend");
    };
    assert_eq!(
        object_storage
            .as_ref()
            .and_then(|storage| storage.credentials.as_ref())
            .and_then(|credentials| credentials.instance_keys),
        Some(true)
    );
    assert_eq!(catalog_settings.duckdb_threads, Some(4));
}

#[test]
fn given_tables_without_stream_type_when_source_validation_then_rejects_tables_without_stream_type()
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

    let error = validate_source_config(&source).expect_err("missing stream type");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidSourceTableStreamType);
    assert!(error.to_string().contains("tenant_entities"));
}

#[test]
fn given_removed_source_scheduler_fields_when_deserialized_then_config_is_rejected() {
    for (field, value) in [
        ("polling_mode", "single_process"),
        ("lease_endpoint_url", "http://storage.example.test"),
    ] {
        let mut input = serde_json::json!({"endpoint_url": "http://source.example.test"});
        input[field] = serde_json::json!(value);
        let error = serde_json::from_value::<AnalyticsSourceConfig>(input)
            .expect_err("each removed scheduler field must be rejected");
        let message = error.to_string();
        assert!(message.contains("unknown field"));
        assert!(message.contains(field));
    }
}

#[test]
fn given_default_ingest_config_when_created_then_processor_is_enabled() {
    let ingest = AnalyticsIngestConfig::default();

    assert!(ingest.processor_enabled);
}

#[test]
fn given_dynamodb_query_with_limit_one_when_retention_validation_then_accepts_dynamodb_query_with_limit_one()
 {
    let retention = AnalyticsRetentionConfig {
        enabled: true,
        tables: vec![retention_table(1)],
        ..AnalyticsRetentionConfig::default()
    };

    validate_retention_config(&retention).expect("valid retention config");
}

#[test]
fn given_query_limit_other_than_one_when_retention_validation_then_rejects_query_limit_other_than_one()
 {
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
fn given_retention_delete_batch_size_at_lower_bound_when_validated_then_config_is_accepted() {
    let retention = AnalyticsRetentionConfig {
        enabled: true,
        delete_batch_size: 1,
        ..AnalyticsRetentionConfig::default()
    };

    validate_retention_config(&retention).expect("lower batch bound is valid");
}

#[test]
fn given_retention_delete_batch_size_at_upper_bound_when_validated_then_config_is_accepted() {
    let retention = AnalyticsRetentionConfig {
        enabled: true,
        delete_batch_size: MAX_RETENTION_DELETE_BATCH_SIZE,
        ..AnalyticsRetentionConfig::default()
    };

    validate_retention_config(&retention).expect("upper batch bound is valid");
}

#[test]
fn given_retention_delete_batch_size_over_upper_bound_when_validated_then_error_is_safe() {
    let retention = AnalyticsRetentionConfig {
        enabled: true,
        delete_batch_size: MAX_RETENTION_DELETE_BATCH_SIZE + 1,
        ..AnalyticsRetentionConfig::default()
    };

    let error = validate_retention_config(&retention).expect_err("over-bound batch is invalid");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidRetentionConfig);
    assert!(error.to_string().contains("at most 10000"));
    assert!(!error.to_string().contains("10001"));
}

#[test]
fn given_override_without_value_when_arguments_are_parsed_then_path_value_syntax_is_required() {
    let error = parse_override_args(&["http.bind_addr".to_string()]).expect_err("invalid arg");

    assert_eq!(error.kind(), ConfigErrorKind::Argument);
    assert!(error.to_string().contains("PATH=VALUE"));
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
                expression_attribute_values: VecMap::from_iter([(
                    ":tenant_id".to_string(),
                    AttributeValue::S("${tenant_id}".to_string()),
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
