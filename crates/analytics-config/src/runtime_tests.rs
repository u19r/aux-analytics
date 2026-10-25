use std::path::PathBuf;

use analytics_contract::AnalyticsManifest;
use serde_json::json;

use crate::{
    AnalyticsCatalogBackend, AnalyticsCatalogConfig, AnalyticsConfig, AnalyticsObjectStorageConfig,
    AnalyticsSourceConfig, BackendOverride, ConfigErrorKind, ConfigRequest, LambdaEnvironment,
    ManifestSource, RootConfig,
    constants::{
        ENV_CONFIG_PATH, ENV_DUCKDB, ENV_MANIFEST_PATH, ENV_OBJECT_REGION,
        ENV_QUERY_LAMBDA_QUERY_ONLY, ENV_QUERY_MAX_RESPONSE_SIZE_KB, ENV_S3_BUCKET, ENV_S3_PREFIX,
        MAX_RETENTION_DELETE_BATCH_SIZE,
    },
    resolve_config,
};

fn manifest_file() -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("manifest file");
    let manifest = AnalyticsManifest::new(vec![analytics_contract::TableRegistration {
        source_table_name: "source_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant-a".to_string()),
        tenant_selector: Default::default(),
        row_identity: Default::default(),
        row_expansion: None,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: None,
        columns: Vec::new(),
        table_scope: Default::default(),
        join_policy: Default::default(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }]);
    std::fs::write(
        file.path(),
        serde_json::to_vec(&manifest).expect("manifest json"),
    )
    .expect("manifest contents");
    file
}

fn backend() -> BackendOverride {
    BackendOverride {
        duckdb: Some(":memory:".to_string()),
        ..BackendOverride::default()
    }
}

fn cli_request(path: Option<String>, manifest: Option<String>) -> ConfigRequest {
    ConfigRequest::cli(path.map(PathBuf::from), manifest, Vec::new(), backend())
}

fn retention_override(limit: Option<u32>) -> String {
    let mut tables = json!([{
        "analytics_table_name": "users",
        "default_period_ms": 1,
        "timestamp": {"kind": "ingested_at"},
        "tenant_policy": {
            "source": "dynamodb",
            "request": {
                "query_table": {
                    "table_name": "retention",
                    "key_condition_expression": "tenant_id = :tenant_id",
                    "expression_attribute_values": {":tenant_id": {"S": "${tenant_id}"}}
                }
            },
            "duration_selector": {"attribute_path": "retention_ms"}
        }
    }]);
    if let Some(limit) = limit {
        tables[0]["tenant_policy"]["request"]["query_table"]["limit"] = json!(limit);
    }
    format!("analytics.retention.tables={tables}")
}

#[test]
fn given_cli_manifest_when_config_file_also_has_manifest_then_argument_wins() {
    let manifest = manifest_file();
    let root = RootConfig {
        analytics: AnalyticsConfig {
            manifest_path: Some("missing.json".to_string()),
            source: AnalyticsSourceConfig::default(),
            ..AnalyticsConfig::default()
        },
        ..RootConfig::default()
    };
    let config = tempfile::NamedTempFile::new().expect("config file");
    std::fs::write(
        config.path(),
        serde_json::to_vec(&root).expect("config json"),
    )
    .expect("config contents");

    let resolved = resolve_config(cli_request(
        Some(config.path().to_string_lossy().into_owned()),
        Some(manifest.path().to_string_lossy().into_owned()),
    ))
    .expect("resolved config");

    assert_eq!(resolved.manifest.source, ManifestSource::ExplicitArgument);
    assert_eq!(resolved.manifest.value.tables.len(), 1);
}

#[test]
fn given_api_without_manifest_when_config_is_valid_then_empty_manifest_and_routes_are_ready() {
    let request = ConfigRequest::api(None, None, Vec::new(), backend());

    let resolved = resolve_config(request).expect("resolved config");

    assert_eq!(resolved.manifest.source, ManifestSource::ApiEmpty);
    assert!(resolved.manifest.value.tables.is_empty());
    assert!(resolved.routes.query_enabled);
    assert!(resolved.routes.ingest_enabled);
}

#[test]
fn given_api_empty_manifest_argument_when_resolved_then_manifest_is_rejected() {
    let error = resolve_config(ConfigRequest::api(
        None,
        Some(String::new()),
        Vec::new(),
        backend(),
    ))
    .expect_err("empty manifest");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidManifestPath);
}

#[test]
fn given_api_ingest_disabled_when_resolved_then_only_ingest_route_is_disabled() {
    let request = ConfigRequest::api(
        None,
        None,
        vec!["analytics.http.ingest_endpoint_enabled=false".to_string()],
        backend(),
    );

    let resolved = resolve_config(request).expect("resolved config");

    assert!(!resolved.routes.ingest_enabled);
    assert!(resolved.routes.query_enabled);
}

#[test]
fn given_operations_store_path_when_configured_then_resolved_config_preserves_it() {
    let manifest = manifest_file();
    let root = RootConfig {
        analytics: AnalyticsConfig {
            operations: crate::AnalyticsOperationsConfig {
                operation_store_path: Some("operations.duckdb".to_string()),
            },
            ..AnalyticsConfig::default()
        },
        ..RootConfig::default()
    };
    let config = tempfile::NamedTempFile::new().expect("config file");
    std::fs::write(
        config.path(),
        serde_json::to_vec(&root).expect("config json"),
    )
    .expect("config contents");

    let resolved = resolve_config(cli_request(
        Some(config.path().to_string_lossy().into_owned()),
        Some(manifest.path().to_string_lossy().into_owned()),
    ))
    .expect("resolved config");

    assert_eq!(
        resolved
            .root
            .analytics
            .operations
            .operation_store_path
            .as_deref(),
        Some("operations.duckdb")
    );
}

#[test]
fn given_empty_operations_store_path_when_configured_then_resolution_rejects_it() {
    let manifest = manifest_file();
    let root = RootConfig {
        analytics: AnalyticsConfig {
            operations: crate::AnalyticsOperationsConfig {
                operation_store_path: Some("  ".to_string()),
            },
            ..AnalyticsConfig::default()
        },
        ..RootConfig::default()
    };
    let config = tempfile::NamedTempFile::new().expect("config file");
    std::fs::write(
        config.path(),
        serde_json::to_vec(&root).expect("config json"),
    )
    .expect("config contents");

    let error = resolve_config(cli_request(
        Some(config.path().to_string_lossy().into_owned()),
        Some(manifest.path().to_string_lossy().into_owned()),
    ))
    .expect_err("empty operation store path");

    assert_eq!(error.kind(), ConfigErrorKind::Argument);
    assert!(error.to_string().contains("operation_store_path"));
}

#[test]
fn given_cli_without_manifest_when_resolved_then_manifest_is_rejected() {
    let error = resolve_config(cli_request(None, None)).expect_err("missing manifest");

    assert_eq!(error.kind(), ConfigErrorKind::MissingManifestPath);
}

#[test]
fn given_empty_cli_manifest_when_resolved_then_manifest_is_rejected() {
    let error = resolve_config(cli_request(None, Some(String::new()))).expect_err("empty manifest");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidManifestPath);
}

#[test]
fn given_malformed_manifest_file_when_resolved_then_routes_are_not_started() {
    let manifest = tempfile::NamedTempFile::new().expect("manifest file");
    std::fs::write(manifest.path(), b"not-json").expect("manifest contents");

    let error = resolve_config(cli_request(
        None,
        Some(manifest.path().to_string_lossy().into_owned()),
    ))
    .expect_err("malformed manifest");

    assert_eq!(error.kind(), ConfigErrorKind::Manifest);
}

#[test]
fn given_source_table_without_stream_when_resolved_then_source_is_rejected() {
    let manifest = manifest_file();
    let request = ConfigRequest::cli(
        None,
        Some(manifest.path().to_string_lossy().into_owned()),
        vec!["analytics.source.tables=[{\"table_name\":\"users\"}]".to_string()],
        backend(),
    );

    let error = resolve_config(request).expect_err("missing source stream");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidSourceTableStreamType);
}

#[test]
fn given_lambda_manifest_env_when_config_file_also_has_manifest_then_env_wins() {
    let manifest = manifest_file();
    let env_path = manifest.path().to_string_lossy().into_owned();
    let root = RootConfig {
        analytics: AnalyticsConfig {
            manifest_path: Some("missing.json".to_string()),
            ..AnalyticsConfig::default()
        },
        ..RootConfig::default()
    };
    let config = tempfile::NamedTempFile::new().expect("config file");
    std::fs::write(
        config.path(),
        serde_json::to_vec(&root).expect("config json"),
    )
    .expect("config contents");
    let config_path = config.path().to_string_lossy().into_owned();
    let environment = LambdaEnvironment::from_lookup(|key| match key {
        ENV_CONFIG_PATH => Some(config_path.clone()),
        ENV_MANIFEST_PATH => Some(env_path.clone()),
        ENV_DUCKDB => Some(":memory:".to_string()),
        _ => None,
    });
    let request = ConfigRequest::lambda(environment);

    let resolved = resolve_config(request).expect("resolved config");

    assert_eq!(resolved.manifest.source, ManifestSource::ExplicitArgument);
}

#[test]
fn given_lambda_config_with_scheme_separator_when_resolved_then_ducklake_uri_is_normalized() {
    let manifest = manifest_file();
    let root = RootConfig {
        analytics: AnalyticsConfig {
            catalog: AnalyticsCatalogConfig {
                backend: Some(AnalyticsCatalogBackend::DucklakeSqlite),
                connection_string: Some("metadata.duckdb".to_string()),
                ..AnalyticsCatalogConfig::default()
            },
            object_storage: AnalyticsObjectStorageConfig {
                scheme: "s3://".to_string(),
                bucket: Some("analytics-bucket".to_string()),
                path: Some("warehouse".to_string()),
                ..AnalyticsObjectStorageConfig::default()
            },
            ..AnalyticsConfig::default()
        },
        ..RootConfig::default()
    };
    let config = tempfile::NamedTempFile::new().expect("config file");
    std::fs::write(
        config.path(),
        serde_json::to_vec(&root).expect("config json"),
    )
    .expect("config contents");
    let config_path = config.path().to_string_lossy().into_owned();
    let manifest_path = manifest.path().to_string_lossy().into_owned();
    let environment = LambdaEnvironment::from_lookup(|key| match key {
        ENV_CONFIG_PATH => Some(config_path.clone()),
        ENV_MANIFEST_PATH => Some(manifest_path.clone()),
        _ => None,
    });

    let resolved = resolve_config(ConfigRequest::lambda(environment)).expect("resolved config");
    let crate::StorageBackend::DuckLake { data_path, .. } =
        resolved.storage_backend.expect("DuckLake backend")
    else {
        panic!("expected DuckLake backend");
    };

    assert_eq!(data_path, "s3://analytics-bucket/warehouse");
}

#[test]
fn given_lambda_query_only_env_when_resolved_then_ingest_route_is_disabled() {
    let manifest = manifest_file();
    let environment = LambdaEnvironment::from_lookup(|key| match key {
        ENV_MANIFEST_PATH => Some(manifest.path().to_string_lossy().into_owned()),
        ENV_DUCKDB => Some(":memory:".to_string()),
        ENV_QUERY_LAMBDA_QUERY_ONLY => Some("true".to_string()),
        _ => None,
    });

    let resolved = resolve_config(ConfigRequest::lambda(environment)).expect("config");

    assert!(!resolved.routes.ingest_enabled);
    assert!(resolved.routes.query_enabled);
}

#[test]
fn given_lambda_empty_manifest_env_when_resolved_then_manifest_is_rejected() {
    let environment = LambdaEnvironment::from_lookup(|key| match key {
        ENV_MANIFEST_PATH => Some(String::new()),
        ENV_DUCKDB => Some(":memory:".to_string()),
        _ => None,
    });

    let error = resolve_config(ConfigRequest::lambda(environment)).expect_err("empty manifest");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidManifestPath);
}

#[test]
fn given_lambda_empty_backend_env_when_resolved_then_backend_is_rejected() {
    let manifest = manifest_file();
    let environment = LambdaEnvironment::from_lookup(|key| match key {
        ENV_MANIFEST_PATH => Some(manifest.path().to_string_lossy().into_owned()),
        ENV_DUCKDB => Some(String::new()),
        _ => None,
    });

    let error = resolve_config(ConfigRequest::lambda(environment)).expect_err("empty backend");

    assert_eq!(error.kind(), ConfigErrorKind::MissingBackend);
}

#[test]
fn given_empty_lambda_object_storage_env_when_resolved_then_value_is_rejected() {
    let manifest = manifest_file();
    for env_name in [ENV_S3_BUCKET, ENV_S3_PREFIX, ENV_OBJECT_REGION] {
        let manifest_path = manifest.path().to_string_lossy().into_owned();
        let environment = LambdaEnvironment::from_lookup(|key| match key {
            ENV_MANIFEST_PATH => Some(manifest_path.clone()),
            ENV_DUCKDB => Some(":memory:".to_string()),
            key if key == env_name => Some(String::new()),
            _ => None,
        });

        let error = resolve_config(ConfigRequest::lambda(environment))
            .expect_err("empty object storage env");

        assert_eq!(error.kind(), ConfigErrorKind::Argument);
        assert!(error.to_string().contains(env_name));
    }
}

#[test]
fn given_invalid_lambda_env_when_resolved_then_variable_name_is_reported() {
    let environment = LambdaEnvironment::from_lookup(|key| match key {
        ENV_MANIFEST_PATH => Some("manifest.json".to_string()),
        ENV_DUCKDB => Some(":memory:".to_string()),
        ENV_QUERY_MAX_RESPONSE_SIZE_KB => Some("many".to_string()),
        _ => None,
    });

    let error = resolve_config(ConfigRequest::lambda(environment)).expect_err("invalid env");

    assert!(error.to_string().contains(ENV_QUERY_MAX_RESPONSE_SIZE_KB));
}

#[test]
fn given_zero_retention_schedule_bound_when_resolved_then_routes_are_not_enabled() {
    let manifest = manifest_file();
    let path = manifest.path().to_string_lossy().into_owned();
    for field in ["sweep_interval_ms", "delete_batch_size"] {
        let overrides = vec![
            "analytics.retention.enabled=true".to_string(),
            format!("analytics.retention.{field}=0"),
        ];
        let request = ConfigRequest::cli(None, Some(path.clone()), overrides, backend());

        let error = resolve_config(request).expect_err("invalid retention");

        assert_eq!(error.kind(), ConfigErrorKind::InvalidRetentionConfig);
    }
}

#[test]
fn given_retention_delete_batch_size_within_bounds_when_api_config_resolves_then_value_is_preserved()
 {
    for batch_size in [1, MAX_RETENTION_DELETE_BATCH_SIZE] {
        let resolved = resolve_config(retention_batch_request(batch_size))
            .expect("bounded retention batch should resolve");

        assert_eq!(
            resolved.root.analytics.retention.delete_batch_size,
            batch_size
        );
    }
}

#[test]
fn given_retention_delete_batch_size_over_bound_when_api_config_resolves_then_startup_rejects_safely()
 {
    let invalid_batch_size = MAX_RETENTION_DELETE_BATCH_SIZE + 1;
    let error = resolve_config(retention_batch_request(invalid_batch_size))
        .expect_err("over-bound retention batch should fail startup resolution");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidRetentionConfig);
    assert!(error.to_string().contains("at most 10000"));
    assert!(
        !error
            .to_string()
            .contains(invalid_batch_size.to_string().as_str())
    );
}

fn retention_batch_request(batch_size: u64) -> ConfigRequest {
    ConfigRequest::api(
        None,
        None,
        vec![
            "analytics.retention.enabled=true".to_string(),
            format!("analytics.retention.delete_batch_size={batch_size}"),
        ],
        backend(),
    )
}

#[test]
fn given_oversized_retention_query_limit_when_resolved_then_limit_is_rejected() {
    let manifest = manifest_file();
    let path = manifest.path().to_string_lossy().into_owned();
    let overrides = vec![
        "analytics.retention.enabled=true".to_string(),
        retention_override(Some(10_000)),
    ];
    let request = ConfigRequest::cli(None, Some(path), overrides, backend());

    let error = resolve_config(request).expect_err("invalid query limit");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidRetentionConfig);
}

#[test]
fn given_omitted_retention_query_limit_when_resolved_then_limit_is_rejected() {
    let manifest = manifest_file();
    let path = manifest.path().to_string_lossy().into_owned();
    let overrides = vec![
        "analytics.retention.enabled=true".to_string(),
        retention_override(None),
    ];

    let error = resolve_config(ConfigRequest::cli(None, Some(path), overrides, backend()))
        .expect_err("omitted query limit");

    assert_eq!(error.kind(), ConfigErrorKind::Json);
}

#[test]
fn given_zero_retention_query_limit_when_resolved_then_limit_is_rejected() {
    let manifest = manifest_file();
    let path = manifest.path().to_string_lossy().into_owned();
    let overrides = vec![
        "analytics.retention.enabled=true".to_string(),
        retention_override(Some(0)),
    ];

    let error = resolve_config(ConfigRequest::cli(None, Some(path), overrides, backend()))
        .expect_err("zero query limit");

    assert_eq!(error.kind(), ConfigErrorKind::InvalidRetentionConfig);
}

#[test]
fn given_explicit_retention_query_limit_one_when_resolved_then_config_is_accepted() {
    let manifest = manifest_file();
    let path = manifest.path().to_string_lossy().into_owned();
    let overrides = vec![
        "analytics.retention.enabled=true".to_string(),
        retention_override(Some(1)),
    ];

    let resolved = resolve_config(ConfigRequest::cli(None, Some(path), overrides, backend()))
        .expect("limit one is valid");

    assert!(resolved.routes.ingest_enabled);
}

#[test]
fn given_missing_manifest_file_when_resolved_then_path_error_is_retained() {
    let request = cli_request(None, Some("/tmp/config-runtime-missing.json".to_string()));

    let error = resolve_config(request).expect_err("missing manifest file");

    assert_eq!(error.kind(), ConfigErrorKind::Manifest);
    assert!(std::error::Error::source(&error).is_some());
}

#[test]
fn given_manifest_only_cli_request_when_resolved_then_backend_is_not_required() {
    let manifest = manifest_file();
    let request = ConfigRequest::cli_manifest_only(
        None,
        Some(manifest.path().to_string_lossy().into_owned()),
        Vec::new(),
    );

    let resolved = resolve_config(request).expect("manifest-only config");

    assert!(resolved.storage_backend.is_none());
}
