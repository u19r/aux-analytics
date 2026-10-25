use std::time::Duration;

use config::{MetricsConfig, RootConfig, Tracing};
use serde_json::json;

use crate::{
    cli::{ApiCli, BackendArgs},
    runtime_config::{load_privacy_policy, resolve_serve_config},
    server::{
        AppStateStartupInput, CorsOrigins, FilterSource, ProcessDuty, cors_origins,
        metrics_endpoint_config, parse_shutdown_grace_period, resolve_filter,
    },
};

#[test]
fn given_valid_configured_log_filter_when_resolved_then_config_source_is_reported() {
    let tracing = Tracing {
        log_level: Some("analytics_api=debug".to_string()),
    };

    let (_filter, source) = resolve_filter(&tracing);

    assert_eq!(source.to_string(), "config");
}

#[test]
fn given_invalid_configured_log_filter_when_resolved_then_default_source_is_reported() {
    let tracing = Tracing {
        log_level: Some("analytics_api[bad".to_string()),
    };

    let (_filter, source) = resolve_filter(&tracing);

    assert_eq!(source.to_string(), "default");
}

#[test]
fn given_filter_source_when_formatted_then_operator_label_is_stable() {
    assert_eq!(FilterSource::Config.to_string(), "config");
    assert_eq!(FilterSource::Default.to_string(), "default");
}

#[test]
fn given_metrics_disabled_when_config_is_built_then_prometheus_recorder_is_not_installed() {
    let metrics = MetricsConfig { enabled: false };

    let config = metrics_endpoint_config(&metrics).expect("metrics config");

    assert!(!config.enabled);
    assert!(config.handle.is_none());
}

#[test]
fn given_wildcard_cors_origin_when_resolved_then_any_origin_is_allowed() {
    let mut root = RootConfig::default();
    root.http.cors.allow_origins = vec!["https://app.example".to_string(), "*".to_string()];

    assert_eq!(cors_origins(&root), CorsOrigins::Any);
}

#[test]
fn given_invalid_cors_origin_when_resolved_then_origin_is_ignored() {
    let mut root = RootConfig::default();
    root.http.cors.allow_origins = vec![
        "https://app.example".to_string(),
        "https://bad.example\ninjected".to_string(),
    ];

    let CorsOrigins::Exact(origins) = cors_origins(&root) else {
        panic!("valid origin should be retained");
    };
    assert_eq!(origins.len(), 1);
    assert_eq!(origins[0], "https://app.example");
}

#[test]
fn given_no_valid_cors_origins_when_resolved_then_no_origin_policy_is_added() {
    let mut root = RootConfig::default();
    root.http.cors.allow_origins = vec!["https://bad.example\ninjected".to_string()];

    assert_eq!(cors_origins(&root), CorsOrigins::None);
}

#[test]
fn given_no_manifest_path_when_api_server_resolves_manifest_then_empty_startup_is_allowed() {
    let resolved = config::resolve_config(config::ConfigRequest::api(
        None,
        None,
        Vec::new(),
        config::BackendOverride {
            duckdb: Some(":memory:".to_string()),
            ..config::BackendOverride::default()
        },
    ))
    .expect("manifest resolution");

    assert_eq!(resolved.manifest.source, config::ManifestSource::ApiEmpty);
    assert!(resolved.manifest.path.is_none());
    assert!(resolved.routes.ingest_enabled);
}

#[test]
fn given_job_store_endpoint_when_api_server_config_resolves_then_it_is_kept_separate_from_source() {
    let args = ApiCli {
        process_duty: ProcessDuty::Combined,
        manifest: None,
        backend: BackendArgs {
            duckdb: Some(":memory:".to_string()),
            ducklake_sqlite_catalog: None,
            ducklake_aux_catalog: None,
            ducklake_data_path: None,
        },
        config: None,
        port: None,
        overrides: vec![
            "analytics.source.endpoint_url=http://stream.example.test".to_string(),
            "analytics.job_store.endpoint_url=http://jobs.example.test".to_string(),
            "analytics.job_store.request_timeout_ms=1200".to_string(),
        ],
    };

    let resolved = resolve_serve_config(&args).expect("server config");

    assert_eq!(
        resolved.root.analytics.source.endpoint_url.as_deref(),
        Some("http://stream.example.test")
    );
    assert_eq!(
        resolved.root.analytics.job_store.endpoint_url.as_deref(),
        Some("http://jobs.example.test")
    );
    assert_eq!(resolved.root.analytics.job_store.request_timeout_ms, 1_200);
}

#[test]
fn given_over_bound_retention_batch_when_api_runtime_config_resolves_then_startup_rejects_safely() {
    let invalid_batch_size = config::MAX_RETENTION_DELETE_BATCH_SIZE + 1;
    let args = ApiCli {
        process_duty: ProcessDuty::Combined,
        manifest: None,
        backend: BackendArgs {
            duckdb: Some(":memory:".to_string()),
            ducklake_sqlite_catalog: None,
            ducklake_aux_catalog: None,
            ducklake_data_path: None,
        },
        config: None,
        port: None,
        overrides: vec![
            "analytics.retention.enabled=true".to_string(),
            format!("analytics.retention.delete_batch_size={invalid_batch_size}"),
        ],
    };

    let error = resolve_serve_config(&args).expect_err("over-bound batch should fail startup");

    assert_eq!(
        error.kind(),
        config::ConfigErrorKind::InvalidRetentionConfig
    );
    assert!(
        error
            .to_string()
            .contains(format!("at most {}", config::MAX_RETENTION_DELETE_BATCH_SIZE).as_str())
    );
    assert!(
        !error
            .to_string()
            .contains(invalid_batch_size.to_string().as_str())
    );
}

#[test]
fn given_invalid_regex_privacy_policy_when_loaded_then_startup_fails_closed() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let policy_path = tempdir.path().join("privacy.json");
    std::fs::write(
        policy_path.as_path(),
        json!({
            "version": "privacy-v1",
            "denied_key_names": [],
            "denied_key_prefixes": [],
            "denied_key_suffixes": [],
            "denied_exact_paths": [],
            "denied_path_regexes": [],
            "denied_value_regexes": ["["]
        })
        .to_string(),
    )
    .expect("policy file");
    let mut root = RootConfig::default();
    root.analytics.privacy.policy_path = Some(policy_path.to_string_lossy().into_owned());

    let error = load_privacy_policy(&root).expect_err("invalid policy should fail");

    assert!(
        error.to_string().contains("analytics configuration failed")
            || error
                .to_string()
                .contains("privacy policy regex is invalid"),
        "{error}"
    );
}

#[test]
fn given_shutdown_grace_seconds_when_parsed_then_shutdown_delay_uses_configured_duration() {
    assert_eq!(
        parse_shutdown_grace_period(Some("30")),
        Duration::from_secs(30)
    );
}

#[test]
fn given_invalid_shutdown_grace_seconds_when_parsed_then_default_shutdown_delay_is_used() {
    assert_eq!(
        parse_shutdown_grace_period(Some("not-a-number")),
        Duration::from_secs(5)
    );
    assert_eq!(parse_shutdown_grace_period(None), Duration::from_secs(5));
}

#[tokio::test]
async fn given_local_backends_and_privacy_selection_when_started_then_routes_preserve_manifest_health_and_rows()
 {
    use std::sync::Arc;

    use analytics_contract::PrivacyPolicy;
    use analytics_engine::{CatalogType, StorageBackend};
    use analytics_fixtures::{user_item, users_manifest};

    for backend_kind in ["memory", "duckdb", "ducklake"] {
        for privacy_enabled in [false, true] {
            // Files are required only for the actual persistent native
            // backends.
            let directory = tempfile::tempdir().expect("persistent backend fixture");
            let backend = match backend_kind {
                "memory" => StorageBackend::DuckDb {
                    path: ":memory:".into(),
                },
                "duckdb" => StorageBackend::DuckDb {
                    path: directory
                        .path()
                        .join("analytics.duckdb")
                        .to_string_lossy()
                        .into_owned(),
                },
                _ => StorageBackend::DuckLake {
                    catalog: CatalogType::Sqlite,
                    catalog_path: directory
                        .path()
                        .join("catalog.sqlite")
                        .to_string_lossy()
                        .into_owned(),
                    data_path: directory.path().join("data").to_string_lossy().into_owned(),
                    object_storage: None,
                    catalog_settings: Default::default(),
                },
            };
            let mut root = RootConfig::default();
            root.analytics.ingest.processor_enabled = false;
            root.analytics.query.max_read_connections = 1;
            let manifest = users_manifest();
            let expected_manifest = serde_json::to_value(&manifest).expect("manifest JSON");
            let privacy = privacy_enabled.then(|| {
                Arc::new(
                    PrivacyPolicy::try_new("startup-policy")
                        .expect("valid policy")
                        .with_denied_key_name("email"),
                )
            });
            let state = super::server::initialize_app_state(AppStateStartupInput {
                root: &root,
                manifest,
                storage_backend: backend,
                privacy_policy: privacy,
                duty: ProcessDuty::Combined,
            })
            .await
            .expect("startup");
            super::server::start_retention_jobs(&state).await;
            let router = crate::server_router(state);
            assert_eq!(
                startup_response(&router, "/ready", None).await,
                json!({"status": "ready"})
            );
            assert_eq!(
                startup_response(&router, "/manifest", None).await,
                expected_manifest
            );
            let health = startup_response(&router, "/health", None).await;
            assert_eq!(health["source"]["status"], "disabled");
            assert_eq!(health["source"]["poller_enabled"], false);
            assert_eq!(health["source"]["total_records_ingested"], 0);
            assert_eq!(health["retention"]["status"], "disabled");
            let diagnostics = startup_response(&router, "/diagnostics", None).await;
            assert_eq!(diagnostics["operations"]["configured"], false);
            let ingested = startup_response(
                &router,
                "/ingest/users",
                Some(json!({
                    "record_key": "startup-user",
                    "record": {"Keys": {}, "SequenceNumber": "1",
                        "NewImage": user_item("startup-user", "visible@example.test", "org-a")}
                })),
            )
            .await;
            assert_eq!(
                ingested["privacy_policy_version"],
                if privacy_enabled {
                    json!("startup-policy")
                } else {
                    json!(null)
                }
            );
            assert_eq!(
                ingested["privacy_dropped_fields"],
                if privacy_enabled {
                    json!(1)
                } else {
                    json!(null)
                }
            );
            let queried = startup_response(
                &router,
                "/tenant-query",
                Some(json!({
                    "target_tenant_id": "tenant_01",
                    "query": {"analytics_table_name": "users",
                        "select": [{"kind": "column", "column_name": "email"}], "limit": 1}
                })),
            )
            .await;
            assert_eq!(
                queried["rows"],
                if privacy_enabled {
                    json!([{"email": null}])
                } else {
                    json!([{"email": "visible@example.test"}])
                }
            );
        }
    }
}

async fn startup_response(
    router: &axum::Router,
    path: &str,
    body: Option<serde_json::Value>,
) -> serde_json::Value {
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;
    let request = match body {
        Some(body) => Request::post(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("POST request"),
        None => Request::get(path).body(Body::empty()).expect("GET request"),
    };
    let response = router.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    let value = serde_json::from_slice(&bytes).expect("response JSON");
    assert_eq!(status, axum::http::StatusCode::OK, "{path}: {value}");
    value
}

#[tokio::test]
async fn given_retention_and_operation_store_configuration_when_started_then_policy_and_fail_closed_health_are_preserved()
 {
    use analytics_engine::StorageBackend;
    use analytics_fixtures::users_manifest;
    for retention_enabled in [false, true] {
        let directory = tempfile::tempdir().expect("operation store fixture");
        let mut root = RootConfig::default();
        root.analytics.ingest.processor_enabled = false;
        root.analytics.operations.operation_store_path = Some(
            directory
                .path()
                .join("operations.duckdb")
                .to_string_lossy()
                .into_owned(),
        );
        root.analytics.retention = serde_json::from_value(json!({
            "enabled": retention_enabled,
            "sweep_interval_ms": 17000,
            "delete_batch_size": 23,
            "tables": [{"analytics_table_name": "users", "default_period_ms": 9000,
                "strict": true, "timestamp": {"kind": "ingested_at"},
                "tenant_policy": {"source": "aux_storage", "endpoint_url": "http://127.0.0.1:1",
                    "request": {"get_item": {"table_name": "policies", "key": {}}},
                    "duration_selector": {"attribute_path": "period_ms"}}}]
        }))
        .expect("retention config");
        let state = super::server::initialize_app_state(AppStateStartupInput {
            root: &root,
            manifest: users_manifest(),
            storage_backend: StorageBackend::DuckDb {
                path: ":memory:".into(),
            },
            privacy_policy: None,
            duty: ProcessDuty::Combined,
        })
        .await
        .expect("startup");
        assert_eq!(state.retention.is_some(), retention_enabled);
        if let Some(retention) = state.retention.as_ref() {
            assert_eq!(retention.table_names().collect::<Vec<_>>(), ["users"]);
            assert_eq!(retention.sweep_interval_ms(), 17000);
            assert_eq!(retention.delete_batch_size(), 23);
        }
        assert!(
            state.background_jobs.is_none(),
            "unfenced local backend cannot enable jobs"
        );
        super::server::start_retention_jobs(&state).await;
        let router = crate::server_router(state);
        let health = startup_response(&router, "/health", None).await;
        assert_eq!(health["retention"]["status"], "disabled");
        assert_eq!(health["retention"]["sweeper_enabled"], false);
        let diagnostics = startup_response(&router, "/diagnostics", None).await;
        assert_eq!(
            diagnostics["operations"],
            json!({
                "configured": true, "reachable": true, "operation_count": 0,
                "running_operations": 0, "degraded_operations": 0, "oldest_running_updated_at_ms": null
            })
        );
    }
}

#[tokio::test]
async fn given_missing_retention_policy_endpoint_when_enabled_then_startup_rejects_configuration() {
    use analytics_engine::StorageBackend;
    use analytics_fixtures::users_manifest;
    let mut root = RootConfig::default();
    root.analytics.ingest.processor_enabled = false;
    root.analytics.retention = serde_json::from_value(json!({
        "enabled": true,
        "tables": [{"analytics_table_name": "users", "default_period_ms": 9000,
            "timestamp": {"kind": "ingested_at"},
            "tenant_policy": {"source": "aux_storage",
                "request": {"get_item": {"table_name": "policies", "key": {}}},
                "duration_selector": {"attribute_path": "period_ms"}}}]
    }))
    .expect("retention config");
    let result = super::server::initialize_app_state(AppStateStartupInput {
        root: &root,
        manifest: users_manifest(),
        storage_backend: StorageBackend::DuckDb {
            path: ":memory:".into(),
        },
        privacy_policy: None,
        duty: ProcessDuty::Combined,
    })
    .await;
    let error = match result {
        Ok(_) => panic!("missing endpoint must reject startup"),
        Err(error) => error,
    };
    let cause = std::error::Error::source(&error)
        .expect("retained storage cause")
        .downcast_ref::<analytics_storage::AnalyticsStorageError>()
        .expect("typed storage error");
    assert_eq!(cause.category(), "missing_retention_policy_endpoint");
    assert_eq!(
        cause.to_string(),
        "analytics retention tenant_policy.endpoint_url is required"
    );
}

#[tokio::test]
async fn given_enabled_worker_configuration_when_api_only_starts_then_queries_work_without_job_runtime_or_async_operations()
 {
    use analytics_engine::StorageBackend;
    use analytics_fixtures::users_manifest;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt as _;

    let mut root = RootConfig::default();
    root.analytics.ingest.processor_enabled = true;
    root.analytics.job_store.endpoint_url = Some("http://127.0.0.1:1".into());
    root.analytics.query.max_read_connections = 1;
    root.analytics.retention = serde_json::from_value(json!({
        "enabled": true, "sweep_interval_ms": 17000, "delete_batch_size": 23,
        "tables": [{"analytics_table_name": "users", "default_period_ms": 9000,
            "timestamp": {"kind": "ingested_at"}, "tenant_policy": {
                "source": "aux_storage", "endpoint_url": "http://127.0.0.1:1",
                "request": {"get_item": {"table_name": "policies", "key": {}}},
                "duration_selector": {"attribute_path": "period_ms"}}}]
    }))
    .unwrap();
    let state = super::server::initialize_app_state(AppStateStartupInput {
        root: &root,
        manifest: users_manifest(),
        storage_backend: StorageBackend::DuckDb {
            path: ":memory:".into(),
        },
        privacy_policy: None,
        duty: ProcessDuty::ApiOnly,
    })
    .await
    .unwrap();
    assert!(state.background_jobs.is_none());
    assert!(state.retention.is_none());
    let router = crate::server_router_with_config(
        state,
        crate::MetricsEndpointConfig::default(),
        crate::EndpointConfig {
            ingest_enabled: true,
            async_operations_enabled: false,
        },
    );
    assert_eq!(
        startup_response(&router, "/ready", None).await,
        json!({"status": "ready"})
    );
    let queried = startup_response(&router, "/tenant-query", Some(json!({
        "target_tenant_id": "tenant_01", "query": {
            "analytics_table_name": "users", "select": [{"kind": "column", "column_name": "email"}], "limit": 1}
    }))).await;
    assert_eq!(queried["rows"], json!([]));
    let response = router
        .clone()
        .oneshot(Request::post("/operations").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let spec = startup_response(&router, "/openapi.json", None).await;
    assert!(spec["paths"].get("/operations").is_none());
    assert!(spec["paths"].get("/tenant-query").is_some());
}
