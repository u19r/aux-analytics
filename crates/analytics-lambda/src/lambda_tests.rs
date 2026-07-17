use std::{collections::BTreeMap, io::Read, sync::Arc};

use analytics_contract::{
    PrivacyPolicy, QueryExpression, QueryPredicate, QuerySelect, StructuredQuery,
};
use analytics_fixtures::{user_item, users_manifest};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use config::{AnalyticsCatalogBackend, RootConfig};
use flate2::read::GzDecoder;

use super::*;

#[tokio::test]
async fn lambda_ingests_and_queries_rows() {
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
            "operation": "unscoped_sql_query",
            "sql": "select email from users",
        }))
        .await
        .expect("query");
    assert_eq!(query_response["rows"][0]["email"], "a@example.com");
}

#[tokio::test]
async fn lambda_ingest_applies_configured_privacy_policy() {
    let handler = test_handler_with_privacy_policy(
        PrivacyPolicy::new("privacy-v1")
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
            "operation": "unscoped_sql_query",
            "sql": "select email from users",
        }))
        .await
        .expect("query");
    assert!(query_response["rows"][0]["email"].is_null());
}

#[tokio::test]
async fn lambda_runs_structured_queries() {
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
async fn lambda_runs_tenant_scoped_queries() {
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
async fn lambda_tenant_query_batch_returns_named_results() {
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
async fn lambda_rejects_query_responses_that_exceed_configured_size() {
    let handler = test_handler_with_response_config(LambdaResponseConfig {
        max_response_size_kb: Some(1),
        response_compression: AnalyticsLambdaResponseCompression::None,
        query_only: false,
        disable_duckdb_interrupt: false,
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
async fn lambda_compresses_query_response_when_raw_json_exceeds_configured_size() {
    let handler = test_handler_with_response_config(LambdaResponseConfig {
        max_response_size_kb: Some(1),
        response_compression: AnalyticsLambdaResponseCompression::GzipBase64,
        query_only: false,
        disable_duckdb_interrupt: false,
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
async fn lambda_query_only_mode_rejects_ingest_operations() {
    let handler = test_handler_with_response_config(LambdaResponseConfig {
        max_response_size_kb: None,
        response_compression: AnalyticsLambdaResponseCompression::None,
        query_only: true,
        disable_duckdb_interrupt: false,
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
async fn lambda_rejects_events_that_fail_generated_schema() {
    let handler = test_handler();

    let err = handler
        .handle_event(json!({
            "operation": "unscoped_sql_query",
            "statement": "select 1",
        }))
        .await
        .expect_err("invalid event");

    assert!(err.to_string().contains("schema validation"));
}

#[tokio::test]
async fn lambda_ingests_batches() {
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
    AnalyticsLambdaHandler::new(
        users_manifest(),
        &StorageBackend::DuckDb {
            path: ":memory:".to_string(),
        },
    )
    .expect("handler")
}

fn test_handler_with_privacy_policy(policy: PrivacyPolicy) -> AnalyticsLambdaHandler {
    AnalyticsLambdaHandler::new_with_privacy_policy(
        users_manifest(),
        &StorageBackend::DuckDb {
            path: ":memory:".to_string(),
        },
        Some(Arc::new(policy)),
    )
    .expect("handler")
}

fn test_handler_with_response_config(
    response_config: LambdaResponseConfig,
) -> AnalyticsLambdaHandler {
    AnalyticsLambdaHandler::new_with_privacy_policy_max_read_connections_and_response_config(
        users_manifest(),
        &StorageBackend::DuckDb {
            path: ":memory:".to_string(),
        },
        None,
        config::DEFAULT_QUERY_MAX_READ_CONNECTIONS,
        response_config,
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
fn given_lambda_manifest_path_in_env_when_resolved_then_it_overrides_config_path() {
    let mut root = RootConfig::default();
    root.analytics.manifest_path = Some("config-manifest.json".to_string());

    let path = resolve_manifest_path(Some("env-manifest.json"), &root).expect("manifest path");

    assert_eq!(path, "env-manifest.json");
}

#[test]
fn given_lambda_without_manifest_path_when_resolved_then_manifest_requirement_is_reported() {
    let root = RootConfig::default();

    let error = resolve_manifest_path(None, &root).unwrap_err();

    assert_eq!(
        error.to_string(),
        "validation error: analytics manifest path is required: set AUX_ANALYTICS_MANIFEST or \
         analytics.manifest_path"
    );
}

#[test]
fn given_aws_lambda_env_when_applied_then_query_and_ducklake_config_are_overridden() {
    let mut root = RootConfig::default();
    let env = env_map([
        (ENV_CATALOG_BACKEND, "ducklake_aux_catalog"),
        (ENV_DUCKLAKE_DATA_PATH, "s3://analytics-bucket/warehouse"),
        (ENV_OBJECT_REGION, "us-east-1"),
        (ENV_CATALOG_DUCKDB_THREADS, "1"),
        (ENV_QUERY_MAX_READ_CONNECTIONS, "1"),
        (ENV_QUERY_MAX_RESPONSE_SIZE_KB, "5800"),
        (ENV_QUERY_LAMBDA_RESPONSE_COMPRESSION, "gzip_base64"),
        (ENV_QUERY_LAMBDA_QUERY_ONLY, "true"),
        (ENV_QUERY_DISABLE_DUCKDB_INTERRUPT, "true"),
    ]);

    apply_lambda_env_config(&mut root, |key| env.get(key).cloned()).expect("env config");

    assert_eq!(
        root.analytics.catalog.backend,
        Some(AnalyticsCatalogBackend::DucklakeAuxCatalog)
    );
    assert_eq!(
        root.analytics.object_storage.bucket.as_deref(),
        Some("analytics-bucket")
    );
    assert_eq!(
        root.analytics.object_storage.path.as_deref(),
        Some("warehouse")
    );
    assert_eq!(
        root.analytics.object_storage.region.as_deref(),
        Some("us-east-1")
    );
    assert_eq!(
        root.analytics.object_storage.endpoint_url.as_deref(),
        Some("s3.us-east-1.amazonaws.com")
    );
    assert!(root.analytics.object_storage.credentials.is_none());
    assert_eq!(root.analytics.catalog.duckdb_threads, Some(1));
    assert_eq!(root.analytics.query.max_read_connections, 1);
    assert_eq!(root.analytics.query.max_response_size_kb, Some(5800));
    assert_eq!(
        root.analytics.query.lambda_response_compression,
        AnalyticsLambdaResponseCompression::GzipBase64
    );
    assert!(root.analytics.query.lambda_query_only);
    assert!(root.analytics.query.disable_duckdb_interrupt);
}

#[test]
fn given_direct_ducklake_env_when_backend_is_resolved_then_object_storage_config_is_preserved() {
    let mut root = RootConfig::default();
    root.analytics.object_storage.bucket = Some("analytics-bucket".to_string());
    root.analytics.object_storage.path = Some("warehouse".to_string());
    root.analytics.object_storage.region = Some("us-east-1".to_string());
    root.analytics.object_storage.credentials = Some(config::RemoteCredentialsConfig {
        instance_keys: Some(true),
        ..config::RemoteCredentialsConfig::default()
    });
    let env = env_map([
        (
            ENV_DUCKLAKE_AUX_CATALOG,
            "/tmp/aux-ducklake/metadata.duckdb",
        ),
        (ENV_DUCKLAKE_DATA_PATH, "s3://analytics-bucket/warehouse"),
    ]);

    let backend =
        resolve_storage_backend_from_env_with(&root, |key| env.get(key).cloned()).expect("backend");

    let StorageBackend::DuckLake {
        catalog,
        catalog_path,
        data_path,
        object_storage,
        ..
    } = backend
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
fn given_aux_catalog_env_when_backend_is_resolved_then_aux_catalog_is_used() {
    let mut root = RootConfig::default();
    root.analytics.object_storage.path = Some("warehouse".to_string());
    let env = env_map([
        (
            ENV_DUCKLAKE_AUX_CATALOG,
            "/tmp/aux-ducklake/metadata.duckdb",
        ),
        (ENV_DUCKLAKE_DATA_PATH, "s3://analytics-bucket/warehouse"),
    ]);

    let backend =
        resolve_storage_backend_from_env_with(&root, |key| env.get(key).cloned()).expect("backend");

    let StorageBackend::DuckLake {
        catalog,
        catalog_path,
        data_path,
        ..
    } = backend
    else {
        panic!("expected DuckLake backend");
    };
    assert_eq!(catalog, analytics_engine::CatalogType::AuxCatalog);
    assert_eq!(catalog_path, "/tmp/aux-ducklake/metadata.duckdb");
    assert_eq!(data_path, "s3://analytics-bucket/warehouse");
}

#[test]
fn given_invalid_lambda_query_env_when_applied_then_validation_error_names_variable() {
    let mut root = RootConfig::default();
    let env = env_map([(ENV_QUERY_MAX_RESPONSE_SIZE_KB, "many")]);

    let error = apply_lambda_env_config(&mut root, |key| env.get(key).cloned()).unwrap_err();

    assert!(error.to_string().contains(ENV_QUERY_MAX_RESPONSE_SIZE_KB));
}

#[test]
fn given_duckdb_catalog_config_when_lambda_backend_is_resolved_then_embedded_duckdb_is_used() {
    let mut root = RootConfig::default();
    root.analytics.catalog.backend = Some(AnalyticsCatalogBackend::Duckdb);
    root.analytics.catalog.connection_string = Some(":memory:".to_string());

    let backend = resolve_storage_backend_from_config(&root).expect("backend");

    assert!(matches!(backend, StorageBackend::DuckDb { path } if path == ":memory:"));
}

fn env_map(
    values: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> BTreeMap<String, String> {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn given_ducklake_object_storage_bucket_when_lambda_data_path_is_resolved_then_uri_is_normalized() {
    let mut root = RootConfig::default();
    root.analytics.object_storage.scheme = "s3://".to_string();
    root.analytics.object_storage.bucket = Some("/analytics-bucket/".to_string());
    root.analytics.object_storage.path = Some("/warehouse".to_string());

    let data_path = ducklake_data_path(&root).expect("data path");

    assert_eq!(data_path, "s3://analytics-bucket/warehouse");
}

#[test]
fn given_ducklake_backend_without_object_storage_path_when_resolved_then_path_requirement_is_reported()
 {
    let mut root = RootConfig::default();
    root.analytics.catalog.backend = Some(AnalyticsCatalogBackend::DucklakeSqlite);
    root.analytics.catalog.connection_string = Some("catalog.sqlite".to_string());

    let error = resolve_storage_backend_from_config(&root).unwrap_err();

    assert_eq!(
        error.to_string(),
        "validation error: analytics.object_storage.path is required for DuckLake backends"
    );
}

#[test]
fn given_lambda_ingest_outcome_when_serialized_then_external_status_names_are_stable() {
    assert_eq!(ingest_outcome_name(IngestOutcome::Inserted), "inserted");
    assert_eq!(ingest_outcome_name(IngestOutcome::Updated), "updated");
    assert_eq!(ingest_outcome_name(IngestOutcome::Deleted), "deleted");
    assert_eq!(ingest_outcome_name(IngestOutcome::Skipped), "skipped");
}
