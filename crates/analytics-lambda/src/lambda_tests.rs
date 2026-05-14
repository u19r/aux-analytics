use analytics_contract::{QueryExpression, QueryPredicate, QuerySelect, StructuredQuery};
use analytics_fixtures::{user_item, users_manifest};
use config::{AnalyticsCatalogBackend, RootConfig};

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
                select: vec![QuerySelect::Column {
                    column_name: "email".to_string(),
                    alias: None,
                }],
                filters: vec![QueryPredicate::Eq {
                    expression: QueryExpression::Column {
                        column_name: "org_id".to_string(),
                    },
                    value: json!("org-a"),
                }],
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: Some(1),
            }
        }))
        .await
        .expect("structured query");

    assert_eq!(query_response["rows"][0]["email"], "structured@example.com");
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
                select: vec![QuerySelect::Column {
                    column_name: "email".to_string(),
                    alias: None,
                }],
                filters: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: None,
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
                select: vec![QuerySelect::Column {
                    column_name: "email".to_string(),
                    alias: None,
                }],
                filters: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: None,
            }
        }))
        .await
        .expect("other tenant query");

    let rows = query_response["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 0);
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
fn given_duckdb_catalog_config_when_lambda_backend_is_resolved_then_embedded_duckdb_is_used() {
    let mut root = RootConfig::default();
    root.analytics.catalog.backend = Some(AnalyticsCatalogBackend::Duckdb);
    root.analytics.catalog.connection_string = Some(":memory:".to_string());

    let backend = resolve_storage_backend_from_config(&root).expect("backend");

    assert!(matches!(backend, StorageBackend::DuckDb { path } if path == ":memory:"));
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
