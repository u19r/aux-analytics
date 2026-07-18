use std::sync::Arc;

use analytics_contract::{
    PrivacyPolicy, QueryExpression, QueryPredicate, QuerySelect, StructuredQuery,
};
use analytics_engine::AnalyticsEngine;
use analytics_fixtures::{storage_key, user_item, users_manifest};
use analytics_operations::{
    OperationActor, OperationId, OperationKind, OperationRequest, OperationStore, RateLimitPolicy,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt as _;
use serde_json::json;
use tower::ServiceExt as _;

use crate::{
    AppState, EndpointConfig, MetricsEndpointConfig, server_router, server_router_with_config,
};

#[tokio::test]
async fn tenant_query_endpoint_returns_rows_after_minimal_ingest() {
    let router = test_router();

    let body = json!({
        "record_key": "user-1",
        "record": {
            "Keys": {},
            "SequenceNumber": "1",
            "NewImage": {
                "profile": {
                    "M": {
                        "email": {
                            "S": "a@example.com"
                        }
                    }
                }
            }
        }
    });
    let response = router
        .clone()
        .oneshot(
            Request::post("/ingest/users")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    assert_query_email(router, "a@example.com").await;
}

#[tokio::test]
async fn tables_endpoint_registers_table_and_updates_active_manifest() {
    let router = test_router();
    let mut table = users_manifest().tables[0].clone();
    table.analytics_table_name = "registered_users".to_string();

    let response = post_json(router.clone(), "/tables", json!(table)).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["analytics_table_name"], "registered_users");

    let response = get(router, "/manifest").await;
    let body = response_json(response).await;
    let tables = body["tables"].as_array().expect("manifest tables");
    assert!(
        tables
            .iter()
            .any(|table| table["analytics_table_name"] == "registered_users"),
        "registered table missing from manifest: {tables:?}"
    );
}

#[tokio::test]
async fn operations_endpoints_return_status_audit_and_accept_cancel() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let operation_id = OperationId::new("op_api_1").expect("operation id");
    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: OperationKind::Backfill,
            actor: OperationActor::new("operator").expect("actor"),
            target_tables: vec!["users".to_string()],
            dry_run: true,
            rate_limit: RateLimitPolicy::default(),
            payload: json!({"mode": "test"}),
        })
        .expect("operation");
    drop(store);

    let router = test_router_with_operation_store(operation_store_path);

    let response = get(router.clone(), "/operations").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["operations"][0]["operation_id"], "op_api_1");

    let response = get(router.clone(), "/operations/op_api_1").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "submitted");

    let response = get(router.clone(), "/operations/op_api_1/audit").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["events"][0]["kind"], "submitted");

    let response = post_json(router.clone(), "/operations/op_api_1/cancel", json!({})).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["cancellation_requested"], true);

    let response = get(router, "/operations/op_api_1").await;
    let body = response_json(response).await;
    assert_eq!(body["status"], "cancelling");
}

#[tokio::test]
async fn diagnostics_endpoint_summarizes_operations_without_payloads() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let operation_id = OperationId::new("op_diag_1").expect("operation id");
    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    store
        .create_operation(&OperationRequest {
            operation_id,
            kind: OperationKind::Backfill,
            actor: OperationActor::new("operator").expect("actor"),
            target_tables: vec!["users".to_string()],
            dry_run: true,
            rate_limit: RateLimitPolicy::default(),
            payload: json!({"raw_sql": "select secret from users", "token": "not-for-diagnostics"}),
        })
        .expect("operation");
    drop(store);

    let response = get(
        test_router_with_operation_store(operation_store_path),
        "/diagnostics",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;

    assert_eq!(body["operations"]["configured"], true);
    assert_eq!(body["operations"]["reachable"], true);
    assert_eq!(body["operations"]["operation_count"], 1);
    assert!(!body.to_string().contains("not-for-diagnostics"));
    assert!(!body.to_string().contains("raw_sql"));
}

#[tokio::test]
async fn tenant_query_endpoint_returns_json_rows_after_ingest() {
    let router = test_router();

    let response = post_json(
        router.clone(),
        "/ingest/users",
        json!({
            "record_key": "user-1",
            "record": {
                "Keys": storage_key("USER", "user-1"),
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", "typed@example.com", "org-a"),
            }
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post_json(
        router.clone(),
        "/tenant-query",
        json!({
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
        }),
    )
    .await;
    let status = response.status();
    let body = response_json(response).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["rows"][0]["email"], "typed@example.com");

    let response = post_json(
        router,
        "/tenant-query",
        json!({
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
                limit: Some(1),
                offset: None,
            }
        }),
    )
    .await;
    let status = response.status();
    let body = response_json(response).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["rows"].as_array().map(Vec::len), Some(0));
}

#[tokio::test]
async fn tenant_query_batch_endpoint_returns_named_results_after_ingest() {
    let router = test_router();

    let response = post_json(
        router.clone(),
        "/ingest/users",
        json!({
            "record_key": "user-1",
            "record": {
                "Keys": storage_key("USER", "user-1"),
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", "typed@example.com", "org-a"),
            }
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post_json(
        router.clone(),
        "/tenant-query-batch",
        json!({
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
                    }
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
                }
            ]
        }),
    )
    .await;
    let status = response.status();
    let body = response_json(response).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["results"][0]["name"], "total_users");
    assert_eq!(body["results"][0]["rows"][0]["count"], 1);
    assert_eq!(body["results"][1]["name"], "matching_users");
    assert_eq!(body["results"][1]["rows"][0]["email"], "typed@example.com");

    let response = post_json(
        router,
        "/tenant-query-batch",
        json!({
            "target_tenant_id": "tenant_02",
            "queries": [{
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
                }
            }]
        }),
    )
    .await;
    let body = response_json(response).await;
    assert_eq!(body["results"][0]["rows"][0]["count"], 0);
}

#[tokio::test]
async fn ingest_endpoint_applies_configured_privacy_policy() {
    let router = test_router_with_privacy_policy(
        PrivacyPolicy::new("privacy-v1")
            .expect("policy")
            .with_denied_key_name("email"),
    );

    let response = post_json(
        router.clone(),
        "/ingest/users",
        json!({
            "record_key": "user-1",
            "record": {
                "Keys": storage_key("USER", "user-1"),
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", "private@example.com", "org-a"),
            }
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["privacy_policy_version"], "privacy-v1");
    assert_eq!(body["privacy_dropped_fields"], 1);

    let response = post_json(
        router,
        "/tenant-query",
        json!({
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
                limit: Some(1),
                offset: None,
            }
        }),
    )
    .await;
    let body = response_json(response).await;
    assert!(body["rows"][0]["email"].is_null(), "{body:?}");
}

#[tokio::test]
async fn ingest_endpoint_accepts_standard_stream_event_record() {
    let router = test_router();

    let body = json!({
        "record": {
            "eventID": "event-1",
            "eventName": "INSERT",
            "dynamodb": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": {
                    "profile": {
                        "M": {
                            "email": {
                                "S": "stream@example.com"
                            }
                        }
                    }
                }
            }
        }
    });
    let response = router
        .clone()
        .oneshot(
            Request::post("/ingest/users")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    assert_query_email(router, "stream@example.com").await;
}

#[tokio::test]
async fn unscoped_sql_query_endpoint_rejects_requests_that_fail_generated_schema() {
    let response = post_json(
        test_router(),
        "/unscoped-sql-query",
        json!({ "statement": "select 1" }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert!(
        body["message"]
            .as_str()
            .or_else(|| body["error"].as_str())
            .is_some_and(|message| message.contains("schema validation")),
        "{body:?}"
    );
}

#[tokio::test]
async fn unscoped_sql_query_endpoint_rejects_read_only_raw_sql() {
    let response = post_json(
        test_router(),
        "/unscoped-sql-query",
        json!({ "sql": "select email from users" }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|message| message.contains("unscoped SQL queries are not available")),
        "{body:?}"
    );
}

#[tokio::test]
async fn unscoped_sql_query_endpoint_rejects_mutating_sql() {
    let response = post_json(
        test_router(),
        "/unscoped-sql-query",
        json!({ "sql": "drop table users" }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|message| message.contains("unscoped SQL queries are not available")),
        "{body:?}"
    );
}

#[tokio::test]
async fn tenant_query_endpoint_requires_target_tenant_id() {
    let response = post_json(
        test_router(),
        "/tenant-query",
        json!({
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
            }
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert!(
        body["message"]
            .as_str()
            .or_else(|| body["error"].as_str())
            .is_some_and(|message| message.contains("schema validation")),
        "{body:?}"
    );
}

#[tokio::test]
async fn tenant_query_endpoint_rejects_empty_conditional_branches_in_json_schema() {
    let response = post_json(
        test_router(),
        "/tenant-query",
        json!({
            "target_tenant_id": "tenant_01",
            "query": {
                "analytics_table_name": "users",
                "select": [{
                    "kind": "expression",
                    "expression": {
                        "kind": "conditional",
                        "branches": [],
                        "else_value": "standard"
                    },
                    "alias": "usage_class"
                }]
            }
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("schema validation")),
        "{body:?}"
    );
}

#[tokio::test]
async fn ingest_endpoint_rejects_requests_that_fail_generated_schema() {
    let response = post_json(
        test_router(),
        "/ingest/users",
        json!({
            "record_key": "user-1",
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert!(
        body["message"]
            .as_str()
            .or_else(|| body["error"].as_str())
            .is_some_and(|message| message.contains("schema validation")),
        "{body:?}"
    );
}

#[tokio::test]
async fn validated_json_preserves_content_type_json_and_size_errors() {
    let missing_content_type = test_router()
        .oneshot(
            Request::post("/ingest/users")
                .body(Body::from("{}"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(missing_content_type.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_json(missing_content_type).await,
        json!({
            "code": "invalid_request",
            "message": "request content-type must be application/json"
        })
    );

    let malformed = test_router()
        .oneshot(
            Request::post("/ingest/users")
                .header("content-type", "application/json")
                .body(Body::from("{"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    let body = response_json(malformed).await;
    assert_eq!(body["code"], "invalid_request");
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.starts_with("request body must be valid JSON:"))
    );

    let oversized = test_router()
        .oneshot(
            Request::post("/ingest/users")
                .header("content-type", "application/json")
                .body(Body::from("x".repeat(2_097_153)))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn unknown_request_path_still_returns_not_found_without_validation() {
    let response = test_router()
        .oneshot(
            Request::post("/unknown")
                .header("content-type", "text/plain")
                .body(Body::from("not-json"))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ingest_batch_endpoint_ingests_records_in_one_request() {
    let router = test_router();
    let response = post_json(
        router.clone(),
        "/ingest-batch/users",
        json!({
            "records": [
                {
                    "record_key": "user-1",
                    "record": {
                        "Keys": {},
                        "SequenceNumber": "1",
                        "NewImage": {
                            "tenant_id": {"S": "tenant_01"},
                            "user_id": {"S": "user-1"},
                            "profile": {"M": {"email": {"S": "first@example.com"}}}
                        }
                    }
                },
                {
                    "record_key": "user-2",
                    "record": {
                        "Keys": {},
                        "SequenceNumber": "2",
                        "NewImage": {
                            "tenant_id": {"S": "tenant_01"},
                            "user_id": {"S": "user-2"},
                            "profile": {"M": {"email": {"S": "second@example.com"}}}
                        }
                    }
                }
            ]
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["record_count"], 2);
    assert_eq!(body["outcomes"][0]["outcome"], "inserted");
    assert_query_email(router, "first@example.com").await;
}

#[tokio::test]
async fn ingest_endpoint_can_be_disabled() {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let router = server_router_with_config(
        Arc::new(AppState::new(engine, manifest)),
        MetricsEndpointConfig::default(),
        EndpointConfig {
            ingest_enabled: false,
        },
    );

    let response = post_json(
        router,
        "/ingest/users",
        json!({
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": {}
            }
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn openapi_endpoint_describes_analytics_routes() {
    let response = test_router()
        .oneshot(
            Request::get("/openapi.json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["paths"]["/unscoped-sql-query"]["post"]["tags"][0],
        "Analytics"
    );
    assert_eq!(
        body["paths"]["/unscoped-structured-query"]["post"]["tags"][0],
        "Analytics"
    );
    assert_eq!(
        body["paths"]["/tenant-query"]["post"]["tags"][0],
        "Analytics"
    );
    assert_eq!(
        body["paths"]["/tenant-query-batch"]["post"]["tags"][0],
        "Analytics"
    );
    assert_eq!(body["paths"]["/tables"]["post"]["tags"][0], "Analytics");
    assert_eq!(
        body["paths"]["/ingest/{analytics_table_name}"]["post"]["tags"][0],
        "Analytics"
    );
    assert_eq!(
        body["paths"]["/ingest-batch/{analytics_table_name}"]["post"]["tags"][0],
        "Analytics"
    );
    assert!(body["components"]["schemas"]["UnscopedSqlQueryRequest"].is_object());
    assert!(body["components"]["schemas"]["UnscopedStructuredQueryRequest"].is_object());
    assert!(body["components"]["schemas"]["TenantQueryRequest"].is_object());
    assert!(body["components"]["schemas"]["TenantQueryBatchRequest"].is_object());
    assert!(body["components"]["schemas"]["QueryBatchResponse"].is_object());
    assert!(body["components"]["schemas"]["StructuredQuery"].is_object());
    assert!(body["components"]["schemas"]["IngestStreamRecordRequest"].is_object());
    assert!(body["components"]["schemas"]["IngestStreamRecordBatchRequest"].is_object());
    assert!(body["components"]["schemas"]["IngestBatchResponse"].is_object());
    assert!(body["components"]["schemas"]["AnalyticsManifest"].is_object());
}

fn test_router() -> Router {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    server_router(Arc::new(AppState::new(engine, manifest)))
}

fn test_router_with_privacy_policy(policy: PrivacyPolicy) -> Router {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    server_router(Arc::new(
        AppState::new(engine, manifest).with_privacy_policy(Some(Arc::new(policy))),
    ))
}

fn test_router_with_operation_store(path: std::path::PathBuf) -> Router {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    server_router(Arc::new(
        AppState::new(engine, manifest).with_operation_store_path(path),
    ))
}

async fn assert_query_email(router: Router, expected_email: &str) {
    let response = post_json(
        router,
        "/tenant-query",
        json!({
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
                limit: Some(1),
                offset: None,
            }
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["rows"][0]["email"], expected_email);
}

async fn post_json(
    router: Router,
    path: &str,
    body: serde_json::Value,
) -> axum::response::Response {
    router
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("response")
}

async fn get(router: Router, path: &str) -> axum::response::Response {
    router
        .oneshot(Request::get(path).body(Body::empty()).expect("request"))
        .await
        .expect("response")
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

#[test]
fn checked_in_openapi_document_matches_utoipa_generation() {
    let expected = crate::openapi::build_json();
    let actual: serde_json::Value =
        serde_json::from_str(include_str!("../openapi/aux-analytics.openapi.json"))
            .expect("checked-in openapi document");

    assert_eq!(actual, expected);
}

#[test]
fn openapi_components_include_defaults_examples_and_descriptions() {
    let spec = crate::openapi::build_json();
    let schemas = &spec["components"]["schemas"];

    let document_column = &schemas["TableRegistration"]["properties"]["document_column"];
    assert_eq!(document_column["default"], "item");
    assert_eq!(document_column["example"], "item");
    assert!(
        document_column["description"]
            .as_str()
            .is_some_and(|value| { value.contains("JSON document column") })
    );

    let filters = &schemas["StructuredQuery"]["properties"]["filters"];
    assert_eq!(filters["default"], serde_json::json!([]));
    assert!(
        filters["example"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
    );
    assert!(
        filters["description"]
            .as_str()
            .is_some_and(|value| { value.contains("filter predicates") })
    );

    for (schema_name, property_name) in [
        ("ProjectionColumn", "column_type"),
        ("StorageStreamRecord", "OldImage"),
        ("StorageStreamRecord", "NewImage"),
        ("StandardStreamStorageRecord", "OldImage"),
        ("StandardStreamStorageRecord", "NewImage"),
        ("TableRegistration", "expression_attribute_values"),
        ("IngestStreamRecordRequest", "record_key"),
    ] {
        let property = &schemas[schema_name]["properties"][property_name];
        assert!(
            property.get("default").is_some(),
            "{schema_name}.{property_name} should include a default"
        );
        assert!(
            property.get("example").is_some(),
            "{schema_name}.{property_name} should include an example"
        );
        assert!(
            property.get("description").is_some(),
            "{schema_name}.{property_name} should include a description"
        );
    }
}
