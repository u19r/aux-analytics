use std::sync::Arc;

use analytics_contract::{QueryExpression, QueryPredicate, QuerySelect, StructuredQuery};
use analytics_engine::AnalyticsEngine;
use analytics_fixtures::{storage_key, user_item, users_manifest};
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
                select: vec![QuerySelect::Column {
                    column_name: "email".to_string(),
                    alias: None,
                }],
                filters: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: Some(1),
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
                select: vec![QuerySelect::Column {
                    column_name: "email".to_string(),
                    alias: None,
                }],
                filters: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: Some(1),
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
        body["paths"]["/ingest/{analytics_table_name}"]["post"]["tags"][0],
        "Analytics"
    );
    assert!(body["components"]["schemas"]["UnscopedSqlQueryRequest"].is_object());
    assert!(body["components"]["schemas"]["UnscopedStructuredQueryRequest"].is_object());
    assert!(body["components"]["schemas"]["TenantQueryRequest"].is_object());
    assert!(body["components"]["schemas"]["StructuredQuery"].is_object());
    assert!(body["components"]["schemas"]["IngestStreamRecordRequest"].is_object());
    assert!(body["components"]["schemas"]["AnalyticsManifest"].is_object());
}

fn test_router() -> Router {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    server_router(Arc::new(AppState::new(engine, manifest)))
}

async fn assert_query_email(router: Router, expected_email: &str) {
    let response = post_json(
        router,
        "/tenant-query",
        json!({
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
                limit: Some(1),
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
