use std::sync::Arc;

use analytics_contract::{
    PrivacyPolicy, QueryExpression, QueryPredicate, QuerySelect, StructuredQuery,
};
use analytics_engine::{AnalyticsEngine, AnalyticsEngineError};
use analytics_fixtures::{storage_key, user_item, users_manifest};
use analytics_operations::{
    MaintenanceRequest, OperationActor, OperationEventKind, OperationId, OperationKind,
    OperationPhase, OperationRequest, OperationStatus, OperationStore, OperationTransition,
    RateLimitPolicy, TrimRequest, TrimTarget,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt as _;
use metrics_exporter_prometheus::PrometheusBuilder;
use serde_json::json;
use tower::ServiceExt as _;

use crate::{
    AppState, EndpointConfig, MetricsEndpointConfig, constants::MAX_TENANT_QUERY_BATCH_ITEMS,
    router::query_error_response, server_router, server_router_with_config,
};

#[tokio::test]
async fn given_tenant_query_endpoint_when_requested_then_rows_after_minimal_ingest_returned() {
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
async fn given_table_registration_when_submitted_then_active_manifest_is_updated() {
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
async fn given_table_batch_when_registered_then_all_tables_enter_active_manifest() {
    let router = test_router();
    let mut first = users_manifest().tables[0].clone();
    first.analytics_table_name = "registered_users_first".to_string();
    let mut second = first.clone();
    second.analytics_table_name = "registered_users_second".to_string();

    let response = post_json(router.clone(), "/tables/batch", json!([first, second])).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body.as_array().map(Vec::len), Some(2));

    let response = get(router, "/manifest").await;
    let body = response_json(response).await;
    let tables = body["tables"].as_array().expect("manifest tables");
    for expected in ["registered_users_first", "registered_users_second"] {
        assert!(
            tables
                .iter()
                .any(|table| table["analytics_table_name"] == expected),
            "registered table {expected} missing from manifest: {tables:?}"
        );
    }
}

#[tokio::test]
async fn given_two_table_registrations_when_submitted_concurrently_then_both_remain_in_the_active_manifest()
 {
    let router = test_router();
    let mut first = users_manifest().tables[0].clone();
    first.analytics_table_name = "concurrent_first".to_owned();
    let mut second = first.clone();
    second.analytics_table_name = "concurrent_second".to_owned();
    let (first_response, second_response) = tokio::join!(
        post_json(router.clone(), "/tables", json!(first)),
        post_json(router.clone(), "/tables", json!(second)),
    );
    assert_eq!(first_response.status(), StatusCode::OK);
    assert_eq!(second_response.status(), StatusCode::OK);
    let manifest = response_json(get(router, "/manifest").await).await;
    let tables = manifest["tables"].as_array().unwrap();
    for name in ["users", "concurrent_first", "concurrent_second"] {
        assert_eq!(
            tables
                .iter()
                .filter(|table| table["analytics_table_name"] == name)
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn given_duplicate_projection_in_candidate_when_registration_is_rejected_then_manifest_and_existing_rows_are_preserved()
 {
    let (router, state) = test_router_and_state();
    let inserted = post_json(router.clone(), "/ingest/users", json!({
        "record_key": "preserved", "record": {"Keys": {}, "SequenceNumber": "1", "NewImage": {"profile": {"M": {"email": {"S": "preserved@example.com"}}}}}
    })).await;
    assert_eq!(inserted.status(), StatusCode::OK);
    let before = state.manifest.read().await.clone();
    let mut table = before.tables[0].clone();
    table.analytics_table_name = "rejected_candidate".to_owned();
    let projections = table.projection_columns.as_mut().unwrap();
    let duplicate = projections[0].clone();
    projections.push(duplicate);
    let response = post_json(router.clone(), "/tables", json!(table)).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("duplicate")),
        "{body:?}"
    );
    let after = state.manifest.read().await.clone();
    assert!(Arc::ptr_eq(&before, &after));
    assert_eq!(
        response_json(get(router.clone(), "/manifest").await).await,
        json!(before)
    );
    assert_query_email(router, "preserved@example.com").await;
}

#[tokio::test]
async fn given_tables_batch_endpoint_when_requested_then_an_empty_batch_rejected() {
    let response = post_json(test_router(), "/tables/batch", json!([])).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["error"], "at least one table registration is required");
}

#[tokio::test]
async fn given_missing_required_tenant_when_ingested_then_record_contract_code_is_returned() {
    let router = test_router();
    let mut table = users_manifest().tables[0].clone();
    table.analytics_table_name = "required_tenant_users".to_string();
    table.tenant_id = None;
    table.tenant_selector = analytics_contract::TenantSelector::Attribute {
        attribute_name: "tenant_id".to_string(),
    };
    let response = post_json(router.clone(), "/tables", json!(table)).await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post_json(
        router,
        "/ingest/required_tenant_users",
        json!({
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": {"profile": {"M": {"email": {"S": "a@example.com"}}}}
            }
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["code"], "record_contract");
    assert!(
        body["error"].as_str().is_some_and(|error| {
            error.contains("record is missing required attribute tenant_id")
        })
    );
}

#[tokio::test]
async fn given_operation_when_observed_and_cancelled_then_status_and_audit_are_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let operation_id = OperationId::try_new("op_api_1").expect("operation id");
    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    let actor = OperationActor::try_new("analytics-http-api").expect("actor");
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: OperationKind::Backfill,
            actor: actor.clone(),
            target_tables: vec!["users".to_string()],
            dry_run: true,
            rate_limit: RateLimitPolicy::default(),
            payload: json!({"mode": "test"}),
        })
        .expect("operation");
    store
        .prepare_for_execution(&operation_id, &actor)
        .expect("prepare operation");
    drop(store);

    let router = test_router_with_operation_store(operation_store_path.clone());

    let response = get(router.clone(), "/operations").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["operations"][0]["operation_id"], "op_api_1");

    let response = get(router.clone(), "/operations/op_api_1").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "running");

    let response = get(router.clone(), "/operations/op_api_1/audit").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["events"][0]["kind"], "submitted");

    let response = router
        .clone()
        .oneshot(
            Request::post("/operations/op_api_1/cancel")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["cancellation_requested"], true);

    let response = get(router, "/operations/op_api_1").await;
    let body = response_json(response).await;
    assert_eq!(body["status"], "cancelling");
}

#[tokio::test]
async fn given_canonical_operation_when_submitted_twice_then_identity_is_stable() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let router = test_router_with_operation_store(operation_store_path.clone());
    let request = MaintenanceRequest::trim(TrimRequest {
        operation_id: OperationId::try_new("api-trim-1").expect("operation id"),
        table_name: "users".to_string(),
        dry_run: true,
        target: TrimTarget::HorizonTrim {
            older_than_ms: u64::MAX,
        },
        confirmation_token: None,
    });

    let response = post_operation(router.clone(), serde_json::to_value(&request).unwrap()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["operation_id"], "api-trim-1");
    assert_eq!(body["outcome"], "created");

    let response = post_operation(router.clone(), serde_json::to_value(&request).unwrap()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_json(response).await["outcome"], "existing");

    let response = get(router.clone(), "/operations/api-trim-1").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_json(response).await["status"], "submitted");

    let conflicting = MaintenanceRequest::trim(TrimRequest {
        operation_id: OperationId::try_new("api-trim-1").expect("operation id"),
        table_name: "orders".to_string(),
        dry_run: true,
        target: TrimTarget::HorizonTrim {
            older_than_ms: u64::MAX,
        },
        confirmation_token: None,
    });
    let response = post_operation(router.clone(), serde_json::to_value(conflicting).unwrap()).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = post_operation(router.clone(), serde_json::to_value(&request).unwrap()).await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post_operation(router.clone(), json!({})).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let zero_horizon = MaintenanceRequest::trim(TrimRequest {
        operation_id: OperationId::try_new("api-trim-zero").expect("operation id"),
        table_name: "users".to_string(),
        dry_run: true,
        target: TrimTarget::HorizonTrim { older_than_ms: 0 },
        confirmation_token: None,
    });
    let response = post_operation(router, serde_json::to_value(zero_horizon).unwrap()).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    assert_eq!(store.list_operations().expect("operations").len(), 1);
}

#[tokio::test]
async fn given_operation_request_when_submitted_then_service_identity_is_used() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let router = test_router_with_operation_store(operation_store_path.clone());
    let request = MaintenanceRequest::trim(TrimRequest {
        operation_id: OperationId::try_new("api-service-owned").expect("operation id"),
        table_name: "users".to_string(),
        dry_run: true,
        target: TrimTarget::HorizonTrim {
            older_than_ms: u64::MAX,
        },
        confirmation_token: None,
    });
    let body = serde_json::to_string(&request).expect("maintenance request JSON");
    let response = router
        .clone()
        .oneshot(
            Request::post("/operations")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_json(response).await["outcome"], "created");

    let response = get(router.clone(), "/operations/api-service-owned").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_json(response).await["actor"], "analytics-http-api");

    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    let actor = OperationActor::try_new("analytics-http-api").expect("actor");
    store
        .prepare_for_execution(
            &OperationId::try_new("api-service-owned").expect("operation id"),
            &actor,
        )
        .expect("prepare operation");
    drop(store);

    let response = router
        .oneshot(
            Request::post("/operations/api-service-owned/cancel")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_json(response).await["cancellation_requested"],
        true
    );
}

#[tokio::test]
async fn given_unsupported_maintenance_request_when_submitted_over_api_then_nothing_is_persisted() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let router = test_router_with_operation_store(operation_store_path.clone());
    let source = tempdir.path().join("source").to_string_lossy().into_owned();
    let clean = tempdir.path().join("clean").to_string_lossy().into_owned();
    let quarantine = tempdir
        .path()
        .join("quarantine")
        .to_string_lossy()
        .into_owned();

    let raw_backup = json!({
        "kind": "raw_backup",
        "request": {
            "operation_id": "api-raw-backup-unfenced",
            "object_id": "object-1",
            "source_identity": "source-users",
            "privacy_policy_version": null,
            "policy_dropped_records": 0,
            "layout": {
                "schema_version": 1,
                "compression": "none",
                "checksum_algorithm": "fnv1a64",
                "encryption": {"type": "none"}
            },
            "compacted_from_object_ids": [],
            "records": [{
                "table_name": "users",
                "record_key": "user-1",
                "source_position": null,
                "record": {"Keys": {}, "SequenceNumber": "1"}
            }]
        }
    });
    let response = post_operation(router.clone(), raw_backup).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("schema validation")),
        "body={body}"
    );

    for (operation_id, dry_run) in [("api-privacy-dry-run", true), ("api-privacy-apply", false)] {
        let privacy_fix = json!({
            "kind": "privacy_fix",
            "request": {
                "operation_id": operation_id,
                "source_backup_prefix": source.clone(),
                "clean_target_prefix": clean.clone(),
                "quarantine_target_prefix": quarantine.clone(),
                "raw_backup_object_ids": ["object-1"],
                "tables": ["users"],
                "policy": {"version": "privacy-v1"},
                "dry_run": dry_run,
                "destructive_action": "none",
                "confirmation_token": null
            }
        });
        let response = post_operation(router.clone(), privacy_fix).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains("schema validation")),
            "body={body}"
        );
    }

    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    assert!(store.list_operations().expect("operations").is_empty());
}

#[tokio::test]
async fn given_operations_submission_when_requested_then_unknown_fields_without_persisting_rejected()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let router = test_router_with_operation_store(operation_store_path.clone());
    let request = MaintenanceRequest::trim(TrimRequest {
        operation_id: OperationId::try_new("api-trim-unknown").expect("operation id"),
        table_name: "users".to_string(),
        dry_run: true,
        target: TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        confirmation_token: None,
    });

    let mut top_level = serde_json::to_value(&request).expect("maintenance request JSON");
    top_level["legacy_mode"] = json!(true);
    let response = post_operation(router.clone(), top_level).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let mut nested = serde_json::to_value(request).expect("maintenance request JSON");
    nested["request"]["target"]["legacy_mode"] = json!(true);
    let response = post_operation(router.clone(), nested).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let privacy_policy = json!({
        "kind": "privacy_fix",
        "request": {
            "operation_id": "api-privacy-unknown",
            "source_backup_prefix": "source",
            "clean_target_prefix": "clean",
            "quarantine_target_prefix": "quarantine",
            "raw_backup_object_ids": ["object-1"],
            "tables": ["users"],
            "policy": {
                "version": "privacy-v1",
                "legacy_mode": true
            },
            "dry_run": true,
            "destructive_action": "none",
            "confirmation_token": null
        }
    });
    let response = post_operation(router.clone(), privacy_policy).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let raw_backup_record = json!({
        "kind": "raw_backup",
        "request": {
            "operation_id": "api-record-unknown",
            "object_id": "object-1",
            "source_identity": "source-users",
            "privacy_policy_version": null,
            "policy_dropped_records": 0,
            "layout": {
                "schema_version": 1,
                "compression": "none",
                "checksum_algorithm": "fnv1a64",
                "encryption": {"type": "none"}
            },
            "compacted_from_object_ids": [],
            "records": [{
                "table_name": "users",
                "record_key": "user-1",
                "source_position": null,
                "record": {
                    "Keys": {},
                    "SequenceNumber": "1",
                    "legacy_mode": true
                }
            }]
        }
    });
    let response = post_operation(router.clone(), raw_backup_record).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let raw_backup_position = json!({
        "kind": "raw_backup",
        "request": {
            "operation_id": "api-position-unknown",
            "object_id": "object-1",
            "source_identity": "source-users",
            "privacy_policy_version": null,
            "policy_dropped_records": 0,
            "layout": {
                "schema_version": 1,
                "compression": "none",
                "checksum_algorithm": "fnv1a64",
                "encryption": {"type": "none"}
            },
            "compacted_from_object_ids": [],
            "records": [{
                "table_name": "users",
                "record_key": "user-1",
                "source_position": {
                    "type": "dynamo_db_stream_sequence",
                    "stream_arn": "stream",
                    "shard_id": "shard-1",
                    "sequence_number": "1",
                    "legacy_mode": true
                },
                "record": {
                    "Keys": {},
                    "SequenceNumber": "1"
                }
            }]
        }
    });
    let response = post_operation(router, raw_backup_position).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    assert!(store.list_operations().expect("operations").is_empty());
}

#[tokio::test]
async fn given_operations_when_diagnostics_requested_then_summary_omits_payloads() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let operation_store_path = tempdir.path().join("operations.duckdb");
    let operation_id = OperationId::try_new("op_diag_1").expect("operation id");
    let store = OperationStore::connect_duckdb(operation_store_path.as_path()).expect("store");
    store
        .create_operation(&OperationRequest {
            operation_id,
            kind: OperationKind::Backfill,
            actor: OperationActor::try_new("operator").expect("actor"),
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

    assert_eq!(
        body["operations"],
        json!({
            "configured": true, "reachable": true, "operation_count": 1,
            "running_operations": 0, "degraded_operations": 0,
            "oldest_running_updated_at_ms": null,
        })
    );
    assert!(!body.to_string().contains("not-for-diagnostics"));
    assert!(!body.to_string().contains("raw_sql"));
}

#[tokio::test]
async fn given_all_operation_states_when_diagnostics_requested_then_only_running_cancelling_and_failed_counts_are_reported()
 {
    let directory = tempfile::tempdir().expect("operation store directory");
    let path = directory.path().join("operations.duckdb");
    let store = OperationStore::connect_duckdb(&path).expect("operation store");
    for (id, status) in [
        ("op_diag_submitted", OperationStatus::Submitted),
        ("op_diag_planned", OperationStatus::Planned),
        ("op_diag_running_old", OperationStatus::Running),
        ("op_diag_running_new", OperationStatus::Running),
        ("op_diag_cancelling", OperationStatus::Cancelling),
        ("op_diag_cancelled", OperationStatus::Cancelled),
        ("op_diag_paused", OperationStatus::Paused),
        ("op_diag_failed", OperationStatus::Failed),
        ("op_diag_succeeded", OperationStatus::Succeeded),
    ] {
        let request = OperationRequest {
            operation_id: OperationId::try_new(id).expect("operation identity"),
            kind: OperationKind::Backfill,
            actor: OperationActor::try_new("operator").expect("operation actor"),
            target_tables: vec!["users".to_owned()],
            dry_run: true,
            rate_limit: RateLimitPolicy::default(),
            payload: json!({"token": "private-diagnostic-payload"}),
        };
        store.create_operation(&request).expect("submit operation");
        if status == OperationStatus::Submitted {
            continue;
        }
        if status != OperationStatus::Planned {
            store
                .prepare_for_execution(&request.operation_id, &request.actor)
                .expect("start operation");
        }
        match status {
            OperationStatus::Running => {}
            OperationStatus::Cancelling | OperationStatus::Cancelled => {
                store
                    .request_cancellation_as(&request.operation_id, &request.actor)
                    .expect("request cancellation");
                if status == OperationStatus::Cancelled {
                    store
                        .accept_cancellation_as(&request.operation_id, &request.actor)
                        .expect("accept cancellation");
                }
            }
            OperationStatus::Failed => store
                .fail_operation_as(
                    &request.operation_id,
                    &request.actor,
                    OperationPhase::Completed,
                    None,
                )
                .expect("fail operation"),
            OperationStatus::Planned | OperationStatus::Paused | OperationStatus::Succeeded => {
                let (phase, event_kind) = match status {
                    OperationStatus::Planned => {
                        (OperationPhase::Planning, OperationEventKind::Planned)
                    }
                    OperationStatus::Paused => {
                        (OperationPhase::Executing, OperationEventKind::Paused)
                    }
                    _ => (OperationPhase::Completed, OperationEventKind::Succeeded),
                };
                let current = store
                    .show_operation(&request.operation_id)
                    .expect("current operation");
                store
                    .transition_with_counts_as(
                        &current,
                        &request.actor,
                        OperationTransition {
                            phase,
                            status,
                            cursor: None,
                            event_kind,
                            counts: json!({}),
                            message: None,
                        },
                    )
                    .expect("operation transition");
            }
            OperationStatus::Submitted => unreachable!("submitted operation already retained"),
        }
    }
    drop(store);
    let fixture = duckdb::Connection::open(&path).expect("native fixture clock");
    fixture
        .execute(
            "UPDATE __analytics_operations SET updated_at_ms = CASE operation_id
         WHEN 'op_diag_cancelling' THEN ? WHEN 'op_diag_running_old' THEN ?
         WHEN 'op_diag_running_new' THEN ? ELSE ? END",
            [100_i64, 1000, 2000, 0],
        )
        .expect("fixed durable update times");
    drop(fixture);
    let response = get(test_router_with_operation_store(path), "/diagnostics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["operations"],
        json!({
            "configured": true, "reachable": true, "operation_count": 9,
            "running_operations": 3, "degraded_operations": 1,
            "oldest_running_updated_at_ms": 100,
        })
    );
    assert!(!body.to_string().contains("private-diagnostic-payload"));
    assert!(!body.to_string().contains("token"));
}

#[tokio::test]
async fn given_unconfigured_unreadable_or_corrupt_operation_store_when_diagnostics_requested_then_unreachable_summaries_are_exact()
 {
    let directory = tempfile::tempdir().expect("operation store directory");
    let path = directory.path().join("corrupt.duckdb");
    let store = OperationStore::connect_duckdb(&path).expect("operation store");
    store
        .create_operation(&OperationRequest {
            operation_id: OperationId::try_new("corrupt-operation").expect("identity"),
            kind: OperationKind::Backfill,
            actor: OperationActor::try_new("operator").expect("actor"),
            target_tables: vec!["users".to_owned()],
            dry_run: true,
            rate_limit: RateLimitPolicy::default(),
            payload: json!({"token": "private-corrupt-payload"}),
        })
        .expect("persist operation");
    drop(store);
    let fixture = duckdb::Connection::open(&path).expect("corrupt persisted input");
    fixture
        .execute(
            "UPDATE __analytics_operations SET target_tables_json = ?",
            [r#"{"invalid":"private-corrupt-payload"}"#],
        )
        .expect("persist wrong-shaped target tables");
    drop(fixture);
    for (router, configured) in [
        (test_router(), false),
        (
            test_router_with_operation_store(directory.path().to_path_buf()),
            true,
        ),
        (test_router_with_operation_store(path), true),
    ] {
        let response = get(router, "/diagnostics").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(
            body["operations"],
            json!({
                "configured": configured, "reachable": false, "operation_count": 0,
                "running_operations": 0, "degraded_operations": 0,
                "oldest_running_updated_at_ms": null,
            })
        );
        assert!(!body.to_string().contains("private-corrupt-payload"));
        assert!(!body.to_string().contains("invalid"));
    }
}

#[tokio::test]
async fn given_tenant_query_endpoint_when_requested_then_json_rows_after_ingest_returned() {
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
async fn given_tenant_query_batch_endpoint_when_requested_then_named_results_after_ingest_returned()
{
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
async fn given_tenant_query_batch_endpoint_when_requested_then_over_cardinality_before_dispatch_rejected()
 {
    let router = test_router();
    let query = StructuredQuery {
        analytics_table_name: "unregistered_table".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::Count {
            alias: "count".to_string(),
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };
    let queries = (0..=MAX_TENANT_QUERY_BATCH_ITEMS)
        .map(|index| {
            json!({
                "name": format!("query-{index}"),
                "query": query,
            })
        })
        .collect::<Vec<_>>();
    let response = post_json(
        router,
        "/tenant-query-batch",
        json!({
            "target_tenant_id": "tenant_01",
            "queries": queries,
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|message| message.contains("at most 16")),
        "unexpected batch error: {body:?}"
    );
}

#[tokio::test]
async fn given_oversized_query_result_when_requested_then_public_error_code_is_stable() {
    let error = AnalyticsEngineError::QueryResultTooLarge {
        actual_bytes: 9,
        maximum_bytes: 8,
    };

    let body = response_json(query_error_response(StatusCode::BAD_REQUEST, &error)).await;

    assert_eq!(body["code"], "query_result_too_large");
    assert_eq!(
        body["error"],
        "query result exceeds the maximum serialized size of 8 bytes (9 bytes)"
    );
}

#[tokio::test]
async fn given_privacy_policy_when_ingested_then_denied_fields_are_filtered() {
    let router = test_router_with_privacy_policy(
        PrivacyPolicy::try_new("privacy-v1")
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
async fn given_standard_stream_event_when_ingested_then_record_is_accepted() {
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
async fn given_unscoped_sql_query_endpoint_when_requested_then_requests_that_fail_generated_schema_rejected()
 {
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
async fn given_read_only_and_mutating_raw_sql_when_unscoped_endpoint_is_requested_then_both_are_rejected()
 {
    for sql in ["select email from users", "drop table users"] {
        let response = post_json(test_router(), "/unscoped-sql-query", json!({ "sql": sql })).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{sql}");
        let body = response_json(response).await;
        assert!(
            body["error"].as_str().is_some_and(|message| {
                message
                    .contains("Use tenant-query with target_tenant_id for tenant-scoped HTTP reads")
            }),
            "{sql}: {body:?}"
        );
    }
}

#[tokio::test]
async fn given_missing_target_tenant_when_queried_then_request_is_rejected() {
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
async fn given_tenant_query_endpoint_when_requested_then_empty_conditional_branches_in_json_schema_rejected()
 {
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
async fn given_ingest_endpoint_when_requested_then_requests_that_fail_generated_schema_rejected() {
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
async fn given_concurrent_requests_with_delayed_bodies_when_completed_then_validated_ingest_and_schema_rejection_remain_independent()
 {
    use std::{future::Future as _, task::Poll};

    let router = test_router();
    let (mut ingest_sender, ingest_body) = http_body_util::Channel::<bytes::Bytes>::new(2);
    let (mut rejected_sender, rejected_body) = http_body_util::Channel::<bytes::Bytes>::new(2);
    let ingest = serde_json::to_vec(&json!({
        "record_key": "delayed-user", "record": {"Keys": {}, "SequenceNumber": "1",
            "NewImage": {"profile": {"M": {"email": {"S": "delayed@example.com"}}}}}
    }))
    .unwrap();
    let (first_chunk, last_chunk) = ingest.split_at(17);
    ingest_sender
        .send_data(bytes::Bytes::copy_from_slice(first_chunk))
        .await
        .unwrap();
    rejected_sender
        .send_data(bytes::Bytes::from_static(b"{\"statement\":"))
        .await
        .unwrap();
    let mut accepted = std::pin::pin!(
        router.clone().oneshot(
            Request::post("/ingest/users")
                .header("content-type", "application/json")
                .body(Body::new(ingest_body))
                .unwrap()
        )
    );
    let mut rejected = std::pin::pin!(
        router.clone().oneshot(
            Request::post("/unscoped-sql-query")
                .header("content-type", "application/json")
                .body(Body::new(rejected_body))
                .unwrap()
        )
    );
    std::future::poll_fn(|context| {
        assert!(accepted.as_mut().poll(context).is_pending());
        assert!(rejected.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    ingest_sender
        .send_data(bytes::Bytes::copy_from_slice(last_chunk))
        .await
        .unwrap();
    rejected_sender
        .send_data(bytes::Bytes::from_static(b"\"select 1\"}"))
        .await
        .unwrap();
    drop(ingest_sender);
    drop(rejected_sender);
    let (accepted, rejected) = tokio::join!(accepted, rejected);
    assert_eq!(accepted.unwrap().status(), StatusCode::OK);
    let rejected = rejected.unwrap();
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let body = response_json(rejected).await;
    assert_eq!(body["code"], "invalid_request");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("schema validation")
    );
    assert_query_email(router, "delayed@example.com").await;
}

#[tokio::test]
async fn given_invalid_json_request_when_extracted_then_content_type_json_and_size_errors_are_preserved()
 {
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
async fn given_unknown_request_path_when_requested_then_not_found_is_returned_without_validation() {
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

    let unfenced_purge = test_router()
        .oneshot(
            Request::post("/tenant-range-purge")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(unfenced_purge.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn given_record_batch_when_submitted_then_records_are_ingested_in_one_request() {
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
    let query_response = post_json(
        router,
        "/tenant-query",
        json!({
            "target_tenant_id": "tenant_01",
            "query": {
                "analytics_table_name": "users",
                "select": [{"kind": "column", "column_name": "email"}],
                "filters": [],
                "group_by": [],
                "order_by": [{
                    "expression": {"kind": "column", "column_name": "email"},
                    "direction": "asc"
                }],
                "limit": 10
            }
        }),
    )
    .await;
    assert_eq!(query_response.status(), StatusCode::OK);
    let query_body = response_json(query_response).await;
    assert_eq!(
        query_body["rows"],
        json!([
            {"email": "first@example.com"},
            {"email": "second@example.com"}
        ])
    );
}

#[tokio::test]
async fn given_disabled_ingest_route_when_requested_then_route_is_unavailable() {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let router = server_router_with_config(
        Arc::new(AppState::new(
            crate::AnalyticsEngineAccess::shared(engine),
            manifest,
        )),
        MetricsEndpointConfig::default(),
        EndpointConfig {
            ingest_enabled: false,
            ..EndpointConfig::default()
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
async fn given_metrics_endpoint_when_request_has_no_authorization_then_payload_is_returned() {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let handle = PrometheusBuilder::new().build_recorder().handle();
    let router = server_router_with_config(
        Arc::new(AppState::new(
            crate::AnalyticsEngineAccess::shared(engine),
            manifest,
        )),
        MetricsEndpointConfig {
            enabled: true,
            handle: Some(handle),
        },
        EndpointConfig::default(),
    );

    let response = get(router, "/metrics").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4")
    );
}

#[tokio::test]
async fn given_analytics_routes_when_openapi_requested_then_routes_are_documented() {
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
        body["paths"]["/tables/batch"]["post"]["tags"][0],
        "Analytics"
    );
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
    test_router_and_state().0
}

fn test_router_and_state() -> (Router, Arc<AppState>) {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let app_state = Arc::new(AppState::new(
        crate::AnalyticsEngineAccess::shared(engine),
        manifest,
    ));
    (server_router(Arc::clone(&app_state)), app_state)
}

fn test_router_with_privacy_policy(policy: PrivacyPolicy) -> Router {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    server_router(Arc::new(
        AppState::new(crate::AnalyticsEngineAccess::shared(engine), manifest)
            .with_privacy_policy(Some(Arc::new(policy))),
    ))
}

fn test_router_with_operation_store(path: std::path::PathBuf) -> Router {
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    server_router(Arc::new(
        AppState::new(crate::AnalyticsEngineAccess::shared(engine), manifest)
            .with_operation_store_path(path),
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

async fn post_operation(router: Router, body: serde_json::Value) -> axum::response::Response {
    let request = Request::post("/operations").header("content-type", "application/json");
    router
        .oneshot(request.body(Body::from(body.to_string())).expect("request"))
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
fn given_checked_in_openapi_when_regenerated_then_document_matches() {
    let expected = crate::openapi::build_json();
    let actual: serde_json::Value =
        serde_json::from_str(include_str!("../openapi/aux-analytics.openapi.json"))
            .expect("checked-in openapi document");

    assert_eq!(actual, expected);
}

#[test]
fn given_openapi_components_when_generated_then_defaults_examples_and_descriptions_are_present() {
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

    let record_key = &schemas["IngestStreamRecordRequest"]["properties"]["record_key"];
    assert!(record_key.get("default").is_none());
    assert!(record_key["example"].is_string());
    assert!(record_key["description"].is_string());
}

#[test]
fn given_unknown_operation_fields_when_schema_validated_then_request_is_rejected() {
    let schemas = &crate::openapi::build_json()["components"]["schemas"];
    for schema_name in ["TableFixRequest", "TableFixValidation", "TrimRequest"] {
        assert_eq!(
            schemas[schema_name]["additionalProperties"], false,
            "OpenAPI schema {schema_name} must reject unknown fields"
        );
    }
}

#[test]
fn given_operation_submission_when_openapi_generated_then_execution_requirements_are_documented() {
    let operation = &crate::openapi::build_json()["paths"]["/operations"]["post"];
    let description = operation["description"]
        .as_str()
        .expect("operations submission description");
    assert!(description.contains("table-fix"));
    assert!(description.contains("trim"));
    assert!(!description.contains("currently"));
    assert!(!description.contains("raw_backup"));
    assert!(!description.contains("privacy_fix"));

    let request_body = &operation["requestBody"];
    assert!(request_body["description"].as_str().is_some_and(|value| {
        let value = value.to_ascii_lowercase();
        value.contains("table-fix") && value.contains("trim")
    }));
    assert_eq!(
        request_body["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/HttpMaintenanceRequest"
    );

    let bad_request = &operation["responses"]["400"];
    assert_eq!(
        bad_request["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/ErrorResponse"
    );
    assert!(
        bad_request["description"]
            .as_str()
            .is_some_and(|value| value.contains("Invalid maintenance request"))
    );
}

#[tokio::test]
async fn given_privacy_filtering_when_a_batch_is_submitted_then_it_is_rejected_without_writing_rows()
 {
    let router = test_router_with_privacy_policy(
        PrivacyPolicy::try_new("privacy-batch-validation")
            .unwrap()
            .with_denied_key_name("email"),
    );
    let response = post_json(
        router.clone(),
        "/ingest-batch/users",
        json!({
            "records": [{
                "record_key": "user-1",
                "record": {
                    "Keys": storage_key("USER", "user-1"),
                    "SequenceNumber": "1",
                    "NewImage": user_item("user-1", "private@example.test", "org-a"),
                }
            }]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_json(response).await["error"],
        "batch ingestion is only supported when privacy filtering and retention lookup are \
         disabled"
    );
    let response = post_json(
        router,
        "/tenant-query",
        json!({
            "target_tenant_id": "tenant_01",
            "query": {
                "analytics_table_name": "users",
                "select": [{"kind": "column", "column_name": "email"}],
                "filters": [], "group_by": [], "order_by": [], "limit": 10,
            }
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_json(response).await["rows"], json!([]));
}
