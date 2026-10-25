use std::sync::Arc;

use analytics_engine::AnalyticsEngine;
use analytics_fixtures::metric_points_manifest;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tower::ServiceExt as _;

use crate::{AnalyticsEngineAccess, AppState, server_router};

struct PurgeFixture {
    router: Router,
    state: Arc<AppState>,
}

impl PurgeFixture {
    async fn create() -> Self {
        let manifest = metric_points_manifest();
        let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
        engine.ensure_manifest(&manifest).expect("manifest");
        let state = Arc::new(AppState::new(
            AnalyticsEngineAccess::shared(engine),
            manifest,
        ));
        let fixture = Self {
            router: server_router(state.clone()),
            state,
        };
        for (tenant, event, series, timestamp) in [
            ("tenant_01", "purge-1", "requests", 1000),
            ("tenant_01", "purge-2", "requests", 2000),
            ("tenant_02", "purge-3", "requests", 1000),
            ("tenant_01", "purge-4", "other", 1000),
        ] {
            let (status, body) = fixture.post("/ingest/metric_points_v1", json!({
                "record_key": event,
                "record": {"Keys": {}, "SequenceNumber": event, "NewImage": {
                    "tenant_id": {"S": tenant}, "event_id": {"S": event},
                    "series_name": {"S": series}, "occurred_at_ms": {"N": timestamp.to_string()},
                    "value_i64": {"N": "1"}
                }}
            })).await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        fixture
    }

    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::post(path)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).expect("JSON response"),
        )
    }

    async fn rows(&self) -> Vec<Value> {
        self.state
            .engine
            .with_read(|engine| {
                engine.query_unscoped_sql_json(
                    "select tenant_id, series_name, event_id from metric_points_v1 order by \
                     event_id",
                )
            })
            .await
            .expect("engine access")
            .expect("remaining rows")
    }
}

fn claim_descriptor(fence: u64) -> Value {
    json!({
        "version": 1, "namespace": [112, 117, 114, 103, 101],
        "job_id": vec![1u8; 16], "execution_id": vec![2u8; 16],
        "owner": vec![3u8; 16], "token": vec![fence as u8; 16],
        "generation": fence, "fence": fence
    })
}

fn purge_request() -> Value {
    json!({
        "request": {
            "table_name": "metric_points_v1", "tenant_id": "tenant_01",
            "timestamp_column": "occurred_at_ms", "start_ms": 0, "end_ms": 3000,
            "equals": {"series_name": "requests"}, "limit": 1
        },
        "request_id": "range-purge-1", "claim": claim_descriptor(1)
    })
}

#[tokio::test]
async fn given_tenant_rows_when_fenced_purge_replays_and_resumes_then_chunks_and_isolation_hold() {
    let fixture = PurgeFixture::create().await;
    let path = "/maintenance/tenant-range-purge";
    let request = purge_request();
    let claim_path = "/maintenance/tenant-range-purge/claim";
    assert_eq!(
        fixture
            .post(claim_path, json!({"claim": claim_descriptor(1)}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .post(claim_path, json!({"claim": claim_descriptor(1)}))
            .await
            .0,
        StatusCode::OK
    );
    let (status, first) = fixture.post(path, request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["rows_deleted"], 1);
    assert_eq!(first["complete"], false);
    assert!(first["cursor"].is_string());
    let remaining = fixture.rows().await;
    assert_eq!(remaining.len(), 3);
    assert_eq!(
        fixture.post(path, request.clone()).await,
        (StatusCode::OK, first.clone())
    );
    assert_eq!(fixture.rows().await, remaining);

    let mut takeover = request.clone();
    takeover["claim"] = claim_descriptor(2);
    assert_eq!(
        fixture
            .post(claim_path, json!({"claim": claim_descriptor(2)}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .post(claim_path, json!({"claim": claim_descriptor(1)}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        fixture.post(path, takeover.clone()).await,
        (StatusCode::OK, first.clone())
    );
    assert_eq!(
        fixture.post(path, request.clone()).await.0,
        StatusCode::CONFLICT
    );
    let mut conflict = takeover.clone();
    conflict["request"]["end_ms"] = json!(4000);
    assert_eq!(fixture.post(path, conflict).await.0, StatusCode::CONFLICT);
    assert_eq!(fixture.rows().await, remaining);
    assert_eq!(
        fixture.post(path, takeover.clone()).await,
        (StatusCode::OK, first.clone())
    );

    let mut second_request = takeover.clone();
    second_request["request_id"] = json!("range-purge-2");
    second_request["request"]["cursor"] = first["cursor"].clone();
    let (status, second) = fixture.post(path, second_request).await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["rows_deleted"], 1);
    assert_eq!(second["complete"], false);
    let mut final_request = takeover;
    final_request["request_id"] = json!("range-purge-3");
    final_request["request"]["cursor"] = second["cursor"].clone();
    let (status, complete) = fixture.post(path, final_request).await;
    assert_eq!(status, StatusCode::OK, "{complete}");
    assert_eq!(complete["rows_deleted"], 0);
    assert_eq!(complete["complete"], true);
    assert_eq!(
        fixture.rows().await,
        vec![
            json!({"tenant_id": "tenant_02", "series_name": "requests", "event_id": "purge-3"}),
            json!({"tenant_id": "tenant_01", "series_name": "other", "event_id": "purge-4"}),
        ]
    );
}

#[tokio::test]
async fn given_purge_request_without_valid_authority_when_submitted_then_no_rows_are_deleted() {
    let fixture = PurgeFixture::create().await;
    let before = fixture.rows().await;
    for (field, value) in [
        ("request_id", json!("")),
        ("request_id", json!(" ")),
        ("request_id", json!("x".repeat(513))),
        ("claim", json!({"fence": 0})),
    ] {
        let mut request = purge_request();
        request[field] = value;
        assert_eq!(
            fixture
                .post("/maintenance/tenant-range-purge", request)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(fixture.rows().await, before);
    }
    let mut unknown = purge_request();
    unknown["unknown_authority"] = json!(true);
    assert_eq!(
        fixture
            .post("/maintenance/tenant-range-purge", unknown)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut missing = purge_request();
    missing.as_object_mut().expect("object").remove("claim");
    assert_eq!(
        fixture
            .post("/maintenance/tenant-range-purge", missing)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut invalid_predicate = purge_request();
    invalid_predicate["request"]["limit"] = json!(0);
    assert_eq!(
        fixture
            .post("/maintenance/tenant-range-purge", invalid_predicate)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(fixture.rows().await, before);
}

#[tokio::test]
async fn given_registered_replacement_before_any_chunk_when_old_claim_purges_then_rows_remain_and_current_claim_succeeds()
 {
    let fixture = PurgeFixture::create().await;
    let before = fixture.rows().await;
    let claim_path = "/maintenance/tenant-range-purge/claim";
    assert_eq!(
        fixture
            .post(claim_path, json!({"claim": claim_descriptor(1)}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .post(claim_path, json!({"claim": claim_descriptor(2)}))
            .await
            .0,
        StatusCode::OK
    );
    let mut request = purge_request();
    assert_eq!(
        fixture
            .post("/maintenance/tenant-range-purge", request.clone())
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(fixture.rows().await, before);
    request["claim"] = claim_descriptor(2);
    let (status, response) = fixture
        .post("/maintenance/tenant-range-purge", request)
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["rows_deleted"], 1);
    assert_eq!(fixture.rows().await.len(), before.len() - 1);
}
