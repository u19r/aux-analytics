use std::{sync::Arc, time::Instant};

use analytics_engine::IngestOutcome;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use metrics_exporter_prometheus::PrometheusHandle;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{
    request_validation::openapi_request_validator,
    types::{
        AppState, DiagnosticsResponse, ErrorResponse, HealthResponse, IngestResponse,
        IngestStreamRecordRequest, QueryResponse, ReadyResponse, TenantQueryRequest,
        UnscopedSqlQueryRequest, UnscopedStructuredQueryRequest,
    },
};

const QUERY_REQUESTS_METRIC: &str = "analytics.query.requests_total";
const QUERY_LATENCY_METRIC: &str = "analytics.query.latency_ms";
const INGEST_REQUESTS_METRIC: &str = "analytics.http.ingest.requests_total";
const INGEST_LATENCY_METRIC: &str = "analytics.http.ingest.latency_ms";

#[derive(Debug, Clone)]
pub struct MetricsEndpointConfig {
    pub enabled: bool,
    pub prometheus: Option<PrometheusMetricsEndpointConfig>,
}

impl Default for MetricsEndpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prometheus: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrometheusMetricsEndpointConfig {
    pub handle: PrometheusHandle,
    pub bearer_token: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct EndpointConfig {
    pub ingest_enabled: bool,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            ingest_enabled: true,
        }
    }
}

#[derive(Debug)]
struct PrometheusMetricsState {
    handle: PrometheusHandle,
    bearer_token: Option<String>,
}

pub fn router(app_state: Arc<AppState>) -> Router {
    router_with_config(app_state, EndpointConfig::default())
}

pub fn router_with_config(app_state: Arc<AppState>, endpoint_config: EndpointConfig) -> Router {
    let mut router = Router::new()
        .route("/up", get(up))
        .route("/ready", get(ready))
        .route("/health", get(health_status))
        .route("/diagnostics", get(diagnostics))
        .route("/manifest", get(manifest))
        .route("/openapi.json", get(openapi_json))
        .route("/unscoped-sql-query", post(unscoped_sql_query))
        .route(
            "/unscoped-structured-query",
            post(unscoped_structured_query),
        )
        .route("/tenant-query", post(tenant_query));
    if endpoint_config.ingest_enabled {
        router = router.route("/ingest/{analytics_table_name}", post(ingest_stream_record));
    }
    router
        .layer(axum::middleware::from_fn(openapi_request_validator))
        .with_state(app_state)
}

pub fn server_router(app_state: Arc<AppState>) -> Router {
    server_router_with_metrics(app_state, MetricsEndpointConfig::default())
}

pub fn server_router_with_metrics(
    app_state: Arc<AppState>,
    metrics_config: MetricsEndpointConfig,
) -> Router {
    server_router_with_config(app_state, metrics_config, EndpointConfig::default())
}

pub fn server_router_with_config(
    app_state: Arc<AppState>,
    metrics_config: MetricsEndpointConfig,
    endpoint_config: EndpointConfig,
) -> Router {
    let mut router = router_with_config(app_state, endpoint_config);
    if metrics_config.enabled
        && let Some(prometheus) = metrics_config.prometheus
    {
        router = router.merge(prometheus_metrics_router(prometheus));
    }
    router
}

#[utoipa::path(
    get,
    path = "/up",
    responses((status = 200, description = "Service process is up")),
    tag = "Health"
)]
pub(crate) async fn up() -> StatusCode {
    StatusCode::OK
}

#[utoipa::path(
    get,
    path = "/ready",
    responses(
        (status = 200, body = ReadyResponse, description = "Service is ready"),
        (status = 503, body = ErrorResponse, description = "Service is not ready")
    ),
    tag = "Health"
)]
pub(crate) async fn ready(State(app_state): State<Arc<AppState>>) -> Response {
    let engine = app_state.engine.lock().await;
    match engine.query_unscoped_sql_json("select 1 as ready") {
        Ok(_) => Json(ReadyResponse {
            status: "ready".to_string(),
        })
        .into_response(),
        Err(err) => error_response(StatusCode::SERVICE_UNAVAILABLE, &err.to_string()),
    }
}

#[utoipa::path(
    get,
    path = "/health",
    responses((status = 200, body = HealthResponse, description = "Service is healthy")),
    tag = "Health"
)]
pub(crate) async fn health_status(State(app_state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let source = app_state.source_health.read().await.clone();
    let retention = app_state.retention_health.read().await.clone();
    Json(HealthResponse {
        status: "healthy".to_string(),
        source,
        retention,
    })
}

#[utoipa::path(
    get,
    path = "/diagnostics",
    responses((status = 200, body = DiagnosticsResponse, description = "Service diagnostics including source polling and checkpoint health")),
    tag = "Health"
)]
pub(crate) async fn diagnostics(
    State(app_state): State<Arc<AppState>>,
) -> Json<DiagnosticsResponse> {
    let source = app_state.source_health.read().await.clone();
    let retention = app_state.retention_health.read().await.clone();
    Json(DiagnosticsResponse {
        status: "ok".to_string(),
        source,
        retention,
    })
}

#[utoipa::path(
    get,
    path = "/manifest",
    responses((status = 200, body = analytics_contract::AnalyticsManifest, description = "Active analytics manifest")),
    tag = "Analytics"
)]
pub(crate) async fn manifest(State(app_state): State<Arc<AppState>>) -> Json<Value> {
    Json(serde_json::to_value(app_state.manifest.as_ref()).unwrap_or_else(|_| json!({})))
}

#[utoipa::path(
    post,
    path = "/unscoped-sql-query",
    request_body = UnscopedSqlQueryRequest,
    responses(
        (status = 400, body = ErrorResponse, description = "Raw SQL is not available through HTTP without an admin boundary")
    ),
    tag = "Analytics"
)]
pub(crate) async fn unscoped_sql_query(
    State(_app_state): State<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Response {
    let started = Instant::now();
    let request = match validate_json::<UnscopedSqlQueryRequest>(payload) {
        Ok(request) => request,
        Err(err) => {
            record_query_metrics("raw", "validation_error", started);
            return err.into_response();
        }
    };
    let _sql_len = request.sql.len();
    record_query_metrics("raw", "rejected", started);
    error_response(
        StatusCode::BAD_REQUEST,
        "unscoped SQL queries are not available through the HTTP API until an admin authorization \
         boundary exists; use tenant-query with target_tenant_id for tenant-scoped reads",
    )
}

#[utoipa::path(
    post,
    path = "/unscoped-structured-query",
    request_body = UnscopedStructuredQueryRequest,
    responses(
        (status = 200, body = QueryResponse, description = "Query explicitly unscoped rows as JSON objects"),
        (status = 400, body = ErrorResponse, description = "Invalid request or query")
    ),
    tag = "Analytics"
)]
pub(crate) async fn unscoped_structured_query(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Response {
    let started = Instant::now();
    let request = match validate_json::<UnscopedStructuredQueryRequest>(payload) {
        Ok(request) => request,
        Err(err) => {
            record_query_metrics("unscoped_structured", "validation_error", started);
            return err.into_response();
        }
    };
    let engine = app_state.engine.lock().await;
    match engine.query_unscoped_structured_json(app_state.manifest.as_ref(), &request.query) {
        Ok(rows) => {
            record_query_metrics("unscoped_structured", "success", started);
            Json(QueryResponse { rows }).into_response()
        }
        Err(err) => {
            record_query_metrics("unscoped_structured", "error", started);
            error_response(StatusCode::BAD_REQUEST, &err.to_string())
        }
    }
}

#[utoipa::path(
    post,
    path = "/tenant-query",
    request_body = TenantQueryRequest,
    responses(
        (status = 200, body = QueryResponse, description = "Query tenant-scoped rows as JSON objects"),
        (status = 400, body = ErrorResponse, description = "Invalid request or query")
    ),
    tag = "Analytics"
)]
pub(crate) async fn tenant_query(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Response {
    let started = Instant::now();
    let request = match validate_json::<TenantQueryRequest>(payload) {
        Ok(request) => request,
        Err(err) => {
            record_query_metrics("tenant_structured", "validation_error", started);
            return err.into_response();
        }
    };
    let engine = app_state.engine.lock().await;
    match engine.query_tenant_structured_json(
        app_state.manifest.as_ref(),
        &request.query,
        request.target_tenant_id.as_str(),
    ) {
        Ok(rows) => {
            record_query_metrics("tenant_structured", "success", started);
            Json(QueryResponse { rows }).into_response()
        }
        Err(err) => {
            record_query_metrics("tenant_structured", "error", started);
            error_response(StatusCode::BAD_REQUEST, &err.to_string())
        }
    }
}

#[utoipa::path(
    post,
    path = "/ingest/{analytics_table_name}",
    params(("analytics_table_name" = String, Path, description = "Registered analytics table name")),
    request_body = IngestStreamRecordRequest,
    responses(
        (status = 200, body = IngestResponse, description = "Ingest outcome"),
        (status = 400, body = ErrorResponse, description = "Invalid request or ingest failure")
    ),
    tag = "Analytics"
)]
pub(crate) async fn ingest_stream_record(
    State(app_state): State<Arc<AppState>>,
    Path(analytics_table_name): Path<String>,
    Json(payload): Json<Value>,
) -> Response {
    let started = Instant::now();
    let request = match validate_json::<IngestStreamRecordRequest>(payload) {
        Ok(request) => request,
        Err(err) => {
            record_ingest_metrics("validation_error", started);
            return err.into_response();
        }
    };
    let (record_key, record) = request.into_contract_record();
    let retention = if let Some(runtime) = app_state.retention.as_ref() {
        runtime
            .retention_for_record(
                app_state.manifest.as_ref(),
                analytics_table_name.as_str(),
                &record,
            )
            .await
    } else {
        None
    };
    let engine = app_state.engine.lock().await;
    match engine.ingest_stream_record_with_retention(
        app_state.manifest.as_ref(),
        analytics_table_name.as_str(),
        record_key.as_bytes(),
        record,
        retention.as_ref(),
    ) {
        Ok(outcome) => {
            record_ingest_metrics("success", started);
            Json(IngestResponse {
                outcome: ingest_outcome_name(outcome).to_string(),
            })
            .into_response()
        }
        Err(err) => {
            record_ingest_metrics("error", started);
            error_response(StatusCode::BAD_REQUEST, &err.to_string())
        }
    }
}

fn record_query_metrics(query_type: &'static str, outcome: &'static str, started: Instant) {
    metrics::counter!(QUERY_REQUESTS_METRIC, "type" => query_type, "outcome" => outcome)
        .increment(1);
    metrics::histogram!(QUERY_LATENCY_METRIC, "type" => query_type, "outcome" => outcome)
        .record(duration_ms(started));
}

fn record_ingest_metrics(outcome: &'static str, started: Instant) {
    metrics::counter!(INGEST_REQUESTS_METRIC, "outcome" => outcome).increment(1);
    metrics::histogram!(INGEST_LATENCY_METRIC, "outcome" => outcome).record(duration_ms(started));
}

fn duration_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

#[utoipa::path(
    get,
    path = "/openapi.json",
    responses((status = 200, body = serde_json::Value, description = "OpenAPI document")),
    tag = "OpenAPI"
)]
pub(crate) async fn openapi_json() -> Json<Value> {
    Json(crate::openapi::build_json())
}

fn prometheus_metrics_router(config: PrometheusMetricsEndpointConfig) -> Router {
    let state = Arc::new(PrometheusMetricsState {
        handle: config.handle,
        bearer_token: config.bearer_token,
    });
    Router::new()
        .route("/metrics", get(prometheus_metrics_endpoint))
        .with_state(state)
}

async fn prometheus_metrics_endpoint(
    State(state): State<Arc<PrometheusMetricsState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(status) = authorize_prometheus_metrics(&headers, state.bearer_token.as_deref()) {
        let message = if status == StatusCode::UNAUTHORIZED {
            "missing bearer token"
        } else {
            "invalid bearer token"
        };
        return (status, message).into_response();
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.handle.render(),
    )
        .into_response()
}

fn authorize_prometheus_metrics(
    headers: &HeaderMap,
    expected_token: Option<&str>,
) -> Result<(), StatusCode> {
    let Some(expected_token) = expected_token else {
        return Ok(());
    };
    let header_value = headers
        .get(header::AUTHORIZATION)
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let header_value = header_value.to_str().map_err(|_| StatusCode::FORBIDDEN)?;
    let token = header_value
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::FORBIDDEN)?;
    if token == expected_token {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
        .into_response()
}

#[derive(Debug)]
struct RequestValidationError {
    status: StatusCode,
    message: String,
}

impl RequestValidationError {
    fn internal(message: String) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message,
        }
    }

    fn bad_request(message: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }

    fn into_response(self) -> Response {
        error_response(self.status, self.message.as_str())
    }
}

fn validate_json<T>(payload: Value) -> Result<T, RequestValidationError>
where T: DeserializeOwned + JsonSchema {
    let schema = schemars::schema_for!(T);
    let schema_value = serde_json::to_value(schema)
        .map_err(|err| RequestValidationError::internal(err.to_string()))?;
    let validator = jsonschema::validator_for(&schema_value)
        .map_err(|err| RequestValidationError::internal(err.to_string()))?;
    let errors = validator
        .iter_errors(&payload)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        return Err(RequestValidationError::bad_request(format!(
            "request body failed schema validation: {}",
            errors.join("; ")
        )));
    }

    serde_json::from_value(payload)
        .map_err(|err| RequestValidationError::bad_request(err.to_string()))
}

fn ingest_outcome_name(outcome: IngestOutcome) -> &'static str {
    match outcome {
        IngestOutcome::Inserted => "inserted",
        IngestOutcome::Updated => "updated",
        IngestOutcome::Deleted => "deleted",
        IngestOutcome::Skipped => "skipped",
    }
}

#[cfg(test)]
#[path = "router_tests.rs"]
mod router_tests;
