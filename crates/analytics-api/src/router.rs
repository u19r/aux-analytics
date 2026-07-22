use std::{fmt, sync::Arc, time::Instant};

use analytics_contract::{
    AnalyticsManifest, TableRegistration, TenantRangePurgeRequest, TenantRangePurgeResponse,
};
use analytics_engine::{
    AnalyticsEngineError, IngestOutcome, StreamRecordBatchItem, prepare_tenant_structured_query,
    prepare_unscoped_structured_query,
};
use analytics_operations::{OperationId, OperationStatus, OperationStore};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::{Value, json};
use tokio::task::JoinSet;

use crate::{
    AnalyticsEngineAccess, AnalyticsEngineAccessError, QueryResponseBuildError,
    build_query_batch_result, build_query_response,
    request_validation::{RequestValidators, ValidatedJson},
    types::{
        AppState, DiagnosticsResponse, ErrorResponse, HealthResponse, IngestBatchResponse,
        IngestResponse, IngestStreamRecordBatchRequest, IngestStreamRecordRequest,
        OperationAuditEventResponse, OperationAuditResponse, OperationCancelResponse,
        OperationDiagnostics, OperationListResponse, OperationStatusResponse, QueryBatchResponse,
        QueryBatchResult, QueryResponse, ReadyResponse, TenantQueryBatchItem,
        TenantQueryBatchRequest, TenantQueryRequest, UnscopedSqlQueryRequest,
        UnscopedStructuredQueryRequest,
    },
};

const QUERY_REQUESTS_METRIC: &str = "analytics.query.requests_total";
const QUERY_LATENCY_METRIC: &str = "analytics.query.latency_ms";
const INGEST_REQUESTS_METRIC: &str = "analytics.http.ingest.requests_total";
const INGEST_LATENCY_METRIC: &str = "analytics.http.ingest.latency_ms";
const INGEST_PRIVACY_DROPS_METRIC: &str = "analytics.http.ingest.privacy_dropped_fields_total";
const INGEST_ERROR_RECORD_CONTRACT: &str = "record_contract";
const INGEST_ERROR_RETRYABLE: &str = "retryable";

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
        .route("/operations", get(list_operations))
        .route("/operations/{operation_id}", get(operation_status))
        .route("/operations/{operation_id}/audit", get(operation_audit))
        .route("/operations/{operation_id}/cancel", post(cancel_operation))
        .route("/manifest", get(manifest))
        .route("/tables", post(register_table))
        .route("/analytics/health", get(health_status))
        .route("/analytics/diagnostics", get(diagnostics))
        .route("/analytics/manifest", get(manifest))
        .route("/openapi.json", get(openapi_json))
        .route("/unscoped-sql-query", post(unscoped_sql_query))
        .route(
            "/unscoped-structured-query",
            post(unscoped_structured_query),
        )
        .route("/tenant-query", post(tenant_query))
        .route("/tenant-query-batch", post(tenant_query_batch))
        .route("/tenant-range-purge", post(tenant_range_purge));
    if endpoint_config.ingest_enabled {
        router = router
            .route("/ingest/{analytics_table_name}", post(ingest_stream_record))
            .route(
                "/ingest-batch/{analytics_table_name}",
                post(ingest_stream_record_batch),
            );
    }
    router
        .layer(axum::Extension(RequestValidators::compile()))
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
    match app_state
        .engine
        .with_read(|engine| engine.query_unscoped_sql_json("select 1 as ready"))
        .await
    {
        Ok(Ok(_)) => Json(ReadyResponse {
            status: "ready".to_string(),
        })
        .into_response(),
        Ok(Err(err)) => error_response(StatusCode::SERVICE_UNAVAILABLE, &err.to_string()),
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
        operations: operation_diagnostics(&app_state),
    })
}

fn operation_diagnostics(app_state: &AppState) -> OperationDiagnostics {
    let Some(path) = app_state.operation_store_path.as_ref() else {
        return OperationDiagnostics {
            configured: false,
            reachable: false,
            operation_count: 0,
            running_operations: 0,
            degraded_operations: 0,
            oldest_running_updated_at_ms: None,
        };
    };
    let Ok(store) = OperationStore::connect_duckdb(path.as_path()) else {
        return OperationDiagnostics {
            configured: true,
            reachable: false,
            operation_count: 0,
            running_operations: 0,
            degraded_operations: 0,
            oldest_running_updated_at_ms: None,
        };
    };
    let Ok(operations) = store.list_operations() else {
        return OperationDiagnostics {
            configured: true,
            reachable: false,
            operation_count: 0,
            running_operations: 0,
            degraded_operations: 0,
            oldest_running_updated_at_ms: None,
        };
    };
    let running_operations = operations
        .iter()
        .filter(|operation| {
            matches!(
                operation.status,
                OperationStatus::Running | OperationStatus::Cancelling
            )
        })
        .count();
    let degraded_operations = operations
        .iter()
        .filter(|operation| matches!(operation.status, OperationStatus::Failed))
        .count();
    let oldest_running_updated_at_ms = operations
        .iter()
        .filter(|operation| {
            matches!(
                operation.status,
                OperationStatus::Running | OperationStatus::Cancelling
            )
        })
        .map(|operation| operation.updated_at_ms)
        .min();
    OperationDiagnostics {
        configured: true,
        reachable: true,
        operation_count: operations.len(),
        running_operations,
        degraded_operations,
        oldest_running_updated_at_ms,
    }
}

#[utoipa::path(
    get,
    path = "/operations",
    responses(
        (status = 200, body = OperationListResponse, description = "Durable operation list"),
        (status = 503, body = ErrorResponse, description = "Operation store is not configured")
    ),
    tag = "Operations"
)]
pub(crate) async fn list_operations(State(app_state): State<Arc<AppState>>) -> Response {
    match operation_store_response(&app_state, |store| {
        let operations = store
            .list_operations()?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(OperationListResponse { operations })
    }) {
        Ok(value) => Json(value).into_response(),
        Err(response) => *response,
    }
}

#[utoipa::path(
    get,
    path = "/operations/{operation_id}",
    params(("operation_id" = String, Path, description = "Operation id")),
    responses(
        (status = 200, body = OperationStatusResponse, description = "Durable operation status"),
        (status = 404, body = ErrorResponse, description = "Operation not found"),
        (status = 503, body = ErrorResponse, description = "Operation store is not configured")
    ),
    tag = "Operations"
)]
pub(crate) async fn operation_status(
    State(app_state): State<Arc<AppState>>,
    Path(operation_id): Path<String>,
) -> Response {
    operation_response_for_id(&app_state, operation_id, |store, operation_id| {
        Ok(OperationStatusResponse::from(
            store.show_operation(operation_id)?,
        ))
    })
}

#[utoipa::path(
    get,
    path = "/operations/{operation_id}/audit",
    params(("operation_id" = String, Path, description = "Operation id")),
    responses(
        (status = 200, body = OperationAuditResponse, description = "Durable operation audit events"),
        (status = 404, body = ErrorResponse, description = "Operation not found"),
        (status = 503, body = ErrorResponse, description = "Operation store is not configured")
    ),
    tag = "Operations"
)]
pub(crate) async fn operation_audit(
    State(app_state): State<Arc<AppState>>,
    Path(operation_id): Path<String>,
) -> Response {
    operation_response_for_id(&app_state, operation_id, |store, operation_id| {
        let events = store
            .audit_events(operation_id)?
            .into_iter()
            .map(OperationAuditEventResponse::from)
            .collect();
        Ok(OperationAuditResponse { events })
    })
}

#[utoipa::path(
    post,
    path = "/operations/{operation_id}/cancel",
    params(("operation_id" = String, Path, description = "Operation id")),
    responses(
        (status = 200, body = OperationCancelResponse, description = "Cancellation request accepted"),
        (status = 404, body = ErrorResponse, description = "Operation not found"),
        (status = 503, body = ErrorResponse, description = "Operation store is not configured")
    ),
    tag = "Operations"
)]
pub(crate) async fn cancel_operation(
    State(app_state): State<Arc<AppState>>,
    Path(operation_id): Path<String>,
) -> Response {
    operation_response_for_id(&app_state, operation_id, |store, operation_id| {
        store.request_cancellation(operation_id)?;
        Ok(OperationCancelResponse {
            operation_id: operation_id.to_string(),
            cancellation_requested: true,
        })
    })
}

#[utoipa::path(
    get,
    path = "/manifest",
    responses((status = 200, body = analytics_contract::AnalyticsManifest, description = "Active analytics manifest")),
    tag = "Analytics"
)]
pub(crate) async fn manifest(State(app_state): State<Arc<AppState>>) -> Json<Value> {
    let manifest = app_state.manifest.read().await.clone();
    Json(serde_json::to_value(manifest.as_ref()).unwrap_or_else(|_| json!({})))
}

#[utoipa::path(
    post,
    path = "/tables",
    request_body = analytics_contract::TableRegistration,
    responses(
        (status = 200, body = analytics_contract::TableRegistration, description = "Registered analytics table"),
        (status = 400, body = ErrorResponse, description = "Invalid table registration")
    ),
    tag = "Analytics"
)]
pub(crate) async fn register_table(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(table): ValidatedJson<TableRegistration>,
) -> Response {
    let mut candidate_manifest = app_state.manifest.read().await.as_ref().clone();
    upsert_manifest_table(&mut candidate_manifest, table.clone());
    if let Err(err) = candidate_manifest.validate() {
        return error_response(StatusCode::BAD_REQUEST, &err.to_string());
    }
    if let Err(err) = app_state
        .engine
        .with_write(|engine| engine.ensure_table(&table))
        .await
    {
        return error_response(StatusCode::BAD_REQUEST, &err.to_string());
    }
    *app_state.manifest.write().await = Arc::new(candidate_manifest);
    Json(table).into_response()
}

fn upsert_manifest_table(manifest: &mut AnalyticsManifest, table: TableRegistration) {
    if let Some(existing) = manifest
        .tables
        .iter_mut()
        .find(|registered| registered.analytics_table_name == table.analytics_table_name)
    {
        *existing = table;
    } else {
        manifest.tables.push(table);
    }
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
    ValidatedJson(request): ValidatedJson<UnscopedSqlQueryRequest>,
) -> Response {
    let started = Instant::now();
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
    ValidatedJson(request): ValidatedJson<UnscopedStructuredQueryRequest>,
) -> Response {
    let started = Instant::now();
    let manifest = app_state.manifest.read().await.clone();
    match app_state
        .engine
        .with_read(|engine| {
            let prepared = prepare_unscoped_structured_query(&manifest, &request.query)?;
            let rows = engine.query_prepared_structured_json(&prepared)?;
            Ok::<_, AnalyticsEngineError>((rows, prepared))
        })
        .await
    {
        Ok(Ok((rows, prepared))) => {
            record_query_metrics("unscoped_structured", "success", started);
            match build_query_response(rows, prepared.metadata(), started) {
                Ok(response) => Json(response).into_response(),
                Err(err) => error_response(StatusCode::BAD_REQUEST, &err.to_string()),
            }
        }
        Ok(Err(err)) => {
            record_query_metrics("unscoped_structured", "error", started);
            error_response(StatusCode::BAD_REQUEST, &err.to_string())
        }
        Err(err) => {
            record_query_metrics("unscoped_structured", "error", started);
            error_response(StatusCode::SERVICE_UNAVAILABLE, &err.to_string())
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
    ValidatedJson(request): ValidatedJson<TenantQueryRequest>,
) -> Response {
    let started = Instant::now();
    let manifest = app_state.manifest.read().await.clone();
    match app_state
        .engine
        .with_read(|engine| {
            let prepared = prepare_tenant_structured_query(
                &manifest,
                &request.query,
                request.target_tenant_id.as_str(),
            )?;
            let rows = engine.query_prepared_structured_json(&prepared)?;
            Ok::<_, AnalyticsEngineError>((rows, prepared))
        })
        .await
    {
        Ok(Ok((rows, prepared))) => {
            record_query_metrics("tenant_structured", "success", started);
            match build_query_response(rows, prepared.metadata(), started) {
                Ok(response) => Json(response).into_response(),
                Err(err) => error_response(StatusCode::BAD_REQUEST, &err.to_string()),
            }
        }
        Ok(Err(err)) => {
            record_query_metrics("tenant_structured", "error", started);
            error_response(StatusCode::BAD_REQUEST, &err.to_string())
        }
        Err(err) => {
            record_query_metrics("tenant_structured", "error", started);
            error_response(StatusCode::SERVICE_UNAVAILABLE, &err.to_string())
        }
    }
}

#[utoipa::path(
    post,
    path = "/tenant-query-batch",
    request_body = TenantQueryBatchRequest,
    responses(
        (status = 200, body = QueryBatchResponse, description = "Query tenant-scoped rows for multiple named structured queries"),
        (status = 400, body = ErrorResponse, description = "Invalid request or query")
    ),
    tag = "Analytics"
)]
pub(crate) async fn tenant_query_batch(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(request): ValidatedJson<TenantQueryBatchRequest>,
) -> Response {
    let started = Instant::now();
    if request.queries.is_empty() {
        record_query_metrics("tenant_structured_batch", "validation_error", started);
        return error_response(
            StatusCode::BAD_REQUEST,
            "queries must contain at least one structured query",
        );
    }

    let manifest = app_state.manifest.read().await.clone();
    match execute_tenant_query_batch(
        app_state.engine.clone(),
        manifest,
        request.target_tenant_id,
        request.queries,
    )
    .await
    {
        Ok(results) => {
            record_query_metrics("tenant_structured_batch", "success", started);
            Json(QueryBatchResponse { results }).into_response()
        }
        Err(err) => {
            record_query_metrics("tenant_structured_batch", "error", started);
            error_response(err.status_code(), &err.to_string())
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
    ValidatedJson(request): ValidatedJson<IngestStreamRecordRequest>,
) -> Response {
    let started = Instant::now();
    let (record_key, record) = request.into_contract_record();
    let manifest = app_state.manifest.read().await.clone();
    let retention = if let Some(runtime) = app_state.retention.as_ref() {
        runtime
            .retention_for_record(&manifest, analytics_table_name.as_str(), &record)
            .await
    } else {
        None
    };
    let ingest_result = if let Some(policy) = app_state.privacy_policy.as_ref() {
        app_state
            .engine
            .with_write(|engine| {
                engine.ingest_stream_record_with_privacy_policy_and_retention(
                    &manifest,
                    analytics_table_name.as_str(),
                    record_key.as_bytes(),
                    record,
                    policy,
                    retention.as_ref(),
                )
            })
            .await
            .map(|outcome| {
                metrics::counter!(
                    INGEST_PRIVACY_DROPS_METRIC,
                    "policy_version" => outcome.policy_version.clone()
                )
                .increment(outcome.dropped_fields);
                (
                    outcome.outcome,
                    Some(outcome.policy_version),
                    Some(outcome.dropped_fields),
                )
            })
    } else {
        app_state
            .engine
            .with_write(|engine| {
                engine.ingest_stream_record_with_retention(
                    &manifest,
                    analytics_table_name.as_str(),
                    record_key.as_bytes(),
                    record,
                    retention.as_ref(),
                )
            })
            .await
            .map(|outcome| (outcome, None, None))
    };
    match ingest_result {
        Ok((outcome, privacy_policy_version, privacy_dropped_fields)) => {
            record_ingest_metrics("success", started);
            Json(IngestResponse {
                outcome: ingest_outcome_name(outcome).to_string(),
                privacy_policy_version,
                privacy_dropped_fields,
            })
            .into_response()
        }
        Err(err) => {
            let code = ingest_error_code(&err);
            record_ingest_metrics("error", started);
            tracing::warn!(
                analytics_table_name,
                error_code = code,
                error = %err,
                "analytics record ingestion failed"
            );
            ingest_error_response(StatusCode::BAD_REQUEST, code, &err.to_string())
        }
    }
}

#[utoipa::path(
    post,
    path = "/ingest-batch/{analytics_table_name}",
    params(("analytics_table_name" = String, Path, description = "Registered analytics table name")),
    request_body = IngestStreamRecordBatchRequest,
    responses(
        (status = 200, body = IngestBatchResponse, description = "Batch ingest outcomes"),
        (status = 400, body = ErrorResponse, description = "Invalid request or batch ingest failure")
    ),
    tag = "Analytics"
)]
pub(crate) async fn ingest_stream_record_batch(
    State(app_state): State<Arc<AppState>>,
    Path(analytics_table_name): Path<String>,
    ValidatedJson(request): ValidatedJson<IngestStreamRecordBatchRequest>,
) -> Response {
    let started = Instant::now();
    if request.records.is_empty() {
        record_ingest_metrics("validation_error", started);
        return error_response(
            StatusCode::BAD_REQUEST,
            "batch ingestion requires at least one record",
        );
    }
    if app_state.privacy_policy.is_some() || app_state.retention.is_some() {
        record_ingest_metrics("unsupported", started);
        return error_response(
            StatusCode::BAD_REQUEST,
            "batch ingestion is only supported when privacy filtering and retention lookup are \
             disabled",
        );
    }

    let records = request
        .records
        .into_iter()
        .map(|request| {
            let (record_key, record) = request.into_contract_record();
            StreamRecordBatchItem {
                analytics_table_name: analytics_table_name.clone(),
                record_key: record_key.into_bytes(),
                record,
            }
        })
        .collect();
    let manifest = app_state.manifest.read().await.clone();
    match app_state
        .engine
        .with_write(|engine| engine.ingest_stream_record_batch(&manifest, records))
        .await
    {
        Ok(outcomes) => {
            record_ingest_metrics("success", started);
            Json(IngestBatchResponse {
                record_count: outcomes.len(),
                outcomes: outcomes
                    .into_iter()
                    .map(|outcome| IngestResponse {
                        outcome: ingest_outcome_name(outcome).to_string(),
                        privacy_policy_version: None,
                        privacy_dropped_fields: None,
                    })
                    .collect(),
            })
            .into_response()
        }
        Err(err) => {
            let code = ingest_error_code(&err);
            record_ingest_metrics("error", started);
            tracing::warn!(
                analytics_table_name,
                error_code = code,
                error = %err,
                "analytics record batch ingestion failed"
            );
            ingest_error_response(StatusCode::BAD_REQUEST, code, &err.to_string())
        }
    }
}

#[utoipa::path(
    post,
    path = "/tenant-range-purge",
    request_body = TenantRangePurgeRequest,
    responses(
        (status = 200, body = TenantRangePurgeResponse, description = "Deleted one bounded tenant-scoped analytics range"),
        (status = 400, body = ErrorResponse, description = "Invalid range purge request or deletion failure")
    ),
    tag = "Analytics"
)]
pub(crate) async fn tenant_range_purge(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(request): ValidatedJson<TenantRangePurgeRequest>,
) -> Response {
    match app_state
        .engine
        .with_write(|engine| engine.purge_tenant_range(&request))
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(err) => error_response(StatusCode::BAD_REQUEST, &err.to_string()),
    }
}

async fn execute_tenant_query_batch(
    engine: AnalyticsEngineAccess,
    manifest: Arc<AnalyticsManifest>,
    target_tenant_id: String,
    queries: Vec<TenantQueryBatchItem>,
) -> Result<Vec<QueryBatchResult>, TenantQueryBatchExecutionError> {
    let mut tasks = JoinSet::new();
    for (index, item) in queries.into_iter().enumerate() {
        let engine = engine.clone();
        let manifest = Arc::clone(&manifest);
        let target_tenant_id = target_tenant_id.clone();
        tasks.spawn(async move {
            let started = Instant::now();
            let (rows, prepared) = engine
                .with_read(|engine| {
                    let prepared = prepare_tenant_structured_query(
                        manifest.as_ref(),
                        &item.query,
                        target_tenant_id.as_str(),
                    )?;
                    let rows = engine.query_prepared_structured_json(&prepared)?;
                    Ok::<_, AnalyticsEngineError>((rows, prepared))
                })
                .await??;
            let response = build_query_batch_result(item.name, rows, prepared.metadata(), started)?;
            Ok::<_, TenantQueryBatchExecutionError>((index, response))
        });
    }

    let mut results = Vec::with_capacity(tasks.len());
    while let Some(joined) = tasks.join_next().await {
        results.push(joined??);
    }
    results.sort_by_key(|(index, _)| *index);
    Ok(results.into_iter().map(|(_, result)| result).collect())
}

#[derive(Debug)]
enum TenantQueryBatchExecutionError {
    Query(AnalyticsEngineError),
    Access(AnalyticsEngineAccessError),
    Join(tokio::task::JoinError),
    Metadata(QueryResponseBuildError),
}

impl From<QueryResponseBuildError> for TenantQueryBatchExecutionError {
    fn from(error: QueryResponseBuildError) -> Self {
        Self::Metadata(error)
    }
}

impl TenantQueryBatchExecutionError {
    const fn status_code(&self) -> StatusCode {
        match self {
            Self::Query(_) => StatusCode::BAD_REQUEST,
            Self::Access(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Join(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Metadata(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl fmt::Display for TenantQueryBatchExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(error) => write!(formatter, "{error}"),
            Self::Access(error) => write!(formatter, "{error}"),
            Self::Join(error) => write!(formatter, "{error}"),
            Self::Metadata(error) => write!(formatter, "{error}"),
        }
    }
}

impl From<AnalyticsEngineError> for TenantQueryBatchExecutionError {
    fn from(error: AnalyticsEngineError) -> Self {
        Self::Query(error)
    }
}

impl From<AnalyticsEngineAccessError> for TenantQueryBatchExecutionError {
    fn from(error: AnalyticsEngineAccessError) -> Self {
        Self::Access(error)
    }
}

impl From<tokio::task::JoinError> for TenantQueryBatchExecutionError {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::Join(error)
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

fn operation_store_response<T>(
    app_state: &AppState,
    build: impl FnOnce(OperationStore) -> Result<T, OperationApiError>,
) -> Result<T, Box<Response>> {
    let Some(path) = app_state.operation_store_path.as_ref() else {
        return Err(Box::new(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "operation store is not configured",
        )));
    };
    let store = OperationStore::connect_duckdb(path.as_path())
        .map_err(|error| Box::new(operation_error_response(error.into())))?;
    build(store).map_err(|error| Box::new(operation_error_response(error)))
}

fn operation_response_for_id<T: serde::Serialize>(
    app_state: &AppState,
    operation_id: String,
    build: impl FnOnce(&OperationStore, &OperationId) -> Result<T, OperationApiError>,
) -> Response {
    let operation_id = match OperationId::new(operation_id) {
        Ok(operation_id) => operation_id,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    match operation_store_response(app_state, |store| build(&store, &operation_id)) {
        Ok(value) => Json(value).into_response(),
        Err(response) => *response,
    }
}

enum OperationApiError {
    Store(analytics_operations::OperationStoreError),
}

impl From<analytics_operations::OperationStoreError> for OperationApiError {
    fn from(error: analytics_operations::OperationStoreError) -> Self {
        Self::Store(error)
    }
}

fn operation_error_response(error: OperationApiError) -> Response {
    match error {
        OperationApiError::Store(analytics_operations::OperationStoreError::OperationNotFound(
            operation_id,
        )) => error_response(
            StatusCode::NOT_FOUND,
            format!("operation {operation_id} not found").as_str(),
        ),
        OperationApiError::Store(error) => {
            error_response(StatusCode::BAD_REQUEST, error.to_string().as_str())
        }
    }
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
            code: None,
        }),
    )
        .into_response()
}

fn ingest_error_response(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.to_string(),
            code: Some(code.to_string()),
        }),
    )
        .into_response()
}

fn ingest_error_code(error: &AnalyticsEngineError) -> &'static str {
    if error.is_record_contract_failure() {
        INGEST_ERROR_RECORD_CONTRACT
    } else {
        INGEST_ERROR_RETRYABLE
    }
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
