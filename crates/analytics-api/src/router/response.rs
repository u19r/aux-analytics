use super::*;

pub(crate) fn record_query_metrics(
    query_type: &'static str,
    outcome: &'static str,
    started: Instant,
) {
    metrics::counter!(QUERY_REQUESTS_METRIC, "type" => query_type, "outcome" => outcome)
        .increment(1);
    metrics::histogram!(QUERY_LATENCY_METRIC, "type" => query_type, "outcome" => outcome)
        .record(duration_ms(started));
}

pub(crate) fn record_ingest_metrics(outcome: &'static str, started: Instant) {
    metrics::counter!(INGEST_REQUESTS_METRIC, "outcome" => outcome).increment(1);
    metrics::histogram!(INGEST_LATENCY_METRIC, "outcome" => outcome).record(duration_ms(started));
}

pub(crate) fn duration_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

pub(crate) fn operation_store_response<T>(
    app_state: &AppState,
    build: impl FnOnce(OperationStore) -> Result<T, OperationApiError>,
) -> Result<T, Box<Response>> {
    let Some(path) = app_state.operation_store_path.as_ref() else {
        return Err(Box::new(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "durable operation storage is unavailable",
        )));
    };
    let store = OperationStore::connect_duckdb(path.as_path())
        .map_err(|error| Box::new(operation_error_response(error.into())))?;
    build(store).map_err(|error| Box::new(operation_error_response(error)))
}

pub(crate) fn operation_response_for_id<T: serde::Serialize>(
    app_state: &AppState,
    operation_id: String,
    build: impl FnOnce(&OperationStore, &OperationId) -> Result<T, OperationApiError>,
) -> Response {
    let operation_id = match OperationId::try_new(operation_id) {
        Ok(operation_id) => operation_id,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    match operation_store_response(app_state, |store| build(&store, &operation_id)) {
        Ok(value) => Json(value).into_response(),
        Err(response) => *response,
    }
}

pub(crate) enum OperationApiError {
    Store(analytics_operations::OperationStoreError),
    Maintenance(analytics_operations::MaintenanceError),
}

impl From<analytics_operations::OperationStoreError> for OperationApiError {
    fn from(error: analytics_operations::OperationStoreError) -> Self {
        Self::Store(error)
    }
}

impl From<analytics_operations::MaintenanceError> for OperationApiError {
    fn from(error: analytics_operations::MaintenanceError) -> Self {
        Self::Maintenance(error)
    }
}

pub(crate) fn operation_error_response(error: OperationApiError) -> Response {
    match error {
        OperationApiError::Store(analytics_operations::OperationStoreError::OperationNotFound(
            operation_id,
        )) => error_response(
            StatusCode::NOT_FOUND,
            format!("operation {operation_id} not found").as_str(),
        ),
        OperationApiError::Store(
            analytics_operations::OperationStoreError::UnauthorizedActor { .. },
        ) => error_response(
            StatusCode::UNAUTHORIZED,
            "operation actor is not authorized",
        ),
        OperationApiError::Store(
            analytics_operations::OperationStoreError::ConflictingOperation(_),
        ) => error_response(
            StatusCode::CONFLICT,
            "operation id already has a different request",
        ),
        OperationApiError::Store(
            analytics_operations::OperationStoreError::InvalidStatusTransition { .. }
            | analytics_operations::OperationStoreError::StaleOperationState { .. }
            | analytics_operations::OperationStoreError::EffectInProgress { .. }
            | analytics_operations::OperationStoreError::PendingEffect { .. },
        ) => error_response(StatusCode::CONFLICT, "operation lifecycle conflict"),
        OperationApiError::Store(error) => {
            error_response(StatusCode::BAD_REQUEST, error.to_string().as_str())
        }
        OperationApiError::Maintenance(
            analytics_operations::MaintenanceError::ConflictingOperation { .. },
        ) => error_response(
            StatusCode::CONFLICT,
            "operation id already has a different request",
        ),
        OperationApiError::Maintenance(analytics_operations::MaintenanceError::Store(
            analytics_operations::OperationStoreError::UnauthorizedActor { .. },
        )) => error_response(
            StatusCode::UNAUTHORIZED,
            "operation actor is not authorized",
        ),
        OperationApiError::Maintenance(analytics_operations::MaintenanceError::Store(
            analytics_operations::OperationStoreError::InvalidStatusTransition { .. }
            | analytics_operations::OperationStoreError::StaleOperationState { .. }
            | analytics_operations::OperationStoreError::EffectInProgress { .. }
            | analytics_operations::OperationStoreError::PendingEffect { .. },
        )) => error_response(StatusCode::CONFLICT, "operation lifecycle conflict"),
        OperationApiError::Maintenance(error) => {
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
pub(crate) async fn openapi_json(
    axum::Extension(endpoints): axum::Extension<super::routing::EndpointConfig>,
) -> Json<Value> {
    let mut document = crate::openapi::build_json();
    if let Some(paths) = document.get_mut("paths").and_then(Value::as_object_mut) {
        paths.retain(|path, _| {
            (endpoints.async_operations_enabled || !path.starts_with("/operations"))
                && (endpoints.ingest_enabled || !path.starts_with("/ingest"))
        });
    }
    Json(document)
}

pub(crate) fn prometheus_metrics_router(handle: PrometheusHandle) -> Router {
    Router::new()
        .route("/metrics", get(prometheus_metrics_endpoint))
        .with_state(handle)
}

async fn prometheus_metrics_endpoint(State(handle): State<PrometheusHandle>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        handle.render(),
    )
        .into_response()
}

pub(crate) fn error_response(status: StatusCode, message: &str) -> Response {
    error_response_with_code(status, message, None)
}

pub(crate) fn error_response_with_code(
    status: StatusCode,
    message: &str,
    code: Option<&str>,
) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.to_string(),
            code: code.map(str::to_string),
        }),
    )
        .into_response()
}

pub(crate) fn query_error_response(status: StatusCode, error: &AnalyticsEngineError) -> Response {
    let code = match error {
        AnalyticsEngineError::QueryTimeout { .. } => "query_timeout",
        AnalyticsEngineError::QueryCancelled => "query_cancelled",
        AnalyticsEngineError::QueryResultTooLarge { .. } => "query_result_too_large",
        AnalyticsEngineError::TenantPredicateConflict => "tenant_predicate_conflict",
        AnalyticsEngineError::InvalidStructuredQuery(_) => "invalid_structured_query",
        AnalyticsEngineError::InvalidQuery => "invalid_query",
        _ => "query_error",
    };
    error_response_with_code(status, &error.to_string(), Some(code))
}

pub(crate) fn ingest_error_response(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.to_string(),
            code: Some(code.to_string()),
        }),
    )
        .into_response()
}

pub(crate) fn ingest_error_code(error: &AnalyticsEngineError) -> &'static str {
    if error.is_record_contract_failure() {
        INGEST_ERROR_RECORD_CONTRACT
    } else {
        INGEST_ERROR_RETRYABLE
    }
}

pub(crate) fn ingest_outcome_name(outcome: IngestOutcome) -> &'static str {
    match outcome {
        IngestOutcome::Inserted => "inserted",
        IngestOutcome::Updated => "updated",
        IngestOutcome::Deleted => "deleted",
        IngestOutcome::Skipped => "skipped",
    }
}

impl From<IngestOutcome> for IngestResponse {
    fn from(outcome: IngestOutcome) -> Self {
        Self {
            outcome: ingest_outcome_name(outcome).to_string(),
            privacy_policy_version: None,
            privacy_dropped_fields: None,
        }
    }
}

impl From<analytics_engine::PrivacyIngestOutcome> for IngestResponse {
    fn from(outcome: analytics_engine::PrivacyIngestOutcome) -> Self {
        Self {
            outcome: ingest_outcome_name(outcome.outcome).to_string(),
            privacy_policy_version: Some(outcome.policy_version),
            privacy_dropped_fields: Some(outcome.dropped_fields),
        }
    }
}

impl IngestResponse {
    pub(super) fn record_privacy_drops(&self) {
        if let (Some(version), Some(dropped)) =
            (&self.privacy_policy_version, self.privacy_dropped_fields)
        {
            metrics::counter!(INGEST_PRIVACY_DROPS_METRIC, "policy_version" => version.clone())
                .increment(dropped);
        }
    }
}

pub(super) fn ingest_failure_response(
    analytics_table_name: &str,
    error: &AnalyticsEngineError,
    started: Instant,
    message: &'static str,
) -> Response {
    let code = ingest_error_code(error);
    record_ingest_metrics("error", started);
    tracing::warn!(analytics_table_name, error_code = code, error = %error, "{message}");
    ingest_error_response(StatusCode::BAD_REQUEST, code, &error.to_string())
}
