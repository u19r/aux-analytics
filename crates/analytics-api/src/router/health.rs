use super::*;

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
        operations: OperationDiagnostics::load_from_store(
            app_state
                .operation_store_path
                .as_ref()
                .map(|path| path.as_path()),
        ),
    })
}

impl OperationDiagnostics {
    fn load_from_store(path: Option<&std::path::Path>) -> Self {
        let Some(path) = path else {
            return Self::default();
        };
        let mut summary = Self {
            configured: true,
            ..Self::default()
        };
        let Ok(store) = OperationStore::connect_duckdb(path) else {
            return summary;
        };
        let Ok(operations) = store.list_operations() else {
            return summary;
        };
        summary.reachable = true;
        summary.operation_count = operations.len();
        for operation in operations {
            match operation.status {
                OperationStatus::Running | OperationStatus::Cancelling => {
                    summary.running_operations += 1;
                    summary.oldest_running_updated_at_ms = Some(
                        summary
                            .oldest_running_updated_at_ms
                            .map_or(operation.updated_at_ms, |oldest| {
                                oldest.min(operation.updated_at_ms)
                            }),
                    );
                }
                OperationStatus::Failed => summary.degraded_operations += 1,
                _ => {}
            }
        }
        summary
    }
}
