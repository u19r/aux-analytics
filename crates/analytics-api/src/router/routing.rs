use super::*;

#[derive(Debug, Clone)]
pub struct MetricsEndpointConfig {
    pub enabled: bool,
    pub handle: Option<PrometheusHandle>,
}

impl Default for MetricsEndpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            handle: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EndpointConfig {
    pub ingest_enabled: bool,
    pub async_operations_enabled: bool,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            ingest_enabled: true,
            async_operations_enabled: true,
        }
    }
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
        .route(
            "/maintenance/tenant-range-purge",
            post(apply_tenant_range_purge),
        )
        .route(
            "/maintenance/tenant-range-purge/claim",
            post(register_range_purge_claim),
        )
        .route("/manifest", get(manifest))
        .route("/tables", post(register_table))
        .route("/tables/batch", post(register_tables))
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
        .route("/tenant-query-batch", post(tenant_query_batch));
    if endpoint_config.async_operations_enabled {
        router = router
            .route("/operations", get(list_operations).post(submit_operation))
            .route("/operations/{operation_id}", get(operation_status))
            .route("/operations/{operation_id}/audit", get(operation_audit))
            .route("/operations/{operation_id}/cancel", post(cancel_operation));
    }
    if endpoint_config.ingest_enabled {
        router = router
            .route("/ingest/{analytics_table_name}", post(ingest_stream_record))
            .route(
                "/ingest-batch/{analytics_table_name}",
                post(ingest_stream_record_batch),
            );
    }
    router
        .layer(axum::Extension(endpoint_config))
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
        && let Some(handle) = metrics_config.handle
    {
        router = router.merge(prometheus_metrics_router(handle));
    }
    router
}
