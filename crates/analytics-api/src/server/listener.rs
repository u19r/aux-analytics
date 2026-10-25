use std::sync::Arc;

use tokio::net::TcpListener;

use super::{
    ProcessDuty,
    composition::{initialize_app_state, start_retention_jobs},
    cors::cors_layer,
    telemetry::{initialize_tracing, metrics_endpoint_config, spawn_prometheus_upkeep},
};
use crate::{
    AppState, EndpointConfig, MetricsEndpointConfig,
    cli::ApiCli,
    error::ApiResult,
    runtime_config::{load_privacy_policy, resolve_serve_config},
    server_router_with_config,
    source_polling::spawn_source_polling,
};

pub(crate) async fn serve(args: ApiCli) -> ApiResult<()> {
    let endpoint = lambda_web_runtime::LambdaWebEnvironment::load()?;
    let duty = args.process_duty.resolve_for_endpoint(endpoint.as_ref())?;
    let resolved = resolve_serve_config(&args)?;
    let endpoint_config = EndpointConfig {
        ingest_enabled: resolved.routes.ingest_enabled,
        async_operations_enabled: duty.can_run_background_work(),
    };
    let storage_backend = resolved.require_storage_backend()?.clone();
    let root = resolved.root;
    initialize_tracing(&root.tracing)?;
    let app_state = initialize_app_state(super::composition::AppStateStartupInput {
        root: &root,
        manifest: resolved.manifest.value,
        storage_backend,
        privacy_policy: load_privacy_policy(&root)?.map(Arc::new),
        duty,
    })
    .await?;
    let metrics_config = start_runtime_services(&root, &app_state, duty).await?;
    let bind_addr = duty.bind_address(&root.http.bind_addr, endpoint.as_ref())?;
    let background_jobs = app_state.background_jobs.clone();
    let router = server_router_with_config(app_state, metrics_config, endpoint_config)
        .layer(cors_layer(&root));
    drop(root);
    let listener = TcpListener::bind(bind_addr).await?;
    println!("Listening on {bind_addr}");
    if let Some(web) = endpoint.as_ref() {
        web.notify_ready().await?;
    }
    super::lifecycle::serve_requests(listener, router, background_jobs, endpoint.as_ref()).await?;
    Ok(())
}

async fn start_runtime_services(
    root: &config::RootConfig,
    app_state: &Arc<AppState>,
    duty: ProcessDuty,
) -> ApiResult<MetricsEndpointConfig> {
    let metrics_config = metrics_endpoint_config(&root.features.metrics)?;
    if duty.can_run_background_work() {
        spawn_source_polling(
            &root.analytics.source,
            &root.analytics.ingest,
            app_state.clone(),
        )
        .await?;
        start_retention_jobs(app_state).await;
        spawn_prometheus_upkeep(&metrics_config);
    }
    Ok(metrics_config)
}
