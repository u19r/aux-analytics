use std::{sync::Arc, time::Duration};

use analytics_api::{
    AppState, EndpointConfig, MetricsEndpointConfig, PrometheusMetricsEndpointConfig,
    retention::{RetentionRuntime, spawn_retention_sweeper},
    server_router_with_config,
};
use analytics_contract::read_manifest;
use analytics_engine::AnalyticsEngine;
use axum::http::HeaderValue;
use config::{RootConfig, Tracing};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::{net::TcpListener, signal};
use tower_http::cors::CorsLayer;
use tracing_subscriber::{
    EnvFilter, fmt,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

use crate::{
    cli::ApiCli,
    error::ApiResult,
    runtime_config::{load_serve_config, resolve_manifest_path, validate_source_config},
    source_polling::spawn_source_polling,
};

const PROMETHEUS_LATENCY_MS_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
    1000.0, 2500.0, 5000.0, 10000.0,
];
const PROMETHEUS_UPKEEP_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) async fn serve(args: ApiCli) -> ApiResult<()> {
    let config = load_serve_config(&args)?;
    let root = config.root.clone();
    let manifest_path = resolve_manifest_path(args.manifest.as_deref(), &root)?;
    let manifest = read_manifest(manifest_path.as_str())?;
    validate_source_config(&root.analytics.source)?;
    config::validate_retention_config(&root.analytics.retention)?;
    let (filter, source) = resolve_filter(&root.tracing);
    println!("Using log filter (source: {source}): {filter}");
    init_tracing(filter)?;

    let storage_backend = config::resolve_storage_backend(&(&args.backend).into(), &root)?;
    tracing::info!(
        source_table_count = root.analytics.source.tables.len(),
        "analytics source configuration loaded"
    );
    let engine = AnalyticsEngine::connect(&storage_backend)?;
    engine.ensure_manifest(&manifest)?;
    for table in &root.analytics.retention.tables {
        engine.ensure_retention_columns(table.analytics_table_name.as_str())?;
    }
    let retention_runtime = RetentionRuntime::from_config(&root.analytics.retention)
        .await?
        .map(Arc::new);
    let app_state = Arc::new(AppState::with_retention(
        engine,
        manifest,
        retention_runtime.clone(),
    ));
    spawn_source_polling(&root.analytics.source, app_state.clone()).await?;
    spawn_retention_sweeper(retention_runtime, app_state.clone()).await;
    let metrics_config = metrics_endpoint_config(&root.features.metrics)?;
    spawn_prometheus_upkeep(&metrics_config);

    let bind_addr = root.http.bind_addr.clone();
    let endpoint_config = endpoint_config(&root);
    let router = server_router_with_config(app_state, metrics_config, endpoint_config)
        .layer(cors_layer(&root));
    let addr: std::net::SocketAddr = bind_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on {bind_addr}");
    axum::serve(listener, router)
        .with_graceful_shutdown(await_shutdown_signal(shutdown_grace_period()))
        .await?;
    Ok(())
}

async fn await_shutdown_signal(grace: Duration) {
    #[cfg(unix)]
    {
        let mut terminate = match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(sig) => sig,
            Err(err) => {
                tracing::warn!(target = "shutdown", error = %err, "failed to install SIGTERM handler; falling back to Ctrl+C only");
                if let Err(err) = signal::ctrl_c().await {
                    tracing::warn!(target = "shutdown", error = %err, "failed to await Ctrl+C shutdown signal");
                }
                tokio::time::sleep(grace).await;
                return;
            }
        };

        tokio::select! {
            res = signal::ctrl_c() => {
                if let Err(err) = res {
                    tracing::warn!(target = "shutdown", error = %err, "failed to await Ctrl+C shutdown signal");
                }
            }
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    if let Err(err) = signal::ctrl_c().await {
        tracing::warn!(target = "shutdown", error = %err, "failed to await Ctrl+C shutdown signal");
    }

    tracing::info!(target = "shutdown", "shutdown signal received");
    tokio::time::sleep(grace).await;
}

fn init_tracing(filter: EnvFilter) -> Result<(), TryInitError> {
    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .json()
                .with_current_span(false)
                .with_span_list(false)
                .with_ansi(false)
                .with_writer(std::io::stdout),
        )
        .try_init()
}

pub(crate) fn resolve_filter(tracing_cfg: &Tracing) -> (EnvFilter, FilterSource) {
    if let Some(spec) = tracing_cfg.log_level.as_deref() {
        match EnvFilter::try_new(spec) {
            Ok(filter) => return (filter, FilterSource::Config),
            Err(err) => {
                eprintln!(
                    "Invalid log level '{spec}' in config ({err}), falling back to default filter"
                );
            }
        }
    }
    (EnvFilter::new("warn"), FilterSource::Default)
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum FilterSource {
    Config,
    Default,
}

impl std::fmt::Display for FilterSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config => write!(f, "config"),
            Self::Default => write!(f, "default"),
        }
    }
}

pub(crate) fn metrics_endpoint_config(
    metrics: &config::MetricsConfig,
) -> Result<MetricsEndpointConfig, metrics_exporter_prometheus::BuildError> {
    if !metrics.enabled {
        return Ok(MetricsEndpointConfig {
            enabled: false,
            prometheus: None,
        });
    }

    let handle = PrometheusBuilder::new()
        .set_buckets(PROMETHEUS_LATENCY_MS_BUCKETS)?
        .install_recorder()?;
    Ok(MetricsEndpointConfig {
        enabled: true,
        prometheus: Some(PrometheusMetricsEndpointConfig {
            handle,
            bearer_token: metrics.prometheus.bearer_token.clone(),
        }),
    })
}

fn spawn_prometheus_upkeep(metrics_config: &MetricsEndpointConfig) {
    let Some(prometheus) = metrics_config.prometheus.as_ref() else {
        return;
    };
    let handle = prometheus.handle.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PROMETHEUS_UPKEEP_INTERVAL);
        loop {
            interval.tick().await;
            handle.run_upkeep();
        }
    });
}

fn cors_layer(root: &RootConfig) -> CorsLayer {
    let mut layer = CorsLayer::new()
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PUT,
            axum::http::Method::PATCH,
            axum::http::Method::DELETE,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
        ]);
    match cors_origins(root) {
        CorsOrigins::Any => layer = layer.allow_origin(tower_http::cors::Any),
        CorsOrigins::Exact(origins) => layer = layer.allow_origin(origins),
        CorsOrigins::None => {}
    }
    layer
}

pub(crate) fn endpoint_config(root: &RootConfig) -> EndpointConfig {
    EndpointConfig {
        ingest_enabled: root.analytics.http.ingest_endpoint_enabled,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CorsOrigins {
    Any,
    Exact(Vec<HeaderValue>),
    None,
}

pub(crate) fn cors_origins(root: &RootConfig) -> CorsOrigins {
    let origins = &root.http.cors.allow_origins;
    if origins.iter().any(|origin| origin == "*") {
        return CorsOrigins::Any;
    }
    let vals: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|origin| HeaderValue::from_str(origin).ok())
        .collect();
    if vals.is_empty() {
        CorsOrigins::None
    } else {
        CorsOrigins::Exact(vals)
    }
}

fn shutdown_grace_period() -> Duration {
    parse_shutdown_grace_period(std::env::var("APP_SHUTDOWN_GRACE_SECONDS").ok().as_deref())
}

pub(crate) fn parse_shutdown_grace_period(raw: Option<&str>) -> Duration {
    raw.and_then(|raw| raw.parse::<u64>().ok())
        .map_or_else(|| Duration::from_secs(5), Duration::from_secs)
}
