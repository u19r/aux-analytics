use config::Tracing;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::{
    EnvFilter, fmt,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

use crate::{
    MetricsEndpointConfig,
    constants::{PROMETHEUS_LATENCY_MS_BUCKETS, PROMETHEUS_UPKEEP_INTERVAL},
};

pub(super) fn initialize_tracing(tracing_cfg: &Tracing) -> Result<(), TryInitError> {
    let (filter, source) = resolve_filter(tracing_cfg);
    println!("Using log filter (source: {source}): {filter}");
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
            handle: None,
        });
    }

    let handle = PrometheusBuilder::new()
        .set_buckets(PROMETHEUS_LATENCY_MS_BUCKETS)?
        .install_recorder()?;
    Ok(MetricsEndpointConfig {
        enabled: true,
        handle: Some(handle),
    })
}

pub(super) fn spawn_prometheus_upkeep(metrics_config: &MetricsEndpointConfig) {
    let Some(handle) = metrics_config.handle.as_ref() else {
        return;
    };
    let handle = handle.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PROMETHEUS_UPKEEP_INTERVAL);
        loop {
            interval.tick().await;
            handle.run_upkeep();
        }
    });
}
