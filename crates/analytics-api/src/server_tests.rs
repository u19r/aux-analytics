use std::time::Duration;

use config::{MetricsConfig, RootConfig, Tracing};

use crate::server::{
    CorsOrigins, FilterSource, cors_origins, endpoint_config, metrics_endpoint_config,
    parse_shutdown_grace_period, resolve_filter,
};

#[test]
fn given_valid_configured_log_filter_when_resolved_then_config_source_is_reported() {
    let tracing = Tracing {
        log_level: Some("analytics_api=debug".to_string()),
    };

    let (_filter, source) = resolve_filter(&tracing);

    assert_eq!(source.to_string(), "config");
}

#[test]
fn given_invalid_configured_log_filter_when_resolved_then_default_source_is_reported() {
    let tracing = Tracing {
        log_level: Some("analytics_api[bad".to_string()),
    };

    let (_filter, source) = resolve_filter(&tracing);

    assert_eq!(source.to_string(), "default");
}

#[test]
fn given_filter_source_when_formatted_then_operator_label_is_stable() {
    assert_eq!(FilterSource::Config.to_string(), "config");
    assert_eq!(FilterSource::Default.to_string(), "default");
}

#[test]
fn given_metrics_disabled_when_config_is_built_then_prometheus_recorder_is_not_installed() {
    let metrics = MetricsConfig {
        enabled: false,
        ..MetricsConfig::default()
    };

    let config = metrics_endpoint_config(&metrics).expect("metrics config");

    assert!(!config.enabled);
    assert!(config.prometheus.is_none());
}

#[test]
fn given_wildcard_cors_origin_when_resolved_then_any_origin_is_allowed() {
    let mut root = RootConfig::default();
    root.http.cors.allow_origins = vec!["https://app.example".to_string(), "*".to_string()];

    assert_eq!(cors_origins(&root), CorsOrigins::Any);
}

#[test]
fn given_invalid_cors_origin_when_resolved_then_origin_is_ignored() {
    let mut root = RootConfig::default();
    root.http.cors.allow_origins = vec![
        "https://app.example".to_string(),
        "https://bad.example\ninjected".to_string(),
    ];

    let CorsOrigins::Exact(origins) = cors_origins(&root) else {
        panic!("valid origin should be retained");
    };
    assert_eq!(origins.len(), 1);
    assert_eq!(origins[0], "https://app.example");
}

#[test]
fn given_no_valid_cors_origins_when_resolved_then_no_origin_policy_is_added() {
    let mut root = RootConfig::default();
    root.http.cors.allow_origins = vec!["https://bad.example\ninjected".to_string()];

    assert_eq!(cors_origins(&root), CorsOrigins::None);
}

#[test]
fn given_ingest_endpoint_disabled_when_endpoint_config_is_built_then_ingest_route_is_disabled() {
    let mut root = RootConfig::default();
    root.analytics.http.ingest_endpoint_enabled = false;

    let config = endpoint_config(&root);

    assert!(!config.ingest_enabled);
}

#[test]
fn given_shutdown_grace_seconds_when_parsed_then_shutdown_delay_uses_configured_duration() {
    assert_eq!(
        parse_shutdown_grace_period(Some("30")),
        Duration::from_secs(30)
    );
}

#[test]
fn given_invalid_shutdown_grace_seconds_when_parsed_then_default_shutdown_delay_is_used() {
    assert_eq!(
        parse_shutdown_grace_period(Some("not-a-number")),
        Duration::from_secs(5)
    );
    assert_eq!(parse_shutdown_grace_period(None), Duration::from_secs(5));
}
