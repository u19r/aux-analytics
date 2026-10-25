use serde_json::json;

use crate::{AnalyticsConfig, AnalyticsJobStoreConfig, AnalyticsSourceConfig, RootConfig};

#[test]
fn given_legacy_metrics_auth_config_when_root_is_deserialized_then_it_is_rejected() {
    let error = serde_json::from_value::<RootConfig>(json!({
        "features": {
            "metrics": {
                "prometheus": {
                    "bearer_token": "metrics-secret"
                }
            }
        }
    }))
    .expect_err("removed metrics auth config must be rejected as an unknown field");

    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn given_metrics_schema_when_generated_then_legacy_auth_config_is_absent() {
    let schema = serde_json::to_value(schemars::schema_for!(RootConfig)).expect("root schema");
    let schema_string = schema.to_string();

    assert!(!schema_string.contains("PrometheusMetricsConfig"));
    assert!(!schema_string.contains("bearer_token"));
}

#[test]
fn given_removed_source_api_key_when_source_config_is_deserialized_then_it_is_rejected() {
    let error = serde_json::from_value::<AnalyticsSourceConfig>(json!({
        "api_key": "source-job-secret"
    }))
    .expect_err("removed API key must be rejected as an unknown field");

    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn given_source_schema_when_generated_then_removed_api_key_is_absent() {
    let schema =
        serde_json::to_value(schemars::schema_for!(AnalyticsSourceConfig)).expect("source schema");
    assert!(!schema.to_string().contains("api_key"));
}

#[test]
fn given_stream_endpoint_without_job_store_endpoint_when_config_is_deserialized_then_endpoints_stay_separate()
 {
    let config = serde_json::from_value::<AnalyticsConfig>(json!({
        "source": {"endpoint_url": "http://stream.example.test"}
    }))
    .expect("config");

    assert_eq!(
        config.source.endpoint_url.as_deref(),
        Some("http://stream.example.test")
    );
    assert!(config.job_store.endpoint_url.is_none());
    assert_eq!(config.job_store.request_timeout_ms, 5_000);
}

#[test]
fn given_job_store_endpoint_when_config_is_deserialized_then_custom_endpoint_and_timeout_are_preserved()
 {
    let config = serde_json::from_value::<AnalyticsConfig>(json!({
        "source": {"endpoint_url": "http://stream.example.test"},
        "job_store": {
            "endpoint_url": "http://jobs.example.test",
            "request_timeout_ms": 1_200
        }
    }))
    .expect("config");

    assert_eq!(
        config.job_store.endpoint_url.as_deref(),
        Some("http://jobs.example.test")
    );
    assert_eq!(config.job_store.request_timeout_ms, 1_200);
    assert_eq!(
        config.source.endpoint_url.as_deref(),
        Some("http://stream.example.test")
    );
}

#[test]
fn given_missing_job_store_endpoint_when_config_is_deserialized_then_job_store_remains_disabled() {
    let config = serde_json::from_value::<AnalyticsConfig>(json!({
        "job_store": {"request_timeout_ms": 1_200}
    }))
    .expect("config");

    assert!(config.job_store.endpoint_url.is_none());
    assert_eq!(config.job_store.request_timeout_ms, 1_200);
}

#[test]
fn given_removed_job_store_auth_field_when_job_store_config_is_deserialized_then_it_is_rejected() {
    let error = serde_json::from_value::<AnalyticsJobStoreConfig>(json!({
        "api_key": "job-store-secret"
    }))
    .expect_err("JobStore auth fields must be rejected as unknown fields");

    assert!(error.to_string().contains("unknown field"));
    assert!(
        serde_json::from_value::<AnalyticsConfig>(json!({
            "job_store": {"api_key": "job-store-secret"}
        }))
        .is_err()
    );
}

#[test]
fn given_analytics_schema_when_generated_then_job_store_is_explicit_and_authless() {
    let schema = serde_json::to_value(schemars::schema_for!(AnalyticsConfig))
        .expect("analytics config schema");
    let schema_string = schema.to_string();

    assert!(schema_string.contains("job_store"));
    assert!(schema_string.contains("request_timeout_ms"));
    assert!(!schema_string.contains("api_key"));
}

#[test]
fn given_query_deadline_configuration_when_serialized_then_cancellation_setting_is_not_implied() {
    let query = serde_json::from_value::<crate::AnalyticsQueryConfig>(json!({
        "disable_query_deadline": true
    }))
    .expect("deadline configuration");
    assert!(query.disable_query_deadline);
    let output = serde_json::to_value(&query).expect("query configuration");
    assert_eq!(output["disable_query_deadline"], true);
    assert!(output.get("disable_duckdb_interrupt").is_none());
    assert!(
        serde_json::from_value::<crate::AnalyticsQueryConfig>(json!({
            "disable_duckdb_interrupt": true
        }))
        .is_err()
    );
}
