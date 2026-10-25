use std::time::Duration;

use analytics_engine::StorageBackend;
use config::{AnalyticsJobStoreConfig, AnalyticsSourceConfig};

use crate::{
    background_jobs::runtime::limits::job_store_settings, constants::MAX_JOB_STORE_REQUEST_TIMEOUT,
};

fn source(endpoint_url: &str) -> AnalyticsSourceConfig {
    AnalyticsSourceConfig {
        endpoint_url: Some(endpoint_url.to_string()),
        ..AnalyticsSourceConfig::default()
    }
}

fn backend() -> StorageBackend {
    StorageBackend::DuckDb {
        path: ":memory:".to_string(),
    }
}

#[test]
fn given_job_store_endpoint_when_settings_are_built_then_only_endpoint_and_timeout_are_used() {
    let job_store = AnalyticsJobStoreConfig {
        endpoint_url: Some("http://aux.example.test".to_string()),
        ..AnalyticsJobStoreConfig::default()
    };
    let settings = job_store_settings(&job_store, &backend())
        .expect("JobStore settings")
        .expect("active source jobs");
    assert_eq!(settings.endpoint, "http://aux.example.test");
    assert_eq!(settings.timeout, Duration::from_millis(5_000));
}

#[test]
fn given_only_stream_endpoint_when_settings_are_built_then_job_store_is_disabled() {
    let source = source("http://stream.example.test");
    let job_store = AnalyticsJobStoreConfig::default();
    let settings = job_store_settings(&job_store, &backend()).expect("JobStore settings");
    assert_eq!(
        source.endpoint_url.as_deref(),
        Some("http://stream.example.test")
    );
    assert!(
        settings.is_none(),
        "the stream endpoint must not configure the scheduled JobStore"
    );
}

#[test]
fn given_missing_job_store_endpoint_when_settings_are_built_then_job_store_is_disabled() {
    let job_store = AnalyticsJobStoreConfig {
        request_timeout_ms: 1_200,
        ..AnalyticsJobStoreConfig::default()
    };
    let settings = job_store_settings(&job_store, &backend()).expect("JobStore settings");

    assert!(settings.is_none());
}

#[test]
fn given_over_bound_job_store_timeout_when_settings_are_built_then_configuration_is_rejected() {
    let job_store = AnalyticsJobStoreConfig {
        endpoint_url: Some("http://jobs.example.test".to_string()),
        request_timeout_ms: MAX_JOB_STORE_REQUEST_TIMEOUT.as_millis() as u64 + 1,
    };

    assert!(job_store_settings(&job_store, &backend()).is_err());
}
