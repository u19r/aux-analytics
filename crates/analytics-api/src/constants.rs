use std::time::Duration;

// Durable source and retention job identity.
pub(crate) const SOURCE_AUX_JOB_KIND: &str = "analytics.source.aux_storage.v1";
pub(crate) const SOURCE_STREAM_JOB_KIND: &str = "analytics.source.storage_stream.v1";
pub(crate) const SOURCE_AUX_QUEUE: &str = "analytics-source-aux-storage";
pub(crate) const SOURCE_STREAM_QUEUE: &str = "analytics-source-storage-stream";
pub(crate) const RETENTION_JOB_KIND: &str = "analytics.retention.batch.v1";
pub(crate) const RETENTION_QUEUE: &str = "analytics-retention";
pub(crate) const SOURCE_SCHEMA_VERSION: u16 = 1;
pub(crate) const RETENTION_SCHEMA_VERSION: u16 = 1;
pub(crate) const SOURCE_PAYLOAD_HEADER: &[u8; 4] = b"ASJ1";
// Two maximum-sized images may expand sixfold when JSON escapes control bytes.
// Keep decoding bounded while admitting one complete source event.
pub(crate) const MAX_SOURCE_DECODED_BYTES: usize = 8 * 1_024 * 1_024;
pub(crate) const MAX_SOURCE_RECORDS: usize = 1_024;
pub(crate) const MAX_SOURCE_CHECKPOINTS: usize = 8;
pub(crate) const MAX_SOURCE_FIELD_BYTES: usize = 1_024;
pub(crate) const MAX_SOURCE_SCOPE_BYTES: usize = 512;
pub(crate) const MAX_SOURCE_AUTHORIZED_IDENTITIES: usize = 1_024;
pub(crate) const MAX_SOURCE_JOB_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const MAX_JOB_STORE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const MAX_SOURCE_DEADLINE_AT_MS: u64 =
    i64::MAX as u64 - MAX_SOURCE_JOB_TIMEOUT.as_millis() as u64;
pub(crate) const MAX_RETENTION_JOB_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const MAX_RETENTION_SCOPE_BYTES: usize = 512;
pub(crate) const MAX_RETENTION_BATCH_SIZE: u64 = config::MAX_RETENTION_DELETE_BATCH_SIZE;
pub(crate) const MAX_RETENTION_DEADLINE_AT_MS: u64 =
    i64::MAX as u64 - MAX_RETENTION_JOB_TIMEOUT.as_millis() as u64;
pub(crate) const RETENTION_SCHEDULE_STATE_KEY: &[u8] =
    b"analytics-api:bg-jobs:v1:retention-schedule:v1";
pub(crate) const RETENTION_SCHEDULE_STATE_VERSION: u8 = 1;
pub(crate) const DRIVER_ERROR_WAKE: Duration = Duration::from_secs(1);

pub(crate) const RETENTION_BATCHES_METRIC: &str = "analytics.retention.delete_batches_total";
pub(crate) const RETENTION_BATCH_DURATION_METRIC: &str =
    "analytics.retention.delete_batch_duration_ms";
pub(crate) const RETENTION_ROWS_METRIC: &str = "analytics.retention.rows_deleted_total";

// Source observations retained across the durable-job cutover.
pub(crate) const SOURCE_POLLS_TOTAL_METRIC: &str = "analytics.source.polls_total";
pub(crate) const SOURCE_POLL_ERRORS_TOTAL_METRIC: &str = "analytics.source.poll_errors_total";
pub(crate) const SOURCE_POLL_DURATION_MS_METRIC: &str = "analytics.source.poll_duration_ms";
pub(crate) const SOURCE_RETRY_DELAY_MS_METRIC: &str = "analytics.source.retry_delay_ms";
pub(crate) const SOURCE_RESPONSES_TOTAL_METRIC: &str = "analytics.source.responses_total";
pub(crate) const SOURCE_NONEMPTY_RESPONSES_TOTAL_METRIC: &str =
    "analytics.source.nonempty_responses_total";
pub(crate) const SOURCE_RECORDS_FETCHED_TOTAL_METRIC: &str =
    "analytics.source.records_fetched_total";
pub(crate) const SOURCE_ENCODED_BYTES_TOTAL_METRIC: &str = "analytics.source.encoded_bytes_total";
pub(crate) const SOURCE_RECORDS_ROUTED_TOTAL_METRIC: &str = "analytics.source.records_routed_total";
pub(crate) const SOURCE_RECORDS_INGESTED_TOTAL_METRIC: &str =
    "analytics.source.records_ingested_total";
pub(crate) const SOURCE_RECORDS_COMMITTED_TOTAL_METRIC: &str =
    "analytics.source.records_committed_total";
pub(crate) const SOURCE_CHECKPOINTS_SAVED_TOTAL_METRIC: &str =
    "analytics.source.checkpoints_saved_total";
pub(crate) const SOURCE_CHECKPOINT_SAVED_TIMESTAMP_SECONDS_METRIC: &str =
    "analytics.source.checkpoint_saved_timestamp_seconds";
pub(crate) const SOURCE_CHECKPOINTS_METRIC: &str = "analytics.source.checkpoints";
pub(crate) const SOURCE_INGEST_ERRORS_TOTAL_METRIC: &str = "analytics.source.ingest_errors_total";
pub(crate) const SOURCE_CHECKPOINT_ERRORS_TOTAL_METRIC: &str =
    "analytics.source.checkpoint_errors_total";
pub(crate) const SOURCE_WRITE_TRANSACTIONS_TOTAL_METRIC: &str =
    "analytics.source.write_transactions_total";
pub(crate) const SOURCE_WRITE_DURATION_MS_METRIC: &str = "analytics.source.write_duration_ms";
pub(crate) const SOURCE_POLLER_ENABLED_METRIC: &str = "analytics.source.poller_enabled";
pub(crate) const SOURCE_BATCH_MEMORY_INCREMENT_BYTES_METRIC: &str =
    "analytics.source.batch_memory_increment_bytes";

// request_validation constants.
pub(crate) const VALIDATION_ERROR_CODE: &str = "invalid_request";
pub(crate) const APPLICATION_JSON: &str = "application/json";

// server constants.
pub(crate) const PROMETHEUS_LATENCY_MS_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
    1000.0, 2500.0, 5000.0, 10000.0,
];
pub(crate) const PROMETHEUS_UPKEEP_INTERVAL: Duration = Duration::from_secs(5);

// runtime_config constants.
pub(crate) const PRIVACY_POLICY_LOAD_FAILURES_METRIC: &str =
    "analytics.privacy.policy_load_failures_total";

// source_polling: health constants.
pub(crate) const SOURCE_JOB_ID: &str = "analytics_source_polling";

// Retention policy lookup metrics.
pub(crate) const RETENTION_LOOKUPS_TOTAL_METRIC: &str = "analytics.retention.lookups_total";
pub(crate) const RETENTION_LOOKUP_LATENCY_METRIC: &str = "analytics.retention.lookup_latency_ms";
pub(crate) const RETENTION_LOOKUP_FAILURES_TOTAL_METRIC: &str =
    "analytics.retention.lookup_failures_total";

// HTTP query and ingest metrics and batch limit.
pub(crate) const QUERY_REQUESTS_METRIC: &str = "analytics.query.requests_total";
pub(crate) const QUERY_LATENCY_METRIC: &str = "analytics.query.latency_ms";
pub(crate) const INGEST_REQUESTS_METRIC: &str = "analytics.http.ingest.requests_total";
pub(crate) const INGEST_LATENCY_METRIC: &str = "analytics.http.ingest.latency_ms";
pub(crate) const INGEST_PRIVACY_DROPS_METRIC: &str =
    "analytics.http.ingest.privacy_dropped_fields_total";
pub(crate) const INGEST_ERROR_RECORD_CONTRACT: &str = "record_contract";
pub(crate) const INGEST_ERROR_RETRYABLE: &str = "retryable";
pub(crate) const MAX_TENANT_QUERY_BATCH_ITEMS: usize = 16;

// Distributed job namespace.
pub(crate) const BG_JOBS_NAMESPACE: &[u8] = b"analytics-api:bg-jobs:v1";

// background_jobs: bdd: retention_lifecycle constants.
#[cfg(feature = "bdd-test-support")]
pub(crate) const RETENTION_TABLE: &str = "audit_events";
#[cfg(feature = "bdd-test-support")]
pub(crate) const RETENTION_CUTOFF_MS: i64 = 2_500;

// Retention cache and configuration measurements.
#[cfg(test)]
pub(crate) const RETENTION_CACHE_PROBE_REPETITIONS: usize = 100;
#[cfg(test)]
pub(crate) const RETENTION_CONFIG_PROBE_MODE: &str = "AUX_ANALYTICS_RETENTION_CONFIG_PROBE";
#[cfg(test)]
pub(crate) const RETENTION_CONFIG_PROBE_TABLES: usize = 64;
#[cfg(test)]
pub(crate) const RETENTION_CONFIG_PROBE_VALUES: usize = 64;
#[cfg(test)]
pub(crate) const RETENTION_CONFIG_PROBE_VALUE_BYTES: usize = 4096;

// Canonical lowercase digest encoding.
pub(crate) const HEX: &[u8; 16] = b"0123456789abcdef";

pub(crate) const SOURCE_RECORDS_PER_POLL_METRIC: &str = "analytics.source.records_per_poll";
pub(crate) const SOURCE_RECORDS_SKIPPED_TOTAL_METRIC: &str =
    "analytics.source.records_skipped_total";
pub(crate) const SOURCE_MEMORY_SAMPLE_INTERVAL: Duration = Duration::from_millis(10);
