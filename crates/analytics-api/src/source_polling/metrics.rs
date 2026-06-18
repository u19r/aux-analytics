use std::time::Duration;

pub(crate) const SOURCE_POLLS_TOTAL_METRIC: &str = "analytics.source.polls_total";
pub(crate) const SOURCE_POLL_ERRORS_TOTAL_METRIC: &str = "analytics.source.poll_errors_total";
pub(crate) const SOURCE_RECORDS_PER_POLL_METRIC: &str = "analytics.source.records_per_poll";
pub(crate) const SOURCE_RECORDS_INGESTED_TOTAL_METRIC: &str =
    "analytics.source.records_ingested_total";
pub(crate) const SOURCE_RECORDS_COMMITTED_TOTAL_METRIC: &str =
    "analytics.source.records_committed_total";
pub(crate) const SOURCE_RECORDS_SKIPPED_TOTAL_METRIC: &str =
    "analytics.source.records_skipped_total";
pub(crate) const SOURCE_INGEST_ERRORS_TOTAL_METRIC: &str = "analytics.source.ingest_errors_total";
pub(crate) const SOURCE_PRIVACY_DROPS_TOTAL_METRIC: &str =
    "analytics.source.privacy_dropped_fields_total";
pub(crate) const SOURCE_CHECKPOINTS_SAVED_TOTAL_METRIC: &str =
    "analytics.source.checkpoints_saved_total";
pub(crate) const SOURCE_CHECKPOINT_ERRORS_TOTAL_METRIC: &str =
    "analytics.source.checkpoint_errors_total";
pub(crate) const SOURCE_CHECKPOINTS_METRIC: &str = "analytics.source.checkpoints";
pub(crate) const INGEST_PROCESSOR_HEARTBEATS_TOTAL_METRIC: &str =
    "analytics.ingest.processor.heartbeats_total";
pub(crate) const INGEST_PROCESSOR_ACTIVE_PROCESSORS_METRIC: &str =
    "analytics.ingest.processor.active_processors";
pub(crate) const INGEST_PROCESSOR_OWNED_SLOTS_METRIC: &str =
    "analytics.ingest.processor.owned_slots";
pub(crate) const INGEST_PROCESSOR_LEASE_ACQUIRE_TOTAL_METRIC: &str =
    "analytics.ingest.processor.lease_acquire_total";
pub(crate) const INGEST_PROCESSOR_LEASE_LOST_TOTAL_METRIC: &str =
    "analytics.ingest.processor.lease_lost_total";
pub(crate) const INGEST_TABLE_RECORDS_TOTAL_METRIC: &str = "analytics.ingest.table.records_total";
pub(crate) const INGEST_TABLE_BYTES_TOTAL_METRIC: &str = "analytics.ingest.table.bytes_total";
pub(crate) const INGEST_TABLE_LAG_MS_METRIC: &str = "analytics.ingest.table.lag_ms";
pub(crate) const INGEST_TABLE_CURSOR_AGE_MS_METRIC: &str = "analytics.ingest.table.cursor_age_ms";
pub(crate) const INGEST_RATE_ROLLUP_WINDOW_MS: i64 = 60_000;
pub(crate) const REGISTERED_TABLES_TABLE_NAME: &str = "analytics_registered_tables";
pub(crate) const REGISTERED_TABLE_READY_STATUS: &str = "Ready";
pub(crate) const SOURCE_POLLING_LEASE_DURATION_MS: i64 = 10_000;
pub(crate) const SOURCE_POLLING_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const SOURCE_POLLING_JOB_ID: &str = "analytics_source_polling";
