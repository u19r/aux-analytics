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
pub(crate) const SOURCE_QUARANTINED_TOTAL_METRIC: &str = "analytics.source.quarantined_total";
pub(crate) const SOURCE_PRIVACY_DROPS_TOTAL_METRIC: &str =
    "analytics.source.privacy_dropped_fields_total";
pub(crate) const SOURCE_CHECKPOINTS_SAVED_TOTAL_METRIC: &str =
    "analytics.source.checkpoints_saved_total";
pub(crate) const SOURCE_CHECKPOINT_ERRORS_TOTAL_METRIC: &str =
    "analytics.source.checkpoint_errors_total";
pub(crate) const SOURCE_CHECKPOINTS_METRIC: &str = "analytics.source.checkpoints";
pub(crate) const SOURCE_POLLING_LEASE_DURATION_MS: i64 = 10_000;
pub(crate) const SOURCE_POLLING_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const SOURCE_POLLING_JOB_ID: &str = "analytics_source_polling";
