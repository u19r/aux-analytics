use std::time::Duration;

pub(crate) const SOURCE_POLLS_TOTAL_METRIC: &str = "analytics.source.polls_total";
pub(crate) const SOURCE_POLL_ERRORS_TOTAL_METRIC: &str = "analytics.source.poll_errors_total";
pub(crate) const SOURCE_POLL_DURATION_MS_METRIC: &str = "analytics.source.poll_duration_ms";
pub(crate) const SOURCE_RECORDS_PER_POLL_METRIC: &str = "analytics.source.records_per_poll";
pub(crate) const SOURCE_RECORDS_FETCHED_TOTAL_METRIC: &str =
    "analytics.source.records_fetched_total";
pub(crate) const SOURCE_RESPONSES_TOTAL_METRIC: &str = "analytics.source.responses_total";
pub(crate) const SOURCE_NONEMPTY_RESPONSES_TOTAL_METRIC: &str =
    "analytics.source.nonempty_responses_total";
pub(crate) const SOURCE_ENCODED_BYTES_TOTAL_METRIC: &str = "analytics.source.encoded_bytes_total";
pub(crate) const SOURCE_RECORDS_ROUTED_TOTAL_METRIC: &str = "analytics.source.records_routed_total";
pub(crate) const SOURCE_RECORDS_INGESTED_TOTAL_METRIC: &str =
    "analytics.source.records_ingested_total";
pub(crate) const SOURCE_RECORDS_COMMITTED_TOTAL_METRIC: &str =
    "analytics.source.records_committed_total";
pub(crate) const SOURCE_RECORDS_SKIPPED_TOTAL_METRIC: &str =
    "analytics.source.records_skipped_total";
pub(crate) const SOURCE_WRITE_TRANSACTIONS_TOTAL_METRIC: &str =
    "analytics.source.write_transactions_total";
pub(crate) const SOURCE_WRITE_DURATION_MS_METRIC: &str = "analytics.source.write_duration_ms";
pub(crate) const SOURCE_BATCH_MEMORY_INCREMENT_BYTES_METRIC: &str =
    "analytics.source.batch_memory_increment_bytes";
pub(crate) const SOURCE_INGEST_ERRORS_TOTAL_METRIC: &str = "analytics.source.ingest_errors_total";
pub(crate) const SOURCE_QUARANTINED_TOTAL_METRIC: &str = "analytics.source.quarantined_total";
pub(crate) const SOURCE_PRIVACY_DROPS_TOTAL_METRIC: &str =
    "analytics.source.privacy_dropped_fields_total";
pub(crate) const SOURCE_CHECKPOINTS_SAVED_TOTAL_METRIC: &str =
    "analytics.source.checkpoints_saved_total";
pub(crate) const SOURCE_CHECKPOINT_ERRORS_TOTAL_METRIC: &str =
    "analytics.source.checkpoint_errors_total";
pub(crate) const SOURCE_CHECKPOINTS_METRIC: &str = "analytics.source.checkpoints";
pub(crate) const SOURCE_CHECKPOINT_SAVED_TIMESTAMP_SECONDS_METRIC: &str =
    "analytics.source.checkpoint_saved_timestamp_seconds";
pub(crate) const SOURCE_LEADER_METRIC: &str = "analytics.source.leader";
pub(crate) const SOURCE_LEASE_REMAINING_MS_METRIC: &str = "analytics.source.lease_remaining_ms";
pub(crate) const SOURCE_LEASE_ATTEMPTS_TOTAL_METRIC: &str = "analytics.source.lease_attempts_total";
pub(crate) const SOURCE_POLLING_LEASE_DURATION_MS: i64 = 10_000;
pub(crate) const SOURCE_POLLING_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const SOURCE_POLLING_JOB_ID: &str = "analytics_source_polling";
