use std::time::Instant;

use analytics_engine::SourceEffectDisposition;
use analytics_storage::PollBatch;

use super::health::SourceBatchOutcome;
use crate::{SourceHealth, constants::*};

pub(crate) struct SourcePollObservation {
    started: Instant,
}

impl SourcePollObservation {
    pub(crate) fn start() -> Self {
        metrics::counter!(SOURCE_POLLS_TOTAL_METRIC).increment(1);
        metrics::gauge!(SOURCE_POLLER_ENABLED_METRIC).set(1.0);
        // The feeder uses its fixed polling interval; it adds no retry backoff.
        metrics::gauge!(SOURCE_RETRY_DELAY_MS_METRIC).set(0.0);
        Self {
            started: Instant::now(),
        }
    }

    pub(crate) fn record_batch(self, batch: &PollBatch) {
        for (name, count) in [
            (SOURCE_RESPONSES_TOTAL_METRIC, batch.source_response_count),
            (
                SOURCE_NONEMPTY_RESPONSES_TOTAL_METRIC,
                batch.source_nonempty_response_count,
            ),
            (
                SOURCE_RECORDS_FETCHED_TOTAL_METRIC,
                batch.source_record_count,
            ),
            (
                SOURCE_ENCODED_BYTES_TOTAL_METRIC,
                batch.source_encoded_bytes,
            ),
            (
                SOURCE_RECORDS_ROUTED_TOTAL_METRIC,
                batch
                    .records
                    .iter()
                    .map(|record| record.analytics_table_names.len())
                    .sum(),
            ),
        ] {
            metrics::counter!(name).increment(u64::try_from(count).unwrap_or(u64::MAX));
        }
        metrics::histogram!(SOURCE_RECORDS_PER_POLL_METRIC)
            .record(batch.source_record_count as f64);
    }
}

impl Drop for SourcePollObservation {
    fn drop(&mut self) {
        metrics::histogram!(SOURCE_POLL_DURATION_MS_METRIC)
            .record(self.started.elapsed().as_secs_f64() * 1000.0);
    }
}

pub(crate) struct SourceWriteObservation {
    started: Instant,
}

impl SourceWriteObservation {
    pub(crate) fn start() -> Self {
        Self {
            started: Instant::now(),
        }
    }

    pub(crate) fn record_result<E>(self, result: &Result<SourceEffectDisposition, E>) {
        let outcome = match result {
            Ok(SourceEffectDisposition::Applied { summary }) => {
                metrics::counter!(SOURCE_WRITE_TRANSACTIONS_TOTAL_METRIC).increment(1);
                metrics::counter!(SOURCE_RECORDS_SKIPPED_TOTAL_METRIC).increment(summary.skipped());
                metrics::counter!(SOURCE_RECORDS_COMMITTED_TOTAL_METRIC).increment(
                    summary
                        .inserted()
                        .saturating_add(summary.updated())
                        .saturating_add(summary.deleted()),
                );
                "applied"
            }
            Ok(SourceEffectDisposition::AlreadyApplied { .. }) => "already_applied",
            Ok(SourceEffectDisposition::StaleFence) => "stale_fence",
            Err(_) => {
                metrics::counter!(SOURCE_INGEST_ERRORS_TOTAL_METRIC).increment(1);
                "error"
            }
        };
        metrics::histogram!(SOURCE_WRITE_DURATION_MS_METRIC, "outcome" => outcome)
            .record(self.started.elapsed().as_secs_f64() * 1000.0);
    }
}

impl SourceBatchOutcome {
    pub(super) fn record_metrics(&self, health: &SourceHealth, observed_at_ms: u128) {
        let outcome = if self.is_committed {
            "success"
        } else {
            "error"
        };
        metrics::counter!(SOURCE_RECORDS_INGESTED_TOTAL_METRIC, "outcome" => outcome)
            .increment(self.records_ingested);
        metrics::counter!(SOURCE_INGEST_ERRORS_TOTAL_METRIC).increment(self.ingest_errors);
        metrics::counter!(SOURCE_CHECKPOINT_ERRORS_TOTAL_METRIC).increment(self.checkpoint_errors);
        metrics::counter!(SOURCE_CHECKPOINTS_SAVED_TOTAL_METRIC).increment(self.checkpoints_saved);
        if self.is_committed && self.checkpoints_saved > 0 {
            // A global SystemStream frontier has no table/shard health entry.
            metrics::gauge!(SOURCE_CHECKPOINTS_METRIC).set(health.checkpoints.len().max(1) as f64);
            metrics::gauge!(SOURCE_CHECKPOINT_SAVED_TIMESTAMP_SECONDS_METRIC)
                .set(observed_at_ms as f64 / 1000.0);
        }
    }
}
