use analytics_storage::SourceCheckpoint as StorageSourceCheckpoint;

use crate::{
    CheckpointHealth, SourceHealth, SourceHealthStatus, SourcePollingPhase,
    constants::{SOURCE_CHECKPOINTS_METRIC, SOURCE_JOB_ID, SOURCE_POLL_ERRORS_TOTAL_METRIC},
    source_polling::batch::is_iterator_checkpoint,
};

pub(crate) fn apply_source_poll_error_health(
    health: &mut SourceHealth,
    error: String,
    observed_at_ms: u128,
) {
    apply_source_poll_failure_health(health, SourcePollingPhase::Degraded, error, observed_at_ms);
}

fn apply_source_poll_failure_health(
    health: &mut SourceHealth,
    phase: SourcePollingPhase,
    error: String,
    observed_at_ms: u128,
) {
    health.status = SourceHealthStatus::Degraded;
    health.phase = phase;
    health.phase_started_at_ms = Some(observed_at_ms);
    health.last_error_at_ms = Some(observed_at_ms);
    health.last_error = Some(error);
    health.total_poll_errors = health.total_poll_errors.saturating_add(1);
    metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
}

pub(crate) fn apply_source_job_phase(
    health: &mut SourceHealth,
    phase: SourcePollingPhase,
    worker_id: &str,
    observed_at_ms: u128,
) {
    health.poller_enabled = true;
    health.job_id = Some(SOURCE_JOB_ID.to_string());
    health.worker_id = Some(worker_id.to_string());
    health.phase = phase;
    health.phase_started_at_ms = Some(observed_at_ms);
    health.status = match health.phase {
        SourcePollingPhase::Disabled => SourceHealthStatus::Disabled,
        SourcePollingPhase::Starting
        | SourcePollingPhase::Polling
        | SourcePollingPhase::Ingesting => SourceHealthStatus::Starting,
        SourcePollingPhase::Healthy => SourceHealthStatus::Healthy,
        SourcePollingPhase::Degraded => SourceHealthStatus::Degraded,
    };
}

pub(crate) struct SourceBatchOutcome {
    pub(crate) is_committed: bool,
    pub(crate) records_ingested: u64,
    pub(crate) ingest_errors: u64,
    pub(crate) checkpoints_saved: u64,
    pub(crate) checkpoint_errors: u64,
}

impl SourceHealth {
    pub(crate) fn apply_source_batch_health(
        &mut self,
        outcome: SourceBatchOutcome,
        checkpoints: &[StorageSourceCheckpoint],
        observed_at_ms: u128,
    ) {
        (self.status, self.phase) = if outcome.is_committed {
            (SourceHealthStatus::Healthy, SourcePollingPhase::Healthy)
        } else {
            (SourceHealthStatus::Degraded, SourcePollingPhase::Degraded)
        };
        self.phase_started_at_ms = Some(observed_at_ms);
        if outcome.is_committed {
            self.last_success_at_ms = Some(observed_at_ms);
            self.last_error = None;
        } else {
            self.last_error_at_ms = Some(observed_at_ms);
            if self.last_error.is_none() {
                self.last_error = Some(
                    "source batch did not commit; durable checkpoint state was preserved"
                        .to_string(),
                );
            }
            self.total_poll_errors = self.total_poll_errors.saturating_add(1);
        }
        self.total_records_ingested = self
            .total_records_ingested
            .saturating_add(outcome.records_ingested);
        self.total_ingest_errors = self
            .total_ingest_errors
            .saturating_add(outcome.ingest_errors);
        self.total_checkpoints_saved = self
            .total_checkpoints_saved
            .saturating_add(outcome.checkpoints_saved);
        self.total_checkpoint_errors = self
            .total_checkpoint_errors
            .saturating_add(outcome.checkpoint_errors);
        if outcome.is_committed {
            self.merge_source_checkpoints(checkpoints, Some(observed_at_ms));
        }
        outcome.record_metrics(self, observed_at_ms);
    }

    fn merge_source_checkpoints(
        &mut self,
        checkpoints: &[StorageSourceCheckpoint],
        observed_at_ms: Option<u128>,
    ) {
        self.checkpoints.reserve(checkpoints.len());
        for checkpoint in checkpoints
            .iter()
            .filter(|checkpoint| !is_iterator_checkpoint(checkpoint))
        {
            upsert_checkpoint_health(
                &mut self.checkpoints,
                CheckpointHealth {
                    source_table_name: checkpoint.source_table_name.clone(),
                    shard_id: checkpoint.shard_id.clone(),
                    position: checkpoint.position.clone(),
                    updated_at_ms: observed_at_ms,
                },
            );
        }
    }
}

pub(crate) fn apply_source_durable_checkpoints(
    health: &mut SourceHealth,
    checkpoints: &[StorageSourceCheckpoint],
) {
    health.checkpoints.clear();
    health.merge_source_checkpoints(checkpoints, None);
    metrics::gauge!(SOURCE_CHECKPOINTS_METRIC).set(health.checkpoints.len() as f64);
}

pub(crate) fn upsert_checkpoint_health(
    checkpoints: &mut Vec<CheckpointHealth>,
    next: CheckpointHealth,
) {
    if let Some(existing) = checkpoints.iter_mut().find(|checkpoint| {
        checkpoint.source_table_name == next.source_table_name
            && checkpoint.shard_id == next.shard_id
    }) {
        *existing = next;
        return;
    }
    checkpoints.push(next);
}
