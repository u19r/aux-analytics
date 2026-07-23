use analytics_api::{CheckpointHealth, SourceHealth, SourceHealthStatus, SourcePollingPhase};
use analytics_storage::SourceCheckpoint as StorageSourceCheckpoint;

use crate::source_polling::{batch::is_iterator_checkpoint, metrics::*};

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
    health.lease_token = None;
    health.lease_until_ms = None;
    health.last_error_at_ms = Some(observed_at_ms);
    health.last_error = Some(error);
    health.total_poll_errors = health.total_poll_errors.saturating_add(1);
}

pub(crate) fn apply_source_job_phase(
    health: &mut SourceHealth,
    phase: SourcePollingPhase,
    worker_id: &str,
    lease_token: Option<&str>,
    lease_until_ms: Option<i64>,
    observed_at_ms: u128,
) {
    health.poller_enabled = true;
    health.job_id = Some(SOURCE_POLLING_JOB_ID.to_string());
    health.worker_id = Some(worker_id.to_string());
    health.lease_token = lease_token.map(str::to_string);
    health.phase = phase;
    health.phase_started_at_ms = Some(observed_at_ms);
    health.lease_until_ms = lease_until_ms;
    health.status = match health.phase {
        SourcePollingPhase::Disabled => SourceHealthStatus::Disabled,
        SourcePollingPhase::Starting
        | SourcePollingPhase::WaitingForLease
        | SourcePollingPhase::Standby
        | SourcePollingPhase::LeaseHeld
        | SourcePollingPhase::Renewing
        | SourcePollingPhase::RefreshingPlan
        | SourcePollingPhase::RebuildingPoller
        | SourcePollingPhase::Polling
        | SourcePollingPhase::Ingesting
        | SourcePollingPhase::Checkpointing => SourceHealthStatus::Starting,
        SourcePollingPhase::LeaseLost | SourcePollingPhase::Timeout => SourceHealthStatus::Degraded,
        SourcePollingPhase::Healthy => SourceHealthStatus::Healthy,
        SourcePollingPhase::Degraded => SourceHealthStatus::Degraded,
    };
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_source_success_health(
    health: &mut SourceHealth,
    all_records_ingested: bool,
    records_ingested: u64,
    ingest_errors: u64,
    checkpoints_saved: u64,
    checkpoint_errors: u64,
    checkpoints: &[StorageSourceCheckpoint],
    observed_at_ms: u128,
) {
    health.status = if all_records_ingested {
        SourceHealthStatus::Healthy
    } else {
        SourceHealthStatus::Degraded
    };
    health.phase = if all_records_ingested {
        SourcePollingPhase::Healthy
    } else {
        SourcePollingPhase::Degraded
    };
    health.phase_started_at_ms = Some(observed_at_ms);
    health.lease_until_ms = None;
    health.last_success_at_ms = Some(observed_at_ms);
    if all_records_ingested {
        health.last_error = None;
    }
    health.total_records_ingested = health
        .total_records_ingested
        .saturating_add(records_ingested);
    health.total_ingest_errors = health.total_ingest_errors.saturating_add(ingest_errors);
    health.total_checkpoints_saved = health
        .total_checkpoints_saved
        .saturating_add(checkpoints_saved);
    health.total_checkpoint_errors = health
        .total_checkpoint_errors
        .saturating_add(checkpoint_errors);
    for checkpoint in checkpoints {
        if is_iterator_checkpoint(checkpoint) {
            continue;
        }
        upsert_checkpoint_health(
            &mut health.checkpoints,
            CheckpointHealth {
                source_table_name: checkpoint.source_table_name.clone(),
                shard_id: checkpoint.shard_id.clone(),
                position: checkpoint.position.clone(),
                updated_at_ms: Some(observed_at_ms),
            },
        );
    }
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
