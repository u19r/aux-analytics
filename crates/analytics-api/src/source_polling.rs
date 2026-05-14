use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use analytics_api::{AppState, CheckpointHealth, SourceHealth, SourceHealthStatus};
use analytics_storage::{PollBatch, SourceCheckpoint as StorageSourceCheckpoint, SourcePoller};
use config::AnalyticsSourceConfig;

use crate::error::ApiResult;

const SOURCE_POLLS_TOTAL_METRIC: &str = "analytics.source.polls_total";
const SOURCE_POLL_ERRORS_TOTAL_METRIC: &str = "analytics.source.poll_errors_total";
const SOURCE_RECORDS_PER_POLL_METRIC: &str = "analytics.source.records_per_poll";
const SOURCE_RECORDS_INGESTED_TOTAL_METRIC: &str = "analytics.source.records_ingested_total";
const SOURCE_INGEST_ERRORS_TOTAL_METRIC: &str = "analytics.source.ingest_errors_total";
const SOURCE_CHECKPOINTS_SAVED_TOTAL_METRIC: &str = "analytics.source.checkpoints_saved_total";
const SOURCE_CHECKPOINT_ERRORS_TOTAL_METRIC: &str = "analytics.source.checkpoint_errors_total";
const SOURCE_CHECKPOINTS_METRIC: &str = "analytics.source.checkpoints";

pub(crate) async fn spawn_source_polling(
    source: &AnalyticsSourceConfig,
    app_state: Arc<AppState>,
) -> ApiResult<()> {
    if source_polling_startup(source.tables.len(), false) == SourcePollingStartup::Disabled {
        *app_state.source_health.write().await = SourceHealth::disabled();
        tracing::info!("analytics source polling disabled: no source tables configured");
        return Ok(());
    }
    *app_state.source_health.write().await = SourceHealth::starting(source.tables.len());
    let checkpoints = {
        let engine = app_state.engine.lock().await;
        engine.load_source_checkpoints()?
    };
    let storage_checkpoints = storage_checkpoints_from_engine(checkpoints);
    let poller =
        SourcePoller::from_config(source, app_state.manifest.as_ref(), &storage_checkpoints)
            .await?;
    if source_polling_startup(source.tables.len(), poller.is_empty())
        == SourcePollingStartup::Disabled
    {
        *app_state.source_health.write().await = SourceHealth::disabled();
        tracing::info!("analytics source polling disabled: no pollable source tables");
        return Ok(());
    }
    tokio::spawn(run_source_poller(poller, app_state));
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourcePollingStartup {
    Disabled,
    Starting,
}

pub(crate) fn source_polling_startup(
    configured_table_count: usize,
    poller_is_empty: bool,
) -> SourcePollingStartup {
    if configured_table_count == 0 || poller_is_empty {
        SourcePollingStartup::Disabled
    } else {
        SourcePollingStartup::Starting
    }
}

pub(crate) fn storage_checkpoints_from_engine(
    checkpoints: Vec<analytics_engine::SourceCheckpoint>,
) -> Vec<StorageSourceCheckpoint> {
    checkpoints
        .into_iter()
        .map(|checkpoint| StorageSourceCheckpoint {
            source_table_name: checkpoint.source_table_name,
            shard_id: checkpoint.shard_id,
            position: checkpoint.position,
        })
        .collect()
}

pub(crate) fn apply_source_poll_error_health(
    health: &mut SourceHealth,
    error: String,
    observed_at_ms: u128,
) {
    health.status = SourceHealthStatus::Degraded;
    health.last_error_at_ms = Some(observed_at_ms);
    health.last_error = Some(error);
    health.total_poll_errors = health.total_poll_errors.saturating_add(1);
}

async fn run_source_poller(mut poller: SourcePoller, app_state: Arc<AppState>) {
    let mut interval = tokio::time::interval(poller.poll_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        {
            let mut health = app_state.source_health.write().await;
            health.last_poll_started_at_ms = Some(now_ms());
            health.total_polls = health.total_polls.saturating_add(1);
        }
        metrics::counter!(SOURCE_POLLS_TOTAL_METRIC).increment(1);
        match poller.poll_once().await {
            Ok(batch) => {
                let outcome = handle_source_batch(&app_state, &batch).await;
                if outcome.should_commit {
                    poller.commit(&batch.checkpoints);
                }
                update_source_success_health(
                    &app_state,
                    outcome.all_records_ingested,
                    outcome.records_ingested,
                    outcome.ingest_errors,
                    outcome.checkpoints_saved,
                    outcome.checkpoint_errors,
                    &batch.checkpoints,
                )
                .await;
            }
            Err(error) => {
                metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                {
                    let mut health = app_state.source_health.write().await;
                    apply_source_poll_error_health(&mut health, error.to_string(), now_ms());
                }
                tracing::warn!(error = %error, "analytics source polling failed");
            }
        }
        let checkpoint_count = usize_to_f64(app_state.source_health.read().await.checkpoints.len());
        metrics::gauge!(SOURCE_CHECKPOINTS_METRIC).set(checkpoint_count);
    }
}

async fn handle_source_batch(app_state: &Arc<AppState>, batch: &PollBatch) -> SourceBatchOutcome {
    metrics::histogram!(SOURCE_RECORDS_PER_POLL_METRIC).record(usize_to_f64(batch.records.len()));
    let mut ingest_results = Vec::with_capacity(batch.records.len());
    for record in &batch.records {
        let retention = if let Some(runtime) = app_state.retention.as_ref() {
            runtime
                .retention_for_record(
                    app_state.manifest.as_ref(),
                    record.analytics_table_name.as_str(),
                    &record.record,
                )
                .await
        } else {
            None
        };
        let engine = app_state.engine.lock().await;
        if let Err(error) = engine.ingest_stream_record_with_retention(
            app_state.manifest.as_ref(),
            record.analytics_table_name.as_str(),
            record.record_key.as_bytes(),
            record.record.clone(),
            retention.as_ref(),
        ) {
            ingest_results.push(false);
            metrics::counter!(SOURCE_INGEST_ERRORS_TOTAL_METRIC).increment(1);
            tracing::warn!(
                analytics_table_name = record.analytics_table_name,
                error = %error,
                "failed to ingest polled analytics stream record"
            );
        } else {
            ingest_results.push(true);
            metrics::counter!(SOURCE_RECORDS_INGESTED_TOTAL_METRIC).increment(1);
        }
    }

    let mut checkpoint_results = Vec::new();
    if ingest_results.iter().all(|result| *result) {
        let engine = app_state.engine.lock().await;
        for checkpoint in persistable_checkpoints(&batch.checkpoints) {
            if let Err(error) = engine.save_source_checkpoint(&analytics_engine::SourceCheckpoint {
                source_table_name: checkpoint.source_table_name.clone(),
                shard_id: checkpoint.shard_id.clone(),
                position: checkpoint.position.clone(),
            }) {
                checkpoint_results.push(false);
                metrics::counter!(SOURCE_CHECKPOINT_ERRORS_TOTAL_METRIC).increment(1);
                tracing::warn!(error = %error, "failed to save analytics source checkpoint");
                break;
            }
            checkpoint_results.push(true);
            metrics::counter!(SOURCE_CHECKPOINTS_SAVED_TOTAL_METRIC).increment(1);
        }
    }

    source_batch_outcome(&ingest_results, &checkpoint_results)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceBatchOutcome {
    pub(crate) all_records_ingested: bool,
    pub(crate) should_commit: bool,
    pub(crate) records_ingested: u64,
    pub(crate) ingest_errors: u64,
    pub(crate) checkpoints_saved: u64,
    pub(crate) checkpoint_errors: u64,
}

pub(crate) fn source_batch_outcome(
    ingest_results: &[bool],
    checkpoint_results: &[bool],
) -> SourceBatchOutcome {
    let records_ingested = ingest_results.iter().filter(|result| **result).count() as u64;
    let ingest_errors = ingest_results.iter().filter(|result| !**result).count() as u64;
    let checkpoints_saved = checkpoint_results.iter().filter(|result| **result).count() as u64;
    let checkpoint_errors = checkpoint_results.iter().filter(|result| !**result).count() as u64;
    let all_records_ingested = ingest_errors == 0 && checkpoint_errors == 0;
    SourceBatchOutcome {
        all_records_ingested,
        should_commit: all_records_ingested,
        records_ingested,
        ingest_errors,
        checkpoints_saved,
        checkpoint_errors,
    }
}

#[cfg(test)]
pub(crate) fn source_batch_integration_outcome(
    ingest_results: &[bool],
    checkpoints: &[StorageSourceCheckpoint],
    checkpoint_save_results: &[bool],
) -> SourceBatchOutcome {
    let mut attempted_checkpoint_results = Vec::new();
    if ingest_results.iter().all(|result| *result) {
        for (index, _checkpoint) in persistable_checkpoints(checkpoints).enumerate() {
            let result = checkpoint_save_results.get(index).copied().unwrap_or(true);
            attempted_checkpoint_results.push(result);
            if !result {
                break;
            }
        }
    }
    source_batch_outcome(ingest_results, &attempted_checkpoint_results)
}

pub(crate) fn persistable_checkpoints(
    checkpoints: &[StorageSourceCheckpoint],
) -> impl Iterator<Item = &StorageSourceCheckpoint> {
    checkpoints
        .iter()
        .filter(|checkpoint| !is_iterator_checkpoint(checkpoint))
}

pub(crate) fn is_iterator_checkpoint(checkpoint: &StorageSourceCheckpoint) -> bool {
    checkpoint.shard_id.starts_with("__iterator:")
}

async fn update_source_success_health(
    app_state: &Arc<AppState>,
    all_records_ingested: bool,
    records_ingested: u64,
    ingest_errors: u64,
    checkpoints_saved: u64,
    checkpoint_errors: u64,
    checkpoints: &[StorageSourceCheckpoint],
) {
    let observed_at_ms = now_ms();
    let mut health = app_state.source_health.write().await;
    apply_source_success_health(
        &mut health,
        all_records_ingested,
        records_ingested,
        ingest_errors,
        checkpoints_saved,
        checkpoint_errors,
        checkpoints,
        observed_at_ms,
    );
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

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}
