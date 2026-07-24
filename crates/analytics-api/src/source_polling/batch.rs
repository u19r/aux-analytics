use std::{sync::Arc, time::Instant};

use analytics_api::AppState;
use analytics_storage::{
    AuxStorageLeaseClient, PollBatch, SourceCheckpoint as StorageSourceCheckpoint,
};

use crate::source_polling::{
    checkpoint_lease::source_polling_lease_permits_progress,
    health::apply_source_success_health,
    legacy_lease::SourcePollingLeaseRenewal,
    memory::{BatchResidentMemorySampler, record_batch_memory_increment},
    metrics::*,
    time::{now_ms, usize_to_f64},
};
pub(crate) async fn handle_source_batch(
    app_state: &Arc<AppState>,
    batch: &PollBatch,
    lease_client: Option<&Arc<AuxStorageLeaseClient>>,
    lease_renewal: Option<&SourcePollingLeaseRenewal>,
) -> SourceBatchOutcome {
    metrics::counter!(SOURCE_RECORDS_ROUTED_TOTAL_METRIC).increment(batch.records.len() as u64);
    metrics::histogram!(SOURCE_RECORDS_PER_POLL_METRIC).record(usize_to_f64(batch.records.len()));
    if app_state.privacy_policy.is_none() && app_state.retention.is_none() {
        return handle_vector_source_batch(app_state, batch, lease_client, lease_renewal).await;
    }
    let mut ingest_results = Vec::with_capacity(batch.records.len());
    if !ingest_source_records_with_lease_guard(
        app_state,
        batch,
        lease_client,
        lease_renewal,
        &mut ingest_results,
    )
    .await
    {
        return source_batch_outcome_with_ownership_loss(&ingest_results, &[]);
    }

    let mut checkpoint_results = Vec::new();
    if ingest_results.iter().all(|result| *result) {
        if !source_polling_lease_permits_progress(lease_client, lease_renewal).await {
            return source_batch_outcome_with_ownership_loss(&ingest_results, &checkpoint_results);
        }
        for checkpoint in persistable_checkpoints(&batch.checkpoints) {
            if !source_polling_lease_permits_progress(lease_client, lease_renewal).await {
                return source_batch_outcome_with_ownership_loss(
                    &ingest_results,
                    &checkpoint_results,
                );
            }
            let checkpoint = analytics_engine::SourceCheckpoint {
                source_table_name: checkpoint.source_table_name.clone(),
                shard_id: checkpoint.shard_id.clone(),
                position: checkpoint.position.clone(),
            };
            if let Err(error) = app_state
                .engine
                .with_write(move |engine| engine.save_source_checkpoint(&checkpoint))
                .await
            {
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

async fn handle_vector_source_batch(
    app_state: &Arc<AppState>,
    batch: &PollBatch,
    lease_client: Option<&Arc<AuxStorageLeaseClient>>,
    lease_renewal: Option<&SourcePollingLeaseRenewal>,
) -> SourceBatchOutcome {
    let memory_sampler = BatchResidentMemorySampler::start();
    if !source_polling_lease_permits_progress(lease_client, lease_renewal).await {
        return source_batch_outcome_with_ownership_loss(&[], &[]);
    }
    let Some(checkpoint) = persistable_checkpoints(&batch.checkpoints).last() else {
        return source_batch_outcome(&[], &[]);
    };
    let engine_checkpoint = analytics_engine::SourceCheckpoint {
        source_table_name: checkpoint.source_table_name.clone(),
        shard_id: checkpoint.shard_id.clone(),
        position: checkpoint.position.clone(),
    };
    let records = batch
        .records
        .iter()
        .map(|record| analytics_engine::StreamRecordBatchItem {
            source_table_name: record.source_table_name.clone(),
            analytics_table_name: record.analytics_table_name.clone(),
            record_key: record.record_key.as_bytes().to_vec(),
            record: record.record.clone(),
        })
        .collect();
    let manifest = app_state.manifest.read().await.clone();
    let write_started = Instant::now();
    let result = app_state
        .engine
        .with_write(move |engine| {
            engine.ingest_stream_page_and_checkpoint(&manifest, records, &engine_checkpoint)
        })
        .await;
    if let Some(increment_bytes) = memory_sampler.finish().await {
        let peak_bytes = record_batch_memory_increment(increment_bytes);
        metrics::gauge!(SOURCE_BATCH_MEMORY_INCREMENT_BYTES_METRIC).set(peak_bytes as f64);
    }
    metrics::histogram!(SOURCE_WRITE_DURATION_MS_METRIC)
        .record(write_started.elapsed().as_secs_f64() * 1_000.0);
    match result {
        Ok(page) => {
            metrics::counter!(SOURCE_WRITE_TRANSACTIONS_TOTAL_METRIC).increment(1);
            let quarantined = page
                .quarantined
                .iter()
                .map(|record| record.input_index)
                .collect::<std::collections::BTreeSet<_>>();
            let valid_records = batch
                .records
                .iter()
                .enumerate()
                .filter_map(|(index, record)| (!quarantined.contains(&index)).then_some(record));
            for (record, outcome) in valid_records.zip(page.outcomes.iter().copied()) {
                record_source_ingest_outcome(record.analytics_table_name.as_str(), outcome);
            }
            for record in &page.quarantined {
                metrics::counter!(
                    SOURCE_QUARANTINED_TOTAL_METRIC,
                    "error_class" => record.error_class
                )
                .increment(1);
                tracing::warn!(
                    source_table_name = record.source_table_name,
                    analytics_table_name = record.analytics_table_name,
                    sequence_number = record.sequence_number,
                    error_class = record.error_class,
                    "quarantined deterministic analytics source record"
                );
            }
            metrics::counter!(SOURCE_CHECKPOINTS_SAVED_TOTAL_METRIC).increment(1);
            SourceBatchOutcome {
                all_records_ingested: true,
                should_commit: true,
                ownership_lost: false,
                records_ingested: page.outcomes.len() as u64,
                ingest_errors: 0,
                checkpoints_saved: 1,
                checkpoint_errors: 0,
            }
        }
        Err(error) => {
            metrics::counter!(SOURCE_INGEST_ERRORS_TOTAL_METRIC).increment(1);
            tracing::warn!(error = %error, "failed to ingest global analytics stream batch");
            SourceBatchOutcome {
                all_records_ingested: false,
                should_commit: false,
                ownership_lost: false,
                records_ingested: 0,
                ingest_errors: batch.records.len() as u64,
                checkpoints_saved: 0,
                checkpoint_errors: 0,
            }
        }
    }
}

async fn ingest_source_records_with_lease_guard(
    app_state: &Arc<AppState>,
    batch: &PollBatch,
    lease_client: Option<&Arc<AuxStorageLeaseClient>>,
    lease_renewal: Option<&SourcePollingLeaseRenewal>,
    ingest_results: &mut Vec<bool>,
) -> bool {
    for record in &batch.records {
        if !source_polling_lease_permits_progress(lease_client, lease_renewal).await {
            return false;
        }
        ingest_source_record(app_state, record, ingest_results).await;
    }
    true
}

async fn ingest_source_record(
    app_state: &Arc<AppState>,
    record: &analytics_storage::PolledRecord,
    ingest_results: &mut Vec<bool>,
) {
    let manifest = app_state.manifest.read().await.clone();
    let analytics_table_name = record.analytics_table_name.clone();
    let record_key = record.record_key.clone();
    let source_record = record.record.clone();
    let retention = if let Some(runtime) = app_state.retention.as_ref() {
        runtime
            .retention_for_record(
                &manifest,
                record.analytics_table_name.as_str(),
                &record.record,
            )
            .await
    } else {
        None
    };
    let ingest_result = if let Some(policy) = app_state.privacy_policy.clone() {
        app_state
            .engine
            .with_write(move |engine| {
                engine.ingest_stream_record_with_privacy_policy_and_retention(
                    &manifest,
                    analytics_table_name.as_str(),
                    record_key.as_bytes(),
                    source_record,
                    policy.as_ref(),
                    retention.as_ref(),
                )
            })
            .await
            .map(|outcome| {
                metrics::counter!(
                    SOURCE_PRIVACY_DROPS_TOTAL_METRIC,
                    "policy_version" => outcome.policy_version.clone()
                )
                .increment(outcome.dropped_fields);
                outcome.outcome
            })
    } else {
        app_state
            .engine
            .with_write(move |engine| {
                engine.ingest_stream_record_with_retention(
                    &manifest,
                    analytics_table_name.as_str(),
                    record_key.as_bytes(),
                    source_record,
                    retention.as_ref(),
                )
            })
            .await
    };
    match ingest_result {
        Ok(outcome) => {
            ingest_results.push(true);
            record_source_ingest_outcome(record.analytics_table_name.as_str(), outcome);
        }
        Err(error) => {
            ingest_results.push(false);
            metrics::counter!(SOURCE_INGEST_ERRORS_TOTAL_METRIC).increment(1);
            tracing::warn!(
                analytics_table_name = record.analytics_table_name,
                error = %error,
                "failed to ingest polled analytics stream record"
            );
        }
    }
}

fn record_source_ingest_outcome(
    analytics_table_name: &str,
    outcome: analytics_engine::IngestOutcome,
) {
    metrics::counter!(
        SOURCE_RECORDS_INGESTED_TOTAL_METRIC,
        "outcome" => source_ingest_outcome_label(outcome)
    )
    .increment(1);
    match outcome {
        analytics_engine::IngestOutcome::Inserted
        | analytics_engine::IngestOutcome::Updated
        | analytics_engine::IngestOutcome::Deleted => {
            metrics::counter!(SOURCE_RECORDS_COMMITTED_TOTAL_METRIC).increment(1);
        }
        analytics_engine::IngestOutcome::Skipped => {
            metrics::counter!(SOURCE_RECORDS_SKIPPED_TOTAL_METRIC).increment(1);
            tracing::debug!(
                analytics_table_name,
                "polled stream record was skipped by analytics ingestion"
            );
        }
    }
}

const fn source_ingest_outcome_label(outcome: analytics_engine::IngestOutcome) -> &'static str {
    match outcome {
        analytics_engine::IngestOutcome::Inserted => "inserted",
        analytics_engine::IngestOutcome::Updated => "updated",
        analytics_engine::IngestOutcome::Deleted => "deleted",
        analytics_engine::IngestOutcome::Skipped => "skipped",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceBatchOutcome {
    pub(crate) all_records_ingested: bool,
    pub(crate) should_commit: bool,
    pub(crate) ownership_lost: bool,
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
        ownership_lost: false,
        records_ingested,
        ingest_errors,
        checkpoints_saved,
        checkpoint_errors,
    }
}

pub(crate) fn source_batch_outcome_with_ownership_loss(
    ingest_results: &[bool],
    checkpoint_results: &[bool],
) -> SourceBatchOutcome {
    let mut outcome = source_batch_outcome(ingest_results, checkpoint_results);
    outcome.all_records_ingested = false;
    outcome.should_commit = false;
    outcome.ownership_lost = true;
    outcome
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

pub(crate) async fn update_source_success_health(
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
