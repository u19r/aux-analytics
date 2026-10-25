use analytics_contract::PrivacyPolicyMatcher;

use super::*;
use crate::OperationStoreError;

pub(crate) fn create_or_resume_operation(
    store: &OperationStore,
    request: &BackfillExecutionRequest,
    privacy_policy: Option<&PrivacyPolicy>,
) -> OperationStoreResult<()> {
    let mut payload = serde_json::json!({
        "mode": "local_backfill_execution",
    });
    if let Some(policy) = privacy_policy {
        payload["privacy_policy"] = serde_json::to_value(policy)?;
    }
    let operation_request = OperationRequest {
        operation_id: request.operation_id.clone(),
        kind: OperationKind::Backfill,
        actor: request.actor.clone(),
        target_tables: request.target_tables.clone(),
        dry_run: false,
        rate_limit: request.rate_limit.clone(),
        payload,
    };
    store
        .create_or_verify_operation(&operation_request)
        .map(|_| ())
}

pub(crate) fn recover_destination_failure(
    store: &OperationStore,
    expected: &StoredOperation,
    attempt: &crate::ExecutionAttempt,
    effect_key: &crate::OperationChunkKey,
) -> Result<(), String> {
    release_stale_effect_for_retry(store, expected, attempt, effect_key)?;
    let current = store
        .show_operation(&expected.operation_id)
        .map_err(|error| error.to_string())?;
    if current.status == OperationStatus::Running {
        store
            .assert_execution_attempt(&current, attempt)
            .map_err(|error| error.to_string())?;
        store
            .transition_with_counts_for_attempt(
                &current,
                attempt,
                crate::OperationTransition {
                    phase: OperationPhase::Executing,
                    status: OperationStatus::Paused,
                    cursor: current.cursor.clone(),
                    event_kind: OperationEventKind::Paused,
                    counts: current.progress.clone(),
                    message: Some("destination effect failed; operation paused for keyed retry"),
                },
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Release a pending destination key only with the attempt that claimed it.
/// A takeover invalidates this worker's fence; it must not release or pause
/// the replacement worker's state while recovering an uncertain destination
/// call.
pub(crate) fn release_stale_effect_for_retry(
    store: &OperationStore,
    expected: &StoredOperation,
    attempt: &crate::ExecutionAttempt,
    effect_key: &crate::OperationChunkKey,
) -> Result<(), String> {
    let current = if store.assert_execution_attempt(expected, attempt).is_ok() {
        expected.clone()
    } else {
        store
            .show_operation(&expected.operation_id)
            .map_err(|error| error.to_string())?
    };
    if current.execution_generation != attempt.generation()
        || current.execution_owner.as_deref() != Some(attempt.owner())
    {
        return Err(OperationStoreError::StaleOperationState {
            operation_id: expected.operation_id.clone(),
            expected_revision: expected.revision,
        }
        .to_string());
    }
    store
        .abandon_effect_for_attempt(&current, attempt, effect_key)
        .map_err(|error| error.to_string())
}

pub(crate) fn fail_after_error(
    store: &OperationStore,
    request: &BackfillExecutionRequest,
    attempt: &crate::ExecutionAttempt,
    error: BackfillExecutionError,
    message: &'static str,
) -> BackfillExecutionError {
    let result = (|| {
        let current = store.show_operation(&request.operation_id)?;
        store.transition_with_counts_for_attempt(
            &current,
            attempt,
            crate::OperationTransition {
                phase: OperationPhase::Completed,
                status: OperationStatus::Failed,
                cursor: current.cursor.clone(),
                event_kind: OperationEventKind::Failed,
                counts: serde_json::json!({}),
                message: Some(message),
            },
        )
    })();
    match result {
        Ok(()) => error,
        Err(persistence_error) => BackfillExecutionError::LifecyclePersistenceFailed {
            operation_error: error.to_string(),
            persistence_error: persistence_error.to_string(),
        },
    }
}

pub(crate) fn pause_for_retry(
    store: &OperationStore,
    request: &BackfillExecutionRequest,
    attempt: &crate::ExecutionAttempt,
    error: BackfillExecutionError,
    message: &'static str,
) -> BackfillExecutionError {
    let current = match store.show_operation(&request.operation_id) {
        Ok(current) => current,
        Err(persistence_error) => {
            return BackfillExecutionError::LifecyclePersistenceFailed {
                operation_error: error.to_string(),
                persistence_error: persistence_error.to_string(),
            };
        }
    };
    if current.status != OperationStatus::Running {
        return error;
    }
    let progress =
        match serde_json::from_value::<BackfillExecutionProgress>(current.progress.clone()) {
            Ok(mut progress) => {
                progress.retryable = true;
                match serde_json::to_value(progress) {
                    Ok(progress) => progress,
                    Err(persistence_error) => {
                        return BackfillExecutionError::LifecyclePersistenceFailed {
                            operation_error: error.to_string(),
                            persistence_error: persistence_error.to_string(),
                        };
                    }
                }
            }
            Err(persistence_error) => {
                return BackfillExecutionError::LifecyclePersistenceFailed {
                    operation_error: error.to_string(),
                    persistence_error: persistence_error.to_string(),
                };
            }
        };
    match store.transition_with_counts_for_attempt(
        &current,
        attempt,
        crate::OperationTransition {
            phase: OperationPhase::Executing,
            status: OperationStatus::Paused,
            cursor: current.cursor.clone(),
            event_kind: OperationEventKind::Paused,
            counts: progress,
            message: Some(message),
        },
    ) {
        Ok(()) => error,
        Err(persistence_error) => BackfillExecutionError::LifecyclePersistenceFailed {
            operation_error: error.to_string(),
            persistence_error: persistence_error.to_string(),
        },
    }
}

pub(crate) fn ensure_operation_can_write(
    store: &OperationStore,
    request: &BackfillExecutionRequest,
    attempt: &crate::ExecutionAttempt,
) -> BackfillExecutionResult<()> {
    let operation = store.show_operation(&request.operation_id)?;
    match operation.status {
        OperationStatus::Running => {
            store.assert_execution_attempt(&operation, attempt)?;
            Ok(())
        }
        OperationStatus::Cancelling => {
            store.accept_cancellation_as(&request.operation_id, &request.actor)?;
            Err(BackfillExecutionError::Cancelled)
        }
        status => Err(BackfillExecutionError::Store(
            OperationStoreError::InvalidStatusTransition {
                operation_id: request.operation_id.clone(),
                current_status: status,
                next_status: OperationStatus::Running,
            },
        )),
    }
}

pub(crate) fn metrics_json(metrics: &BackfillExecutionMetrics) -> serde_json::Value {
    serde_json::to_value(BackfillExecutionProgress {
        metrics: metrics.clone(),
        retryable: false,
    })
    .unwrap_or_else(|_| serde_json::json!({}))
}

pub(crate) fn effective_rows_per_second(rows_written: u64, throttle_pause_ms: u64) -> u64 {
    if throttle_pause_ms == 0 {
        return rows_written;
    }
    rows_written.saturating_mul(1_000) / throttle_pause_ms
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PrivacyBackfillMetrics {
    pub(crate) dropped_fields: u64,
    pub(crate) dropped_records: u64,
}

pub(crate) fn filter_chunks(
    chunks: Vec<LocalBackfillChunk>,
    policy: &PrivacyPolicy,
) -> BackfillExecutionResult<(Vec<LocalBackfillChunk>, PrivacyBackfillMetrics)> {
    let matcher = policy.validated()?;
    let mut metrics = PrivacyBackfillMetrics::default();
    let mut filtered_chunks = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let (records, drops) = filter_records(chunk.records, &matcher);
        metrics.dropped_fields += drops.dropped_fields;
        metrics.dropped_records += drops.dropped_records;
        filtered_chunks.push(LocalBackfillChunk::new(chunk.chunk_id, records));
    }
    Ok((filtered_chunks, metrics))
}

pub(crate) fn filter_stream_updates(
    updates: Vec<LocalStreamUpdate>,
    policy: &PrivacyPolicy,
) -> BackfillExecutionResult<(Vec<LocalStreamUpdate>, PrivacyBackfillMetrics)> {
    let matcher = policy.validated()?;
    let mut metrics = PrivacyBackfillMetrics::default();
    let mut filtered_updates = Vec::with_capacity(updates.len());
    for update in updates {
        let (records, drops) = filter_records(update.records, &matcher);
        metrics.dropped_fields += drops.dropped_fields;
        metrics.dropped_records += drops.dropped_records;
        filtered_updates.push(LocalStreamUpdate::new(update.cursor, records));
    }
    Ok((filtered_updates, metrics))
}

pub(crate) fn filter_records(
    records: Vec<StorageStreamRecord>,
    matcher: &PrivacyPolicyMatcher<'_>,
) -> (Vec<StorageStreamRecord>, PrivacyBackfillMetrics) {
    let mut metrics = PrivacyBackfillMetrics::default();
    let mut filtered_records = Vec::with_capacity(records.len());
    for mut record in records {
        let had_keys = !record.keys.is_empty();
        let keys = matcher.filter_item(std::mem::take(&mut record.keys));
        metrics.dropped_fields += keys.dropped_fields;
        record.keys = keys.item;
        if let Some(old_image) = record.old_image.take() {
            let filtered = matcher.filter_item(old_image);
            metrics.dropped_fields += filtered.dropped_fields;
            record.old_image = Some(filtered.item);
        }
        if let Some(new_image) = record.new_image.take() {
            let filtered = matcher.filter_item(new_image);
            metrics.dropped_fields += filtered.dropped_fields;
            record.new_image = Some(filtered.item);
        }
        if had_keys && record.keys.is_empty() {
            metrics.dropped_records += 1;
            continue;
        }
        filtered_records.push(record);
    }
    (filtered_records, metrics)
}
