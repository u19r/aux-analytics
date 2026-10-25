use super::{batch::BackfillBatch, *};
use crate::enforce_rate_limit_with_limiter;

/// One attempt owns the snapshot, catch-up, and completion fences.
pub(super) struct BackfillExecution<'a, Destination, Clock> {
    pub(super) store: &'a OperationStore,
    pub(super) request: &'a BackfillExecutionRequest,
    pub(super) destination: &'a mut Destination,
    pub(super) attempt: crate::ExecutionAttempt,
    pub(super) clock: &'a Clock,
    pub(super) limiter: &'a OperationRateLimiter,
}

pub(super) struct PreparedBackfillRows<'a> {
    pub(super) chunks: &'a [LocalBackfillChunk],
    pub(super) stream_updates: &'a [LocalStreamUpdate],
    pub(super) privacy_policy_version: Option<String>,
    pub(super) privacy_metrics: PrivacyBackfillMetrics,
    pub(super) initial_status: OperationStatus,
}

impl<Destination: BackfillDestinationWriter, Clock: RateLimitClock>
    BackfillExecution<'_, Destination, Clock>
{
    pub(super) fn fetch_private_snapshot(
        &self,
        source: &impl SnapshotChunkSource,
        policy: &PrivacyPolicy,
    ) -> BackfillExecutionResult<(Vec<LocalBackfillChunk>, PrivacyBackfillMetrics)> {
        let chunks = self.fetch_snapshot(
            source,
            RateLimitUsage {
                rows: 1,
                source_requests: 1,
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
        )?;
        self.filter_privacy(filter_chunks(chunks, policy))
    }
    pub(super) fn fetch_private_stream(
        &self,
        source: &impl StreamCatchupSource,
        policy: &PrivacyPolicy,
    ) -> BackfillExecutionResult<(Vec<LocalStreamUpdate>, PrivacyBackfillMetrics)> {
        let updates = self.fetch_stream(
            source,
            RateLimitUsage {
                rows: 1,
                source_requests: 1,
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
        )?;
        self.filter_privacy(filter_stream_updates(updates, policy))
    }
    pub(super) fn fetch_snapshot(
        &self,
        source: &impl SnapshotChunkSource,
        usage: RateLimitUsage,
    ) -> BackfillExecutionResult<Vec<LocalBackfillChunk>> {
        self.reserve_source(usage, "backfill source rate limit denied")?;
        source.snapshot_chunks().map_err(|error| {
            pause_for_retry(
                self.store,
                self.request,
                &self.attempt,
                error,
                "backfill snapshot source failed",
            )
        })
    }
    pub(super) fn fetch_stream(
        &self,
        source: &impl StreamCatchupSource,
        usage: RateLimitUsage,
    ) -> BackfillExecutionResult<Vec<LocalStreamUpdate>> {
        self.reserve_source(usage, "backfill stream source rate limit denied")?;
        source.stream_updates_after_snapshot().map_err(|error| {
            pause_for_retry(
                self.store,
                self.request,
                &self.attempt,
                error,
                "backfill stream source failed",
            )
        })
    }
    fn reserve_source(
        &self,
        usage: RateLimitUsage,
        message: &'static str,
    ) -> BackfillExecutionResult<()> {
        enforce_rate_limit_with_limiter(self.limiter, usage, self.clock).map_err(|error| {
            pause_for_retry(
                self.store,
                self.request,
                &self.attempt,
                error.into(),
                message,
            )
        })
    }
    fn filter_privacy<T>(
        &self,
        filtered: BackfillExecutionResult<T>,
    ) -> BackfillExecutionResult<T> {
        filtered.map_err(|error| {
            fail_after_error(
                self.store,
                self.request,
                &self.attempt,
                error,
                "backfill privacy filtering failed",
            )
        })
    }
    pub(super) fn execute(
        mut self,
        mut rows: PreparedBackfillRows<'_>,
    ) -> BackfillExecutionResult<BackfillExecutionReport> {
        self.validate_sources(rows.chunks)?;
        let mut state = self.load_state(&mut rows)?;
        let resumed_from_chunk = state.checkpoint;
        for batch in rows
            .chunks
            .iter()
            .map(BackfillBatch::Snapshot)
            .chain(rows.stream_updates.iter().map(BackfillBatch::Stream))
        {
            if batch.is_completed(&state) {
                continue;
            }
            self.commit_batch(batch, &mut state)?;
        }
        self.finish_execution(state, resumed_from_chunk)
    }

    fn validate_sources(&self, chunks: &[LocalBackfillChunk]) -> BackfillExecutionResult<()> {
        let failure = if chunks.is_empty() {
            Some((
                BackfillExecutionError::EmptyFixture,
                "backfill fixture is empty",
            ))
        } else if self.request.rate_limit.max_parallel_chunks == 0 {
            Some((
                BackfillExecutionError::InvalidParallelism,
                "backfill parallelism is invalid",
            ))
        } else {
            None
        };
        if let Some((error, message)) = failure {
            return Err(fail_after_error(
                self.store,
                self.request,
                &self.attempt,
                error,
                message,
            ));
        }
        Ok(())
    }

    fn load_state(
        &self,
        rows: &mut PreparedBackfillRows<'_>,
    ) -> BackfillExecutionResult<BackfillExecutionState> {
        let mut state = BackfillExecutionState::from_store(
            self.store,
            &self.request.operation_id,
            self.request,
            rows.chunks,
        )
        .map_err(|error| {
            fail_after_error(
                self.store,
                self.request,
                &self.attempt,
                error.into(),
                "backfill progress could not be decoded",
            )
        })?;
        state.metrics.privacy_policy_version = rows.privacy_policy_version.take();
        state.metrics.privacy_dropped_fields = state
            .metrics
            .privacy_dropped_fields
            .max(rows.privacy_metrics.dropped_fields);
        state.metrics.privacy_dropped_records = state
            .metrics
            .privacy_dropped_records
            .max(rows.privacy_metrics.dropped_records);
        state.metrics.audit_events_emitted += match rows.initial_status {
            OperationStatus::Submitted => 2,
            OperationStatus::Planned | OperationStatus::Paused => 1,
            _ => 0,
        };
        Ok(state)
    }

    fn commit_batch(
        &mut self,
        batch: BackfillBatch<'_>,
        state: &mut BackfillExecutionState,
    ) -> BackfillExecutionResult<()> {
        let accounting = self.prepare_batch(batch, state)?;
        let expected = self.store.show_operation(&self.request.operation_id)?;
        let key = crate::OperationChunkKey::for_operation(
            &self.request.operation_id,
            batch.label(),
            batch.position(),
        );
        if !self
            .store
            .claim_effect_for_attempt(&expected, &self.attempt, &key)?
        {
            return Ok(());
        }
        self.assert_batch_fence(&expected, &key, batch.before_write_message())?;
        if accounting.rows > 0 {
            self.write_destination_batch(batch, &expected, &key)?;
        }
        self.assert_batch_fence(&expected, &key, batch.after_write_message())?;
        batch.apply_to_state(state, accounting.rows);
        state.metrics.audit_events_emitted += 1;
        self.complete_batch(batch, state, &expected, &key)
    }

    fn prepare_batch(
        &mut self,
        batch: BackfillBatch<'_>,
        state: &mut BackfillExecutionState,
    ) -> BackfillExecutionResult<BackfillBatchAccounting> {
        ensure_operation_can_write(self.store, self.request, &self.attempt)?;
        let accounting = batch.accounting().map_err(|error| {
            fail_after_error(
                self.store,
                self.request,
                &self.attempt,
                error,
                batch.encoding_failure_message(),
            )
        })?;
        self.retry_injected_chunk(batch, state)?;
        accounting
            .reserve_destination(self.limiter, self.clock)
            .map_err(|error| {
                pause_for_retry(
                    self.store,
                    self.request,
                    &self.attempt,
                    error.into(),
                    "backfill destination batch rate limit denied",
                )
            })?;
        Ok(accounting)
    }

    fn complete_batch(
        &self,
        batch: BackfillBatch<'_>,
        state: &BackfillExecutionState,
        expected: &StoredOperation,
        key: &crate::OperationChunkKey,
    ) -> BackfillExecutionResult<()> {
        self.store
            .complete_effect_for_attempt(
                expected,
                &self.attempt,
                key,
                crate::OperationEffectCompletion {
                    phase: OperationPhase::Executing,
                    status: OperationStatus::Running,
                    cursor: Some(OperationCursor {
                        label: batch.label().to_string(),
                        position: batch.position(),
                    }),
                    counts: metrics_json(&state.metrics),
                    message: Some(batch.commit_message()),
                },
            )
            .map_err(|error| {
                self.recover_stale_batch(
                    expected,
                    key,
                    error.into(),
                    batch.commit_failure_message(),
                )
            })
    }

    fn retry_injected_chunk(
        &mut self,
        batch: BackfillBatch<'_>,
        state: &mut BackfillExecutionState,
    ) -> BackfillExecutionResult<()> {
        let BackfillBatch::Snapshot(chunk) = batch else {
            return Ok(());
        };
        if self.request.fail_once_at_chunk != Some(chunk.chunk_id)
            || state.metrics.chunks_retried != 0
        {
            return Ok(());
        }
        state.mark_chunk_retried();
        let delay = self.limiter.retry_backoff_ms(state.metrics.chunks_retried);
        state.metrics.retry_delay_ms = state.metrics.retry_delay_ms.saturating_add(delay);
        state.metrics.throttle_pause_ms = state.metrics.throttle_pause_ms.saturating_add(delay);
        let current = self.store.show_operation(&self.request.operation_id)?;
        self.store.transition_with_counts_for_attempt(
            &current,
            &self.attempt,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Paused,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: state.checkpoint,
                }),
                event_kind: OperationEventKind::Paused,
                counts: metrics_json(&state.metrics),
                message: Some("chunk failed before destination commit; retrying"),
            },
        )?;
        self.attempt = self
            .store
            .prepare_for_execution(&self.request.operation_id, &self.request.actor)?;
        state.metrics.audit_events_emitted += 2;
        Ok(())
    }

    fn assert_batch_fence(
        &self,
        expected: &StoredOperation,
        key: &crate::OperationChunkKey,
        message: &'static str,
    ) -> BackfillExecutionResult<()> {
        self.store
            .assert_execution_attempt(expected, &self.attempt)
            .map_err(|error| self.recover_stale_batch(expected, key, error.into(), message))
    }

    fn recover_stale_batch(
        &self,
        expected: &StoredOperation,
        key: &crate::OperationChunkKey,
        error: BackfillExecutionError,
        message: &'static str,
    ) -> BackfillExecutionError {
        if let Err(recovery_error) =
            release_stale_effect_for_retry(self.store, expected, &self.attempt, key)
        {
            return BackfillExecutionError::EffectRecoveryFailed {
                destination_error: error.to_string(),
                recovery_error,
            };
        }
        pause_for_retry(self.store, self.request, &self.attempt, error, message)
    }

    fn write_destination_batch(
        &mut self,
        batch: BackfillBatch<'_>,
        expected: &StoredOperation,
        key: &crate::OperationChunkKey,
    ) -> BackfillExecutionResult<()> {
        let result = match batch {
            BackfillBatch::Snapshot(chunk) => self.destination.write_snapshot_chunk(key, chunk),
            BackfillBatch::Stream(update) => self.destination.write_stream_update(key, update),
        };
        result.map_err(|error| {
            match recover_destination_failure(self.store, expected, &self.attempt, key) {
                Ok(()) => error,
                Err(recovery_error) => BackfillExecutionError::EffectRecoveryFailed {
                    destination_error: error.to_string(),
                    recovery_error,
                },
            }
        })
    }

    fn finish_execution(
        &self,
        mut state: BackfillExecutionState,
        resumed_from_chunk: u64,
    ) -> BackfillExecutionResult<BackfillExecutionReport> {
        self.validate_output(&mut state)?;
        let report = BackfillExecutionReport {
            operation_id: self.request.operation_id.clone(),
            metrics: state.metrics,
            completed_chunks: state.completed_chunks.into_iter().collect(),
            checkpoint: state.checkpoint,
            resumed_from_chunk,
        };
        let validating = self.store.show_operation(&self.request.operation_id)?;
        self.store.save_backfill_report_and_succeed_for_attempt(
            &validating,
            &self.attempt,
            &report,
        )?;
        Ok(report)
    }
    fn validate_output(&self, state: &mut BackfillExecutionState) -> BackfillExecutionResult<()> {
        ensure_operation_can_write(self.store, self.request, &self.attempt)?;
        state.metrics.effective_rows_per_second =
            effective_rows_per_second(state.metrics.rows_written, state.metrics.throttle_pause_ms);
        state.metrics.validation_runs += 1;
        let current = self.store.show_operation(&self.request.operation_id)?;
        self.store.transition_with_counts_for_attempt(
            &current,
            &self.attempt,
            crate::OperationTransition {
                phase: OperationPhase::Validating,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "stream".to_string(),
                    position: state.checkpoint,
                }),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: metrics_json(&state.metrics),
                message: Some("stream catch-up applied and validation started"),
            },
        )?;
        state.metrics.audit_events_emitted += 2;
        Ok(())
    }
}
