use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, SourceBatchCommit,
    SourceBatchExecution, SourceBatchFailureContext, SourceBatchIngestOutcome,
    SourceBatchTransactionContext, SourceEffectDecision, execution::SourceBatchFence,
    transaction::EngineTransaction,
};
use crate::source_effect::{
    self, SourceEffectDisposition, SourceEffectRequest, SourceEffectRequestParts,
    SourceEffectSummary,
};

impl AnalyticsEngine {
    /// Applies one validated source scope under a destination fence. Rows,
    /// quarantine, checkpoints, authority, and the durable effect receipt are
    /// committed together, or the worker receives a stale-fence disposition.
    pub fn ingest_fenced_source_batch(
        &self,
        request: SourceEffectRequest<'_>,
    ) -> AnalyticsEngineResult<SourceEffectDisposition> {
        if !self.destination_fence.supports_sources() {
            return Err(AnalyticsEngineError::SourceEffectFenceUnavailable);
        }
        let SourceEffectRequestParts {
            manifest,
            manifest_digest,
            authority,
            scope,
            records,
            checkpoints,
            privacy_policy: policy,
        } = request.into_parts();
        let content_digest = source_effect::content_digest(source_effect::ContentDigestInput {
            manifest_digest: &manifest_digest,
            previous_position: scope.table_previous_position(),
            records: &records,
            checkpoints: &checkpoints,
            policy,
        });
        match self.ingest_source_batch_core(SourceBatchExecution {
            manifest,
            records,
            checkpoints,
            policy,
            fence: Some(SourceBatchFence {
                authority,
                scope,
                content_digest,
            }),
        })? {
            SourceBatchCommit::Applied { summary, .. } => {
                Ok(SourceEffectDisposition::Applied { summary })
            }
            SourceBatchCommit::AlreadyApplied { summary } => {
                Ok(SourceEffectDisposition::AlreadyApplied { summary })
            }
            SourceBatchCommit::StaleFence => Ok(SourceEffectDisposition::StaleFence),
        }
    }

    pub(super) fn ingest_source_batch_core(
        &self,
        execution: SourceBatchExecution<'_>,
    ) -> AnalyticsEngineResult<SourceBatchCommit> {
        let fenced = execution.fence.is_some();
        let _source_process_guard = if fenced {
            self.repair_receipt_lock
                .as_ref()
                .map(|lock| {
                    lock.lock().map_err(|_| {
                        AnalyticsEngineError::Io(std::io::Error::other(
                            "source effect lock is poisoned",
                        ))
                    })
                })
                .transpose()?
        } else {
            None
        };
        let _source_file_guard = if fenced {
            self.lock_repair_receipt_file()?
        } else {
            None
        };

        self.run_source_batch_transaction(execution)
    }

    fn run_source_batch_transaction(
        &self,
        execution: SourceBatchExecution<'_>,
    ) -> AnalyticsEngineResult<SourceBatchCommit> {
        let mut execution = execution;
        let mut transaction = if execution.fence.is_some() {
            EngineTransaction::begin_fenced(self)?
        } else {
            EngineTransaction::begin(self)?
        };
        let mut failure_context = None;
        let result = self.apply_source_batch_transaction(
            &mut execution,
            &mut failure_context,
            &mut transaction,
        );
        self.finish_source_batch_transaction(
            result,
            SourceBatchTransactionContext {
                fence: execution.fence.as_ref(),
                failure_context,
            },
            transaction,
        )
    }

    fn finish_source_batch_transaction(
        &self,
        result: AnalyticsEngineResult<SourceBatchCommit>,
        context: SourceBatchTransactionContext<'_>,
        transaction: EngineTransaction<'_>,
    ) -> AnalyticsEngineResult<SourceBatchCommit> {
        match result {
            Ok(commit @ SourceBatchCommit::AlreadyApplied { .. })
            | Ok(commit @ SourceBatchCommit::StaleFence) => {
                let _ = transaction.rollback();
                Ok(commit)
            }
            Ok(applied @ SourceBatchCommit::Applied { .. }) => {
                transaction.commit()?;
                Ok(applied)
            }
            Err(error) => {
                let _ = transaction.rollback();
                self.handle_source_batch_error(error, context)
            }
        }
    }

    fn handle_source_batch_error(
        &self,
        error: AnalyticsEngineError,
        context: SourceBatchTransactionContext<'_>,
    ) -> AnalyticsEngineResult<SourceBatchCommit> {
        if error.is_record_contract_failure()
            && let (Some(fence), Some(failure_context)) =
                (context.fence, context.failure_context.as_ref())
        {
            return match self.persist_fenced_source_batch_failure(
                &fence.scope,
                &fence.authority,
                failure_context,
                &error,
            ) {
                Ok(true) => Err(error),
                Ok(false) => Ok(SourceBatchCommit::StaleFence),
                Err(record_error) => Err(AnalyticsEngineError::SourceBatchFailureNotRecorded {
                    message: record_error.to_string(),
                }),
            };
        }
        if context.fence.is_none() {
            return self.record_source_batch_error(error, context.failure_context.as_ref());
        }
        Err(error)
    }

    fn apply_source_batch_transaction(
        &self,
        execution: &mut SourceBatchExecution<'_>,
        failure_context: &mut Option<SourceBatchFailureContext>,
        transaction: &mut EngineTransaction<'_>,
    ) -> AnalyticsEngineResult<SourceBatchCommit> {
        if let Some(fence) = execution.fence.as_ref() {
            match self.source_effect_decision(
                &fence.scope,
                &fence.authority,
                &fence.content_digest,
            )? {
                SourceEffectDecision::AlreadyApplied { summary } => {
                    return Ok(SourceBatchCommit::AlreadyApplied { summary });
                }
                SourceEffectDecision::StaleFence => return Ok(SourceBatchCommit::StaleFence),
                SourceEffectDecision::New => {}
            }
        }
        self.reject_blocked_source_batch(
            execution.records.iter().map(|record| &record.item),
            execution.checkpoints.as_slice(),
        )?;
        let source_owners = super::source_record_state_batch::SourceRecordStateBatch::try_new(
            self,
            execution,
            failure_context,
        )?;

        let system_owner_authorized = execution
            .fence
            .as_ref()
            .is_some_and(|fence| fence.scope.is_system());
        let (outcomes, dropped_fields) = self.apply_source_batch_records(
            execution,
            failure_context,
            system_owner_authorized,
            transaction,
        )?;
        if let Some(source_owners) = source_owners {
            source_owners.flush()?;
        }
        self.write_source_batch_checkpoints(execution.checkpoints.as_slice())?;
        if let Some(fence) = execution.fence.as_ref()
            && let Some((source_scope, checkpoint)) = fence.scope.system_parts()
        {
            self.write_system_stream_checkpoint(source_scope, checkpoint)?;
        }
        let outcome = SourceBatchIngestOutcome {
            outcomes,
            dropped_fields,
        };
        let checkpoint_count = execution.checkpoints.len()
            + usize::from(
                execution
                    .fence
                    .as_ref()
                    .is_some_and(|fence| fence.scope.is_system()),
            );
        let summary = SourceEffectSummary::from_ingest(&outcome, checkpoint_count);
        if let Some(fence) = execution.fence.as_ref() {
            source_effect::insert_receipt(
                &self.conn,
                &fence.scope,
                &fence.authority,
                &summary,
                &fence.content_digest,
            )?;
            source_effect::replace_authority(&self.conn, &fence.scope, &fence.authority)?;
        }
        Ok(SourceBatchCommit::Applied { outcome, summary })
    }
}
