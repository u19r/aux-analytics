use super::*;

impl<'a> MaintenanceOperations<'a> {
    /// Prepare one execution attempt, refusing a duplicate while the original
    /// executor owns the running operation. A succeeded retry reads its
    /// durable report instead of replaying side effects; resumable states use
    /// the store's canonical lifecycle transition and return its final state.
    pub(super) fn prepare_submission(
        &self,
        submission: &MaintenanceSubmission,
        actor: &OperationActor,
    ) -> MaintenanceResult<Option<(StoredOperation, ExecutionAttempt)>> {
        let operation = self.store.show_operation(&submission.operation_id)?;
        match operation.status {
            OperationStatus::Succeeded
                if submission.outcome == MaintenanceSubmissionOutcome::Existing =>
            {
                Ok(None)
            }
            _ => {
                let attempt = self
                    .store
                    .prepare_for_execution(&submission.operation_id, actor)
                    .map_err(|error| match error {
                        OperationStoreError::ExecutionAttemptInProgress { operation_id } => {
                            MaintenanceError::OperationInProgress { operation_id }
                        }
                        error => MaintenanceError::Store(error),
                    })?;
                let operation = self.store.show_operation(&submission.operation_id)?;
                Ok(Some((operation, attempt)))
            }
        }
    }

    pub(super) fn begin_execution_effect(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        actor: &OperationActor,
        label: &'static str,
    ) -> MaintenanceResult<(StoredOperation, OperationChunkKey)> {
        let expected = self.store.show_operation(&expected.operation_id)?;
        if attempt.actor() != actor {
            return Err(MaintenanceError::Store(
                OperationStoreError::UnauthorizedActor {
                    operation_id: expected.operation_id.clone(),
                    actor: actor.clone(),
                },
            ));
        }
        self.store.assert_execution_attempt(&expected, attempt)?;
        let report_label = format!("{label}-report");
        let key = OperationChunkKey::for_operation(&expected.operation_id, &report_label, 0);
        Ok((expected, key))
    }

    pub(super) fn execution_effects<'execution>(
        &'execution self,
        attempt: &'execution ExecutionAttempt,
    ) -> ExecutionEffects<'execution> {
        ExecutionEffects::new(self.store, attempt)
    }

    pub(super) fn fail_execution_after_effect<T>(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        error: MaintenanceError,
    ) -> MaintenanceResult<T> {
        let recovery = self.pause_failed_execution(expected, attempt);
        match recovery {
            Ok(()) => Err(error),
            Err(recovery_error) => Err(MaintenanceError::ExecutionTerminalUpdateFailed {
                operation_id: expected.operation_id.clone(),
                executor_error: error.to_string(),
                terminal_error: recovery_error.to_string(),
            }),
        }
    }
    fn pause_failed_execution(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
    ) -> Result<(), OperationStoreError> {
        let current = self.store.show_operation(&expected.operation_id)?;
        if current.status == OperationStatus::Running {
            self.store.assert_execution_attempt(&current, attempt)?;
            self.store.transition_with_counts_for_attempt(
                &current,
                attempt,
                crate::OperationTransition {
                    phase: OperationPhase::Executing,
                    status: OperationStatus::Paused,
                    cursor: current.cursor.clone(),
                    event_kind: crate::OperationEventKind::Paused,
                    counts: serde_json::json!({}),
                    message: Some("maintenance effect failed; retryable operation paused"),
                },
            )?;
        }
        Ok(())
    }
}
