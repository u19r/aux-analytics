use super::*;

pub(super) struct ExecutionEffectLease {
    pub(super) expected: StoredOperation,
    pub(super) claimed: bool,
}

pub(super) struct ExecutionEffects<'a> {
    store: &'a OperationStore,
    attempt: &'a ExecutionAttempt,
}

impl<'a> ExecutionEffects<'a> {
    pub(super) fn new(store: &'a OperationStore, attempt: &'a ExecutionAttempt) -> Self {
        Self { store, attempt }
    }

    pub(super) fn missing_effect_lease(&self, key: &OperationChunkKey) -> OperationStoreError {
        OperationStoreError::EffectInProgress {
            operation_id: self.attempt.operation_id().clone(),
            effect_key: key.as_str().to_owned(),
        }
    }

    pub(super) fn begin_effect(
        &self,
        key: &OperationChunkKey,
    ) -> Result<ExecutionEffectLease, OperationStoreError> {
        {
            let expected = self.store.show_operation(self.attempt.operation_id())?;
            let claimed = self.store.admit_effect_for_attempt(
                &expected,
                self.attempt,
                key,
                EffectAdmissionMode::MaintenanceReapply,
            )?;
            if let Err(error) = self.store.assert_execution_attempt(&expected, self.attempt) {
                let lease = ExecutionEffectLease { expected, claimed };
                let _ = self.abandon_effect(&lease, key);
                return Err(error);
            }
            Ok(ExecutionEffectLease { expected, claimed })
        }
    }

    pub(super) fn complete_effect(
        &self,
        lease: &ExecutionEffectLease,
        key: &OperationChunkKey,
    ) -> Result<(), OperationStoreError> {
        if !lease.claimed {
            return Ok(());
        }
        self.store.complete_effect_for_attempt(
            &lease.expected,
            self.attempt,
            key,
            crate::OperationEffectCompletion {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: lease.expected.cursor.clone(),
                counts: serde_json::json!({}),
                message: Some("maintenance effect committed"),
            },
        )?;
        Ok(())
    }

    pub(super) fn abandon_effect(
        &self,
        lease: &ExecutionEffectLease,
        key: &OperationChunkKey,
    ) -> Result<(), OperationStoreError> {
        if !lease.claimed {
            return Ok(());
        }
        match self
            .store
            .abandon_effect_for_attempt(&lease.expected, self.attempt, key)
        {
            Ok(()) => Ok(()),
            Err(OperationStoreError::StaleOperationState { .. }) => {
                // A same-operation progress update may legally win while the
                // destination call is in flight.  Re-read the durable state
                // before releasing the pending marker; otherwise an uncertain
                // external effect leaves the operation permanently
                // blocked behind an unreclaimable lease.
                let current = self.store.show_operation(self.attempt.operation_id())?;
                self.store
                    .abandon_effect_for_attempt(&current, self.attempt, key)
            }
            Err(error) => Err(error),
        }
    }
}

impl ExecutionEffects<'_> {
    pub(super) fn persist_report(
        &self,
        lease: &ExecutionEffectLease,
        key: &OperationChunkKey,
        write: crate::store::OperationReportWrite<'_>,
    ) -> MaintenanceResult<()> {
        let result = self
            .store
            .assert_execution_attempt(&lease.expected, self.attempt)
            .and_then(|()| {
                self.store.save_report_and_complete_effect_for_attempt(
                    &lease.expected,
                    self.attempt,
                    key,
                    write,
                )
            });
        let Err(error) = result else {
            return Ok(());
        };
        match self
            .store
            .abandon_effect_for_attempt(&lease.expected, self.attempt, key)
        {
            Ok(()) => Err(error.into()),
            Err(recovery_error) => Err(MaintenanceError::ExecutionTerminalUpdateFailed {
                operation_id: lease.expected.operation_id.clone(),
                executor_error: error.to_string(),
                terminal_error: recovery_error.to_string(),
            }),
        }
    }
}
