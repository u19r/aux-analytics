use super::*;

impl<'a> MaintenanceOperations<'a> {
    pub(super) fn finish_execution_effect<T: serde::Serialize>(
        &self,
        attempt: &ExecutionAttempt,
        effect_key: &OperationChunkKey,
        report: ExecutionReport<'_, T>,
    ) -> MaintenanceResult<()> {
        // Serialization can fail. Complete it before reserving a durable
        // effect.
        let write = report.into_write()?;
        let expected = self.store.show_operation(attempt.operation_id())?;
        if !self
            .store
            .claim_effect_for_attempt(&expected, attempt, effect_key)?
        {
            return Err(MaintenanceError::OperationInProgress {
                operation_id: expected.operation_id.clone(),
            });
        }
        let lease = ExecutionEffectLease {
            expected,
            claimed: true,
        };
        self.execution_effects(attempt)
            .persist_report(&lease, effect_key, write)
    }
}

pub(super) struct ExecutionReport<'a, T> {
    pub(super) kind: OperationReportKind,
    pub(super) report: &'a T,
    pub(super) counts: serde_json::Value,
    pub(super) message: &'static str,
}

impl<T: serde::Serialize> ExecutionReport<'_, T> {
    fn into_write(self) -> MaintenanceResult<crate::store::OperationReportWrite<'static>> {
        Ok(crate::store::OperationReportWrite {
            kind: self.kind,
            document: serde_json::to_value(self.report)?,
            transition: crate::OperationTransition {
                phase: OperationPhase::Completed,
                status: OperationStatus::Succeeded,
                cursor: None,
                event_kind: OperationEventKind::Succeeded,
                counts: self.counts,
                message: Some(self.message),
            },
        })
    }
}
