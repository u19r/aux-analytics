use super::{transition::ensure_actor, *};

impl OperationStore {
    pub fn request_cancellation_as(
        &self,
        operation_id: &OperationId,
        actor: &OperationActor,
    ) -> OperationStoreResult<()> {
        let operation = self.show_operation(operation_id)?;
        ensure_actor(operation_id, &operation.actor, actor)?;
        self.transition_with_counts_from(
            &operation,
            crate::OperationTransition {
                phase: operation.phase,
                status: OperationStatus::Cancelling,
                cursor: operation.cursor.clone(),
                event_kind: OperationEventKind::CancellationRequested,
                counts: serde_json::json!({}),
                message: Some("cancellation requested"),
            },
        )
    }

    /// Accept a cancellation after checking the operation owner.
    pub fn accept_cancellation_as(
        &self,
        operation_id: &OperationId,
        actor: &OperationActor,
    ) -> OperationStoreResult<()> {
        let operation = self.show_operation(operation_id)?;
        ensure_actor(operation_id, &operation.actor, actor)?;
        self.transition_with_counts_from(
            &operation,
            crate::OperationTransition {
                phase: OperationPhase::Completed,
                status: OperationStatus::Cancelled,
                cursor: operation.cursor.clone(),
                event_kind: OperationEventKind::CancellationAccepted,
                counts: serde_json::json!({}),
                message: Some("cancellation accepted"),
            },
        )
    }

    pub fn fail_operation_as(
        &self,
        operation_id: &OperationId,
        actor: &OperationActor,
        phase: OperationPhase,
        message: Option<&str>,
    ) -> OperationStoreResult<()> {
        let operation = self.show_operation(operation_id)?;
        ensure_actor(operation_id, &operation.actor, actor)?;
        self.transition_with_counts_as(
            &operation,
            actor,
            crate::OperationTransition {
                phase,
                status: OperationStatus::Failed,
                cursor: operation.cursor.clone(),
                event_kind: OperationEventKind::Failed,
                counts: serde_json::json!({}),
                message,
            },
        )
    }
}
