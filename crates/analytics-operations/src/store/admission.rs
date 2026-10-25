use super::{
    connection::show_operation_from_connection,
    transition::{append_event_on, ensure_actor, transition_in_transaction_on},
    *,
};

pub(super) fn prepare_for_execution_on(
    conn: &Connection,
    operation_id: &OperationId,
    actor: &OperationActor,
    now_ms: i64,
) -> OperationStoreResult<ExecutionAttempt> {
    let operation = show_operation_from_connection(conn, operation_id)?;
    ensure_actor(operation_id, &operation.actor, actor)?;
    if operation.status == OperationStatus::Running && operation.execution_lease_until_ms > now_ms {
        return Err(OperationStoreError::ExecutionAttemptInProgress {
            operation_id: operation_id.clone(),
        });
    }

    let generation = operation.execution_generation.saturating_add(1).max(1);
    let attempt = ExecutionAttempt {
        operation_id: operation_id.clone(),
        actor: actor.clone(),
        generation,
        owner: format!("{}:{generation}", operation_id.as_str()),
    };
    if operation.status == OperationStatus::Running {
        attempt.take_over_operation(conn, &operation, now_ms)?;
        return Ok(attempt);
    }
    let operation = plan_submitted_operation(conn, operation)?;
    start_operation(conn, &operation)?;
    let running = show_operation_from_connection(conn, operation_id)?;
    attempt.assign_to_operation(conn, &running, now_ms)?;
    Ok(attempt)
}

fn plan_submitted_operation(
    conn: &Connection,
    operation: StoredOperation,
) -> OperationStoreResult<StoredOperation> {
    if operation.status != OperationStatus::Submitted {
        return Ok(operation);
    }
    transition_in_transaction_on(
        conn,
        &operation,
        crate::OperationTransition {
            phase: OperationPhase::Planning,
            status: OperationStatus::Planned,
            cursor: operation.cursor.clone(),
            event_kind: OperationEventKind::Planned,
            counts: serde_json::json!({}),
            message: Some("operation planned"),
        },
    )?;
    show_operation_from_connection(conn, &operation.operation_id)
}

fn start_operation(conn: &Connection, operation: &StoredOperation) -> OperationStoreResult<()> {
    let (event_kind, message) = match operation.status {
        OperationStatus::Planned => (OperationEventKind::Started, "operation started"),
        OperationStatus::Paused => (OperationEventKind::Resumed, "operation resumed"),
        status => {
            return Err(OperationStoreError::InvalidStatusTransition {
                operation_id: operation.operation_id.clone(),
                current_status: status,
                next_status: OperationStatus::Running,
            });
        }
    };
    transition_in_transaction_on(
        conn,
        operation,
        crate::OperationTransition {
            phase: OperationPhase::Executing,
            status: OperationStatus::Running,
            cursor: operation.cursor.clone(),
            event_kind,
            counts: serde_json::json!({}),
            message: Some(message),
        },
    )
}

impl ExecutionAttempt {
    fn assign_to_operation(
        &self,
        conn: &Connection,
        expected: &StoredOperation,
        now_ms: i64,
    ) -> OperationStoreResult<()> {
        let lease_until_ms = now_ms.saturating_add(EXECUTION_LEASE_MS);
        let updated = conn.execute(
            "UPDATE __analytics_operations
             SET execution_generation = ?, execution_owner = ?, execution_lease_until_ms = ?,
                 updated_at_ms = now_ms()
             WHERE operation_id = ? AND revision = ? AND phase = ? AND status = ?
               AND cancellation_state = ? AND cursor_json = ? AND progress_json = ?
               AND execution_generation = ? AND execution_owner = ?",
            params![
                i64::try_from(self.generation).unwrap_or(i64::MAX),
                &self.owner,
                lease_until_ms,
                expected.operation_id.as_str(),
                i64::try_from(expected.revision).unwrap_or(i64::MAX),
                expected.phase.as_str(),
                expected.status.as_str(),
                expected.cancellation_state.as_str(),
                serde_json::to_string(&expected.cursor)?,
                serde_json::to_string(&expected.progress)?,
                i64::try_from(expected.execution_generation).unwrap_or(i64::MAX),
                expected.execution_owner.as_deref().unwrap_or(""),
            ],
        )?;
        if updated != 1 {
            return Err(OperationStoreError::StaleOperationState {
                operation_id: expected.operation_id.clone(),
                expected_revision: expected.revision,
            });
        }
        Ok(())
    }

    fn take_over_operation(
        &self,
        conn: &Connection,
        expected: &StoredOperation,
        now_ms: i64,
    ) -> OperationStoreResult<()> {
        if self.write_takeover(conn, expected, now_ms)? != 1 {
            return Err(OperationStoreError::StaleOperationState {
                operation_id: expected.operation_id.clone(),
                expected_revision: expected.revision,
            });
        }
        append_event_on(
            conn,
            &expected.operation_id,
            crate::OperationTransition {
                event_kind: OperationEventKind::CursorAdvanced,
                phase: expected.phase,
                status: OperationStatus::Running,
                cursor: expected.cursor.clone(),
                counts: serde_json::json!({ "execution_takeover": true }),
                message: Some("expired execution lease taken over"),
            },
        )
    }

    fn write_takeover(
        &self,
        conn: &Connection,
        expected: &StoredOperation,
        now_ms: i64,
    ) -> OperationStoreResult<usize> {
        let lease_until_ms = now_ms.saturating_add(EXECUTION_LEASE_MS);
        Ok(conn.execute(
            "UPDATE __analytics_operations
             SET execution_generation = ?, execution_owner = ?, execution_lease_until_ms = ?,
                 updated_at_ms = now_ms(), revision = revision + 1
             WHERE operation_id = ? AND revision = ? AND phase = ? AND status = ?
               AND cancellation_state = ? AND cursor_json = ? AND progress_json = ?
               AND execution_generation = ? AND execution_owner = ?
               AND execution_lease_until_ms <= ?",
            params![
                i64::try_from(self.generation).unwrap_or(i64::MAX),
                &self.owner,
                lease_until_ms,
                expected.operation_id.as_str(),
                i64::try_from(expected.revision).unwrap_or(i64::MAX),
                expected.phase.as_str(),
                expected.status.as_str(),
                expected.cancellation_state.as_str(),
                serde_json::to_string(&expected.cursor)?,
                serde_json::to_string(&expected.progress)?,
                i64::try_from(expected.execution_generation).unwrap_or(i64::MAX),
                expected.execution_owner.as_deref().unwrap_or(""),
                now_ms,
            ],
        )?)
    }
}

pub(super) fn operation_matches_request(
    existing: &StoredOperation,
    request: &OperationRequest,
) -> bool {
    existing.operation_id == request.operation_id
        && existing.kind == request.kind
        && existing.actor == request.actor
        && existing.target_tables == request.target_tables
        && existing.dry_run == request.dry_run
        && existing.rate_limit == request.rate_limit
        && existing.payload == request.payload
}
