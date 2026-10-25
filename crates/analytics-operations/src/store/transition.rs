use super::*;

pub(super) fn append_event_on(
    conn: &Connection,
    operation_id: &OperationId,
    transition: crate::OperationTransition<'_>,
) -> OperationStoreResult<()> {
    let crate::OperationTransition {
        phase,
        status,
        cursor,
        event_kind: kind,
        counts,
        message,
    } = transition;
    conn.execute(
        "INSERT INTO __analytics_operation_events
            (operation_id, occurred_at_ms, kind, phase, status, cursor_json, counts_json,
             message)
         VALUES (?, now_ms(), ?, ?, ?, ?, ?, ?)",
        params![
            operation_id.as_str(),
            kind.as_str(),
            phase.as_str(),
            status.as_str(),
            serde_json::to_string(&cursor)?,
            serde_json::to_string(&counts)?,
            message,
        ],
    )?;
    Ok(())
}

pub(in crate::store) fn expected_state_exists_on(
    conn: &Connection,
    expected: &StoredOperation,
) -> OperationStoreResult<bool> {
    let cursor_json = serde_json::to_string(&expected.cursor)?;
    let progress_json = serde_json::to_string(&expected.progress)?;
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM __analytics_operations
         WHERE operation_id = ? AND revision = ? AND phase = ? AND status = ?
           AND cancellation_state = ? AND cursor_json = ? AND progress_json = ?
           AND execution_generation = ? AND execution_owner = ?
           AND (execution_generation = 0 OR execution_lease_until_ms > now_ms())",
        params![
            expected.operation_id.as_str(),
            i64::try_from(expected.revision).unwrap_or(i64::MAX),
            expected.phase.as_str(),
            expected.status.as_str(),
            expected.cancellation_state.as_str(),
            cursor_json,
            progress_json,
            i64::try_from(expected.execution_generation).unwrap_or(i64::MAX),
            expected.execution_owner.as_deref().unwrap_or(""),
        ],
        |row| row.get(0),
    )?;
    Ok(count == 1)
}

pub(super) fn transition_in_transaction_on(
    conn: &Connection,
    expected: &StoredOperation,
    transition: crate::OperationTransition<'_>,
) -> OperationStoreResult<()> {
    ensure_terminal_effects_resolved_on(conn, expected, transition.status)?;
    write_operation_state_on(conn, expected, &transition)?;
    append_event_on(conn, &expected.operation_id, transition)
}

fn ensure_terminal_effects_resolved_on(
    conn: &Connection,
    expected: &StoredOperation,
    status: OperationStatus,
) -> OperationStoreResult<()> {
    if expected.status != OperationStatus::Running || status == OperationStatus::Running {
        return Ok(());
    }
    // A pause may retain an expired generation's retryable marker. Other
    // terminal transitions require every pending effect to be resolved.
    let generation = (status == OperationStatus::Paused).then_some(expected.execution_generation);
    if let Some(effect_key) = pending_effect_key_on(conn, &expected.operation_id, generation)? {
        return Err(OperationStoreError::PendingEffect {
            operation_id: expected.operation_id.clone(),
            effect_key,
        });
    }
    Ok(())
}

fn write_operation_state_on(
    conn: &Connection,
    expected: &StoredOperation,
    transition: &crate::OperationTransition<'_>,
) -> OperationStoreResult<()> {
    let progress = if transition
        .counts
        .as_object()
        .is_some_and(serde_json::Map::is_empty)
    {
        &expected.progress
    } else {
        &transition.counts
    };
    let updated = conn.execute(
        crate::constants::UPDATE_OPERATION_STATE_SQL,
        params![
            transition.phase.as_str(),
            transition.status.as_str(),
            cancellation_state_for_transition(expected, transition.status).as_str(),
            serde_json::to_string(&transition.cursor)?,
            serde_json::to_string(progress)?,
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
    if updated == 0 {
        return Err(OperationStoreError::StaleOperationState {
            operation_id: expected.operation_id.clone(),
            expected_revision: expected.revision,
        });
    }
    Ok(())
}

fn pending_effect_key_on(
    conn: &Connection,
    operation_id: &OperationId,
    attempt_generation: Option<u64>,
) -> OperationStoreResult<Option<String>> {
    match attempt_generation {
        Some(attempt_generation) => conn
            .query_row(
                "SELECT effect_key FROM __analytics_operation_effects
                 WHERE operation_id = ? AND state = 'pending'
                   AND attempt_generation = ?
                 ORDER BY effect_key LIMIT 1",
                params![operation_id.as_str(), attempt_generation],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into),
        None => conn
            .query_row(
                "SELECT effect_key FROM __analytics_operation_effects
                 WHERE operation_id = ? AND state = 'pending'
                 ORDER BY effect_key LIMIT 1",
                params![operation_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into),
    }
}
pub(super) fn validate_transition(
    current: &StoredOperation,
    next_phase: OperationPhase,
    next_status: OperationStatus,
    event_kind: OperationEventKind,
) -> OperationStoreResult<()> {
    if !current.status.can_transition_to(next_status)
        || !next_phase.accepts_status(next_status)
        || !event_kind.permits_transition(current, next_phase, next_status)
    {
        return Err(OperationStoreError::InvalidStatusTransition {
            operation_id: current.operation_id.clone(),
            current_status: current.status,
            next_status,
        });
    }
    Ok(())
}

impl OperationStatus {
    fn can_transition_to(self, next_status: Self) -> bool {
        matches!(
            (self, next_status),
            (
                OperationStatus::Submitted,
                OperationStatus::Planned | OperationStatus::Failed
            ) | (
                OperationStatus::Planned,
                OperationStatus::Running | OperationStatus::Cancelling | OperationStatus::Failed
            ) | (
                OperationStatus::Running,
                OperationStatus::Running
                    | OperationStatus::Paused
                    | OperationStatus::Cancelling
                    | OperationStatus::Failed
                    | OperationStatus::Succeeded
            ) | (
                OperationStatus::Paused,
                OperationStatus::Running | OperationStatus::Cancelling
            ) | (OperationStatus::Cancelling, OperationStatus::Cancelled)
        )
    }
}

impl OperationPhase {
    fn accepts_status(self, next_status: OperationStatus) -> bool {
        matches!(
            (self, next_status),
            (OperationPhase::Submitted, OperationStatus::Submitted)
                | (OperationPhase::Planning, OperationStatus::Planned)
                | (OperationPhase::Planning, OperationStatus::Cancelling)
                | (
                    OperationPhase::Executing,
                    OperationStatus::Running | OperationStatus::Paused
                )
                | (
                    OperationPhase::Executing,
                    OperationStatus::Cancelling | OperationStatus::Failed
                )
                | (OperationPhase::Validating, OperationStatus::Running)
                | (
                    OperationPhase::Validating,
                    OperationStatus::Cancelling | OperationStatus::Failed
                )
                | (
                    OperationPhase::Completed,
                    OperationStatus::Cancelled
                        | OperationStatus::Failed
                        | OperationStatus::Succeeded
                )
        )
    }
}

impl OperationEventKind {
    fn permits_transition(
        self,
        current: &StoredOperation,
        next_phase: OperationPhase,
        next_status: OperationStatus,
    ) -> bool {
        match self {
            Self::Planned | Self::Started | Self::Resumed | Self::Paused => {
                self.permits_execution_transition(current.status, next_phase, next_status)
            }
            Self::CursorAdvanced => {
                current.status == OperationStatus::Running
                    && next_status == OperationStatus::Running
                    && matches!(
                        next_phase,
                        OperationPhase::Executing | OperationPhase::Validating
                    )
            }
            Self::CancellationRequested | Self::CancellationAccepted => {
                self.permits_cancellation_transition(current, next_phase, next_status)
            }
            Self::Failed => {
                matches!(
                    current.status,
                    OperationStatus::Submitted
                        | OperationStatus::Planned
                        | OperationStatus::Running
                ) && next_status == OperationStatus::Failed
            }
            Self::Succeeded => {
                current.status == OperationStatus::Running
                    && current.cancellation_state == CancellationState::NotRequested
                    && next_status == OperationStatus::Succeeded
                    && next_phase == OperationPhase::Completed
            }
            Self::Submitted => false,
        }
    }

    fn permits_execution_transition(
        self,
        status: OperationStatus,
        phase: OperationPhase,
        next: OperationStatus,
    ) -> bool {
        let required = match self {
            Self::Planned => (
                OperationStatus::Submitted,
                OperationPhase::Planning,
                OperationStatus::Planned,
            ),
            Self::Started => (
                OperationStatus::Planned,
                OperationPhase::Executing,
                OperationStatus::Running,
            ),
            Self::Resumed => (
                OperationStatus::Paused,
                OperationPhase::Executing,
                OperationStatus::Running,
            ),
            Self::Paused => (
                OperationStatus::Running,
                OperationPhase::Executing,
                OperationStatus::Paused,
            ),
            _ => return false,
        };
        (status, phase, next) == required
    }

    fn permits_cancellation_transition(
        self,
        current: &StoredOperation,
        phase: OperationPhase,
        next: OperationStatus,
    ) -> bool {
        match self {
            Self::CancellationRequested => {
                matches!(
                    current.status,
                    OperationStatus::Planned | OperationStatus::Running | OperationStatus::Paused
                ) && next == OperationStatus::Cancelling
            }
            Self::CancellationAccepted => {
                current.status == OperationStatus::Cancelling
                    && current.cancellation_state == CancellationState::Requested
                    && next == OperationStatus::Cancelled
                    && phase == OperationPhase::Completed
            }
            _ => false,
        }
    }
}

fn cancellation_state_for_transition(
    current: &StoredOperation,
    next_status: OperationStatus,
) -> CancellationState {
    match next_status {
        OperationStatus::Cancelling => CancellationState::Requested,
        OperationStatus::Cancelled => CancellationState::Accepted,
        _ => current.cancellation_state,
    }
}

pub(in crate::store) fn ensure_actor(
    operation_id: &OperationId,
    owner: &OperationActor,
    actor: &OperationActor,
) -> OperationStoreResult<()> {
    if owner == actor {
        Ok(())
    } else {
        Err(OperationStoreError::UnauthorizedActor {
            operation_id: operation_id.clone(),
            actor: actor.clone(),
        })
    }
}

pub(in crate::store) fn execution_attempt_for(
    expected: &StoredOperation,
) -> OperationStoreResult<ExecutionAttempt> {
    expected
        .execution_attempt()
        .ok_or_else(|| OperationStoreError::StaleOperationState {
            operation_id: expected.operation_id.clone(),
            expected_revision: expected.revision,
        })
}

pub(in crate::store) fn same_durable_state(
    left: &StoredOperation,
    right: &StoredOperation,
) -> bool {
    left.operation_id == right.operation_id
        && left.revision == right.revision
        && left.phase == right.phase
        && left.status == right.status
        && left.cancellation_state == right.cancellation_state
        && left.cursor == right.cursor
        && left.progress == right.progress
        && left.execution_generation == right.execution_generation
        && left.execution_owner == right.execution_owner
}

pub(in crate::store) fn ensure_attempt_matches(
    expected: &StoredOperation,
    attempt: &ExecutionAttempt,
) -> OperationStoreResult<()> {
    if expected.operation_id != attempt.operation_id
        || expected.actor != attempt.actor
        || expected.execution_generation != attempt.generation
        || expected.execution_owner.as_deref() != Some(attempt.owner.as_str())
    {
        return Err(OperationStoreError::StaleOperationState {
            operation_id: expected.operation_id.clone(),
            expected_revision: expected.revision,
        });
    }
    Ok(())
}

/// One durable lifecycle write and its audit evidence. Cursor and progress
/// remain part of the same compare-and-swap operation.
pub struct OperationTransition<'a> {
    pub phase: OperationPhase,
    pub status: OperationStatus,
    pub cursor: Option<OperationCursor>,
    pub event_kind: OperationEventKind,
    pub counts: serde_json::Value,
    pub message: Option<&'a str>,
}

/// An effect commit always emits CursorAdvanced audit evidence.
pub struct OperationEffectCompletion<'a> {
    pub phase: OperationPhase,
    pub status: OperationStatus,
    pub cursor: Option<OperationCursor>,
    pub counts: serde_json::Value,
    pub message: Option<&'a str>,
}
