use super::{
    reports::validate_audit_counts,
    transition::{
        ensure_actor, ensure_attempt_matches, execution_attempt_for, expected_state_exists_on,
        transition_in_transaction_on, validate_transition,
    },
    *,
};

impl OperationStore {
    pub fn claim_effect_as(
        &self,
        expected: &StoredOperation,
        actor: &OperationActor,
        effect_key: &OperationChunkKey,
    ) -> OperationStoreResult<bool> {
        ensure_actor(&expected.operation_id, &expected.actor, actor)?;
        let attempt = execution_attempt_for(expected)?;
        self.claim_effect_for_attempt(expected, &attempt, effect_key)
    }

    /// Reserve an effect while proving that the caller still owns the durable
    /// execution lease. The lease check is repeated by the transactional
    /// operation-state predicate, so expiry cannot race a destination call.
    pub(crate) fn claim_effect_for_attempt(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        effect_key: &OperationChunkKey,
    ) -> OperationStoreResult<bool> {
        self.admit_effect_for_attempt(
            expected,
            attempt,
            effect_key,
            EffectAdmissionMode::OrdinaryClaim,
        )
    }

    /// Admit one keyed effect under the attempt's in-memory identity and the
    /// transaction's strict expected-state fence.
    pub(crate) fn admit_effect_for_attempt(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        effect_key: &OperationChunkKey,
        mode: EffectAdmissionMode,
    ) -> OperationStoreResult<bool> {
        ensure_attempt_matches(expected, attempt)?;
        let conn = self.database.open_connection()?;
        Self::commit_transaction(&conn, |conn| {
            admit_effect_on(conn, expected, effect_key, mode)
        })
    }

    pub fn complete_effect_as(
        &self,
        expected: &StoredOperation,
        actor: &OperationActor,
        effect_key: &OperationChunkKey,
        transition: crate::OperationEffectCompletion<'_>,
    ) -> OperationStoreResult<()> {
        let crate::OperationEffectCompletion {
            phase,
            status,
            cursor,
            counts,
            message,
        } = transition;
        ensure_actor(&expected.operation_id, &expected.actor, actor)?;
        let attempt = execution_attempt_for(expected)?;
        self.complete_effect_for_attempt(
            expected,
            &attempt,
            effect_key,
            crate::OperationEffectCompletion {
                phase,
                status,
                cursor,
                counts,
                message,
            },
        )
    }

    pub(crate) fn complete_effect_for_attempt(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        effect_key: &OperationChunkKey,
        transition: crate::OperationEffectCompletion<'_>,
    ) -> OperationStoreResult<()> {
        let crate::OperationEffectCompletion {
            phase,
            status,
            cursor,
            counts,
            message,
        } = transition;
        self.assert_execution_attempt(expected, attempt)?;
        validate_audit_counts(&counts)?;
        validate_transition(expected, phase, status, OperationEventKind::CursorAdvanced)?;
        let conn = self.database.open_connection()?;
        Self::commit_transaction(&conn, |conn| {
            complete_effect_on(
                conn,
                expected,
                effect_key,
                crate::OperationEffectCompletion {
                    phase,
                    status,
                    cursor: cursor.or_else(|| expected.cursor.clone()),
                    counts,
                    message,
                },
            )
        })
    }

    pub fn abandon_effect_as(
        &self,
        expected: &StoredOperation,
        actor: &OperationActor,
        effect_key: &OperationChunkKey,
    ) -> OperationStoreResult<()> {
        ensure_actor(&expected.operation_id, &expected.actor, actor)?;
        let attempt = execution_attempt_for(expected)?;
        self.abandon_effect_for_attempt(expected, &attempt, effect_key)
    }

    pub(crate) fn abandon_effect_for_attempt(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        effect_key: &OperationChunkKey,
    ) -> OperationStoreResult<()> {
        self.assert_execution_attempt(expected, attempt)?;
        let conn = self.database.open_connection()?;
        Self::commit_transaction(&conn, |conn| abandon_effect_on(conn, expected, effect_key))
    }
}

fn admit_effect_on(
    conn: &Connection,
    expected: &StoredOperation,
    effect_key: &OperationChunkKey,
    mode: EffectAdmissionMode,
) -> OperationStoreResult<bool> {
    if !expected_state_exists_on(conn, expected)? {
        return Err(OperationStoreError::StaleOperationState {
            operation_id: expected.operation_id.clone(),
            expected_revision: expected.revision,
        });
    }
    if insert_pending_effect_on(conn, expected, effect_key)? {
        return Ok(true);
    }
    let effect = conn
        .query_row(
            "SELECT state, attempt_generation FROM __analytics_operation_effects
             WHERE operation_id = ? AND effect_key = ?",
            params![expected.operation_id.as_str(), effect_key.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((state, generation)) = effect else {
        return Err(effect_in_progress(expected, effect_key));
    };
    if state == "committed" && mode == EffectAdmissionMode::OrdinaryClaim {
        return Ok(false);
    }
    let expected_generation = i64::try_from(expected.execution_generation).unwrap_or(i64::MAX);
    if !matches!(state.as_str(), "pending" | "committed") || generation == expected_generation {
        return Err(effect_in_progress(expected, effect_key));
    }
    if reclaim_effect_on(conn, expected, effect_key, (&state, generation))? != 1 {
        return Err(effect_in_progress(expected, effect_key));
    }
    Ok(true)
}

fn insert_pending_effect_on(
    conn: &Connection,
    expected: &StoredOperation,
    effect_key: &OperationChunkKey,
) -> OperationStoreResult<bool> {
    let inserted = conn.execute(
        "INSERT OR IGNORE INTO __analytics_operation_effects
            (operation_id, effect_key, state, started_revision, attempt_generation, updated_at_ms)
         VALUES (?, ?, 'pending', ?, ?, now_ms())",
        params![
            expected.operation_id.as_str(),
            effect_key.as_str(),
            i64::try_from(expected.revision).unwrap_or(i64::MAX),
            i64::try_from(expected.execution_generation).unwrap_or(i64::MAX),
        ],
    )?;
    Ok(inserted != 0)
}

fn reclaim_effect_on(
    conn: &Connection,
    expected: &StoredOperation,
    effect_key: &OperationChunkKey,
    observed_effect: (&str, i64),
) -> OperationStoreResult<usize> {
    Ok(conn.execute(
        "UPDATE __analytics_operation_effects
         SET state = 'pending', started_revision = ?, attempt_generation = ?,
             updated_at_ms = now_ms()
         WHERE operation_id = ? AND effect_key = ? AND state = ? AND attempt_generation = ?",
        params![
            i64::try_from(expected.revision).unwrap_or(i64::MAX),
            i64::try_from(expected.execution_generation).unwrap_or(i64::MAX),
            expected.operation_id.as_str(),
            effect_key.as_str(),
            observed_effect.0,
            observed_effect.1,
        ],
    )?)
}

fn effect_in_progress(
    expected: &StoredOperation,
    effect_key: &OperationChunkKey,
) -> OperationStoreError {
    OperationStoreError::EffectInProgress {
        operation_id: expected.operation_id.clone(),
        effect_key: effect_key.as_str().to_string(),
    }
}

fn complete_effect_on(
    conn: &Connection,
    expected: &StoredOperation,
    effect_key: &OperationChunkKey,
    transition: crate::OperationEffectCompletion<'_>,
) -> OperationStoreResult<()> {
    let crate::OperationEffectCompletion {
        phase,
        status,
        cursor,
        counts,
        message,
    } = transition;
    if !expected_state_exists_on(conn, expected)? {
        return Err(OperationStoreError::StaleOperationState {
            operation_id: expected.operation_id.clone(),
            expected_revision: expected.revision,
        });
    }
    commit_effect_on(conn, expected, effect_key)?;
    transition_in_transaction_on(
        conn,
        expected,
        crate::OperationTransition {
            phase,
            status,
            cursor,
            event_kind: OperationEventKind::CursorAdvanced,
            counts,
            message,
        },
    )
}

fn abandon_effect_on(
    conn: &Connection,
    expected: &StoredOperation,
    effect_key: &OperationChunkKey,
) -> OperationStoreResult<()> {
    if !expected_state_exists_on(conn, expected)? {
        return Err(OperationStoreError::StaleOperationState {
            operation_id: expected.operation_id.clone(),
            expected_revision: expected.revision,
        });
    }
    conn.execute(
        "DELETE FROM __analytics_operation_effects
         WHERE operation_id = ? AND effect_key = ? AND state = 'pending'
           AND attempt_generation = ?",
        params![
            expected.operation_id.as_str(),
            effect_key.as_str(),
            i64::try_from(expected.execution_generation).unwrap_or(i64::MAX),
        ],
    )?;
    Ok(())
}

pub(super) fn commit_effect_on(
    conn: &Connection,
    expected: &StoredOperation,
    effect_key: &OperationChunkKey,
) -> OperationStoreResult<()> {
    let committed = conn.execute(
        "UPDATE __analytics_operation_effects
         SET state = 'committed', updated_at_ms = now_ms()
         WHERE operation_id = ? AND effect_key = ? AND state = 'pending'
           AND attempt_generation = ?",
        params![
            expected.operation_id.as_str(),
            effect_key.as_str(),
            i64::try_from(expected.execution_generation).unwrap_or(i64::MAX),
        ],
    )?;
    if committed == 0 {
        return Err(OperationStoreError::EffectInProgress {
            operation_id: expected.operation_id.clone(),
            effect_key: effect_key.as_str().to_string(),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectAdmissionMode {
    /// Reuse a committed effect without replaying its external side effect.
    OrdinaryClaim,
    /// Reapply a committed effect only after a newer execution generation takes
    /// over.
    MaintenanceReapply,
}
