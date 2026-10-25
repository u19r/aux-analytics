use super::{
    admission::{operation_matches_request, prepare_for_execution_on},
    connection::current_time_ms,
    reports::validate_audit_counts,
    state::OperationDatabase,
    transition::{
        append_event_on, ensure_actor, ensure_attempt_matches, same_durable_state,
        transition_in_transaction_on, validate_transition,
    },
    *,
};

impl OperationStore {
    pub fn connect_duckdb(path: impl AsRef<Path>) -> OperationStoreResult<Self> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path)?;
        Self::initialize_schema(&conn)?;
        // Close schema initialization before fresh file sessions can commit.
        drop(conn);
        Ok(Self {
            database: OperationDatabase::File(path),
        })
    }

    pub fn connect_in_memory() -> OperationStoreResult<Self> {
        let conn = Connection::open_in_memory()?;
        Self::initialize_schema(&conn)?;
        Ok(Self {
            database: OperationDatabase::Memory(conn),
        })
    }

    pub fn create_operation(&self, request: &OperationRequest) -> OperationStoreResult<()> {
        let conn = self.database.open_connection()?;
        Self::commit_transaction(&conn, |conn| {
            Self::create_operation_in_transaction(conn, request)
        })
    }

    /// Create an operation once, or verify that a retry carries the same
    /// request.
    ///
    /// The persisted request is the idempotency authority. A retry with the
    /// same operation id cannot silently change its owner, scope, limits,
    /// or executor payload.
    pub(crate) fn create_or_verify_operation(
        &self,
        request: &OperationRequest,
    ) -> OperationStoreResult<bool> {
        match self.create_operation(request) {
            Ok(()) => Ok(true),
            Err(OperationStoreError::DuplicateOperation(operation_id)) => {
                let existing = self.show_operation(&operation_id)?;
                if operation_matches_request(&existing, request) {
                    Ok(false)
                } else {
                    Err(OperationStoreError::ConflictingOperation(operation_id))
                }
            }
            Err(error) => Err(error),
        }
    }

    fn create_operation_in_transaction(
        conn: &Connection,
        request: &OperationRequest,
    ) -> OperationStoreResult<()> {
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO __analytics_operations
                (operation_id, kind, actor, target_tables_json, dry_run, rate_limit_json,
                 payload_json, phase, status, cancellation_state, cursor_json, created_at_ms,
                 updated_at_ms, revision, progress_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now_ms(), now_ms(), ?, ?)",
            params![
                request.operation_id.as_str(),
                request.kind.as_str(),
                request.actor.as_str(),
                serde_json::to_string(&request.target_tables)?,
                request.dry_run,
                serde_json::to_string(&request.rate_limit)?,
                serde_json::to_string(&request.payload)?,
                OperationPhase::Submitted.as_str(),
                OperationStatus::Submitted.as_str(),
                CancellationState::NotRequested.as_str(),
                serde_json::to_string(&Option::<OperationCursor>::None)?,
                0_i64,
                serde_json::json!({}).to_string(),
            ],
        )?;
        if inserted == 0 {
            return Err(OperationStoreError::DuplicateOperation(
                request.operation_id.clone(),
            ));
        }
        append_event_on(
            conn,
            &request.operation_id,
            crate::OperationTransition {
                event_kind: OperationEventKind::Submitted,
                phase: OperationPhase::Submitted,
                status: OperationStatus::Submitted,
                cursor: None,
                counts: serde_json::json!({ "target_table_count": request.target_tables.len() }),
                message: Some("operation submitted"),
            },
        )?;
        Ok(())
    }

    /// Resume a paused operation after checking its recorded owner.
    pub fn resume_operation(
        &self,
        operation_id: &OperationId,
        actor: &OperationActor,
    ) -> OperationStoreResult<()> {
        self.prepare_for_execution(operation_id, actor).map(|_| ())
    }

    /// Authorize an operation owner and move a resumable operation into
    /// `Running`.
    pub fn prepare_for_execution(
        &self,
        operation_id: &OperationId,
        actor: &OperationActor,
    ) -> OperationStoreResult<ExecutionAttempt> {
        let conn = self.database.open_connection()?;
        let now_ms = current_time_ms(&conn)?;
        Self::commit_transaction(&conn, |conn| {
            prepare_for_execution_on(conn, operation_id, actor, now_ms)
        })
    }

    /// Apply a transition only if `expected` is still the durable operation
    /// state.
    ///
    /// Every worker that performs an external effect must retain the snapshot
    /// returned by the preceding store transition and use this method
    /// afterwards. The update predicate includes the revision, lifecycle
    /// state, cancellation state, cursor, and progress document so a
    /// stale worker cannot overwrite a newer cancellation or checkpoint.
    pub(crate) fn transition_with_counts_from(
        &self,
        expected: &StoredOperation,
        transition: crate::OperationTransition<'_>,
    ) -> OperationStoreResult<()> {
        let crate::OperationTransition {
            phase,
            status,
            cursor,
            event_kind,
            counts,
            message,
        } = transition;
        let next_cursor = cursor.or_else(|| expected.cursor.clone());
        if let (Some(current_cursor), Some(next)) = (&expected.cursor, &next_cursor)
            && next.position < current_cursor.position
        {
            return Err(OperationTypeError::CursorMovedBackwards {
                current: current_cursor.position,
                next: next.position,
            }
            .into());
        }
        validate_audit_counts(&counts)?;
        validate_transition(expected, phase, status, event_kind)?;

        let conn = self.database.open_connection()?;
        Self::commit_transaction(&conn, |conn| {
            transition_in_transaction_on(
                conn,
                expected,
                crate::OperationTransition {
                    phase,
                    status,
                    cursor: next_cursor,
                    event_kind,
                    counts,
                    message,
                },
            )
        })
    }

    /// Apply a transition on behalf of the operation owner. External callers
    /// must provide the actor explicitly; the persisted owner is never used
    /// to self-authorize a control action.
    pub fn transition_with_counts_as(
        &self,
        expected: &StoredOperation,
        actor: &OperationActor,
        transition: crate::OperationTransition<'_>,
    ) -> OperationStoreResult<()> {
        let crate::OperationTransition {
            phase,
            status,
            cursor,
            event_kind,
            counts,
            message,
        } = transition;
        ensure_actor(&expected.operation_id, &expected.actor, actor)?;
        self.transition_with_counts_from(
            expected,
            crate::OperationTransition {
                phase,
                status,
                cursor,
                event_kind,
                counts,
                message,
            },
        )
    }

    /// Apply a transition while proving that the caller still owns the
    /// execution attempt. The transactional state predicate repeats the
    /// generation, owner, and lease checks, so a takeover cannot interleave
    /// between this fence and the write.
    pub(crate) fn transition_with_counts_for_attempt(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        transition: crate::OperationTransition<'_>,
    ) -> OperationStoreResult<()> {
        let crate::OperationTransition {
            phase,
            status,
            cursor,
            event_kind,
            counts,
            message,
        } = transition;
        self.assert_execution_attempt(expected, attempt)?;
        self.transition_with_counts_from(
            expected,
            crate::OperationTransition {
                phase,
                status,
                cursor,
                event_kind,
                counts,
                message,
            },
        )
    }

    /// Check an execution worker's fence immediately before or after an
    /// external effect.
    pub fn assert_execution_fence(
        &self,
        expected: &StoredOperation,
        actor: &OperationActor,
    ) -> OperationStoreResult<()> {
        ensure_actor(&expected.operation_id, &expected.actor, actor)?;
        if let Some(attempt) = expected.execution_attempt() {
            return self.assert_execution_attempt(expected, &attempt);
        }
        let current = self.show_operation(&expected.operation_id)?;
        if !same_durable_state(expected, &current) {
            return Err(OperationStoreError::StaleOperationState {
                operation_id: expected.operation_id.clone(),
                expected_revision: expected.revision,
            });
        }
        if current.status != OperationStatus::Running {
            return Err(OperationStoreError::InvalidStatusTransition {
                operation_id: current.operation_id,
                current_status: current.status,
                next_status: OperationStatus::Running,
            });
        }
        Ok(())
    }

    /// Check that an execution worker still owns the current lease. This is
    /// the required fence for external effects and terminal writes.
    pub fn assert_execution_attempt(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
    ) -> OperationStoreResult<()> {
        let now_ms = current_time_ms(&self.database.open_connection()?)?;
        self.assert_execution_attempt_at(expected, attempt, now_ms)
    }

    pub(crate) fn assert_execution_attempt_at(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        now_ms: i64,
    ) -> OperationStoreResult<()> {
        ensure_attempt_matches(expected, attempt)?;
        let current = self.show_operation(&expected.operation_id)?;
        if !same_durable_state(expected, &current)
            || current.execution_generation != attempt.generation
            || current.execution_owner.as_deref() != Some(attempt.owner.as_str())
            || current.execution_lease_until_ms <= now_ms
        {
            return Err(OperationStoreError::StaleOperationState {
                operation_id: expected.operation_id.clone(),
                expected_revision: expected.revision,
            });
        }
        if current.status != OperationStatus::Running {
            return Err(OperationStoreError::InvalidStatusTransition {
                operation_id: current.operation_id,
                current_status: current.status,
                next_status: OperationStatus::Running,
            });
        }
        Ok(())
    }
}
