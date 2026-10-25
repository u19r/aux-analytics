use super::{
    connection::show_operation_from_connection,
    rows::{row_to_event, row_to_operation},
    transition::{
        ensure_actor, expected_state_exists_on, transition_in_transaction_on, validate_transition,
    },
    *,
};

impl OperationStore {
    pub fn list_operations(&self) -> OperationStoreResult<Vec<StoredOperation>> {
        self.with_read_connection(|conn| {
            let mut statement = conn.prepare(
                "SELECT operation_id, kind, actor, target_tables_json, dry_run, rate_limit_json,
                        payload_json, phase, status, cancellation_state, cursor_json, \
                 created_at_ms,
                        updated_at_ms, revision, progress_json, execution_generation,
                        execution_owner, execution_lease_until_ms
                 FROM __analytics_operations
                 ORDER BY created_at_ms, operation_id",
            )?;
            let rows = statement.query_map([], row_to_operation)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
    }

    pub fn show_operation(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<StoredOperation> {
        self.with_read_connection(|conn| show_operation_from_connection(conn, operation_id))
    }

    /// Check that a caller owns the operation before it controls execution.
    pub fn authorize_actor(
        &self,
        operation_id: &OperationId,
        actor: &OperationActor,
    ) -> OperationStoreResult<()> {
        let operation = self.show_operation(operation_id)?;
        ensure_actor(operation_id, &operation.actor, actor)
    }

    pub fn audit_events(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<Vec<OperationEvent>> {
        self.with_read_connection(|conn| {
            let mut statement = conn.prepare(
                "SELECT operation_id, event_id, occurred_at_ms, kind, phase, status, cursor_json,
                        counts_json, message
                 FROM __analytics_operation_events
                 WHERE operation_id = ?
                 ORDER BY event_id",
            )?;
            let rows = statement.query_map(params![operation_id.as_str()], row_to_event)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
    }

    pub fn backfill_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::BackfillExecutionReport> {
        self.load_report(operation_id, OperationReportKind::Backfill)
    }

    pub fn check_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::CheckReport> {
        self.load_report(operation_id, OperationReportKind::Check)
    }

    pub fn table_fix_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::TableFixReport> {
        self.load_report(operation_id, OperationReportKind::TableFix)
    }

    pub fn privacy_fix_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::PrivacyFixReport> {
        self.load_report(operation_id, OperationReportKind::PrivacyFix)
    }

    pub fn trim_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::TrimReport> {
        self.load_report(operation_id, OperationReportKind::Trim)
    }

    pub fn raw_backup_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::RawBackupWriteReport> {
        self.load_report(operation_id, OperationReportKind::RawBackup)
    }

    fn load_report<T>(
        &self,
        operation_id: &OperationId,
        report_kind: OperationReportKind,
    ) -> OperationStoreResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.with_read_connection(|conn| {
            let report_json = conn
                .query_row(
                    "SELECT report_json
                     FROM __analytics_operation_reports
                     WHERE operation_id = ? AND report_kind = ?",
                    params![operation_id.as_str(), report_kind.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .ok_or_else(|| OperationStoreError::OperationNotFound(operation_id.clone()))?;
            serde_json::from_str(report_json.as_str()).map_err(Into::into)
        })
    }

    /// Commit a backfill report and completion transition under the execution
    /// attempt that performed the work. A stale or expired worker cannot
    /// publish a report after a takeover.
    pub(crate) fn save_backfill_report_and_succeed_for_attempt(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        report: &crate::BackfillExecutionReport,
    ) -> OperationStoreResult<()> {
        self.assert_execution_attempt(expected, attempt)?;
        self.save_report_and_transition_from(
            expected,
            crate::store::OperationReportWrite {
                kind: OperationReportKind::Backfill,
                document: serde_json::to_value(report)?,
                transition: crate::OperationTransition {
                    phase: crate::OperationPhase::Completed,
                    status: OperationStatus::Succeeded,
                    cursor: None,
                    event_kind: OperationEventKind::Succeeded,
                    counts: serde_json::to_value(&report.metrics)?,
                    message: Some("backfill execution completed"),
                },
            },
        )
    }

    /// Commit a check report and terminal state only for the attempt that read
    /// the rows. A takeover rolls back the report and leaves the replacement
    /// attempt untouched.
    pub(crate) fn save_check_report_and_finish_for_attempt(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        report: &crate::CheckReport,
    ) -> OperationStoreResult<()> {
        self.assert_execution_attempt(expected, attempt)?;
        let (status, event_kind) = report.outcome.completion_state();
        self.save_report_and_transition_from(
            expected,
            crate::store::OperationReportWrite {
                kind: OperationReportKind::Check,
                document: serde_json::to_value(report)?,
                transition: crate::OperationTransition {
                    phase: crate::OperationPhase::Completed,
                    status,
                    cursor: Some(OperationCursor {
                        label: "check".to_string(),
                        position: report.metrics.checked_rows,
                    }),
                    event_kind,
                    counts: serde_json::to_value(&report.metrics)?,
                    message: Some("check execution completed"),
                },
            },
        )
    }

    /// Commit an external effect's durable marker, its typed report, and the
    /// terminal lifecycle transition in one local transaction. The caller
    /// must fence immediately before invoking the destination and pass the
    /// same expected revision here; a stale worker rolls back all
    /// three local writes and leaves the idempotency key retryable.
    pub(crate) fn save_report_and_complete_effect_for_attempt(
        &self,
        expected: &StoredOperation,
        attempt: &ExecutionAttempt,
        effect_key: &OperationChunkKey,
        write: super::OperationReportWrite<'_>,
    ) -> OperationStoreResult<()> {
        let super::OperationReportWrite {
            kind: report_kind,
            document: report,
            transition,
        } = write;
        let crate::OperationTransition {
            status,
            cursor,
            event_kind,
            counts,
            message,
            ..
        } = transition;
        self.assert_execution_attempt(expected, attempt)?;
        validate_audit_counts(&counts)?;
        validate_transition(expected, OperationPhase::Completed, status, event_kind)?;
        let conn = self.database.open_connection()?;
        Self::commit_transaction(&conn, |conn| {
            super::effects::commit_effect_on(conn, expected, effect_key)?;
            if !expected_state_exists_on(conn, expected)? {
                return Err(OperationStoreError::StaleOperationState {
                    operation_id: expected.operation_id.clone(),
                    expected_revision: expected.revision,
                });
            }
            insert_report_on(conn, &expected.operation_id, report_kind, &report)?;
            transition_in_transaction_on(
                conn,
                expected,
                crate::OperationTransition {
                    phase: OperationPhase::Completed,
                    status,
                    cursor: cursor.or_else(|| expected.cursor.clone()),
                    event_kind,
                    counts,
                    message,
                },
            )
        })
    }

    fn save_report_and_transition_from(
        &self,
        expected: &StoredOperation,
        write: super::OperationReportWrite<'_>,
    ) -> OperationStoreResult<()> {
        let super::OperationReportWrite {
            kind: report_kind,
            document: report,
            transition,
        } = write;
        let crate::OperationTransition {
            status,
            cursor,
            event_kind,
            counts,
            message,
            ..
        } = transition;
        validate_audit_counts(&counts)?;
        validate_transition(expected, OperationPhase::Completed, status, event_kind)?;
        let conn = self.database.open_connection()?;
        Self::commit_transaction(&conn, |conn| {
            insert_report_on(conn, &expected.operation_id, report_kind, &report)?;
            transition_in_transaction_on(
                conn,
                expected,
                crate::OperationTransition {
                    phase: OperationPhase::Completed,
                    status,
                    cursor: cursor.or_else(|| expected.cursor.clone()),
                    event_kind,
                    counts,
                    message,
                },
            )
        })
    }
}

pub(super) fn validate_audit_counts(counts: &serde_json::Value) -> OperationStoreResult<()> {
    match counts {
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                if is_sensitive_audit_key(key) {
                    return Err(OperationStoreError::UnsafeAuditCountField(key.clone()));
                }
                validate_audit_counts(value)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                validate_audit_counts(value)?;
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
    Ok(())
}

fn insert_report_on(
    conn: &Connection,
    operation_id: &OperationId,
    report_kind: OperationReportKind,
    report: &serde_json::Value,
) -> OperationStoreResult<()> {
    conn.execute(
        "INSERT OR REPLACE INTO __analytics_operation_reports
            (operation_id, report_kind, report_json, created_at_ms)
         VALUES (?, ?, ?, now_ms())",
        params![
            operation_id.as_str(),
            report_kind.as_str(),
            report.to_string()
        ],
    )?;
    Ok(())
}

fn is_sensitive_audit_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "payload"
            | "payload_json"
            | "raw_payload"
            | "record"
            | "record_json"
            | "raw_record"
            | "raw_sql"
            | "sql"
            | "secret"
            | "credential"
            | "credentials"
            | "token"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationReportKind {
    Backfill,
    Check,
    TableFix,
    PrivacyFix,
    Trim,
    RawBackup,
}

impl OperationReportKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Backfill => "backfill",
            Self::Check => "check",
            Self::TableFix => "table_fix",
            Self::PrivacyFix => "privacy_fix",
            Self::Trim => "trim",
            Self::RawBackup => "raw_backup",
        }
    }
}

pub(crate) struct OperationReportWrite<'a> {
    pub(crate) kind: OperationReportKind,
    pub(crate) document: serde_json::Value,
    pub(crate) transition: OperationTransition<'a>,
}
