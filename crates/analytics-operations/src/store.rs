use std::{path::Path, str::FromStr};

use duckdb::{Connection, OptionalExt, params};
use thiserror::Error;

use crate::{
    CancellationState, OperationCursor, OperationEvent, OperationEventKind, OperationId,
    OperationKind, OperationPhase, OperationRequest, OperationStatus, types::OperationTypeError,
};

#[derive(Debug, Error)]
pub enum OperationStoreError {
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Type(#[from] OperationTypeError),
    #[error("operation {0} already exists")]
    DuplicateOperation(OperationId),
    #[error("operation {0} not found")]
    OperationNotFound(OperationId),
    #[error("operation {operation_id} cannot transition from {current_status} to {next_status}")]
    InvalidStatusTransition {
        operation_id: OperationId,
        current_status: OperationStatus,
        next_status: OperationStatus,
    },
    #[error("operation audit counts cannot include sensitive field {0}")]
    UnsafeAuditCountField(String),
}

pub type OperationStoreResult<T> = Result<T, OperationStoreError>;

pub struct OperationStore {
    conn: Connection,
}

impl OperationStore {
    pub fn connect_duckdb(path: impl AsRef<Path>) -> OperationStoreResult<Self> {
        let conn = Connection::open(path)?;
        let store = Self { conn };
        store.ensure_tables()?;
        Ok(store)
    }

    pub fn connect_in_memory() -> OperationStoreResult<Self> {
        let conn = Connection::open_in_memory()?;
        let store = Self { conn };
        store.ensure_tables()?;
        Ok(store)
    }

    pub fn create_operation(&self, request: &OperationRequest) -> OperationStoreResult<()> {
        let inserted = self.conn.execute(
            "INSERT OR IGNORE INTO __analytics_operations
                (operation_id, kind, actor, target_tables_json, dry_run, rate_limit_json,
                 payload_json, phase, status, cancellation_state, cursor_json, created_at_ms,
                 updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now_ms(), now_ms())",
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
            ],
        )?;
        if inserted == 0 {
            return Err(OperationStoreError::DuplicateOperation(
                request.operation_id.clone(),
            ));
        }
        self.append_event(
            &request.operation_id,
            OperationEventKind::Submitted,
            OperationPhase::Submitted,
            OperationStatus::Submitted,
            None,
            serde_json::json!({ "target_table_count": request.target_tables.len() }),
            Some("operation submitted"),
        )?;
        Ok(())
    }

    pub fn transition(
        &self,
        operation_id: &OperationId,
        phase: OperationPhase,
        status: OperationStatus,
        cursor: Option<OperationCursor>,
        event_kind: OperationEventKind,
        message: Option<&str>,
    ) -> OperationStoreResult<()> {
        self.transition_with_counts(
            operation_id,
            phase,
            status,
            cursor,
            event_kind,
            serde_json::json!({}),
            message,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn transition_with_counts(
        &self,
        operation_id: &OperationId,
        phase: OperationPhase,
        status: OperationStatus,
        cursor: Option<OperationCursor>,
        event_kind: OperationEventKind,
        counts: serde_json::Value,
        message: Option<&str>,
    ) -> OperationStoreResult<()> {
        let current = self.show_operation(operation_id)?;
        if let (Some(current), Some(next)) = (&current.cursor, &cursor)
            && next.position < current.position
        {
            return Err(OperationTypeError::CursorMovedBackwards {
                current: current.position,
                next: next.position,
            }
            .into());
        }
        validate_status_transition(operation_id, current.status, status)?;
        let updated = self.conn.execute(
            "UPDATE __analytics_operations
             SET phase = ?, status = ?, cursor_json = ?, updated_at_ms = now_ms()
             WHERE operation_id = ?",
            params![
                phase.as_str(),
                status.as_str(),
                serde_json::to_string(&cursor)?,
                operation_id.as_str(),
            ],
        )?;
        if updated == 0 {
            return Err(OperationStoreError::OperationNotFound(operation_id.clone()));
        }
        self.append_event(
            operation_id,
            event_kind,
            phase,
            status,
            cursor,
            counts,
            message,
        )
    }

    pub fn request_cancellation(&self, operation_id: &OperationId) -> OperationStoreResult<()> {
        let operation = self.show_operation(operation_id)?;
        let updated = self.conn.execute(
            "UPDATE __analytics_operations
             SET status = ?, cancellation_state = ?, updated_at_ms = now_ms()
             WHERE operation_id = ?",
            params![
                OperationStatus::Cancelling.as_str(),
                CancellationState::Requested.as_str(),
                operation_id.as_str(),
            ],
        )?;
        if updated == 0 {
            return Err(OperationStoreError::OperationNotFound(operation_id.clone()));
        }
        self.append_event(
            operation_id,
            OperationEventKind::CancellationRequested,
            operation.phase,
            OperationStatus::Cancelling,
            operation.cursor,
            serde_json::json!({}),
            Some("cancellation requested"),
        )
    }

    pub fn list_operations(&self) -> OperationStoreResult<Vec<StoredOperation>> {
        let mut statement = self.conn.prepare(
            "SELECT operation_id, kind, actor, target_tables_json, dry_run, rate_limit_json,
                    payload_json, phase, status, cancellation_state, cursor_json, created_at_ms,
                    updated_at_ms
             FROM __analytics_operations
             ORDER BY created_at_ms, operation_id",
        )?;
        let rows = statement.query_map([], row_to_operation)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn show_operation(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<StoredOperation> {
        let mut statement = self.conn.prepare(
            "SELECT operation_id, kind, actor, target_tables_json, dry_run, rate_limit_json,
                    payload_json, phase, status, cancellation_state, cursor_json, created_at_ms,
                    updated_at_ms
             FROM __analytics_operations
             WHERE operation_id = ?",
        )?;
        statement
            .query_row(params![operation_id.as_str()], row_to_operation)
            .optional()?
            .ok_or_else(|| OperationStoreError::OperationNotFound(operation_id.clone()))
    }

    pub fn audit_events(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<Vec<OperationEvent>> {
        let mut statement = self.conn.prepare(
            "SELECT operation_id, event_id, occurred_at_ms, kind, phase, status, cursor_json,
                    counts_json, message
             FROM __analytics_operation_events
             WHERE operation_id = ?
             ORDER BY event_id",
        )?;
        let rows = statement.query_map(params![operation_id.as_str()], row_to_event)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn save_check_report(
        &self,
        operation_id: &OperationId,
        report: &crate::CheckReport,
    ) -> OperationStoreResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO __analytics_check_reports
                (operation_id, report_json, created_at_ms)
             VALUES (?, ?, now_ms())",
            params![operation_id.as_str(), serde_json::to_string(report)?],
        )?;
        Ok(())
    }

    pub fn check_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::CheckReport> {
        let mut statement = self.conn.prepare(
            "SELECT report_json
             FROM __analytics_check_reports
             WHERE operation_id = ?",
        )?;
        let report_json = statement
            .query_row(params![operation_id.as_str()], |row| {
                row.get::<_, String>(0)
            })
            .optional()?
            .ok_or_else(|| OperationStoreError::OperationNotFound(operation_id.clone()))?;
        serde_json::from_str(report_json.as_str()).map_err(Into::into)
    }

    pub fn save_table_fix_report(
        &self,
        operation_id: &OperationId,
        report: &crate::TableFixReport,
    ) -> OperationStoreResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO __analytics_table_fix_reports
                (operation_id, report_json, created_at_ms)
             VALUES (?, ?, now_ms())",
            params![operation_id.as_str(), serde_json::to_string(report)?],
        )?;
        Ok(())
    }

    pub fn table_fix_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::TableFixReport> {
        let mut statement = self.conn.prepare(
            "SELECT report_json
             FROM __analytics_table_fix_reports
             WHERE operation_id = ?",
        )?;
        let report_json = statement
            .query_row(params![operation_id.as_str()], |row| {
                row.get::<_, String>(0)
            })
            .optional()?
            .ok_or_else(|| OperationStoreError::OperationNotFound(operation_id.clone()))?;
        serde_json::from_str(report_json.as_str()).map_err(Into::into)
    }

    pub fn save_privacy_fix_report(
        &self,
        operation_id: &OperationId,
        report: &crate::PrivacyFixReport,
    ) -> OperationStoreResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO __analytics_privacy_fix_reports
                (operation_id, report_json, created_at_ms)
             VALUES (?, ?, now_ms())",
            params![operation_id.as_str(), serde_json::to_string(report)?],
        )?;
        Ok(())
    }

    pub fn privacy_fix_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::PrivacyFixReport> {
        let mut statement = self.conn.prepare(
            "SELECT report_json
             FROM __analytics_privacy_fix_reports
             WHERE operation_id = ?",
        )?;
        let report_json = statement
            .query_row(params![operation_id.as_str()], |row| {
                row.get::<_, String>(0)
            })
            .optional()?
            .ok_or_else(|| OperationStoreError::OperationNotFound(operation_id.clone()))?;
        serde_json::from_str(report_json.as_str()).map_err(Into::into)
    }

    pub fn save_trim_report(
        &self,
        operation_id: &OperationId,
        report: &crate::TrimReport,
    ) -> OperationStoreResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO __analytics_trim_reports
                (operation_id, report_json, created_at_ms)
             VALUES (?, ?, now_ms())",
            params![operation_id.as_str(), serde_json::to_string(report)?],
        )?;
        Ok(())
    }

    pub fn trim_report(
        &self,
        operation_id: &OperationId,
    ) -> OperationStoreResult<crate::TrimReport> {
        let mut statement = self.conn.prepare(
            "SELECT report_json
             FROM __analytics_trim_reports
             WHERE operation_id = ?",
        )?;
        let report_json = statement
            .query_row(params![operation_id.as_str()], |row| {
                row.get::<_, String>(0)
            })
            .optional()?
            .ok_or_else(|| OperationStoreError::OperationNotFound(operation_id.clone()))?;
        serde_json::from_str(report_json.as_str()).map_err(Into::into)
    }

    fn ensure_tables(&self) -> OperationStoreResult<()> {
        self.conn.execute_batch(
            "CREATE MACRO IF NOT EXISTS now_ms() AS CAST(epoch_ms(current_timestamp) AS BIGINT);
             CREATE TABLE IF NOT EXISTS __analytics_operations (
                operation_id VARCHAR PRIMARY KEY,
                kind VARCHAR NOT NULL,
                actor VARCHAR NOT NULL,
                target_tables_json JSON NOT NULL,
                dry_run BOOLEAN NOT NULL,
                rate_limit_json JSON NOT NULL,
                payload_json JSON NOT NULL,
                phase VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                cancellation_state VARCHAR NOT NULL,
                cursor_json JSON NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL
             );
             CREATE SEQUENCE IF NOT EXISTS __analytics_operation_event_seq START 1;
             CREATE TABLE IF NOT EXISTS __analytics_operation_events (
                operation_id VARCHAR NOT NULL,
                event_id BIGINT DEFAULT nextval('__analytics_operation_event_seq'),
                occurred_at_ms BIGINT NOT NULL,
                kind VARCHAR NOT NULL,
                phase VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                cursor_json JSON NOT NULL,
                counts_json JSON NOT NULL,
                message VARCHAR,
                PRIMARY KEY (operation_id, event_id)
             );
             CREATE TABLE IF NOT EXISTS __analytics_check_reports (
                operation_id VARCHAR PRIMARY KEY,
                report_json JSON NOT NULL,
                created_at_ms BIGINT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS __analytics_table_fix_reports (
                operation_id VARCHAR PRIMARY KEY,
                report_json JSON NOT NULL,
                created_at_ms BIGINT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS __analytics_privacy_fix_reports (
                operation_id VARCHAR PRIMARY KEY,
                report_json JSON NOT NULL,
                created_at_ms BIGINT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS __analytics_trim_reports (
                operation_id VARCHAR PRIMARY KEY,
                report_json JSON NOT NULL,
                created_at_ms BIGINT NOT NULL
             );",
        )?;
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::needless_pass_by_value,
        reason = "operation event persistence keeps the transition fields explicit at the store \
                  boundary"
    )]
    fn append_event(
        &self,
        operation_id: &OperationId,
        kind: OperationEventKind,
        phase: OperationPhase,
        status: OperationStatus,
        cursor: Option<OperationCursor>,
        counts: serde_json::Value,
        message: Option<&str>,
    ) -> OperationStoreResult<()> {
        validate_audit_counts(&counts)?;
        self.conn.execute(
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
}

fn validate_audit_counts(counts: &serde_json::Value) -> OperationStoreResult<()> {
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

fn validate_status_transition(
    operation_id: &OperationId,
    current_status: OperationStatus,
    next_status: OperationStatus,
) -> OperationStoreResult<()> {
    if matches!(
        current_status,
        OperationStatus::Cancelling | OperationStatus::Cancelled
    ) && next_status != OperationStatus::Cancelled
    {
        return Err(OperationStoreError::InvalidStatusTransition {
            operation_id: operation_id.clone(),
            current_status,
            next_status,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StoredOperation {
    pub operation_id: OperationId,
    pub kind: OperationKind,
    pub actor: String,
    pub target_tables: Vec<String>,
    pub dry_run: bool,
    pub rate_limit: crate::RateLimitPolicy,
    pub payload: serde_json::Value,
    pub phase: OperationPhase,
    pub status: OperationStatus,
    pub cancellation_state: CancellationState,
    pub cursor: Option<OperationCursor>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

fn row_to_operation(row: &duckdb::Row<'_>) -> duckdb::Result<StoredOperation> {
    let operation_id: String = row.get(0)?;
    let kind: String = row.get(1)?;
    let target_tables_json: String = row.get(3)?;
    let rate_limit_json: String = row.get(5)?;
    let payload_json: String = row.get(6)?;
    let phase: String = row.get(7)?;
    let status: String = row.get(8)?;
    let cancellation_state: String = row.get(9)?;
    let cursor_json: String = row.get(10)?;
    Ok(StoredOperation {
        operation_id: parse_json_error(OperationId::new(operation_id))?,
        kind: parse_json_error(OperationKind::from_str(kind.as_str()))?,
        actor: row.get(2)?,
        target_tables: parse_json_error(serde_json::from_str(target_tables_json.as_str()))?,
        dry_run: row.get(4)?,
        rate_limit: parse_json_error(serde_json::from_str(rate_limit_json.as_str()))?,
        payload: parse_json_error(serde_json::from_str(payload_json.as_str()))?,
        phase: parse_json_error(OperationPhase::from_str(phase.as_str()))?,
        status: parse_json_error(OperationStatus::from_str(status.as_str()))?,
        cancellation_state: parse_json_error(CancellationState::from_str(
            cancellation_state.as_str(),
        ))?,
        cursor: parse_json_error(serde_json::from_str(cursor_json.as_str()))?,
        created_at_ms: row.get(11)?,
        updated_at_ms: row.get(12)?,
    })
}

fn row_to_event(row: &duckdb::Row<'_>) -> duckdb::Result<OperationEvent> {
    let operation_id: String = row.get(0)?;
    let kind: String = row.get(3)?;
    let phase: String = row.get(4)?;
    let status: String = row.get(5)?;
    let cursor_json: String = row.get(6)?;
    let counts_json: String = row.get(7)?;
    Ok(OperationEvent {
        operation_id: parse_json_error(OperationId::new(operation_id))?,
        event_id: row.get(1)?,
        occurred_at_ms: row.get(2)?,
        kind: parse_json_error(OperationEventKind::from_str(kind.as_str()))?,
        phase: parse_json_error(OperationPhase::from_str(phase.as_str()))?,
        status: parse_json_error(OperationStatus::from_str(status.as_str()))?,
        cursor: parse_json_error(serde_json::from_str(cursor_json.as_str()))?,
        counts: parse_json_error(serde_json::from_str(counts_json.as_str()))?,
        message: row.get(8)?,
    })
}

fn parse_json_error<T, E: std::error::Error + Send + Sync + 'static>(
    result: Result<T, E>,
) -> duckdb::Result<T> {
    result.map_err(|error| duckdb::Error::ToSqlConversionFailure(Box::new(error)))
}
