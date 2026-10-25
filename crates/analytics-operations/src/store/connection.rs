use super::{rows::row_to_operation, state::OperationDatabase, *};

impl OperationStore {
    /// Commit durable state before requesting its checkpoint. A failed body
    /// rolls back while preserving its original error, including CAS failures.
    pub(super) fn commit_transaction<T>(
        conn: &Connection,
        write: impl FnOnce(&Connection) -> OperationStoreResult<T>,
    ) -> OperationStoreResult<T> {
        conn.execute_batch("BEGIN TRANSACTION")?;
        let value = match write(conn) {
            Ok(value) => value,
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(error);
            }
        };
        conn.execute_batch("COMMIT")?;
        conn.execute_batch("CHECKPOINT")?;
        Ok(value)
    }

    pub(super) fn with_read_connection<T>(
        &self,
        read: impl FnOnce(&Connection) -> OperationStoreResult<T>,
    ) -> OperationStoreResult<T> {
        let conn = self.database.open_connection()?;
        read(&conn)
    }
}

impl OperationDatabase {
    pub(super) fn open_connection(&self) -> duckdb::Result<Connection> {
        match self {
            Self::File(path) => Connection::open(path),
            Self::Memory(conn) => conn.try_clone(),
        }
    }
}

pub(super) fn show_operation_from_connection(
    conn: &Connection,
    operation_id: &OperationId,
) -> OperationStoreResult<StoredOperation> {
    conn.query_row(
        "SELECT operation_id, kind, actor, target_tables_json, dry_run, rate_limit_json,
                payload_json, phase, status, cancellation_state, cursor_json, created_at_ms,
                updated_at_ms, revision, progress_json, execution_generation, execution_owner,
                execution_lease_until_ms
         FROM __analytics_operations
         WHERE operation_id = ?",
        params![operation_id.as_str()],
        row_to_operation,
    )
    .optional()?
    .ok_or_else(|| OperationStoreError::OperationNotFound(operation_id.clone()))
}

pub(super) fn current_time_ms(conn: &Connection) -> OperationStoreResult<i64> {
    Ok(conn.query_row("SELECT now_ms()", [], |row| row.get(0))?)
}
