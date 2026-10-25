use std::cell::Cell;

use duckdb::Connection;

use super::{OperationStore, OperationStoreError, OperationStoreResult};
use crate::OperationId;

#[test]
fn given_body_failure_when_transaction_writes_then_original_error_and_rollback_are_preserved() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE transaction_evidence (value BIGINT)")
        .unwrap();
    let id = OperationId::try_new("rollback-evidence").unwrap();
    let result: OperationStoreResult<()> = OperationStore::commit_transaction(&conn, |conn| {
        conn.execute_batch("INSERT INTO transaction_evidence VALUES (7)")?;
        Err(OperationStoreError::DuplicateOperation(id.clone()))
    });
    assert!(matches!(result, Err(OperationStoreError::DuplicateOperation(actual)) if actual == id));
    let count: i64 = conn
        .query_row("SELECT count(*) FROM transaction_evidence", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 0);
    OperationStore::commit_transaction(&conn, |conn| {
        conn.execute_batch("INSERT INTO transaction_evidence VALUES (9)")?;
        Ok(())
    })
    .unwrap();
    let value: i64 = conn
        .query_row("SELECT value FROM transaction_evidence", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(value, 9);
}

#[test]
fn given_active_transaction_when_begin_fails_then_body_is_not_called() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("BEGIN TRANSACTION").unwrap();
    let called = Cell::new(false);
    let result = OperationStore::commit_transaction(&conn, |_| {
        called.set(true);
        Ok(())
    });
    assert!(matches!(result, Err(OperationStoreError::DuckDb(_))));
    assert!(!called.get());
    conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn given_supplied_file_connection_when_transaction_commits_then_reopen_observes_result() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("transactions.duckdb");
    let conn = Connection::open(&path).unwrap();
    let value = OperationStore::commit_transaction(&conn, |conn| {
        conn.execute_batch("CREATE TABLE transaction_evidence AS SELECT 19 AS value")?;
        Ok(19)
    })
    .unwrap();
    assert_eq!(value, 19);
    drop(conn);
    let reopened = Connection::open(path).unwrap();
    let stored: i64 = reopened
        .query_row("SELECT value FROM transaction_evidence", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(stored, value);
}

#[test]
fn given_ended_transaction_when_body_fails_then_rollback_failure_does_not_replace_original_cause() {
    let conn = Connection::open_in_memory().unwrap();
    let id = OperationId::try_new("rollback-cause").unwrap();
    let result: OperationStoreResult<()> = OperationStore::commit_transaction(&conn, |conn| {
        conn.execute_batch("ROLLBACK")?;
        Err(OperationStoreError::DuplicateOperation(id.clone()))
    });
    assert!(matches!(result, Err(OperationStoreError::DuplicateOperation(actual)) if actual == id));
}

#[test]
fn given_ended_transaction_when_body_succeeds_then_commit_error_is_returned() {
    let conn = Connection::open_in_memory().unwrap();
    let expected = conn.execute_batch("COMMIT").unwrap_err().to_string();
    let result = OperationStore::commit_transaction(&conn, |conn| {
        conn.execute_batch("ROLLBACK")?;
        Ok(())
    });
    let Err(OperationStoreError::DuckDb(error)) = result else {
        panic!("commit failure must retain its DuckDB cause");
    };
    assert_eq!(error.to_string(), expected);
}
