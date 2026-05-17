use crate::{
    OperationActor, OperationCursor, OperationEventKind, OperationId, OperationKind,
    OperationPhase, OperationRequest, OperationStatus, OperationStore, RateLimitPolicy,
};

fn request(id: &str) -> OperationRequest {
    OperationRequest {
        operation_id: OperationId::new(id).unwrap(),
        kind: OperationKind::Backfill,
        actor: OperationActor::new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        dry_run: true,
        rate_limit: RateLimitPolicy::default(),
        payload: serde_json::json!({ "mode": "plan" }),
    }
}

#[test]
fn given_new_operation_when_created_then_it_is_listed_with_audit_event() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_1");

    store.create_operation(&operation).unwrap();

    let listed = store.list_operations().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].operation_id, operation.operation_id);
    assert_eq!(listed[0].status, OperationStatus::Submitted);
    let events = store.audit_events(&operation.operation_id).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, OperationEventKind::Submitted);
}

#[test]
fn given_existing_operation_when_duplicate_created_then_it_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_1");

    store.create_operation(&operation).unwrap();

    let error = store.create_operation(&operation).unwrap_err();
    assert!(error.to_string().contains("already exists"));
}

#[test]
fn given_durable_cursor_when_resumed_then_cursor_cannot_move_backwards() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_1");
    store.create_operation(&operation).unwrap();
    store
        .transition(
            &operation.operation_id,
            OperationPhase::Executing,
            OperationStatus::Running,
            Some(OperationCursor {
                label: "chunk".to_string(),
                position: 10,
            }),
            OperationEventKind::CursorAdvanced,
            Some("chunk committed"),
        )
        .unwrap();

    let error = store
        .transition(
            &operation.operation_id,
            OperationPhase::Executing,
            OperationStatus::Running,
            Some(OperationCursor {
                label: "chunk".to_string(),
                position: 9,
            }),
            OperationEventKind::Resumed,
            Some("resume attempted"),
        )
        .unwrap_err();
    assert!(error.to_string().contains("cannot move backwards"));
}

#[test]
fn given_running_operation_when_cancel_requested_then_status_and_audit_update() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_1");
    store.create_operation(&operation).unwrap();

    store.request_cancellation(&operation.operation_id).unwrap();

    let stored = store.show_operation(&operation.operation_id).unwrap();
    assert_eq!(stored.status, OperationStatus::Cancelling);
    let events = store.audit_events(&operation.operation_id).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].kind, OperationEventKind::CancellationRequested);
}

#[test]
fn given_operation_store_reopened_when_operation_resumes_then_durable_cursor_is_preserved() {
    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join("operations.duckdb");
    let operation = request("op_1");
    {
        let store = OperationStore::connect_duckdb(db_path.as_path()).unwrap();
        store.create_operation(&operation).unwrap();
        store
            .transition(
                &operation.operation_id,
                OperationPhase::Executing,
                OperationStatus::Running,
                Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 7,
                }),
                OperationEventKind::CursorAdvanced,
                Some("chunk committed"),
            )
            .unwrap();
    }

    let reopened = OperationStore::connect_duckdb(db_path.as_path()).unwrap();
    let stored = reopened.show_operation(&operation.operation_id).unwrap();

    assert_eq!(stored.status, OperationStatus::Running);
    assert_eq!(stored.cursor.map(|cursor| cursor.position), Some(7));
}

#[test]
fn given_cancel_requested_when_worker_attempts_later_write_then_transition_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_1");
    store.create_operation(&operation).unwrap();
    store.request_cancellation(&operation.operation_id).unwrap();

    let error = store
        .transition(
            &operation.operation_id,
            OperationPhase::Executing,
            OperationStatus::Running,
            Some(OperationCursor {
                label: "chunk".to_string(),
                position: 1,
            }),
            OperationEventKind::CursorAdvanced,
            Some("late write"),
        )
        .unwrap_err();

    assert!(error.to_string().contains("cannot transition"));
    let stored = store.show_operation(&operation.operation_id).unwrap();
    assert_eq!(stored.status, OperationStatus::Cancelling);
    assert!(stored.cursor.is_none());
}

#[test]
fn given_audit_counts_include_raw_payload_key_when_transitioned_then_it_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_sensitive_counts");
    store.create_operation(&operation).unwrap();

    let error = store
        .transition_with_counts(
            &operation.operation_id,
            OperationPhase::Executing,
            OperationStatus::Running,
            Some(OperationCursor {
                label: "chunk".to_string(),
                position: 1,
            }),
            OperationEventKind::CursorAdvanced,
            serde_json::json!({
                "rows_written": 1,
                "payload": {
                    "email": "private@example.test"
                }
            }),
            Some("chunk committed"),
        )
        .unwrap_err();

    assert!(error.to_string().contains("payload"));
    assert_eq!(
        store.audit_events(&operation.operation_id).unwrap().len(),
        1
    );
}

#[test]
fn given_audit_counts_include_raw_sql_key_when_transitioned_then_it_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_raw_sql_counts");
    store.create_operation(&operation).unwrap();

    let error = store
        .transition_with_counts(
            &operation.operation_id,
            OperationPhase::Executing,
            OperationStatus::Running,
            None,
            OperationEventKind::Started,
            serde_json::json!({
                "phase": "planning",
                "raw_sql": "select * from users where email = 'private@example.test'"
            }),
            Some("operation started"),
        )
        .unwrap_err();

    assert!(error.to_string().contains("raw_sql"));
    assert_eq!(
        store.audit_events(&operation.operation_id).unwrap().len(),
        1
    );
}
