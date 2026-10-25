use crate::{
    OperationActor, OperationChunkKey, OperationCursor, OperationEventKind, OperationId,
    OperationKind, OperationPhase, OperationRequest, OperationStatus, OperationStore,
    OperationStoreError, RateLimitPolicy,
    store::{EffectAdmissionMode, OperationReportKind},
    store_test_support::OperationStoreTestFixture,
};

fn request(id: &str) -> OperationRequest {
    OperationRequest {
        operation_id: OperationId::try_new(id).unwrap(),
        kind: OperationKind::Backfill,
        actor: OperationActor::try_new("operator").unwrap(),
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
    let store_fixture = OperationStoreTestFixture::new();
    store_fixture
        .prepare(&store, &operation.operation_id, &operation.actor)
        .unwrap();
    store_fixture
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 10,
                }),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: serde_json::json!({}),
                message: Some("chunk committed"),
            },
        )
        .unwrap();

    let error = store_fixture
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 9,
                }),
                event_kind: OperationEventKind::Resumed,
                counts: serde_json::json!({}),
                message: Some("resume attempted"),
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("cannot move backwards"));
}

#[test]
fn given_running_operation_when_cancel_requested_then_status_and_audit_update() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_1");
    store.create_operation(&operation).unwrap();
    OperationStoreTestFixture::new()
        .prepare(&store, &operation.operation_id, &operation.actor)
        .unwrap();

    store
        .request_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap();

    let stored = store.show_operation(&operation.operation_id).unwrap();
    assert_eq!(stored.status, OperationStatus::Cancelling);
    let events = store.audit_events(&operation.operation_id).unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(
        events.last().unwrap().kind,
        OperationEventKind::CancellationRequested
    );
}

#[test]
fn given_terminal_operation_when_cancel_requested_then_state_is_unchanged() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_terminal_cancel");
    store.create_operation(&operation).unwrap();
    let store_fixture = OperationStoreTestFixture::new();
    store_fixture
        .prepare(&store, &operation.operation_id, &operation.actor)
        .unwrap();
    store_fixture
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Completed,
                status: OperationStatus::Succeeded,
                cursor: None,
                event_kind: OperationEventKind::Succeeded,
                counts: serde_json::json!({}),
                message: Some("operation completed"),
            },
        )
        .unwrap();

    let error = store
        .request_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap_err();

    assert!(matches!(
        error,
        crate::OperationStoreError::InvalidStatusTransition {
            current_status: OperationStatus::Succeeded,
            next_status: OperationStatus::Cancelling,
            ..
        }
    ));
    assert_eq!(
        store
            .show_operation(&operation.operation_id)
            .unwrap()
            .status,
        OperationStatus::Succeeded
    );
    assert_eq!(
        store.audit_events(&operation.operation_id).unwrap().len(),
        4
    );
}

#[test]
fn given_succeeded_operation_when_prepared_for_execution_then_transition_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_succeeded_prepare");
    store.create_operation(&operation).unwrap();
    let store_fixture = OperationStoreTestFixture::new();
    store_fixture
        .prepare(&store, &operation.operation_id, &operation.actor)
        .unwrap();
    store_fixture
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Completed,
                status: OperationStatus::Succeeded,
                cursor: None,
                event_kind: OperationEventKind::Succeeded,
                counts: serde_json::json!({}),
                message: Some("operation completed"),
            },
        )
        .unwrap();

    let before =
        serde_json::to_value(store.show_operation(&operation.operation_id).unwrap()).unwrap();
    let events_before = store.audit_events(&operation.operation_id).unwrap();
    let error = store
        .prepare_for_execution(&operation.operation_id, &operation.actor)
        .unwrap_err();

    assert!(matches!(
        error,
        crate::OperationStoreError::InvalidStatusTransition {
            current_status: OperationStatus::Succeeded,
            next_status: OperationStatus::Running,
            ..
        }
    ));
    assert_eq!(
        serde_json::to_value(store.show_operation(&operation.operation_id).unwrap()).unwrap(),
        before,
    );
    assert_eq!(
        store.audit_events(&operation.operation_id).unwrap(),
        events_before
    );
}

#[test]
fn given_operation_store_reopened_when_operation_resumes_then_durable_cursor_is_preserved() {
    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join("operations.duckdb");
    let operation = request("op_1");
    {
        let store = OperationStore::connect_duckdb(db_path.as_path()).unwrap();
        store.create_operation(&operation).unwrap();
        let store_fixture = OperationStoreTestFixture::for_database(db_path.as_path());
        store_fixture
            .prepare(&store, &operation.operation_id, &operation.actor)
            .unwrap();
        store_fixture
            .transition(
                &store,
                &operation.operation_id,
                crate::OperationTransition {
                    phase: OperationPhase::Executing,
                    status: OperationStatus::Running,
                    cursor: Some(OperationCursor {
                        label: "chunk".to_string(),
                        position: 7,
                    }),
                    event_kind: OperationEventKind::CursorAdvanced,
                    counts: serde_json::json!({}),
                    message: Some("chunk committed"),
                },
            )
            .unwrap();
    }

    let reopened = OperationStore::connect_duckdb(db_path.as_path()).unwrap();
    let stored = reopened.show_operation(&operation.operation_id).unwrap();

    assert_eq!(stored.status, OperationStatus::Running);
    assert_eq!(stored.cursor.map(|cursor| cursor.position), Some(7));
}

#[test]
fn given_report_kind_when_loaded_through_another_typed_reader_then_report_is_missing() {
    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join("operations.duckdb");
    let store = OperationStore::connect_duckdb(db_path.as_path()).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(db_path.as_path());
    let operation_id = OperationId::try_new("op_report_kind").unwrap();
    let report = crate::TableFixReport {
        operation_id: operation_id.clone(),
        table_name: "users".to_string(),
        dry_run: true,
        selected_rows: 1,
        repaired_rows: 0,
        missing_rows: 1,
        destination_mutated: false,
        sample_keys: vec!["user-1".to_string()],
    };

    store_fixture
        .save_report(&operation_id, OperationReportKind::TableFix, &report)
        .unwrap();

    assert_eq!(store.table_fix_report(&operation_id).unwrap(), report);
    assert!(matches!(
        store.check_report(&operation_id),
        Err(OperationStoreError::OperationNotFound(id)) if id == operation_id
    ));
    assert!(matches!(
        store.backfill_report(&operation_id),
        Err(OperationStoreError::OperationNotFound(id)) if id == operation_id
    ));
}

#[test]
fn given_malformed_report_shape_when_loaded_then_a_json_error_is_returned() {
    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join("operations.duckdb");
    let store = OperationStore::connect_duckdb(db_path.as_path()).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(db_path.as_path());
    let operation_id = OperationId::try_new("op_malformed_report").unwrap();
    let report = crate::TableFixReport {
        operation_id: operation_id.clone(),
        table_name: "users".to_string(),
        dry_run: true,
        selected_rows: 1,
        repaired_rows: 0,
        missing_rows: 1,
        destination_mutated: false,
        sample_keys: vec!["user-1".to_string()],
    };
    store_fixture
        .save_report(&operation_id, OperationReportKind::TableFix, &report)
        .unwrap();

    let connection = duckdb::Connection::open(db_path.as_path()).unwrap();
    connection
        .execute(
            "UPDATE __analytics_operation_reports
             SET report_json = '{}'
             WHERE operation_id = 'op_malformed_report'",
            [],
        )
        .unwrap();

    assert!(matches!(
        store.table_fix_report(&operation_id),
        Err(OperationStoreError::Json(_))
    ));
}

#[test]
fn given_missing_effect_when_report_completion_fails_then_report_and_state_roll_back() {
    let store = OperationStore::connect_in_memory().unwrap();
    let mut operation = request("op_report_rollback");
    operation.kind = OperationKind::TableFix;
    store.create_operation(&operation).unwrap();
    let attempt = store
        .prepare_for_execution(&operation.operation_id, &operation.actor)
        .unwrap();
    let expected = store.show_operation(&operation.operation_id).unwrap();
    let effect_key = OperationChunkKey::for_operation(&operation.operation_id, "report", 0);

    let error = store
        .save_report_and_complete_effect_for_attempt(
            &expected,
            &attempt,
            &effect_key,
            crate::store::OperationReportWrite {
                kind: OperationReportKind::TableFix,
                document: serde_json::json!({
                    "operation_id": operation.operation_id,
                    "table_name": "users",
                }),
                transition: crate::OperationTransition {
                    phase: crate::OperationPhase::Completed,
                    status: OperationStatus::Succeeded,
                    cursor: None,
                    event_kind: OperationEventKind::Succeeded,
                    counts: serde_json::json!({ "repaired_rows": 1 }),
                    message: Some("table fix completed"),
                },
            },
        )
        .unwrap_err();

    assert!(matches!(
        error,
        OperationStoreError::EffectInProgress { .. }
    ));
    assert!(matches!(
        store.table_fix_report(&operation.operation_id),
        Err(OperationStoreError::OperationNotFound(_))
    ));
    let current = store.show_operation(&operation.operation_id).unwrap();
    assert_eq!(current.status, OperationStatus::Running);
    assert_eq!(current.revision, expected.revision);
}

#[test]
fn given_cancel_requested_when_worker_attempts_later_write_then_transition_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_1");
    store.create_operation(&operation).unwrap();
    let store_fixture = OperationStoreTestFixture::new();
    store_fixture
        .prepare(&store, &operation.operation_id, &operation.actor)
        .unwrap();
    store
        .request_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap();

    let error = store_fixture
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 1,
                }),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: serde_json::json!({}),
                message: Some("late write"),
            },
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

    let error = OperationStoreTestFixture::new()
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 1,
                }),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: serde_json::json!({
                    "rows_written": 1,
                    "payload": {
                        "email": "private@example.test"
                    }
                }),
                message: Some("chunk committed"),
            },
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

    let error = OperationStoreTestFixture::new()
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: None,
                event_kind: OperationEventKind::Started,
                counts: serde_json::json!({
                    "phase": "planning",
                    "raw_sql": "select * from users where email = 'private@example.test'"
                }),
                message: Some("operation started"),
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("raw_sql"));
    assert_eq!(
        store.audit_events(&operation.operation_id).unwrap().len(),
        1
    );
}

#[test]
fn given_submitted_operation_when_succeeded_directly_then_transition_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_submitted_success");
    store.create_operation(&operation).unwrap();

    let error = OperationStoreTestFixture::new()
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Completed,
                status: OperationStatus::Succeeded,
                cursor: None,
                event_kind: OperationEventKind::Succeeded,
                counts: serde_json::json!({}),
                message: Some("invalid direct success"),
            },
        )
        .unwrap_err();

    assert!(matches!(
        error,
        crate::OperationStoreError::InvalidStatusTransition {
            current_status: OperationStatus::Submitted,
            next_status: OperationStatus::Succeeded,
            ..
        }
    ));
    assert_eq!(
        store.audit_events(&operation.operation_id).unwrap().len(),
        1
    );
}

#[test]
fn given_cancelling_operation_when_cancel_is_accepted_then_cancel_is_terminal() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_cancel_accept");
    store.create_operation(&operation).unwrap();
    let store_fixture = OperationStoreTestFixture::new();
    store_fixture
        .prepare(&store, &operation.operation_id, &operation.actor)
        .unwrap();
    store_fixture
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 3,
                }),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: serde_json::json!({}),
                message: Some("chunk committed"),
            },
        )
        .unwrap();
    store
        .request_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap();
    store
        .accept_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap();

    let stored = store.show_operation(&operation.operation_id).unwrap();
    assert_eq!(stored.status, OperationStatus::Cancelled);
    assert_eq!(
        stored.cancellation_state,
        crate::CancellationState::Accepted
    );
    assert_eq!(stored.cursor.map(|cursor| cursor.position), Some(3));
    assert!(
        store_fixture
            .transition(
                &store,
                &operation.operation_id,
                crate::OperationTransition {
                    phase: OperationPhase::Completed,
                    status: OperationStatus::Succeeded,
                    cursor: None,
                    event_kind: OperationEventKind::Succeeded,
                    counts: serde_json::json!({}),
                    message: Some("late success"),
                },
            )
            .is_err()
    );
}

#[test]
fn given_wrong_actor_when_cancel_or_resume_is_requested_then_authority_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_authority");
    let wrong_actor = OperationActor::try_new("other-operator").unwrap();
    store.create_operation(&operation).unwrap();
    let store_fixture = OperationStoreTestFixture::new();
    store_fixture
        .prepare(&store, &operation.operation_id, &operation.actor)
        .unwrap();

    let error = store
        .request_cancellation_as(&operation.operation_id, &wrong_actor)
        .unwrap_err();
    assert!(matches!(
        error,
        crate::OperationStoreError::UnauthorizedActor { .. }
    ));
    assert_eq!(
        store
            .show_operation(&operation.operation_id)
            .unwrap()
            .status,
        OperationStatus::Running
    );

    store_fixture
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Paused,
                cursor: None,
                event_kind: OperationEventKind::Paused,
                counts: serde_json::json!({}),
                message: Some("pause"),
            },
        )
        .unwrap();
    let error = store
        .resume_operation(&operation.operation_id, &wrong_actor)
        .unwrap_err();
    assert!(matches!(
        error,
        crate::OperationStoreError::UnauthorizedActor { .. }
    ));
    assert_eq!(
        store
            .show_operation(&operation.operation_id)
            .unwrap()
            .status,
        OperationStatus::Paused
    );
}

#[test]
fn given_paused_operation_when_required_phase_fails_then_transition_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_paused_failure");
    store.create_operation(&operation).unwrap();
    let store_fixture = OperationStoreTestFixture::new();
    store_fixture
        .prepare(&store, &operation.operation_id, &operation.actor)
        .unwrap();
    store_fixture
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Paused,
                cursor: None,
                event_kind: OperationEventKind::Paused,
                counts: serde_json::json!({}),
                message: Some("pause before failure"),
            },
        )
        .unwrap();

    let error = store
        .fail_operation_as(
            &operation.operation_id,
            &operation.actor,
            OperationPhase::Executing,
            Some("paused failure"),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        crate::OperationStoreError::InvalidStatusTransition {
            current_status: OperationStatus::Paused,
            next_status: OperationStatus::Failed,
            ..
        }
    ));
    assert_eq!(
        store
            .show_operation(&operation.operation_id)
            .unwrap()
            .status,
        OperationStatus::Paused
    );
}

#[test]
fn given_two_store_connections_when_stale_worker_commits_after_cancel_then_cas_rejects_it() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let first = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let operation = request("op_cas_cancel");
    first.create_operation(&operation).unwrap();
    OperationStoreTestFixture::new()
        .prepare(&first, &operation.operation_id, &operation.actor)
        .unwrap();
    let stale_worker = first.show_operation(&operation.operation_id).unwrap();
    let second = OperationStore::connect_duckdb(path.as_path()).unwrap();

    second
        .request_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap();
    assert_eq!(
        second
            .show_operation(&operation.operation_id)
            .unwrap()
            .status,
        OperationStatus::Cancelling
    );
    let error = first
        .transition_with_counts_from(
            &stale_worker,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 1,
                }),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: serde_json::json!({ "rows_written": 1 }),
                message: Some("stale worker write"),
            },
        )
        .unwrap_err();

    assert!(matches!(
        error,
        crate::OperationStoreError::StaleOperationState {
            expected_revision: 2,
            ..
        }
    ));
    let current = second.show_operation(&operation.operation_id).unwrap();
    assert_eq!(current.status, OperationStatus::Cancelling);
    assert!(current.cursor.is_none());
}

#[test]
fn given_pending_effect_when_cancellation_interleaves_then_effect_fence_blocks_terminal_state() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let first = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let operation = request("op_cas_effect");
    first.create_operation(&operation).unwrap();
    OperationStoreTestFixture::new()
        .prepare(&first, &operation.operation_id, &operation.actor)
        .unwrap();
    let expected = first.show_operation(&operation.operation_id).unwrap();
    let second = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let effect_key = crate::OperationChunkKey::for_operation(&operation.operation_id, "chunk", 1);
    let attempt = expected
        .execution_attempt()
        .expect("started operation attempt");
    assert!(
        first
            .claim_effect_for_attempt(&expected, &attempt, &effect_key)
            .unwrap()
    );
    assert_eq!(
        first
            .show_operation(&operation.operation_id)
            .unwrap()
            .revision,
        expected.revision
    );

    let error = second
        .request_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap_err();
    assert!(matches!(
        error,
        crate::OperationStoreError::PendingEffect { .. }
    ));
    assert_eq!(
        second
            .show_operation(&operation.operation_id)
            .unwrap()
            .status,
        OperationStatus::Running
    );

    first
        .complete_effect_for_attempt(
            &expected,
            &attempt,
            &effect_key,
            crate::OperationEffectCompletion {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 1,
                }),
                counts: serde_json::json!({ "rows_written": 1 }),
                message: Some("effect committed"),
            },
        )
        .unwrap();
    second
        .request_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap();
    second
        .accept_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap();
    assert_eq!(
        second
            .show_operation(&operation.operation_id)
            .unwrap()
            .status,
        OperationStatus::Cancelled
    );
}

#[test]
fn given_active_execution_attempt_when_second_worker_prepares_then_admission_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let first = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let second = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let operation = request("op_attempt_admission");
    first.create_operation(&operation).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(path.as_path());
    let now_ms = current_time_ms();

    let attempt = store_fixture
        .prepare_at(&first, &operation.operation_id, &operation.actor, now_ms)
        .unwrap();
    let error = store_fixture
        .prepare_at(
            &second,
            &operation.operation_id,
            &operation.actor,
            now_ms + 1,
        )
        .unwrap_err();

    assert_eq!(attempt.generation(), 1);
    assert!(matches!(
        error,
        OperationStoreError::ExecutionAttemptInProgress { .. }
    ));
    let stored = second.show_operation(&operation.operation_id).unwrap();
    assert_eq!(stored.execution_generation, attempt.generation());
    assert_eq!(stored.execution_owner.as_deref(), Some(attempt.owner()));
    assert_eq!(stored.revision, 2);
    let events = second.audit_events(&operation.operation_id).unwrap();
    assert_eq!(
        events.iter().map(|event| event.kind).collect::<Vec<_>>(),
        vec![
            OperationEventKind::Submitted,
            OperationEventKind::Planned,
            OperationEventKind::Started,
        ],
    );
}

#[test]
fn given_cancellation_after_effect_snapshot_when_admission_runs_then_it_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let worker = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let controller = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let operation = request("op_effect_admission_cancel");
    worker.create_operation(&operation).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(path.as_path());
    let now_ms = current_time_ms();
    let attempt = store_fixture
        .prepare_at(&worker, &operation.operation_id, &operation.actor, now_ms)
        .unwrap();
    let expected = worker.show_operation(&operation.operation_id).unwrap();
    let effect_key = OperationChunkKey::for_operation(&operation.operation_id, "chunk", 1);

    controller
        .request_cancellation_as(&operation.operation_id, &operation.actor)
        .unwrap();

    assert!(matches!(
        worker.admit_effect_for_attempt(
            &expected,
            &attempt,
            &effect_key,
            EffectAdmissionMode::OrdinaryClaim,
        ),
        Err(OperationStoreError::StaleOperationState { .. })
    ));
}

#[test]
fn given_takeover_after_effect_snapshot_when_admission_runs_then_it_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let store = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(path.as_path());
    let operation = request("op_effect_admission_takeover");
    store.create_operation(&operation).unwrap();
    let now_ms = current_time_ms();
    let first = store_fixture
        .prepare_at(&store, &operation.operation_id, &operation.actor, now_ms)
        .unwrap();
    let expected = store.show_operation(&operation.operation_id).unwrap();
    let effect_key = OperationChunkKey::for_operation(&operation.operation_id, "chunk", 1);
    store_fixture
        .prepare_at(
            &store,
            &operation.operation_id,
            &operation.actor,
            now_ms + 300_001,
        )
        .unwrap();

    assert!(matches!(
        store.admit_effect_for_attempt(
            &expected,
            &first,
            &effect_key,
            EffectAdmissionMode::OrdinaryClaim,
        ),
        Err(OperationStoreError::StaleOperationState { .. })
    ));
}

#[test]
fn given_same_generation_revision_drift_when_effect_admission_runs_then_it_is_rejected() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operation = request("op_effect_admission_revision");
    store.create_operation(&operation).unwrap();
    let attempt = store
        .prepare_for_execution(&operation.operation_id, &operation.actor)
        .unwrap();
    let expected = store.show_operation(&operation.operation_id).unwrap();
    let effect_key = OperationChunkKey::for_operation(&operation.operation_id, "chunk", 1);
    store
        .transition_with_counts_for_attempt(
            &expected,
            &attempt,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 1,
                }),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: serde_json::json!({ "rows_written": 1 }),
                message: Some("same generation progress"),
            },
        )
        .unwrap();

    assert!(matches!(
        store.admit_effect_for_attempt(
            &expected,
            &attempt,
            &effect_key,
            EffectAdmissionMode::OrdinaryClaim,
        ),
        Err(OperationStoreError::StaleOperationState { .. })
    ));
}

#[test]
fn given_expired_execution_attempt_when_worker_retries_then_generation_is_taken_over() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let store = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(path.as_path());
    let operation = request("op_attempt_takeover");
    store.create_operation(&operation).unwrap();
    let now_ms = current_time_ms();
    let first = store_fixture
        .prepare_at(&store, &operation.operation_id, &operation.actor, now_ms)
        .unwrap();
    let stale_snapshot = store.show_operation(&operation.operation_id).unwrap();

    let second = store_fixture
        .prepare_at(
            &store,
            &operation.operation_id,
            &operation.actor,
            now_ms + 300_001,
        )
        .unwrap();
    let current = store.show_operation(&operation.operation_id).unwrap();

    assert_eq!(first.generation(), 1);
    assert_eq!(second.generation(), 2);
    assert_ne!(first.owner(), second.owner());
    assert_eq!(current.execution_generation, second.generation());
    assert_eq!(current.revision, stale_snapshot.revision + 1);
    assert_eq!(current.cursor, stale_snapshot.cursor);
    let events = store.audit_events(&operation.operation_id).unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(events[3].kind, OperationEventKind::CursorAdvanced);
    assert_eq!(
        events[3].counts,
        serde_json::json!({ "execution_takeover": true })
    );
    assert!(matches!(
        store.assert_execution_attempt_at(&stale_snapshot, &first, now_ms + 300_001),
        Err(OperationStoreError::StaleOperationState { .. })
    ));
    assert!(
        store
            .assert_execution_attempt_at(&current, &second, now_ms + 300_001)
            .is_ok()
    );
}

#[test]
fn given_expired_paused_attempt_when_worker_resumes_then_operation_is_not_wedged() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let store = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(path.as_path());
    let operation = request("op_paused_resume_after_expiry");
    store.create_operation(&operation).unwrap();
    let now_ms = current_time_ms();
    let first = store_fixture
        .prepare_at(&store, &operation.operation_id, &operation.actor, now_ms)
        .unwrap();

    store_fixture
        .transition(
            &store,
            &operation.operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Paused,
                cursor: None,
                event_kind: OperationEventKind::Paused,
                counts: serde_json::json!({}),
                message: Some("retryable attempt paused"),
            },
        )
        .unwrap();

    let paused = store.show_operation(&operation.operation_id).unwrap();
    let resumed = store_fixture
        .prepare_at(
            &store,
            &operation.operation_id,
            &operation.actor,
            now_ms + 300_001,
        )
        .unwrap();
    let current = store.show_operation(&operation.operation_id).unwrap();

    assert_eq!(first.generation(), 1);
    assert_eq!(resumed.generation(), 2);
    assert_eq!(current.status, OperationStatus::Running);
    assert_eq!(current.execution_generation, resumed.generation());
    assert_eq!(current.revision, paused.revision + 1);
    assert_eq!(current.cursor, paused.cursor);
    let events = store.audit_events(&operation.operation_id).unwrap();
    assert_eq!(events.len(), 5);
    assert_eq!(events[4].kind, OperationEventKind::Resumed);
    assert!(
        store
            .assert_execution_attempt_at(&current, &resumed, now_ms + 300_001)
            .is_ok()
    );
}

#[test]
fn given_pending_effect_when_attempt_is_taken_over_then_only_new_generation_can_commit_once() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let store = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(path.as_path());
    let operation = request("op_attempt_effect_fence");
    store.create_operation(&operation).unwrap();
    let now_ms = current_time_ms();
    let first = store_fixture
        .prepare_at(&store, &operation.operation_id, &operation.actor, now_ms)
        .unwrap();
    let expected_first = store.show_operation(&operation.operation_id).unwrap();
    let effect_key = OperationChunkKey::for_operation(&operation.operation_id, "chunk", 1);

    assert!(
        store
            .claim_effect_for_attempt(&expected_first, &first, &effect_key)
            .unwrap()
    );
    let second = store_fixture
        .prepare_at(
            &store,
            &operation.operation_id,
            &operation.actor,
            now_ms + 300_001,
        )
        .unwrap();
    let expected_second = store.show_operation(&operation.operation_id).unwrap();

    assert!(matches!(
        store.complete_effect_for_attempt(
            &expected_first,
            &first,
            &effect_key,
            crate::OperationEffectCompletion {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 1,
                }),
                counts: serde_json::json!({ "rows_written": 1 }),
                message: Some("stale completion")
            }
        ),
        Err(OperationStoreError::StaleOperationState { .. })
    ));
    assert!(
        store
            .admit_effect_for_attempt(
                &expected_second,
                &second,
                &effect_key,
                EffectAdmissionMode::MaintenanceReapply,
            )
            .unwrap()
    );
    assert!(matches!(
        store.admit_effect_for_attempt(
            &expected_second,
            &second,
            &effect_key,
            EffectAdmissionMode::MaintenanceReapply,
        ),
        Err(OperationStoreError::EffectInProgress { .. })
    ));
    store
        .complete_effect_for_attempt(
            &expected_second,
            &second,
            &effect_key,
            crate::OperationEffectCompletion {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 1,
                }),
                counts: serde_json::json!({ "rows_written": 1 }),
                message: Some("effect committed"),
            },
        )
        .unwrap();
    let completed = store.show_operation(&operation.operation_id).unwrap();
    assert!(
        !store
            .claim_effect_for_attempt(&completed, &second, &effect_key)
            .unwrap()
    );
}

#[test]
fn given_stale_pending_effect_when_replacement_pauses_then_retry_is_not_wedged() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let store = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(path.as_path());
    let operation = request("op_stale_effect_pause");
    store.create_operation(&operation).unwrap();
    let now_ms = current_time_ms();
    let first = store_fixture
        .prepare_at(&store, &operation.operation_id, &operation.actor, now_ms)
        .unwrap();
    let expected_first = store.show_operation(&operation.operation_id).unwrap();
    let effect_key = OperationChunkKey::for_operation(&operation.operation_id, "chunk", 1);
    assert!(
        store
            .claim_effect_for_attempt(&expected_first, &first, &effect_key)
            .unwrap()
    );

    let second = store_fixture
        .prepare_at(
            &store,
            &operation.operation_id,
            &operation.actor,
            now_ms + 300_001,
        )
        .unwrap();
    let expected_second = store.show_operation(&operation.operation_id).unwrap();
    store
        .transition_with_counts_for_attempt(
            &expected_second,
            &second,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Paused,
                cursor: expected_second.cursor.clone(),
                event_kind: OperationEventKind::Paused,
                counts: serde_json::json!({}),
                message: Some("retryable source failure"),
            },
        )
        .unwrap();
    assert_eq!(
        store
            .show_operation(&operation.operation_id)
            .unwrap()
            .status,
        OperationStatus::Paused
    );

    let third = store_fixture
        .prepare_at(
            &store,
            &operation.operation_id,
            &operation.actor,
            now_ms + 600_002,
        )
        .unwrap();
    let expected_third = store.show_operation(&operation.operation_id).unwrap();
    assert!(
        store
            .admit_effect_for_attempt(
                &expected_third,
                &third,
                &effect_key,
                EffectAdmissionMode::MaintenanceReapply,
            )
            .unwrap()
    );
}

#[test]
fn given_committed_prior_generation_when_maintenance_reapplies_then_admission_succeeds_once() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("operations.duckdb");
    let store = OperationStore::connect_duckdb(path.as_path()).unwrap();
    let store_fixture = OperationStoreTestFixture::for_database(path.as_path());
    let operation = request("op_committed_effect_reapply");
    store.create_operation(&operation).unwrap();
    let now_ms = current_time_ms();
    let first = store_fixture
        .prepare_at(&store, &operation.operation_id, &operation.actor, now_ms)
        .unwrap();
    let expected_first = store.show_operation(&operation.operation_id).unwrap();
    let effect_key = OperationChunkKey::for_operation(&operation.operation_id, "chunk", 1);

    assert!(
        store
            .admit_effect_for_attempt(
                &expected_first,
                &first,
                &effect_key,
                EffectAdmissionMode::OrdinaryClaim,
            )
            .unwrap()
    );
    store
        .complete_effect_for_attempt(
            &expected_first,
            &first,
            &effect_key,
            crate::OperationEffectCompletion {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: Some(OperationCursor {
                    label: "chunk".to_string(),
                    position: 1,
                }),
                counts: serde_json::json!({ "rows_written": 1 }),
                message: Some("effect committed"),
            },
        )
        .unwrap();

    let second = store_fixture
        .prepare_at(
            &store,
            &operation.operation_id,
            &operation.actor,
            now_ms + 300_001,
        )
        .unwrap();
    let expected_second = store.show_operation(&operation.operation_id).unwrap();

    assert!(
        !store
            .admit_effect_for_attempt(
                &expected_second,
                &second,
                &effect_key,
                EffectAdmissionMode::OrdinaryClaim,
            )
            .unwrap()
    );
    assert!(
        store
            .admit_effect_for_attempt(
                &expected_second,
                &second,
                &effect_key,
                EffectAdmissionMode::MaintenanceReapply,
            )
            .unwrap()
    );
    assert!(matches!(
        store.admit_effect_for_attempt(
            &expected_second,
            &second,
            &effect_key,
            EffectAdmissionMode::MaintenanceReapply,
        ),
        Err(OperationStoreError::EffectInProgress { .. })
    ));
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
