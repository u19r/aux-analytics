use std::cell::Cell;

use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};

use crate::{
    CheckComparisonStrategy, CheckMetrics, CheckOutcome, CheckReport, CheckSamples,
    FilesystemRawBackupStore, MaintenanceError, MaintenanceExecutionReport, MaintenanceOperations,
    MaintenanceRequest, MaintenanceSubmissionOutcome, OperationActor, OperationEventKind,
    OperationId, OperationPhase, OperationRequest, OperationStatus, OperationStore,
    PrivacyFixRequest, RateLimitPolicy, RawBackupDestinationKind, RawBackupError,
    RawBackupObjectId, RawBackupObjectIndex, RawBackupObjectStore, RawBackupRecord,
    RawBackupReplayReport, RawBackupResult, RawBackupVerifyReport, RawBackupWriteRequest,
    TableFixConfirmation, TableFixRequest, TableFixSelection, TableFixSource,
    TableFixSourceSnapshot, TableFixTruthRecord, TableFixValidation, TrimCandidateSnapshot,
    TrimCandidateSource, TrimDestination, TrimRequest, TrimResult, TrimTarget,
    VerifiedTableFixDestination, VersionedTableFixCurrentSource,
};

#[test]
fn given_repeated_submission_when_payload_changes_then_conflicts_are_rejected_and_confirmation_is_redacted()
 {
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("operator").unwrap();
    let object_id = RawBackupObjectId::try_new("backup-1").unwrap();
    let mut request = TableFixRequest {
        operation_id: OperationId::try_new("table-fix-1").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        source: TableFixSource::RawBackup {
            object_ids: vec![object_id],
        },
        validation: crate::TableFixValidation::default(),
    };
    request.validation.confirmation =
        Some(TableFixConfirmation::try_new("confirmation-secret-one").unwrap());
    let submission = MaintenanceRequest::table_fix(request.clone());

    assert_eq!(
        operations.submit(&submission, &actor).unwrap().outcome,
        MaintenanceSubmissionOutcome::Created
    );
    assert_eq!(
        operations.submit(&submission, &actor).unwrap().outcome,
        MaintenanceSubmissionOutcome::Existing
    );
    assert_eq!(store.audit_events(&request.operation_id).unwrap().len(), 1);
    let stored = store.show_operation(&request.operation_id).unwrap();
    assert!(stored.payload.get("confirmation").is_none());
    assert!(stored.payload["validation"].get("confirmation").is_none());
    assert_eq!(stored.payload["validation"]["has_confirmation"], true);
    assert!(
        !serde_json::to_string(&stored)
            .unwrap()
            .contains("confirmation-secret-one")
    );
    request.validation.confirmation =
        Some(TableFixConfirmation::try_new("confirmation-secret-two").unwrap());
    assert_eq!(
        operations
            .submit(&MaintenanceRequest::table_fix(request.clone()), &actor)
            .unwrap()
            .outcome,
        MaintenanceSubmissionOutcome::Existing
    );
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().payload,
        stored.payload
    );
    assert_eq!(store.audit_events(&request.operation_id).unwrap().len(), 1);

    let conflicting = MaintenanceRequest::table_fix(TableFixRequest {
        table_name: "orders".to_string(),
        ..request
    });
    assert!(matches!(
        operations.submit(&conflicting, &actor),
        Err(MaintenanceError::ConflictingOperation { .. })
    ));
}

#[test]
fn given_confirmed_trim_when_token_changes_then_persisted_identity_excludes_secrets() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("operator").unwrap();
    let mut request = TrimRequest {
        operation_id: OperationId::try_new("trim-secret-identity").unwrap(),
        table_name: "entries".to_string(),
        dry_run: false,
        target: TrimTarget::RowKeys {
            keys: vec![b"entry-1".to_vec()],
        },
        confirmation_token: Some("first-confirmation-secret".to_string()),
    };
    operations
        .submit(&MaintenanceRequest::trim(request.clone()), &actor)
        .unwrap();
    let stored = store.show_operation(&request.operation_id).unwrap();
    assert!(stored.payload.get("confirmation_token").is_none());
    assert!(
        !serde_json::to_string(&stored)
            .unwrap()
            .contains("first-confirmation-secret")
    );
    request.confirmation_token = Some("second-confirmation-secret".to_string());
    assert_eq!(
        operations
            .submit(&MaintenanceRequest::trim(request.clone()), &actor)
            .unwrap()
            .outcome,
        MaintenanceSubmissionOutcome::Existing
    );
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().payload,
        stored.payload
    );
    request.confirmation_token = None;
    assert!(matches!(
        operations.submit(&MaintenanceRequest::trim(request.clone()), &actor),
        Err(MaintenanceError::Trim(
            crate::TrimError::MissingConfirmationToken
        ))
    ));
    assert_eq!(store.audit_events(&request.operation_id).unwrap().len(), 1);
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().payload,
        stored.payload
    );
}

#[test]
fn given_missing_or_foreign_check_report_when_repair_is_requested_then_no_source_read_or_operation_is_created()
 {
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("operator").unwrap();
    let request = check_report_table_fix_request("table-fix-missing-report", "check-missing");
    let source = CountingTableFixSource::default();
    let mut destination = NoopTableFixDestination;

    let error = operations
        .execute_table_fix_check_report_verified(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        MaintenanceError::Store(crate::OperationStoreError::OperationNotFound(operation_id))
            if operation_id == OperationId::try_new("check-missing").unwrap()
    ));
    assert_eq!(source.calls.get(), 0);
    assert!(store.list_operations().unwrap().is_empty());

    let check_operation_id = OperationId::try_new("check-persisted").unwrap();
    let wrong_actor = OperationActor::try_new("different-operator").unwrap();
    persist_mismatched_check_report(&store, &check_operation_id, &wrong_actor);
    let mismatched_request =
        check_report_table_fix_request("table-fix-wrong-actor", "check-persisted");
    let source = CountingTableFixSource::default();
    let mut destination = NoopTableFixDestination;

    let error = operations
        .execute_table_fix_check_report_verified(
            &mismatched_request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        MaintenanceError::TableFix(crate::TableFixError::CheckReportProvenanceMismatch)
    ));
    assert_eq!(source.calls.get(), 0);
    assert_eq!(store.list_operations().unwrap().len(), 1);
}

#[test]
fn given_persisted_check_report_when_repair_is_executed_then_verified_row_is_repaired_once() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("operator").unwrap();
    let check_operation_id = OperationId::try_new("check-valid").unwrap();
    persist_mismatched_check_report(&store, &check_operation_id, &actor);
    let request = check_report_table_fix_request("table-fix-valid", "check-valid");
    let source = CountingTableFixSource::default();
    let mut destination = NoopTableFixDestination;

    let report = operations
        .execute_table_fix_check_report_verified(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        )
        .unwrap();

    let MaintenanceExecutionReport::TableFix(report) = report else {
        panic!("table-fix facade returned a different report kind");
    };
    assert_eq!(report.repaired_rows, 1);
    assert_eq!(source.calls.get(), 1);
}

#[test]
fn given_raw_backup_submission_when_public_submit_called_then_fence_rejection_is_not_persisted() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("operator").unwrap();
    let request = raw_backup_write_request("raw-backup-fingerprint", "object-fingerprint");

    let error = operations
        .submit(&MaintenanceRequest::raw_backup(request), &actor)
        .unwrap_err();

    assert!(matches!(
        error,
        MaintenanceError::RawBackup(RawBackupError::DestinationFenceUnavailable {
            destination: RawBackupDestinationKind::Generic
        })
    ));
    assert!(store.list_operations().unwrap().is_empty());
}

#[test]
fn given_completed_trim_when_execution_is_repeated_then_persisted_report_is_returned_without_new_effects()
 {
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("operator").unwrap();
    let request = TrimRequest {
        operation_id: OperationId::try_new("trim-1").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        target: TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        confirmation_token: None,
    };
    let source = FixedTrimSource {
        calls: Cell::new(0),
    };
    let mut destination = FixedTrimDestination { calls: 0 };

    let report = operations
        .execute_trim(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        )
        .unwrap();
    assert!(matches!(report, MaintenanceExecutionReport::Trim(_)));
    assert_eq!(source.calls.get(), 1);
    assert_eq!(destination.calls, 0);
    assert_eq!(
        store
            .trim_report(&request.operation_id)
            .unwrap()
            .candidate_rows,
        3
    );
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Succeeded
    );

    let second = operations
        .execute_trim(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        )
        .unwrap();
    assert_eq!(second, report);
    assert_eq!(source.calls.get(), 1);
    assert_eq!(destination.calls, 0);
}

#[test]
fn given_running_trim_when_execution_is_repeated_then_source_and_destination_are_not_called() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("operator").unwrap();
    let request = TrimRequest {
        operation_id: OperationId::try_new("trim-running").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        target: TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        confirmation_token: None,
    };
    operations
        .submit(&MaintenanceRequest::trim(request.clone()), &actor)
        .unwrap();
    operations.prepare(&request.operation_id, &actor).unwrap();
    let source = FixedTrimSource {
        calls: Cell::new(0),
    };
    let mut destination = FixedTrimDestination { calls: 0 };

    assert!(matches!(
        operations.execute_trim(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        ),
        Err(MaintenanceError::OperationInProgress { .. })
    ));
    assert_eq!(source.calls.get(), 0);
    assert_eq!(destination.calls, 0);
}

#[test]
fn given_cancellation_from_another_connection_when_trim_starts_then_no_source_or_destination_is_called()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("operations.duckdb");
    let owner_store = OperationStore::connect_duckdb(database_path.as_path()).unwrap();
    let cancellation_store = OperationStore::connect_duckdb(database_path.as_path()).unwrap();
    let operations = MaintenanceOperations::new(&owner_store);
    let actor = OperationActor::try_new("operator").unwrap();
    let request = TrimRequest {
        operation_id: OperationId::try_new("trim-cancel-before-effect").unwrap(),
        table_name: "users".to_string(),
        dry_run: false,
        target: TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        confirmation_token: Some("trim-confirmed".to_string()),
    };
    let submission = MaintenanceRequest::trim(request.clone());
    operations.submit(&submission, &actor).unwrap();
    operations.prepare(&request.operation_id, &actor).unwrap();

    // The second connection wins the lifecycle race while the worker is between
    // preparation and its effect claim.  The facade must observe Cancelling and
    // never invoke the destination.
    cancellation_store
        .request_cancellation_as(&request.operation_id, &actor)
        .unwrap();
    let source = FixedTrimSource {
        calls: Cell::new(0),
    };
    let mut destination = FixedTrimDestination { calls: 0 };

    let error = operations
        .execute_trim(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        MaintenanceError::Store(crate::OperationStoreError::InvalidStatusTransition { .. })
    ));
    assert_eq!(source.calls.get(), 0);
    assert_eq!(destination.calls, 0);
    assert_eq!(
        owner_store
            .show_operation(&request.operation_id)
            .unwrap()
            .status,
        OperationStatus::Cancelling
    );
}

#[test]
fn given_cancellation_during_source_read_when_trim_continues_then_destination_is_not_called() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("operations.duckdb");
    let owner_store = OperationStore::connect_duckdb(database_path.as_path()).unwrap();
    let cancellation_store = OperationStore::connect_duckdb(database_path.as_path()).unwrap();
    let operations = MaintenanceOperations::new(&owner_store);
    let actor = OperationActor::try_new("operator").unwrap();
    let request = TrimRequest {
        operation_id: OperationId::try_new("trim-cancel-during-source").unwrap(),
        table_name: "users".to_string(),
        dry_run: false,
        target: TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        confirmation_token: Some("trim-confirmed".to_string()),
    };
    operations
        .submit(&MaintenanceRequest::trim(request.clone()), &actor)
        .unwrap();
    let source = CancellingTrimSource {
        store: &cancellation_store,
        actor: actor.clone(),
        operation_id: request.operation_id.clone(),
        calls: Cell::new(0),
    };
    let mut destination = FixedTrimDestination { calls: 0 };
    let error = operations
        .execute_trim(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        MaintenanceError::Trim(crate::TrimError::OperationStore(
            crate::OperationStoreError::InvalidStatusTransition { .. }
        ))
    ));
    assert_eq!(source.calls.get(), 1);
    assert_eq!(destination.calls, 0);
    assert_eq!(
        owner_store
            .show_operation(&request.operation_id)
            .unwrap()
            .status,
        OperationStatus::Cancelling
    );
}

#[test]
fn given_concurrent_progress_during_trim_effect_when_lease_is_stale_then_operation_pauses_and_retry_succeeds()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("operations.duckdb");
    let owner_store = OperationStore::connect_duckdb(database_path.as_path()).unwrap();
    let progress_store = OperationStore::connect_duckdb(database_path.as_path()).unwrap();
    let operations = MaintenanceOperations::new(&owner_store);
    let actor = OperationActor::try_new("operator").unwrap();
    let request = TrimRequest {
        operation_id: OperationId::try_new("trim-stale-effect").unwrap(),
        table_name: "users".to_string(),
        dry_run: false,
        target: TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        confirmation_token: Some("trim-confirmed".to_string()),
    };
    let source = FixedTrimSource {
        calls: Cell::new(0),
    };
    let mut destination = ProgressAdvancingTrimDestination {
        store: &progress_store,
        actor: actor.clone(),
        operation_id: request.operation_id.clone(),
        advanced: Cell::new(false),
    };

    let error = operations
        .execute_trim(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        MaintenanceError::Trim(crate::TrimError::OperationStore(
            crate::OperationStoreError::StaleOperationState { .. }
        ))
    ));
    assert_eq!(
        owner_store
            .show_operation(&request.operation_id)
            .unwrap()
            .status,
        OperationStatus::Paused
    );

    let report = operations
        .execute_trim(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        )
        .expect("stale destination lease should be retryable");
    assert!(matches!(report, MaintenanceExecutionReport::Trim(_)));
    assert_eq!(
        owner_store
            .show_operation(&request.operation_id)
            .unwrap()
            .status,
        OperationStatus::Succeeded
    );
}

#[test]
fn given_owned_trim_when_another_actor_cancels_then_rejection_preserves_owner_cancellation_authority()
 {
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let owner = OperationActor::try_new("owner").unwrap();
    let other = OperationActor::try_new("other").unwrap();
    let request = TrimRequest {
        operation_id: OperationId::try_new("trim-cancel").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        target: TrimTarget::FullTableDrop,
        confirmation_token: None,
    };
    let submission = MaintenanceRequest::trim(request);
    operations.submit(&submission, &owner).unwrap();
    operations
        .prepare(submission.operation_id(), &owner)
        .unwrap();

    assert!(matches!(
        operations.request_cancellation(submission.operation_id(), &other),
        Err(MaintenanceError::Store(
            crate::OperationStoreError::UnauthorizedActor { .. }
        ))
    ));
    operations
        .request_cancellation(submission.operation_id(), &owner)
        .unwrap();
    operations
        .accept_cancellation(submission.operation_id(), &owner)
        .unwrap();
    assert_eq!(
        store
            .show_operation(submission.operation_id())
            .unwrap()
            .status,
        OperationStatus::Cancelled
    );
}

#[test]
fn given_unfenced_filesystem_raw_backup_when_apply_is_requested_then_nothing_is_persisted() {
    let parent = tempfile::tempdir().unwrap();
    let backup_root = parent.path().join("backup");
    let operation_store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&operation_store);
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("raw-backup-unfenced").unwrap(),
        RawBackupObjectId::try_new("object-unfenced").unwrap(),
        "source-users",
        None,
        vec![
            RawBackupRecord::try_new(
                "users",
                "user-1",
                None,
                StorageStreamRecord {
                    keys: StorageItem::new(),
                    sequence_number: "1".to_string(),
                    old_image: None,
                    new_image: None,
                },
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let actor = OperationActor::try_new("operator").unwrap();

    let error = operations
        .execute_raw_backup_write(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &FilesystemRawBackupStore::new(&backup_root),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        MaintenanceError::RawBackup(RawBackupError::DestinationFenceUnavailable {
            destination: RawBackupDestinationKind::Filesystem
        })
    ));
    assert!(operation_store.list_operations().unwrap().is_empty());
    assert!(!backup_root.exists());
}

#[test]
fn given_unfenced_generic_raw_backup_when_apply_is_requested_then_no_effect_or_operation_is_created()
 {
    let operation_store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&operation_store);
    let request = raw_backup_write_request("raw-backup-generic-unfenced", "object-generic");
    let actor = OperationActor::try_new("operator").unwrap();
    let store = UnfencedRawBackupStore {
        writes: Cell::new(0),
    };

    let error = operations
        .execute_raw_backup_write(&request, &actor, RateLimitPolicy::default(), &store)
        .unwrap_err();

    assert!(matches!(
        error,
        MaintenanceError::RawBackup(RawBackupError::DestinationFenceUnavailable {
            destination: RawBackupDestinationKind::Generic
        })
    ));
    assert_eq!(store.writes.get(), 0);
    assert!(operation_store.list_operations().unwrap().is_empty());
}

#[test]
fn given_unfenced_filesystem_privacy_fix_when_apply_is_requested_then_no_effect_or_operation_is_created()
 {
    let parent = tempfile::tempdir().unwrap();
    let source_root = parent.path().join("source");
    let clean_root = parent.path().join("clean");
    let quarantine_root = parent.path().join("quarantine");
    let operation_store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&operation_store);
    let actor = OperationActor::try_new("operator").unwrap();
    let request = PrivacyFixRequest {
        operation_id: OperationId::try_new("privacy-fix-unfenced").unwrap(),
        source_backup_prefix: source_root.to_string_lossy().into_owned(),
        clean_target_prefix: clean_root.to_string_lossy().into_owned(),
        quarantine_target_prefix: quarantine_root.to_string_lossy().into_owned(),
        raw_backup_object_ids: vec![RawBackupObjectId::try_new("privacy-object").unwrap()],
        tables: vec!["users".to_string()],
        policy: PrivacyPolicy::try_new("privacy-v1").unwrap(),
        dry_run: false,
        destructive_action: crate::PrivacyFixDestructiveAction::None,
        confirmation_token: None,
    };
    let source_store = FilesystemRawBackupStore::new(&source_root);
    let clean_store = FilesystemRawBackupStore::new(&clean_root);

    let error = operations
        .execute_privacy_fix_raw_backup(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &[],
            &source_store,
            &clean_store,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        MaintenanceError::PrivacyFix(crate::PrivacyFixError::RawBackup(
            RawBackupError::DestinationFenceUnavailable {
                destination: RawBackupDestinationKind::Filesystem
            }
        ))
    ));
    assert!(operation_store.list_operations().unwrap().is_empty());
    assert!(!source_root.exists());
    assert!(!clean_root.exists());
    assert!(!quarantine_root.exists());
}

#[test]
fn given_privacy_fix_submission_when_public_submit_called_then_fence_rejection_is_not_persisted() {
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("operator").unwrap();

    for (operation_id, dry_run) in [("privacy-fix-dry-run", true), ("privacy-fix-apply", false)] {
        let request = PrivacyFixRequest {
            operation_id: OperationId::try_new(operation_id).unwrap(),
            source_backup_prefix: std::env::temp_dir()
                .join(format!("analytics-maintenance-{operation_id}-source"))
                .to_string_lossy()
                .into_owned(),
            clean_target_prefix: std::env::temp_dir()
                .join(format!("analytics-maintenance-{operation_id}-clean"))
                .to_string_lossy()
                .into_owned(),
            quarantine_target_prefix: std::env::temp_dir()
                .join(format!("analytics-maintenance-{operation_id}-quarantine"))
                .to_string_lossy()
                .into_owned(),
            raw_backup_object_ids: vec![RawBackupObjectId::try_new("object-1").unwrap()],
            tables: vec!["users".to_string()],
            policy: PrivacyPolicy::try_new("privacy-v1").unwrap(),
            dry_run,
            destructive_action: crate::PrivacyFixDestructiveAction::None,
            confirmation_token: None,
        };
        let submission = MaintenanceRequest::privacy_fix(request);

        assert!(matches!(
            operations.submit(&submission, &actor),
            Err(MaintenanceError::PrivacyFix(
                crate::PrivacyFixError::RawBackup(RawBackupError::DestinationFenceUnavailable {
                    destination: RawBackupDestinationKind::Generic
                })
            ))
        ));
    }
    assert!(store.list_operations().unwrap().is_empty());
}

#[test]
fn given_local_privacy_fix_dry_run_when_executed_then_report_is_persisted() {
    let parent = tempfile::tempdir().unwrap();
    let source_root = parent.path().join("source");
    let clean_root = parent.path().join("clean");
    let quarantine_root = parent.path().join("quarantine");
    let source_store = FilesystemRawBackupStore::new(&source_root);
    let clean_store = FilesystemRawBackupStore::new(&clean_root);
    let object_id = RawBackupObjectId::try_new("privacy-local-dry-run-object").unwrap();
    source_store
        .write_fixture_object(&raw_backup_write_request(
            "privacy-local-dry-run-seed",
            object_id.as_str(),
        ))
        .unwrap();
    let mut request = PrivacyFixRequest {
        operation_id: OperationId::try_new("privacy-local-dry-run").unwrap(),
        source_backup_prefix: source_root.to_string_lossy().into_owned(),
        clean_target_prefix: clean_root.to_string_lossy().into_owned(),
        quarantine_target_prefix: quarantine_root.to_string_lossy().into_owned(),
        raw_backup_object_ids: vec![object_id],
        tables: vec!["users".to_string()],
        policy: PrivacyPolicy::try_new("privacy-v1").unwrap(),
        dry_run: true,
        destructive_action: crate::PrivacyFixDestructiveAction::None,
        confirmation_token: Some("privacy-confirmation-secret-one".to_string()),
    };
    let store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("operator").unwrap();

    let report = operations
        .execute_privacy_fix_raw_backup(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &[],
            &source_store,
            &clean_store,
        )
        .unwrap();

    let MaintenanceExecutionReport::PrivacyFix(report) = report else {
        panic!("privacy facade returned a different report kind");
    };
    assert!(report.dry_run);
    assert_eq!(report.scanned_objects, 1);
    assert_eq!(report.rewritten_objects, 0);
    assert_eq!(store.list_operations().unwrap().len(), 1);
    assert_eq!(
        store.privacy_fix_report(&request.operation_id).unwrap(),
        report
    );
    let stored = store.show_operation(&request.operation_id).unwrap();
    assert_eq!(stored.target_tables, vec!["users".to_string()]);
    assert!(stored.payload.get("confirmation_token").is_none());
    assert!(
        !serde_json::to_string(&stored)
            .unwrap()
            .contains("privacy-confirmation-secret-one")
    );
    let event_count = store.audit_events(&request.operation_id).unwrap().len();
    request.confirmation_token = Some("privacy-confirmation-secret-two".to_string());
    let repeated = operations
        .execute_privacy_fix_raw_backup(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &[],
            &source_store,
            &clean_store,
        )
        .unwrap();
    assert_eq!(repeated, MaintenanceExecutionReport::PrivacyFix(report));
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().payload,
        stored.payload
    );
    assert_eq!(
        store.audit_events(&request.operation_id).unwrap().len(),
        event_count
    );
    assert!(!clean_root.exists());
    assert!(!quarantine_root.exists());
}

#[test]
fn given_zero_source_rate_limit_when_backup_repair_starts_then_operation_pauses_before_destination_creation()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let operation_store = OperationStore::connect_in_memory().unwrap();
    let operations = MaintenanceOperations::new(&operation_store);
    let actor = OperationActor::try_new("operator").unwrap();
    let request = TableFixRequest {
        operation_id: OperationId::try_new("table-fix-rate-limit").unwrap(),
        table_name: "users".to_string(),
        dry_run: false,
        selection: TableFixSelection::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        source: TableFixSource::RawBackup {
            object_ids: vec![RawBackupObjectId::try_new("missing-index").unwrap()],
        },
        validation: TableFixValidation {
            require_post_fix_check: true,
            max_rows: 1_000,
            confirmation: Some(TableFixConfirmation::try_new("table-fix-confirmed").unwrap()),
        },
    };
    request.validate().unwrap();
    let rate_limit = RateLimitPolicy {
        max_source_requests_per_second: Some(0),
        ..RateLimitPolicy::default()
    };
    let source_store = FilesystemRawBackupStore::new(tempdir.path().join("source"));
    let factory_called = Cell::new(false);
    let error = operations
        .execute_table_fix_raw_backup_verified_from_store(
            &request,
            &actor,
            rate_limit,
            &source_store,
            || {
                factory_called.set(true);
                Ok(NoopTableFixDestination)
            },
        )
        .unwrap_err();

    assert!(matches!(error, MaintenanceError::RateLimit(_)));
    assert!(!factory_called.get());
    assert_eq!(
        operation_store
            .show_operation(&request.operation_id)
            .unwrap()
            .status,
        OperationStatus::Paused
    );
}

fn raw_backup_write_request(operation_id: &str, object_id: &str) -> RawBackupWriteRequest {
    RawBackupWriteRequest::try_new(
        OperationId::try_new(operation_id).unwrap(),
        RawBackupObjectId::try_new(object_id).unwrap(),
        "source-users",
        None,
        vec![
            RawBackupRecord::try_new(
                "users",
                "user-1",
                None,
                StorageStreamRecord {
                    keys: StorageItem::new(),
                    sequence_number: "1".to_string(),
                    old_image: None,
                    new_image: None,
                },
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

struct UnfencedRawBackupStore {
    writes: Cell<u32>,
}

impl RawBackupObjectStore for UnfencedRawBackupStore {
    fn verify_object_report(
        &self,
        _index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupVerifyReport> {
        Err(RawBackupError::ObjectStore(
            "unexpected generic verify".to_string(),
        ))
    }

    fn replay_verified_object(
        &self,
        _index: &RawBackupObjectIndex,
    ) -> RawBackupResult<(RawBackupVerifyReport, RawBackupReplayReport)> {
        Err(RawBackupError::ObjectStore(
            "unexpected generic replay".to_string(),
        ))
    }

    fn read_index(&self, _object_id: &RawBackupObjectId) -> RawBackupResult<RawBackupObjectIndex> {
        Err(RawBackupError::ObjectStore(
            "unexpected generic index read".to_string(),
        ))
    }
}

struct FixedTrimSource {
    calls: Cell<u32>,
}

struct NoopTableFixDestination;

#[derive(Default)]
struct CountingTableFixSource {
    calls: Cell<u32>,
}

impl VersionedTableFixCurrentSource for CountingTableFixSource {
    fn read_truth_snapshot(
        &self,
        _source_identity: &str,
        _table_name: &str,
        _row_keys: &[Vec<u8>],
    ) -> crate::TableFixResult<TableFixSourceSnapshot> {
        self.calls.set(self.calls.get() + 1);
        Ok(TableFixSourceSnapshot {
            source_revision: "check-report-source-v1".to_string(),
            records: vec![TableFixTruthRecord {
                table_name: "users".to_string(),
                record_key: b"user-1".to_vec(),
                record: StorageStreamRecord {
                    keys: StorageItem::from_iter([(
                        "pk".to_string(),
                        StorageValue::S("user-1".to_string()),
                    )]),
                    sequence_number: "1".to_string(),
                    old_image: None,
                    new_image: Some(StorageItem::from_iter([(
                        "email".to_string(),
                        StorageValue::S("fixed@example.test".to_string()),
                    )])),
                },
                stale: false,
            }],
        })
    }
}

fn check_report_table_fix_request(operation_id: &str, check_operation_id: &str) -> TableFixRequest {
    TableFixRequest {
        operation_id: OperationId::try_new(operation_id).unwrap(),
        table_name: "users".to_string(),
        dry_run: false,
        selection: TableFixSelection::CheckReport {
            operation_id: OperationId::try_new(check_operation_id).unwrap(),
        },
        source: TableFixSource::CheckReportMismatchSet {
            operation_id: OperationId::try_new(check_operation_id).unwrap(),
        },
        validation: TableFixValidation {
            require_post_fix_check: true,
            max_rows: 1_000,
            confirmation: Some(TableFixConfirmation::try_new("table-fix-confirmed").unwrap()),
        },
    }
}

fn persist_mismatched_check_report(
    store: &OperationStore,
    operation_id: &OperationId,
    actor: &OperationActor,
) {
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: crate::OperationKind::Check,
            actor: actor.clone(),
            target_tables: vec!["users".to_string()],
            dry_run: true,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::json!({"mode": "check"}),
        })
        .unwrap();
    let attempt = store.prepare_for_execution(operation_id, actor).unwrap();
    let expected = store.show_operation(operation_id).unwrap();
    let report = CheckReport {
        operation_id: operation_id.clone(),
        outcome: CheckOutcome::Mismatched,
        strategy: CheckComparisonStrategy::FullRows,
        metrics: CheckMetrics {
            mismatched_rows: 1,
            ..CheckMetrics::default()
        },
        samples: CheckSamples {
            mismatched_keys: vec!["users:user-1".to_string()],
            ..CheckSamples::default()
        },
    };
    store
        .save_check_report_and_finish_for_attempt(&expected, &attempt, &report)
        .unwrap();
}

impl VerifiedTableFixDestination for NoopTableFixDestination {
    fn apply_repair_with_key(
        &mut self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &crate::OperationChunkKey,
        _table_name: &str,
        _record_key: &[u8],
        _record: StorageStreamRecord,
        _expected_source_revision: &str,
    ) -> crate::TableFixResult<crate::TableFixApplyOutcome> {
        Ok(crate::TableFixApplyOutcome::Applied)
    }

    fn verify_repair(
        &self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &crate::OperationChunkKey,
        _table_name: &str,
        _record_key: &[u8],
        _expected: &StorageStreamRecord,
        _expected_source_revision: &str,
    ) -> crate::TableFixResult<bool> {
        Ok(true)
    }
}

struct CancellingTrimSource<'a> {
    store: &'a OperationStore,
    actor: OperationActor,
    operation_id: OperationId,
    calls: Cell<u32>,
}

impl TrimCandidateSource for CancellingTrimSource<'_> {
    fn candidate_snapshot(
        &self,
        _table_name: &str,
        _target: &TrimTarget,
    ) -> TrimResult<TrimCandidateSnapshot> {
        self.calls.set(self.calls.get() + 1);
        self.store
            .request_cancellation_as(&self.operation_id, &self.actor)
            .unwrap();
        Ok(TrimCandidateSnapshot {
            revision: "fixed-trim".to_string(),
            candidate_keys: vec!["candidate-0".to_string()],
        })
    }
}

impl TrimCandidateSource for FixedTrimSource {
    fn candidate_snapshot(
        &self,
        _table_name: &str,
        _target: &TrimTarget,
    ) -> TrimResult<TrimCandidateSnapshot> {
        self.calls.set(self.calls.get() + 1);
        Ok(TrimCandidateSnapshot {
            revision: "fixed-trim".to_string(),
            candidate_keys: (0..3).map(|index| format!("candidate-{index}")).collect(),
        })
    }
}

struct FixedTrimDestination {
    calls: u32,
}

impl TrimDestination for FixedTrimDestination {
    fn delete_candidate_snapshot_with_key(
        &mut self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &crate::OperationChunkKey,
        _table_name: &str,
        _target: &TrimTarget,
        expected: &TrimCandidateSnapshot,
    ) -> TrimResult<crate::TrimDeletionOutcome> {
        self.calls += 1;
        Ok(crate::TrimDeletionOutcome {
            candidate_rows: expected.candidate_rows(),
            rows_deleted: 3,
        })
    }
}

struct ProgressAdvancingTrimDestination<'a> {
    store: &'a OperationStore,
    actor: OperationActor,
    operation_id: OperationId,
    advanced: Cell<bool>,
}

impl TrimDestination for ProgressAdvancingTrimDestination<'_> {
    fn delete_candidate_snapshot_with_key(
        &mut self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &crate::OperationChunkKey,
        _table_name: &str,
        _target: &TrimTarget,
        expected: &TrimCandidateSnapshot,
    ) -> TrimResult<crate::TrimDeletionOutcome> {
        if !self.advanced.replace(true) {
            let operation = self.store.show_operation(&self.operation_id).unwrap();
            self.store
                .transition_with_counts_as(
                    &operation,
                    &self.actor,
                    crate::OperationTransition {
                        phase: OperationPhase::Executing,
                        status: OperationStatus::Running,
                        cursor: operation.cursor.clone(),
                        event_kind: OperationEventKind::CursorAdvanced,
                        counts: serde_json::json!({"destination_progress": 1}),
                        message: Some("test progress interleaving"),
                    },
                )
                .unwrap();
        }
        Ok(crate::TrimDeletionOutcome {
            candidate_rows: expected.candidate_rows(),
            rows_deleted: expected.candidate_rows(),
        })
    }
}
