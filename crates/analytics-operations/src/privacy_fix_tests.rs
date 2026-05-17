use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};

use crate::{
    FilesystemRawBackupStore, OperationActor, OperationEventKind, OperationId, OperationKind,
    OperationPhase, OperationRequest, OperationStatus, OperationStore, PrivacyFixDestructiveAction,
    PrivacyFixError, PrivacyFixExecutor, PrivacyFixRequest, RateLimitPolicy, RawBackupObjectId,
    RawBackupRecord, RawBackupWriteRequest,
};

#[test]
fn given_privacy_fix_dry_run_when_raw_backup_is_tainted_then_report_counts_without_rewrite() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_fix_dry_run");
    let request = privacy_fix_request(index.object_id.clone(), true);

    let report = PrivacyFixExecutor
        .execute_raw_backup_rewrite(&request, &source_store, &clean_store, &[index])
        .unwrap();

    assert!(report.dry_run);
    assert_eq!(report.scanned_objects, 1);
    assert_eq!(report.tainted_objects, 1);
    assert_eq!(report.rewritten_objects, 0);
    assert_eq!(report.privacy_dropped_fields, 1);
    assert_eq!(report.affected_tables, vec!["users"]);
}

#[test]
fn given_privacy_fix_apply_when_raw_backup_is_tainted_then_clean_prefix_replay_is_verified() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_fix_apply");
    let request = privacy_fix_request(index.object_id.clone(), false);

    let report = PrivacyFixExecutor
        .execute_raw_backup_rewrite(
            &request,
            &source_store,
            &clean_store,
            std::slice::from_ref(&index),
        )
        .unwrap();
    let clean_index = clean_store.read_index(&index.object_id).unwrap();
    let clean_replay = clean_store.replay_object(&clean_index).unwrap();

    assert!(!report.dry_run);
    assert_eq!(report.rewritten_objects, 1);
    assert!(report.clean_replay_verified);
    assert!(
        !clean_replay[0]
            .record
            .new_image
            .as_ref()
            .unwrap()
            .contains_key("email")
    );
}

#[test]
fn given_destructive_privacy_fix_without_confirmation_then_request_is_rejected() {
    let mut request = privacy_fix_request(RawBackupObjectId::new("object_1").unwrap(), true);
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;

    let error = request.validate().unwrap_err();

    assert!(matches!(error, PrivacyFixError::MissingConfirmationToken));
}

#[test]
fn given_delete_privacy_fix_with_confirmation_then_tainted_backup_is_removed_after_verification() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_fix_delete");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());

    let report = PrivacyFixExecutor
        .execute_raw_backup_rewrite(
            &request,
            &source_store,
            &clean_store,
            std::slice::from_ref(&index),
        )
        .unwrap();

    assert!(report.clean_replay_verified);
    assert_eq!(report.deleted_tainted_objects, 1);
    assert!(source_store.read_index(&index.object_id).is_err());
    assert!(clean_store.read_index(&index.object_id).is_ok());
}

#[test]
fn given_quarantine_privacy_fix_with_confirmation_then_tainted_backup_is_copied_then_removed() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_fix_quarantine");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.destructive_action = PrivacyFixDestructiveAction::QuarantineTainted;
    request.confirmation_token = Some("quarantine-tainted".to_string());

    let report = PrivacyFixExecutor
        .execute_raw_backup_rewrite(
            &request,
            &source_store,
            &clean_store,
            std::slice::from_ref(&index),
        )
        .unwrap();
    let quarantine_index = clean_store
        .read_index(&RawBackupObjectId::new("quarantine_privacy_fix_quarantine").unwrap())
        .unwrap();

    assert!(report.clean_replay_verified);
    assert_eq!(report.quarantined_tainted_objects, 1);
    assert!(source_store.read_index(&index.object_id).is_err());
    assert_eq!(quarantine_index.checksum, index.checksum);
}

#[test]
fn given_privacy_fix_report_when_persisted_then_audit_counts_exclude_raw_values() {
    let tempdir = tempfile::tempdir().unwrap();
    let operation_id = OperationId::new("privacy_fix_audit").unwrap();
    let store = OperationStore::connect_duckdb(tempdir.path().join("operations.duckdb")).unwrap();
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: OperationKind::PrivacyFix,
            actor: OperationActor::new("test").unwrap(),
            target_tables: vec!["users".to_string()],
            dry_run: false,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::json!({}),
        })
        .unwrap();
    let source_store = FilesystemRawBackupStore::new(tempdir.path().join("source"));
    let clean_store = FilesystemRawBackupStore::new(tempdir.path().join("clean"));
    let index = write_tainted_backup(&source_store, "privacy_fix_audit_object");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id = operation_id.clone();
    let report = PrivacyFixExecutor
        .execute_raw_backup_rewrite(&request, &source_store, &clean_store, &[index])
        .unwrap();

    store
        .save_privacy_fix_report(&operation_id, &report)
        .unwrap();
    store
        .transition_with_counts(
            &operation_id,
            OperationPhase::Completed,
            OperationStatus::Succeeded,
            None,
            OperationEventKind::Succeeded,
            report.operation_counts(),
            Some("privacy fix applied"),
        )
        .unwrap();
    let events = store.audit_events(&operation_id).unwrap();
    let counts = &events.last().unwrap().counts;

    assert_eq!(counts["tainted_objects"], 1);
    assert_eq!(counts["rewritten_objects"], 1);
    assert_eq!(counts["affected_tables"][0], "users");
    assert!(counts.get("email").is_none());
    assert!(counts.get("private@example.test").is_none());
    assert_eq!(
        store
            .privacy_fix_report(&operation_id)
            .unwrap()
            .privacy_dropped_fields,
        1
    );
}

fn privacy_fix_request(object_id: RawBackupObjectId, dry_run: bool) -> PrivacyFixRequest {
    PrivacyFixRequest {
        operation_id: OperationId::new("privacy_fix_1").unwrap(),
        source_backup_prefix: "source".to_string(),
        clean_target_prefix: "clean".to_string(),
        raw_backup_object_ids: vec![object_id],
        tables: vec!["users".to_string()],
        policy: PrivacyPolicy::new("privacy-v1")
            .unwrap()
            .with_denied_key_name("email"),
        dry_run,
        destructive_action: PrivacyFixDestructiveAction::None,
        confirmation_token: None,
    }
}

fn write_tainted_backup(
    store: &FilesystemRawBackupStore,
    object_id: &str,
) -> crate::RawBackupObjectIndex {
    store
        .write_object_report(
            &RawBackupWriteRequest::new(
                OperationId::new("backup_1").unwrap(),
                RawBackupObjectId::new(object_id).unwrap(),
                "source_users",
                None,
                vec![
                    RawBackupRecord::new(
                        "users",
                        "user-1",
                        None,
                        StorageStreamRecord {
                            keys: StorageItem::new(),
                            sequence_number: "1".to_string(),
                            old_image: None,
                            new_image: Some(StorageItem::from([(
                                "email".to_string(),
                                StorageValue::S("private@example.test".to_string()),
                            )])),
                        },
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        )
        .unwrap()
        .index
}
