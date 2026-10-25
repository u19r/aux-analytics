use std::{fs, path::Path};

use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};

use crate::{
    FilesystemRawBackupStore, OperationActor, OperationChunkKey, OperationEventKind, OperationId,
    OperationKind, OperationPhase, OperationRateLimiter, OperationRequest, OperationStatus,
    OperationStore, OperationStoreError, PrivacyFixDestructiveAction, PrivacyFixError,
    PrivacyFixExecutor, PrivacyFixRequest, RateLimitClock, RateLimitPolicy,
    RawBackupCleanupMetadata, RawBackupError, RawBackupObjectId, RawBackupRecord,
    RawBackupWriteRequest,
    privacy_fix::{NoopPrivacyFixEffectBoundary, PrivacyFixEffectBoundary},
    store_test_support::OperationStoreTestFixture,
};

#[test]
fn given_privacy_fix_dry_run_when_raw_backup_is_tainted_then_report_counts_without_rewrite() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_fix_dry_run");
    let request = privacy_fix_request(index.object_id.clone(), true);
    let mut effects = NoopPrivacyFixEffectBoundary;

    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[index],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    assert!(report.dry_run);
    assert_eq!(report.scanned_objects, 1);
    assert_eq!(report.tainted_objects, 1);
    assert_eq!(report.rewritten_objects, 0);
    assert_eq!(report.privacy_dropped_fields, 1);
    assert!(!report.clean_replay_verified);
    assert_eq!(report.affected_tables, vec!["users"]);
}

#[test]
fn given_dry_run_with_cleanup_journal_when_planning_then_all_stores_and_journal_remain_unchanged() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_dry_run_journal");
    let mut request = privacy_fix_request(index.object_id.clone(), true);
    request.source_backup_prefix = source_dir.path().to_string_lossy().into_owned();
    request.clean_target_prefix = clean_dir.path().to_string_lossy().into_owned();
    request.quarantine_target_prefix = quarantine_dir.path().to_string_lossy().into_owned();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    let progress = begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 0,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: false,
        },
    )
    .unwrap();
    source_store.save_cleanup_progress(&progress).unwrap();
    let before = snapshot_paths(&[source_dir.path(), clean_dir.path(), quarantine_dir.path()]);
    let mut effects = CountingPrivacyEffects::default();

    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    assert_eq!(report.tainted_objects, 1);
    assert_eq!(
        snapshot_paths(&[source_dir.path(), clean_dir.path(), quarantine_dir.path()]),
        before
    );
    assert_eq!(effects.calls, 0);
}

#[test]
fn given_absent_dry_run_destinations_when_planning_then_destinations_remain_absent() {
    let source_dir = tempfile::tempdir().unwrap();
    let destination_parent = tempfile::tempdir().unwrap();
    let clean_path = destination_parent.path().join("clean");
    let quarantine_path = destination_parent.path().join("quarantine");
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_dry_run_absent");
    let mut request = privacy_fix_request(index.object_id.clone(), true);
    request.source_backup_prefix = source_dir.path().to_string_lossy().into_owned();
    request.clean_target_prefix = clean_path.to_string_lossy().into_owned();
    request.quarantine_target_prefix = quarantine_path.to_string_lossy().into_owned();
    let mut effects = CountingPrivacyEffects::default();

    crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &FilesystemRawBackupStore::new(&clean_path),
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    assert!(!clean_path.exists());
    assert!(!quarantine_path.exists());
    assert_eq!(effects.calls, 0);
}

#[test]
fn given_missing_supplied_privacy_index_when_rewriting_then_source_index_fallback_is_used() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_index_fallback");
    let request = privacy_fix_request(index.object_id.clone(), true);
    let mut effects = NoopPrivacyFixEffectBoundary;

    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    assert_eq!(report.scanned_objects, 1);
    assert_eq!(report.tainted_objects, 1);
    assert_eq!(report.affected_tables, vec!["users"]);
}

#[test]
fn given_privacy_replay_records_in_multiple_tables_when_rewritten_then_first_seen_order_is_kept() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_multi_table_backup(&source_store, "privacy_affected_tables");
    let mut request = privacy_fix_request(index.object_id.clone(), true);
    request.tables = vec!["users".to_string(), "orders".to_string()];
    let mut effects = NoopPrivacyFixEffectBoundary;

    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    assert_eq!(report.affected_tables, vec!["users", "orders"]);
}

#[test]
fn given_privacy_replay_record_outside_requested_tables_when_executing_privacy_fix_then_nothing_is_published_or_cleaned()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_multi_table_backup(&source_store, "privacy_table_scope");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    let mut effects = NoopPrivacyFixEffectBoundary;

    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::TableOutsideRequestScope));
    assert!(source_store.read_index(&index.object_id).is_ok());
    assert!(clean_store.read_index(&index.object_id).is_err());
}

#[test]
fn given_mismatched_source_index_when_destructive_privacy_cleanup_runs_then_no_other_object_is_touched()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let quarantine_store = FilesystemRawBackupStore::new(quarantine_dir.path());
    let requested = write_tainted_backup(&source_store, "privacy_mismatch_a");
    let protected = write_tainted_backup(&source_store, "privacy_mismatch_b");
    let mut tampered = serde_json::to_value(&requested).unwrap();
    tampered["object_id"] = serde_json::json!(protected.object_id.as_str());
    std::fs::write(
        requested.index_path.as_str(),
        serde_json::to_vec(&tampered).unwrap(),
    )
    .unwrap();
    let mut request = privacy_fix_request(requested.object_id.clone(), false);
    request.source_backup_prefix = source_dir.path().to_string_lossy().into_owned();
    request.clean_target_prefix = clean_dir.path().to_string_lossy().into_owned();
    request.quarantine_target_prefix = quarantine_dir.path().to_string_lossy().into_owned();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    let mut effects = NoopPrivacyFixEffectBoundary;

    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(
        error,
        PrivacyFixError::RawBackup(RawBackupError::ObjectIdMismatch { requested: actual_requested, actual })
            if actual_requested == request.raw_backup_object_ids[0]
                && actual == protected.object_id
    ));
    assert!(source_dir.path().join("privacy_mismatch_a.jsonl").is_file());
    assert!(
        source_dir
            .path()
            .join("privacy_mismatch_a.index.json")
            .is_file()
    );
    assert_eq!(
        source_store.read_index(&protected.object_id).unwrap(),
        protected
    );
    assert!(source_dir.path().join("privacy_mismatch_b.jsonl").is_file());
    assert!(
        source_dir
            .path()
            .join("privacy_mismatch_b.index.json")
            .is_file()
    );
    assert!(
        !clean_dir
            .path()
            .join("privacy_mismatch_a.index.json")
            .exists()
    );
    assert!(
        !clean_dir
            .path()
            .join("privacy_mismatch_b.index.json")
            .exists()
    );
    assert!(
        !quarantine_dir
            .path()
            .join("quarantine_privacy_mismatch_b.index.json")
            .exists()
    );
    assert!(
        quarantine_store
            .read_index(&RawBackupObjectId::try_new("quarantine_privacy_mismatch_b").unwrap())
            .is_err()
    );
}

#[test]
fn given_privacy_fix_apply_when_raw_backup_is_tainted_then_clean_prefix_replay_is_verified() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_fix_apply");
    let request = privacy_fix_request(index.object_id.clone(), false);
    let mut effects = NoopPrivacyFixEffectBoundary;

    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
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
fn given_destructive_privacy_fix_without_confirmation_when_executing_privacy_fix_then_request_is_rejected()
 {
    let mut request = privacy_fix_request(RawBackupObjectId::try_new("object_1").unwrap(), true);
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;

    let error = request.validate().unwrap_err();

    assert!(matches!(error, PrivacyFixError::MissingConfirmationToken));
}

#[test]
fn given_blank_destructive_privacy_confirmation_when_executing_privacy_fix_then_request_is_rejected()
 {
    let mut request =
        privacy_fix_request(RawBackupObjectId::try_new("object_blank").unwrap(), true);
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("  ".to_string());

    assert!(matches!(
        request.validate(),
        Err(PrivacyFixError::MissingConfirmationToken)
    ));
}

#[test]
fn given_privacy_request_without_a_distinct_quarantine_destination_when_executing_privacy_fix_then_it_is_rejected()
 {
    let object_id = RawBackupObjectId::try_new("object_quarantine_destination").unwrap();
    let mut request = privacy_fix_request(object_id, true);
    request.quarantine_target_prefix = request.clean_target_prefix.clone();

    assert!(matches!(
        request.validate(),
        Err(PrivacyFixError::InvalidBackupPrefixes)
    ));
}

#[cfg(unix)]
#[test]
fn given_privacy_stores_with_a_symlink_alias_when_executing_privacy_fix_then_execution_is_rejected()
{
    let source_dir = tempfile::tempdir().unwrap();
    let clean_parent = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let clean_alias = clean_parent.path().join("clean-alias");
    std::os::unix::fs::symlink(source_dir.path(), &clean_alias).unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(&clean_alias);
    let index = write_tainted_backup(&source_store, "privacy_symlink_alias");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.source_backup_prefix = source_dir.path().to_string_lossy().into_owned();
    request.clean_target_prefix = clean_alias.to_string_lossy().into_owned();
    request.quarantine_target_prefix = quarantine_dir.path().to_string_lossy().into_owned();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    let mut effects = NoopPrivacyFixEffectBoundary;

    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::OverlappingBackupStores));
    assert!(source_store.read_index(&index.object_id).is_ok());
}

#[test]
fn given_privacy_store_with_a_relative_root_when_executing_privacy_fix_then_execution_is_rejected()
{
    let clean_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new("privacy-relative-source");
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let object_id = RawBackupObjectId::try_new("privacy_relative_root").unwrap();
    let mut request = privacy_fix_request(object_id, true);
    request.clean_target_prefix = clean_dir.path().to_string_lossy().into_owned();
    request.quarantine_target_prefix = quarantine_dir.path().to_string_lossy().into_owned();
    let mut effects = NoopPrivacyFixEffectBoundary;

    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(
        error,
        PrivacyFixError::RawBackup(RawBackupError::RelativeStoreRoot)
    ));
}

#[test]
fn given_privacy_policy_denies_record_identity_when_executing_privacy_fix_then_clean_output_is_not_persisted()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_denied_record_identity");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.policy = PrivacyPolicy::try_new("privacy-identity")
        .unwrap()
        .with_denied_key_name("user-1");
    let mut effects = NoopPrivacyFixEffectBoundary;

    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(
        error,
        PrivacyFixError::RawBackup(RawBackupError::PrivacyIdentityDenied)
    ));
    assert!(clean_store.read_index(&index.object_id).is_err());
}

#[test]
fn given_delete_privacy_fix_with_confirmation_when_executing_privacy_fix_then_tainted_backup_is_removed_after_verification()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_fix_delete");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    let mut effects = NoopPrivacyFixEffectBoundary;

    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    assert!(report.clean_replay_verified);
    assert_eq!(report.deleted_tainted_objects, 1);
    assert!(source_store.read_index(&index.object_id).is_err());
    assert!(clean_store.read_index(&index.object_id).is_ok());
}

#[test]
fn given_late_privacy_replay_failure_when_executing_privacy_fix_then_prior_source_objects_remain_and_staged_clean_objects_are_removed()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let first = write_tainted_backup(&source_store, "privacy_fix_late_failure_first");
    let second = write_tainted_backup(&source_store, "privacy_fix_late_failure_second");
    std::fs::write(second.object_path.as_str(), b"corrupt\n").unwrap();

    let mut request = privacy_fix_request(first.object_id.clone(), false);
    request.raw_backup_object_ids = vec![first.object_id.clone(), second.object_id.clone()];
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    let mut effects = NoopPrivacyFixEffectBoundary;

    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[first.clone(), second.clone()],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(
        error,
        PrivacyFixError::RawBackup(RawBackupError::ChecksumMismatch { .. })
    ));
    assert!(source_store.read_index(&first.object_id).is_ok());
    assert!(clean_store.read_index(&first.object_id).is_err());
}

#[test]
fn given_quarantine_privacy_fix_with_confirmation_when_executing_privacy_fix_then_tainted_backup_is_copied_then_removed()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let quarantine_store = FilesystemRawBackupStore::new(quarantine_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_fix_quarantine");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.quarantine_target_prefix = quarantine_dir.path().to_string_lossy().into_owned();
    request.destructive_action = PrivacyFixDestructiveAction::QuarantineTainted;
    request.confirmation_token = Some("quarantine-tainted".to_string());
    let mut effects = NoopPrivacyFixEffectBoundary;

    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();
    let quarantine_index = quarantine_store
        .read_index(&RawBackupObjectId::try_new("quarantine_privacy_fix_quarantine").unwrap())
        .unwrap();

    assert!(report.clean_replay_verified);
    assert_eq!(report.quarantined_tainted_objects, 1);
    assert!(source_store.read_index(&index.object_id).is_err());
    assert!(
        clean_store
            .read_index(&RawBackupObjectId::try_new("quarantine_privacy_fix_quarantine").unwrap())
            .is_err()
    );
    assert_eq!(quarantine_index.checksum, index.checksum);
}

#[test]
fn given_claimed_quarantine_with_missing_source_index_when_resuming_cleanup_then_verified_copy_is_retained()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let quarantine_store = FilesystemRawBackupStore::new(quarantine_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_quarantine_resume");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.quarantine_target_prefix = quarantine_dir.path().to_string_lossy().into_owned();

    let mut effects = NoopPrivacyFixEffectBoundary;
    crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    request.destructive_action = PrivacyFixDestructiveAction::QuarantineTainted;
    request.confirmation_token = Some("quarantine-tainted".to_string());
    source_store
        .quarantine_object(&index, &quarantine_store)
        .unwrap();
    source_store.delete_object_if_present(&index).unwrap();
    let mut progress = begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "quarantine_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();
    progress.claimed_object_ids.push(index.object_id.clone());
    source_store.save_cleanup_progress(&progress).unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    assert_eq!(report.quarantined_tainted_objects, 1);
    assert!(source_store.read_index(&index.object_id).is_err());
    assert!(
        quarantine_store
            .read_index(
                &RawBackupObjectId::try_new("quarantine_privacy_quarantine_resume").unwrap()
            )
            .is_ok()
    );
}

#[test]
fn given_claimed_quarantine_with_missing_source_index_but_object_remaining_when_resuming_cleanup_then_cleanup_is_rejected()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let quarantine_store = FilesystemRawBackupStore::new(quarantine_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_quarantine_missing_index");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.quarantine_target_prefix = quarantine_dir.path().to_string_lossy().into_owned();

    let mut effects = NoopPrivacyFixEffectBoundary;
    crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    request.destructive_action = PrivacyFixDestructiveAction::QuarantineTainted;
    request.confirmation_token = Some("quarantine-tainted".to_string());
    source_store
        .quarantine_object(&index, &quarantine_store)
        .unwrap();
    std::fs::remove_file(index.index_path.as_str()).unwrap();
    let mut progress = begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "quarantine_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();
    progress.claimed_object_ids.push(index.object_id.clone());
    source_store.save_cleanup_progress(&progress).unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::CleanupAuthorityMismatch));
    assert!(std::path::Path::new(index.object_path.as_str()).is_file());
    assert!(
        quarantine_store
            .read_index(
                &RawBackupObjectId::try_new("quarantine_privacy_quarantine_missing_index").unwrap(),
            )
            .is_ok()
    );
}

#[test]
fn given_claimed_quarantine_with_a_changed_source_fingerprint_when_resuming_cleanup_then_source_cleanup_is_rejected()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let quarantine_store = FilesystemRawBackupStore::new(quarantine_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_quarantine_fingerprint");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.quarantine_target_prefix = quarantine_dir.path().to_string_lossy().into_owned();

    let mut effects = NoopPrivacyFixEffectBoundary;
    crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();
    request.destructive_action = PrivacyFixDestructiveAction::QuarantineTainted;
    request.confirmation_token = Some("quarantine-tainted".to_string());
    source_store
        .quarantine_object(&index, &quarantine_store)
        .unwrap();
    source_store.delete_object_if_present(&index).unwrap();
    let mut progress = begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "quarantine_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();
    progress.authority.as_mut().unwrap().expected_source_indexes[0].checksum =
        "fnv1a64:0000000000000000".to_string();
    progress.claimed_object_ids.push(index.object_id.clone());
    source_store.save_cleanup_progress(&progress).unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::CleanupAuthorityMismatch));
    assert!(source_store.read_index(&index.object_id).is_err());
    assert!(
        quarantine_store
            .read_index(
                &RawBackupObjectId::try_new("quarantine_privacy_quarantine_fingerprint").unwrap()
            )
            .is_ok()
    );
}

#[test]
fn given_resume_source_index_changes_after_cleanup_is_claimed_when_resuming_cleanup_then_cleanup_is_rejected()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_resume_index_change");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id = OperationId::try_new("privacy_resume_index_change_operation").unwrap();
    request.policy = PrivacyPolicy::try_new("privacy-v1").unwrap();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    write_tainted_backup(&clean_store, index.object_id.as_str());
    begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();

    let mut persisted_index: crate::RawBackupObjectIndex =
        serde_json::from_slice(&std::fs::read(index.index_path.as_str()).unwrap()).unwrap();
    persisted_index.source_identity = "another-source".to_string();
    std::fs::write(
        index.index_path.as_str(),
        serde_json::to_vec_pretty(&persisted_index).unwrap(),
    )
    .unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::CleanupAuthorityMismatch));
    assert!(std::path::Path::new(index.object_path.as_str()).is_file());
}

#[test]
fn given_privacy_cleanup_fails_midway_when_retried_then_completed_source_is_not_reread() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let first = write_tainted_backup(&source_store, "privacy_retry_first");
    let second = write_tainted_backup(&source_store, "privacy_retry_second");
    let mut request = privacy_fix_request(first.object_id.clone(), false);
    request.operation_id = OperationId::try_new("privacy_cleanup_retry").unwrap();
    request.raw_backup_object_ids = vec![first.object_id.clone(), second.object_id.clone()];
    let second_object_bytes = std::fs::read(second.object_path.as_str()).unwrap();

    // Publish both clean objects first. The cleanup journal is created only
    // after the complete rewrite, so seed it explicitly to exercise the failure
    // window between source deletions.
    let rate_limit = RateLimitPolicy {
        max_source_requests_per_second: Some(100),
        max_destination_writes_per_second: Some(100),
        ..RateLimitPolicy::default()
    };
    let mut effects = NoopPrivacyFixEffectBoundary;
    let rewrite_report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[first.clone(), second.clone()],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &rate_limit,
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();
    assert!(rewrite_report.clean_replay_verified);
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    let progress = begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        &[first.object_id.clone(), second.object_id.clone()],
        &[first.clone(), second.clone()],
        RawBackupCleanupMetadata {
            scanned_objects: rewrite_report.scanned_objects,
            tainted_objects: rewrite_report.tainted_objects,
            rewritten_objects: rewrite_report.rewritten_objects,
            affected_tables: rewrite_report.affected_tables.clone(),
            policy_version: rewrite_report.policy_version.clone(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: rewrite_report.privacy_dropped_fields,
            clean_replay_verified: rewrite_report.clean_replay_verified,
        },
    )
    .unwrap();
    assert!(progress.completed_object_ids.is_empty());
    source_store.delete_object_if_present(&first).unwrap();
    let mut progress = source_store
        .load_cleanup_progress(&request.operation_id)
        .unwrap()
        .unwrap();
    progress
        .pending_object_ids
        .retain(|pending| pending != &first.object_id);
    progress.completed_object_ids.push(first.object_id.clone());
    source_store.save_cleanup_progress(&progress).unwrap();

    let second_object_path = std::path::Path::new(second.object_path.as_str());
    std::fs::remove_file(second_object_path).unwrap();
    std::fs::create_dir(second_object_path).unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let first_error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[first.clone(), second.clone()],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &rate_limit,
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();
    assert!(matches!(first_error, PrivacyFixError::RawBackup(_)));
    assert!(source_store.read_index(&first.object_id).is_err());
    let progress = source_store
        .load_cleanup_progress(&request.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(progress.completed_object_ids, vec![first.object_id.clone()]);

    std::fs::remove_dir(second_object_path).unwrap();
    std::fs::write(second.object_path.as_str(), second_object_bytes).unwrap();
    let mut effects = NoopPrivacyFixEffectBoundary;
    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[first.clone(), second.clone()],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &rate_limit,
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    assert_eq!(report.scanned_objects, 2);
    assert_eq!(report.tainted_objects, 2);
    assert_eq!(report.rewritten_objects, 2);
    assert_eq!(report.deleted_tainted_objects, 2);
    assert!(
        source_store
            .load_cleanup_progress(&request.operation_id)
            .unwrap()
            .is_none()
    );
    assert!(source_store.read_index(&second.object_id).is_err());
}

#[test]
fn given_privacy_rewrite_with_multiple_objects_when_source_budget_is_exhausted_then_later_replay_is_rejected()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let first = write_tainted_backup(&source_store, "privacy_cumulative_rate_first");
    let second = write_tainted_backup(&source_store, "privacy_cumulative_rate_second");
    let mut request = privacy_fix_request(first.object_id.clone(), true);
    request.operation_id = OperationId::try_new("privacy_cumulative_rate_operation").unwrap();
    request.raw_backup_object_ids = vec![first.object_id.clone(), second.object_id.clone()];
    let rate_limit = RateLimitPolicy {
        max_source_requests_per_second: Some(3),
        ..RateLimitPolicy::default()
    };
    let mut effects = NoopPrivacyFixEffectBoundary;

    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[first, second],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &rate_limit,
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::RateLimit(_)));
    assert!(
        clean_store
            .read_index(&request.raw_backup_object_ids[0])
            .is_err()
    );
    assert!(
        clean_store
            .read_index(&request.raw_backup_object_ids[1])
            .is_err()
    );
}

#[test]
fn given_shared_privacy_limiter_when_execution_crosses_epoch_then_budget_resets() {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_epoch_rate_reset");
    let mut request = privacy_fix_request(index.object_id.clone(), true);
    request.operation_id = OperationId::try_new("privacy_epoch_rate_operation").unwrap();
    let rate_limit = RateLimitPolicy {
        max_source_requests_per_second: Some(3),
        ..RateLimitPolicy::default()
    };
    let limiter = OperationRateLimiter::new(rate_limit);

    let mut effects = NoopPrivacyFixEffectBoundary;
    crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_limiter_at(
            &input,
            &limiter,
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    let same_epoch_error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_limiter_at(
            &input,
            &limiter,
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();
    assert!(matches!(same_epoch_error, PrivacyFixError::RateLimit(_)));

    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_limiter_at(
            &input,
            &limiter,
            &EpochRateLimitClock { now_ms: 1_000 },
            &mut effects,
        )
    })
    .unwrap();
    assert_eq!(report.scanned_objects, 1);
}

#[test]
fn given_resumed_privacy_cleanup_is_rate_limited_when_resuming_cleanup_then_clean_output_is_not_read()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let object_id = RawBackupObjectId::try_new("privacy_resume_rate_limited").unwrap();
    let index = write_tainted_backup(&source_store, object_id.as_str());
    let mut request = privacy_fix_request(object_id.clone(), false);
    request.operation_id = OperationId::try_new("privacy_resume_rate_limited_operation").unwrap();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();
    let rate_limit = RateLimitPolicy {
        max_source_requests_per_second: Some(0),
        ..RateLimitPolicy::default()
    };

    let mut effects = NoopPrivacyFixEffectBoundary;
    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &rate_limit,
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::RateLimit(_)));
}

#[test]
fn given_privacy_cleanup_metadata_mismatch_when_resuming_cleanup_then_progress_is_rejected_without_rewrite()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_progress_metadata_mismatch");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id =
        OperationId::try_new("privacy_progress_metadata_mismatch_operation").unwrap();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: "privacy-v0".to_string(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();
    let before = source_store
        .load_cleanup_progress(&request.operation_id)
        .unwrap()
        .unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::CleanupAuthorityMismatch));
    assert_eq!(
        source_store
            .load_cleanup_progress(&request.operation_id)
            .unwrap(),
        Some(before)
    );
}

#[test]
fn given_privacy_cleanup_partition_has_cross_state_overlap_when_resuming_cleanup_then_cleanup_is_rejected()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_progress_overlap");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id = OperationId::try_new("privacy_progress_overlap_operation").unwrap();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();
    let mut progress = source_store
        .load_cleanup_progress(&request.operation_id)
        .unwrap()
        .unwrap();
    progress.completed_object_ids.push(index.object_id.clone());
    source_store.save_cleanup_progress(&progress).unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::CleanupAuthorityMismatch));
    assert!(source_store.read_index(&index.object_id).is_ok());
}

#[test]
fn given_privacy_cleanup_journal_reorders_the_requested_objects_when_resuming_cleanup_then_cleanup_is_rejected()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let first = write_tainted_backup(&source_store, "privacy_progress_order_first");
    let second = write_tainted_backup(&source_store, "privacy_progress_order_second");
    let mut request = privacy_fix_request(first.object_id.clone(), false);
    request.operation_id = OperationId::try_new("privacy_progress_order_operation").unwrap();
    request.raw_backup_object_ids = vec![first.object_id.clone(), second.object_id.clone()];
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        &[first.object_id.clone(), second.object_id.clone()],
        &[first.clone(), second.clone()],
        RawBackupCleanupMetadata {
            scanned_objects: 2,
            tainted_objects: 2,
            rewritten_objects: 2,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: 2,
            clean_replay_verified: true,
        },
    )
    .unwrap();
    let mut progress = source_store
        .load_cleanup_progress(&request.operation_id)
        .unwrap()
        .unwrap();
    progress
        .authority
        .as_mut()
        .unwrap()
        .requested_object_ids
        .reverse();
    source_store.save_cleanup_progress(&progress).unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::CleanupAuthorityMismatch));
    assert!(source_store.read_index(&first.object_id).is_ok());
    assert!(source_store.read_index(&second.object_id).is_ok());
}

#[test]
fn given_privacy_cleanup_journal_for_another_clean_store_when_resuming_cleanup_then_cleanup_is_rejected()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let authorized_clean_dir = tempfile::tempdir().unwrap();
    let redirected_clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let authorized_clean_store = FilesystemRawBackupStore::new(authorized_clean_dir.path());
    let redirected_clean_store = FilesystemRawBackupStore::new(redirected_clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_progress_store_mismatch");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id =
        OperationId::try_new("privacy_progress_store_mismatch_operation").unwrap();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    let quarantine_store = FilesystemRawBackupStore::new(&request.quarantine_target_prefix);
    begin_cleanup_progress_with_authority(
        &source_store,
        &authorized_clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &redirected_clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::CleanupAuthorityMismatch));
    assert!(source_store.read_index(&index.object_id).is_ok());
    assert!(redirected_clean_store.read_index(&index.object_id).is_err());
    assert!(
        quarantine_store
            .read_index(
                &RawBackupObjectId::try_new("quarantine_privacy_progress_store_mismatch").unwrap()
            )
            .is_err()
    );
}

#[test]
fn given_privacy_cleanup_invalid_claim_when_resuming_cleanup_then_progress_and_metadata_are_preserved()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_progress_invalid_claim");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id =
        OperationId::try_new("privacy_progress_invalid_claim_operation").unwrap();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();
    let mut progress = source_store
        .load_cleanup_progress(&request.operation_id)
        .unwrap()
        .unwrap();
    progress
        .claimed_object_ids
        .push(RawBackupObjectId::try_new("not-pending").unwrap());
    source_store.save_cleanup_progress(&progress).unwrap();
    let before = source_store
        .load_cleanup_progress(&request.operation_id)
        .unwrap()
        .unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(error, PrivacyFixError::CleanupAuthorityMismatch));
    assert_eq!(
        source_store
            .load_cleanup_progress(&request.operation_id)
            .unwrap(),
        Some(before)
    );
}

#[test]
fn given_cleanup_progress_save_fails_after_delete_when_resuming_cleanup_then_retry_uses_claimed_intent()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_cleanup_save_failure");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id = OperationId::try_new("privacy_cleanup_save_failure_operation").unwrap();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());

    source_store.fail_next_cleanup_progress_save_after_delete();
    let mut effects = NoopPrivacyFixEffectBoundary;
    let first_error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();
    assert!(matches!(first_error, PrivacyFixError::CleanupFailed { .. }));
    assert!(source_store.read_index(&index.object_id).is_err());
    let progress = source_store
        .load_cleanup_progress(&request.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(progress.claimed_object_ids, vec![index.object_id.clone()]);
    assert!(progress.completed_object_ids.is_empty());

    let mut effects = NoopPrivacyFixEffectBoundary;
    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();
    assert_eq!(report.deleted_tainted_objects, 1);
    assert!(
        source_store
            .load_cleanup_progress(&request.operation_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn given_execution_attempt_is_stale_before_journal_write_when_resuming_cleanup_then_cleanup_is_not_started()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_journal_write_fence");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id = OperationId::try_new("privacy_journal_write_fence_operation").unwrap();
    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    let mut effects =
        RejectPrivacyJournalEffectBoundary::for_label("privacy-cleanup-journal-create");

    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(
        error,
        PrivacyFixError::OperationStore(OperationStoreError::StaleOperationState { .. })
    ));
    assert!(source_store.read_index(&index.object_id).is_ok());
    assert!(
        source_store
            .load_cleanup_progress(&request.operation_id)
            .unwrap()
            .is_none()
    );
    assert!(clean_store.read_index(&index.object_id).is_err());
}

#[test]
fn given_execution_attempt_is_stale_before_journal_clear_when_resuming_cleanup_then_progress_is_retained()
 {
    let source_dir = tempfile::tempdir().unwrap();
    let clean_dir = tempfile::tempdir().unwrap();
    let source_store = FilesystemRawBackupStore::new(source_dir.path());
    let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
    let index = write_tainted_backup(&source_store, "privacy_journal_clear_fence");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id = OperationId::try_new("privacy_journal_clear_fence_operation").unwrap();

    let mut effects = NoopPrivacyFixEffectBoundary;
    crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        std::slice::from_ref(&index),
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
    request.confirmation_token = Some("delete-tainted".to_string());
    begin_cleanup_progress_with_authority(
        &source_store,
        &clean_store,
        &request,
        std::slice::from_ref(&index.object_id),
        std::slice::from_ref(&index),
        RawBackupCleanupMetadata {
            scanned_objects: 1,
            tainted_objects: 1,
            rewritten_objects: 1,
            affected_tables: vec!["users".to_string()],
            policy_version: request.policy.version.clone(),
            destructive_action: "delete_tainted".to_string(),
            privacy_dropped_fields: 1,
            clean_replay_verified: true,
        },
    )
    .unwrap();
    let mut effects =
        RejectPrivacyJournalEffectBoundary::for_label("privacy-cleanup-journal-clear");

    let error = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap_err();

    assert!(matches!(
        error,
        PrivacyFixError::OperationStore(OperationStoreError::StaleOperationState { .. })
    ));
    assert!(source_store.read_index(&index.object_id).is_err());
    let progress = source_store
        .load_cleanup_progress(&request.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(progress.completed_object_ids, vec![index.object_id]);
}

#[test]
fn given_privacy_fix_report_when_persisted_then_audit_counts_exclude_raw_values() {
    let tempdir = tempfile::tempdir().unwrap();
    let operation_id = OperationId::try_new("privacy_fix_audit").unwrap();
    let store = OperationStore::connect_duckdb(tempdir.path().join("operations.duckdb")).unwrap();
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: OperationKind::PrivacyFix,
            actor: OperationActor::try_new("test").unwrap(),
            target_tables: vec!["users".to_string()],
            dry_run: false,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::json!({}),
        })
        .unwrap();
    let store_fixture =
        OperationStoreTestFixture::for_database(tempdir.path().join("operations.duckdb"));
    store_fixture
        .prepare(
            &store,
            &operation_id,
            &OperationActor::try_new("test").unwrap(),
        )
        .unwrap();
    let source_store = FilesystemRawBackupStore::new(tempdir.path().join("source"));
    let clean_store = FilesystemRawBackupStore::new(tempdir.path().join("clean"));
    let index = write_tainted_backup(&source_store, "privacy_fix_audit_object");
    let mut request = privacy_fix_request(index.object_id.clone(), false);
    request.operation_id = operation_id.clone();
    let mut effects = NoopPrivacyFixEffectBoundary;
    let report = crate::privacy_fix::PrivacyRewriteInput::try_new(
        &request,
        &source_store,
        &clean_store,
        &[index],
    )
    .and_then(|input| {
        PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
            &input,
            &RateLimitPolicy::default(),
            &FixedRateLimitClock,
            &mut effects,
        )
    })
    .unwrap();

    store_fixture
        .save_report(
            &operation_id,
            crate::store::OperationReportKind::PrivacyFix,
            &report,
        )
        .unwrap();
    store_fixture
        .transition(
            &store,
            &operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Completed,
                status: OperationStatus::Succeeded,
                cursor: None,
                event_kind: OperationEventKind::Succeeded,
                counts: report.operation_counts(),
                message: Some("privacy fix applied"),
            },
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
    let suffix = object_id.as_str();
    PrivacyFixRequest {
        operation_id: OperationId::try_new("privacy_fix_1").unwrap(),
        source_backup_prefix: std::env::temp_dir()
            .join(format!("analytics-privacy-source-{suffix}"))
            .to_string_lossy()
            .into_owned(),
        clean_target_prefix: std::env::temp_dir()
            .join(format!("analytics-privacy-clean-{suffix}"))
            .to_string_lossy()
            .into_owned(),
        quarantine_target_prefix: std::env::temp_dir()
            .join(format!("analytics-privacy-quarantine-{suffix}"))
            .to_string_lossy()
            .into_owned(),
        raw_backup_object_ids: vec![object_id],
        tables: vec!["users".to_string()],
        policy: PrivacyPolicy::try_new("privacy-v1")
            .unwrap()
            .with_denied_key_name("email"),
        dry_run,
        destructive_action: PrivacyFixDestructiveAction::None,
        confirmation_token: None,
    }
}

fn begin_cleanup_progress_with_authority(
    source_store: &FilesystemRawBackupStore,
    clean_store: &FilesystemRawBackupStore,
    request: &PrivacyFixRequest,
    object_ids: &[RawBackupObjectId],
    expected_source_indexes: &[crate::RawBackupObjectIndex],
    metadata: RawBackupCleanupMetadata,
) -> crate::PrivacyFixResult<crate::RawBackupCleanupProgress> {
    let quarantine_store = FilesystemRawBackupStore::new(&request.quarantine_target_prefix);
    let source_root = source_store.canonical_root()?;
    let clean_root = clean_store.canonical_root()?;
    let quarantine_root = quarantine_store.canonical_root()?;
    Ok(
        source_store.begin_cleanup_progress_with_metadata_and_authority(
            &request.operation_id,
            object_ids,
            Some(metadata),
            crate::privacy_fix::cleanup_authority(
                request,
                crate::privacy_fix::PrivacyBackupRoots {
                    source: source_root.as_path(),
                    clean: clean_root.as_path(),
                    quarantine: quarantine_root.as_path(),
                },
                expected_source_indexes,
            )?,
        )?,
    )
}

fn write_tainted_backup(
    store: &FilesystemRawBackupStore,
    object_id: &str,
) -> crate::RawBackupObjectIndex {
    store
        .write_object_report(
            &RawBackupWriteRequest::try_new(
                OperationId::try_new("backup_1").unwrap(),
                RawBackupObjectId::try_new(object_id).unwrap(),
                "source_users",
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
                            new_image: Some(StorageItem::from_iter([(
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

fn write_multi_table_backup(
    store: &FilesystemRawBackupStore,
    object_id: &str,
) -> crate::RawBackupObjectIndex {
    store
        .write_object_report(
            &RawBackupWriteRequest::try_new(
                OperationId::try_new("backup_multi_table").unwrap(),
                RawBackupObjectId::try_new(object_id).unwrap(),
                "source_multi_table",
                None,
                ["users", "orders", "users"]
                    .into_iter()
                    .enumerate()
                    .map(|(index, table_name)| {
                        RawBackupRecord::try_new(
                            table_name,
                            format!("row-{index}"),
                            None,
                            StorageStreamRecord {
                                keys: StorageItem::new(),
                                sequence_number: index.to_string(),
                                old_image: None,
                                new_image: Some(StorageItem::from_iter([(
                                    "email".to_string(),
                                    StorageValue::S(format!("private-{index}@example.test")),
                                )])),
                            },
                        )
                        .unwrap()
                    })
                    .collect(),
            )
            .unwrap(),
        )
        .unwrap()
        .index
}

struct FixedRateLimitClock;

impl RateLimitClock for FixedRateLimitClock {
    fn minute_utc(&self) -> u16 {
        0
    }
}

struct EpochRateLimitClock {
    now_ms: u64,
}

impl RateLimitClock for EpochRateLimitClock {
    fn minute_utc(&self) -> u16 {
        ((self.now_ms / 1_000) % 86_400 / 60) as u16
    }

    fn now_ms(&self) -> u64 {
        self.now_ms
    }
}

#[derive(Debug)]
struct RejectPrivacyJournalEffectBoundary {
    reject_label: &'static str,
}

#[derive(Debug, Default)]
struct CountingPrivacyEffects {
    calls: usize,
}

impl PrivacyFixEffectBoundary for CountingPrivacyEffects {
    fn before_effect(&mut self, _key: &OperationChunkKey) -> crate::PrivacyFixResult<()> {
        self.calls += 1;
        Ok(())
    }

    fn after_effect(&mut self, _key: &OperationChunkKey) -> crate::PrivacyFixResult<()> {
        self.calls += 1;
        Ok(())
    }

    fn abandon_effect(&mut self, _key: &OperationChunkKey) -> crate::PrivacyFixResult<()> {
        self.calls += 1;
        Ok(())
    }
}

fn snapshot_paths(roots: &[&Path]) -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::new();
    for root in roots {
        snapshot_directory(root, &mut files);
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn snapshot_directory(root: &Path, files: &mut Vec<(String, Vec<u8>)>) {
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            // Retain directories too, so an unexpected empty directory is
            // visible.
            files.push((format!("{}/", path.display()), Vec::new()));
            snapshot_directory(&path, files);
        } else {
            files.push((path.to_string_lossy().into_owned(), fs::read(path).unwrap()));
        }
    }
}

impl RejectPrivacyJournalEffectBoundary {
    fn for_label(reject_label: &'static str) -> Self {
        Self { reject_label }
    }
}

impl PrivacyFixEffectBoundary for RejectPrivacyJournalEffectBoundary {
    fn before_effect(&mut self, key: &OperationChunkKey) -> crate::PrivacyFixResult<()> {
        if key.as_str().contains(self.reject_label) {
            return Err(PrivacyFixError::OperationStore(
                OperationStoreError::StaleOperationState {
                    operation_id: OperationId::try_new("privacy_journal_fence").unwrap(),
                    expected_revision: 1,
                },
            ));
        }
        Ok(())
    }

    fn after_effect(&mut self, _key: &OperationChunkKey) -> crate::PrivacyFixResult<()> {
        Ok(())
    }

    fn abandon_effect(&mut self, _key: &OperationChunkKey) -> crate::PrivacyFixResult<()> {
        Ok(())
    }
}

#[test]
fn given_nested_backup_roots_when_planning_or_rewriting_then_no_backup_state_changes() {
    for dry_run in [true, false] {
        for nested_pair in ["source_clean", "source_quarantine", "clean_quarantine"] {
            let root = tempfile::tempdir().unwrap();
            let source_path = root.path().join("source");
            let clean_path = if nested_pair == "source_clean" {
                source_path.join("clean")
            } else {
                root.path().join("clean")
            };
            let quarantine_path = match nested_pair {
                "source_quarantine" => source_path.join("quarantine"),
                "clean_quarantine" => clean_path.join("quarantine"),
                _ => root.path().join("quarantine"),
            };
            for path in [&source_path, &clean_path, &quarantine_path] {
                fs::create_dir_all(path).unwrap();
            }
            let source = FilesystemRawBackupStore::new(&source_path);
            let clean = FilesystemRawBackupStore::new(&clean_path);
            let index = write_tainted_backup(&source, "nested_privacy_roots");
            let mut request = privacy_fix_request(index.object_id.clone(), dry_run);
            request.source_backup_prefix = source_path.to_str().unwrap().to_owned();
            request.clean_target_prefix = clean_path.to_str().unwrap().to_owned();
            request.quarantine_target_prefix = quarantine_path.to_str().unwrap().to_owned();
            let before = snapshot_paths(&[root.path()]);
            let input = crate::privacy_fix::PrivacyRewriteInput::try_new(
                &request,
                &source,
                &clean,
                std::slice::from_ref(&index),
            )
            .unwrap();
            let mut effects = NoopPrivacyFixEffectBoundary;
            let result = PrivacyFixExecutor.execute_raw_backup_rewrite_with_effects_at(
                &input,
                &RateLimitPolicy::default(),
                &FixedRateLimitClock,
                &mut effects,
            );
            assert!(
                matches!(result, Err(PrivacyFixError::OverlappingBackupStores)),
                "dry_run={dry_run}, pair={nested_pair}"
            );
            assert_eq!(snapshot_paths(&[root.path()]), before);
        }
    }
}
