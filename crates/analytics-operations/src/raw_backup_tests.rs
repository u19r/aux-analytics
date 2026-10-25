use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};

use crate::{
    FilesystemRawBackupStore, OperationId, RAW_BACKUP_SCHEMA_VERSION, RawBackupCompression,
    RawBackupEncryption, RawBackupError, RawBackupObjectId, RawBackupObjectLayout,
    RawBackupObjectStore, RawBackupRecord, RawBackupWriteRequest,
};

#[test]
fn given_path_traversal_object_id_when_created_then_it_is_rejected() {
    for object_id in ["../outside", "nested/object", r"C:\\outside", ".", ".."] {
        assert!(matches!(
            RawBackupObjectId::try_new(object_id),
            Err(RawBackupError::UnsafeObjectId)
        ));
    }
}

#[test]
fn given_relative_filesystem_root_when_writing_then_the_store_boundary_rejects_it() {
    let store = FilesystemRawBackupStore::new("relative-raw-backup-root");
    let error = store
        .write_fixture_object(&write_request(
            "relative_root_operation",
            "relative_root_object",
            "user-1",
        ))
        .unwrap_err();

    assert!(matches!(error, RawBackupError::RelativeStoreRoot));
}

#[test]
fn given_records_when_filesystem_backup_writes_then_index_tracks_checksum_and_replay_records() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_1").unwrap(),
        RawBackupObjectId::try_new("users_0001").unwrap(),
        "source_users",
        Some("policy-v1".to_string()),
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap();

    let index = store.write_fixture_object(&request).unwrap();
    let persisted_index = store
        .read_index(&RawBackupObjectId::try_new("users_0001").unwrap())
        .unwrap();
    let replayed = store.replay_object(&index).unwrap();

    assert_eq!(index.record_count, 1);
    assert_eq!(persisted_index, index);
    assert_eq!(index.privacy_policy_version.as_deref(), Some("policy-v1"));
    assert!(index.checksum.starts_with("fnv1a64:"));
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].table_name, "users");
    assert_eq!(replayed[0].record_key, b"user-1");
    assert_eq!(replayed[0].record.sequence_number, "user-1");
}

#[test]
fn given_write_report_requested_when_backup_writes_then_metrics_are_emitted() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let object_id = RawBackupObjectId::for_operation(
        &OperationId::try_new("backup/metrics").unwrap(),
        "source users",
        7,
    )
    .unwrap();
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_metrics").unwrap(),
        object_id.clone(),
        "source_users",
        Some("policy-v1".to_string()),
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap()
    .with_policy_dropped_records(2);

    let report = store.write_fixture_object_report(&request).unwrap();
    let replay = store.replay_object_report(&report.index).unwrap();

    assert_eq!(
        object_id.as_str(),
        "backup_metrics_source_users_7",
        "object id parts are sanitized deterministically"
    );
    assert_eq!(report.metrics.records_written, 1);
    assert!(report.metrics.bytes_written > 0);
    assert_eq!(report.metrics.compression_ratio_per_mille, 1000);
    assert_eq!(report.metrics.policy_drops, 2);
    assert_eq!(replay.metrics.replayed_records, 1);
}

#[test]
fn given_unsafe_operation_and_source_parts_when_object_id_is_built_then_parts_are_sanitized() {
    let object_id = RawBackupObjectId::for_operation(
        &OperationId::try_new("backup/with spaces").unwrap(),
        r"source/users\archive",
        u64::MAX,
    )
    .unwrap();

    assert_eq!(
        object_id.as_str(),
        "backup_with_spaces_source_users_archive_18446744073709551615"
    );
}

#[test]
fn given_run_length_compression_when_backup_replays_then_records_are_restored() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_compressed").unwrap(),
        RawBackupObjectId::try_new("users_compressed").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "aaaaaaaaaaaaaaaa")],
    )
    .unwrap()
    .with_layout(RawBackupObjectLayout {
        compression: RawBackupCompression::RunLength,
        ..RawBackupObjectLayout::default()
    });

    let report = store.write_fixture_object_report(&request).unwrap();
    let replay = store.replay_object_report(&report.index).unwrap();

    assert_eq!(
        report.index.layout.compression,
        RawBackupCompression::RunLength
    );
    assert_eq!(replay.records.len(), 1);
}

#[test]
fn given_empty_external_encryption_key_when_request_built_then_it_is_rejected() {
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_encrypted").unwrap(),
        RawBackupObjectId::try_new("users_encrypted").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap()
    .with_layout(RawBackupObjectLayout {
        encryption: RawBackupEncryption::ExternalKey {
            key_id: " ".to_string(),
        },
        ..RawBackupObjectLayout::default()
    });

    let error = FilesystemRawBackupStore::new(tempfile::tempdir().unwrap().path())
        .write_fixture_object(&request)
        .unwrap_err();

    assert!(matches!(error, RawBackupError::EmptyEncryptionKeyId));
}

#[test]
fn given_small_backup_objects_when_compacted_then_source_object_ids_are_preserved() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let first = store
        .write_fixture_object(&write_request("backup_compact", "users_1", "user-1"))
        .unwrap();
    let second = store
        .write_fixture_object(&write_request("backup_compact", "users_2", "user-2"))
        .unwrap();

    let compacted = store
        .compact_objects(
            OperationId::try_new("backup_compact").unwrap(),
            RawBackupObjectId::try_new("users_compacted").unwrap(),
            "source_users",
            None,
            &[first.clone(), second.clone()],
        )
        .unwrap();
    let replay = store.replay_object(&compacted.index).unwrap();

    assert_eq!(
        compacted.index.compacted_from_object_ids,
        vec![first.object_id, second.object_id]
    );
    assert_eq!(replay.len(), 2);
}

#[test]
fn given_filesystem_store_when_used_as_object_store_trait_then_backup_round_trips() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let object_store: &dyn RawBackupObjectStore = &store;

    let report = store
        .write_fixture_object_report(&write_request("backup_trait", "users_trait", "user-1"))
        .unwrap();
    let replay = object_store.replay_object_report(&report.index).unwrap();

    assert_eq!(replay.records.len(), 1);
}

#[test]
fn given_existing_object_when_backup_writes_again_then_publication_is_idempotent() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_4").unwrap(),
        RawBackupObjectId::try_new("users_0004").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap();

    let first = store.write_fixture_object_report(&request).unwrap();
    let second = store.write_fixture_object_report(&request).unwrap();

    assert_eq!(second, first);
}

#[test]
fn given_index_failure_after_object_write_when_retried_then_publication_reconciles() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = write_request("backup_index_failure", "users_index_failure", "user-1");
    let index_path = tempdir.path().join("users_index_failure.index.json");

    std::fs::create_dir(index_path.as_path()).unwrap();
    assert!(store.write_fixture_object_report(&request).is_err());
    assert!(tempdir.path().join("users_index_failure.jsonl").is_file());
    assert!(
        tempdir
            .path()
            .join(".users_index_failure.publish.json")
            .is_file()
    );

    std::fs::remove_dir(index_path.as_path()).unwrap();
    let report = store.write_fixture_object_report(&request).unwrap();

    assert_eq!(store.read_index(&request.object_id).unwrap(), report.index);
    assert!(
        !tempdir
            .path()
            .join(".users_index_failure.publish.json")
            .exists()
    );
}

#[test]
fn given_cleanup_failure_midway_when_progress_is_reopened_then_completed_objects_are_skipped() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let first = store
        .write_fixture_object_report(&write_request("cleanup_resume", "cleanup-first", "user-1"))
        .unwrap()
        .index;
    let second = store
        .write_fixture_object_report(&write_request("cleanup_resume", "cleanup-second", "user-2"))
        .unwrap()
        .index;
    let operation_id = OperationId::try_new("cleanup_resume").unwrap();
    let mut progress = store
        .begin_cleanup_progress(
            &operation_id,
            &[first.object_id.clone(), second.object_id.clone()],
        )
        .unwrap();

    store.delete_object_if_present(&first).unwrap();
    progress.completed_object_ids.push(first.object_id.clone());
    store.save_cleanup_progress(&progress).unwrap();

    std::fs::remove_file(tempdir.path().join("cleanup-second.jsonl")).unwrap();
    std::fs::remove_file(tempdir.path().join("cleanup-second.index.json")).unwrap();
    std::fs::create_dir(tempdir.path().join("cleanup-second.index.json")).unwrap();
    assert!(store.delete_object_if_present(&second).is_err());

    let reopened = store.load_cleanup_progress(&operation_id).unwrap().unwrap();
    assert_eq!(reopened.completed_object_ids, vec![first.object_id.clone()]);
    assert!(reopened.completed_object_ids.contains(&first.object_id));
    assert!(!reopened.completed_object_ids.contains(&second.object_id));

    std::fs::remove_dir(tempdir.path().join("cleanup-second.index.json")).unwrap();
    store.delete_object_if_present(&second).unwrap();
    progress.completed_object_ids.push(second.object_id.clone());
    store.save_cleanup_progress(&progress).unwrap();
    store.clear_cleanup_progress(&operation_id).unwrap();
    assert!(
        store
            .load_cleanup_progress(&operation_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn given_cleanup_progress_is_modified_on_disk_when_loading_cleanup_progress_then_integrity_check_rejects_it()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let operation_id = OperationId::try_new("cleanup_integrity").unwrap();
    let object_id = RawBackupObjectId::try_new("cleanup-integrity-object").unwrap();
    store
        .begin_cleanup_progress(&operation_id, std::slice::from_ref(&object_id))
        .unwrap();
    let progress_path = std::fs::read_dir(tempdir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".privacy-cleanup-"))
        })
        .expect("cleanup progress path");
    let mut document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(progress_path.as_path()).unwrap()).unwrap();
    document["checksum"] = serde_json::Value::String("fnv1a64:0000000000000000".to_string());
    std::fs::write(
        progress_path.as_path(),
        serde_json::to_vec_pretty(&document).unwrap(),
    )
    .unwrap();

    let error = store.load_cleanup_progress(&operation_id).unwrap_err();

    assert!(matches!(error, RawBackupError::CleanupProgressIntegrity));
}

#[test]
fn given_corrupt_existing_quarantine_when_retried_then_source_is_not_deleted() {
    let source_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source = FilesystemRawBackupStore::new(source_dir.path());
    let quarantine = FilesystemRawBackupStore::new(quarantine_dir.path());
    let index = source
        .write_fixture_object(&write_request(
            "quarantine_integrity",
            "tainted-source",
            "user-1",
        ))
        .unwrap();

    let quarantine_index = source.quarantine_object(&index, &quarantine).unwrap();
    std::fs::write(quarantine_index.object_path.as_str(), b"corrupt\n").unwrap();

    let error = source.quarantine_object(&index, &quarantine).unwrap_err();

    assert!(matches!(error, RawBackupError::ChecksumMismatch { .. }));
    assert!(std::path::Path::new(index.object_path.as_str()).is_file());
    assert!(std::path::Path::new(index.index_path.as_str()).is_file());
}

#[test]
fn given_source_object_changes_before_quarantine_when_quarantining_backup_then_copy_is_rejected() {
    let source_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source = FilesystemRawBackupStore::new(source_dir.path());
    let quarantine = FilesystemRawBackupStore::new(quarantine_dir.path());
    let index = source
        .write_fixture_object(&write_request(
            "quarantine_source_integrity",
            "tainted-source",
            "user-1",
        ))
        .unwrap();
    std::fs::write(index.object_path.as_str(), b"corrupt\n").unwrap();

    let error = source.quarantine_object(&index, &quarantine).unwrap_err();

    assert!(matches!(error, RawBackupError::ChecksumMismatch { .. }));
    assert!(std::path::Path::new(index.object_path.as_str()).is_file());
    assert!(
        !quarantine_dir
            .path()
            .join("quarantine_tainted-source.index.json")
            .is_file()
    );
}

#[test]
fn given_quarantine_index_failure_after_object_write_when_retried_then_publication_reconciles() {
    let source_dir = tempfile::tempdir().unwrap();
    let quarantine_dir = tempfile::tempdir().unwrap();
    let source = FilesystemRawBackupStore::new(source_dir.path());
    let quarantine = FilesystemRawBackupStore::new(quarantine_dir.path());
    let index = source
        .write_fixture_object(&write_request(
            "quarantine_index_failure",
            "tainted-source",
            "user-1",
        ))
        .unwrap();
    let quarantine_object_id = RawBackupObjectId::try_new("quarantine_tainted-source").unwrap();
    std::fs::create_dir(
        quarantine_dir
            .path()
            .join(format!("{}.index.json", quarantine_object_id.as_str())),
    )
    .unwrap();

    assert!(source.quarantine_object(&index, &quarantine).is_err());
    assert!(
        quarantine_dir
            .path()
            .join(format!("{}.jsonl", quarantine_object_id.as_str()))
            .is_file()
    );
    assert!(
        quarantine_dir
            .path()
            .join(format!(".{}.publish.json", quarantine_object_id.as_str()))
            .is_file()
    );

    std::fs::remove_dir(
        quarantine_dir
            .path()
            .join(format!("{}.index.json", quarantine_object_id.as_str())),
    )
    .unwrap();
    let quarantined = source.quarantine_object(&index, &quarantine).unwrap();

    assert!(std::path::Path::new(quarantined.index_path.as_str()).is_file());
    assert!(
        !quarantine_dir
            .path()
            .join(format!(".{}.publish.json", quarantine_object_id.as_str()))
            .exists()
    );
    assert!(std::path::Path::new(index.object_path.as_str()).is_file());
    assert!(std::path::Path::new(index.index_path.as_str()).is_file());
}

#[test]
fn given_corrupt_backup_object_when_replayed_then_checksum_failure_is_reported() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_2").unwrap(),
        RawBackupObjectId::try_new("users_0002").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap();
    let index = store.write_fixture_object(&request).unwrap();
    std::fs::write(index.object_path.as_str(), b"corrupt\n").unwrap();

    let error = store.replay_object(&index).unwrap_err();

    assert!(matches!(
        error,
        RawBackupError::ChecksumMismatch { object_id, .. } if object_id.as_str() == "users_0002"
    ));
}

#[test]
fn given_tampered_filesystem_index_paths_when_replayed_then_it_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let index = store
        .write_fixture_object(&write_request("backup_path", "users_path", "user-1"))
        .unwrap();
    let mut tampered = index.clone();
    tampered.object_path = tempdir
        .path()
        .join("outside.jsonl")
        .to_string_lossy()
        .into_owned();

    let error = store.replay_object(&tampered).unwrap_err();

    assert!(matches!(error, RawBackupError::ObjectStore(message) if message.contains("paths")));
}

#[test]
fn given_corrupt_backup_object_when_verified_then_checksum_failure_metrics_are_reported() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_verify").unwrap(),
        RawBackupObjectId::try_new("users_verify").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap();
    let index = store.write_fixture_object(&request).unwrap();
    std::fs::write(index.object_path.as_str(), b"corrupt\n").unwrap();

    let report = store.verify_object_report(&index).unwrap();

    assert!(!report.checksum_valid);
    assert_eq!(report.object_id.as_str(), "users_verify");
    assert_eq!(report.expected_checksum, index.checksum);
    assert!(report.actual_checksum.starts_with("fnv1a64:"));
    assert_eq!(report.byte_count, b"corrupt\n".len() as u64);
    assert_eq!(report.metrics.checksum_failures, 1);
}

#[test]
fn given_empty_records_when_write_request_built_then_it_is_rejected() {
    let error = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_3").unwrap(),
        RawBackupObjectId::try_new("users_0003").unwrap(),
        "source_users",
        None,
        Vec::new(),
    )
    .unwrap_err();

    assert!(matches!(error, RawBackupError::EmptyObject));
}

#[test]
fn given_unsupported_schema_version_when_write_request_is_validated_then_it_is_rejected() {
    let request = write_request("backup_schema", "users_schema", "user-1").with_layout(
        RawBackupObjectLayout {
            schema_version: RAW_BACKUP_SCHEMA_VERSION + 1,
            ..RawBackupObjectLayout::default()
        },
    );

    match request.validate() {
        Err(RawBackupError::UnsupportedSchemaVersion { expected, actual }) => {
            assert_eq!(expected, RAW_BACKUP_SCHEMA_VERSION);
            assert_eq!(actual, RAW_BACKUP_SCHEMA_VERSION + 1);
        }
        other => panic!("expected unsupported schema version, got {other:?}"),
    }
}

#[test]
fn given_unsupported_schema_version_when_index_is_read_then_it_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let object_id = RawBackupObjectId::try_new("users_schema_read").unwrap();
    let index = store
        .write_fixture_object(&write_request(
            "backup_schema_read",
            object_id.as_str(),
            "user-1",
        ))
        .unwrap();
    let mut persisted = serde_json::to_value(&index).unwrap();
    persisted["layout"]["schema_version"] = serde_json::json!(RAW_BACKUP_SCHEMA_VERSION + 1);
    std::fs::write(
        index.index_path.as_str(),
        serde_json::to_vec(&persisted).unwrap(),
    )
    .unwrap();

    match store.read_index(&object_id) {
        Err(RawBackupError::UnsupportedSchemaVersion { expected, actual }) => {
            assert_eq!(expected, RAW_BACKUP_SCHEMA_VERSION);
            assert_eq!(actual, RAW_BACKUP_SCHEMA_VERSION + 1);
        }
        other => panic!("expected unsupported schema version, got {other:?}"),
    }
}

#[test]
fn given_index_with_a_different_embedded_object_id_when_read_then_it_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let requested = store
        .write_fixture_object(&write_request("backup_id_mismatch_a", "object_a", "user-a"))
        .unwrap();
    let other = store
        .write_fixture_object(&write_request("backup_id_mismatch_b", "object_b", "user-b"))
        .unwrap();
    let mut tampered = serde_json::to_value(&requested).unwrap();
    tampered["object_id"] = serde_json::json!(other.object_id.as_str());
    std::fs::write(
        requested.index_path.as_str(),
        serde_json::to_vec(&tampered).unwrap(),
    )
    .unwrap();

    let error = store.read_index(&requested.object_id).unwrap_err();

    assert!(matches!(
        error,
        RawBackupError::ObjectIdMismatch { requested: actual_requested, actual }
            if actual_requested == requested.object_id && actual == other.object_id
    ));
    assert_eq!(store.read_index(&other.object_id).unwrap(), other);
}

#[test]
fn given_privacy_policy_when_backup_request_filtered_then_denied_fields_are_removed() {
    let policy = PrivacyPolicy::try_new("privacy-v2")
        .unwrap()
        .with_denied_key_name("email");
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_privacy").unwrap(),
        RawBackupObjectId::try_new("users_privacy").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap()
    .with_privacy_policy(&policy)
    .unwrap();

    assert_eq!(
        request.privacy_policy_version.as_deref(),
        Some("privacy-v2")
    );
    assert_eq!(request.policy_dropped_records, 1);
    let new_image = request.records[0].record.new_image.as_ref().unwrap();
    assert!(!new_image.contains_key("email"));
}

#[test]
fn given_privacy_policy_denies_record_identity_when_backup_request_filtered_then_it_fails_closed() {
    let policy = PrivacyPolicy::try_new("privacy-identity")
        .unwrap()
        .with_denied_key_name("user-1");
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_privacy_identity").unwrap(),
        RawBackupObjectId::try_new("users_privacy_identity").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap();

    let error = request.with_privacy_policy(&policy).unwrap_err();

    assert!(matches!(error, RawBackupError::PrivacyIdentityDenied));
}

#[test]
fn given_value_regex_when_backup_request_filtered_then_old_and_new_images_are_filtered() {
    let policy = PrivacyPolicy::try_new("privacy-v3")
        .unwrap()
        .with_denied_value_regex("private");
    let mut record = backup_record("users", "user-1", "private@example.test");
    record.record.old_image = record.record.new_image.clone();
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("backup_privacy_regex").unwrap(),
        RawBackupObjectId::try_new("users_privacy_regex").unwrap(),
        "source_users",
        None,
        vec![record],
    )
    .unwrap()
    .with_privacy_policy(&policy)
    .unwrap();

    assert_eq!(request.policy_dropped_records, 2);
    assert!(
        !request.records[0]
            .record
            .old_image
            .as_ref()
            .unwrap()
            .contains_key("email")
    );
    assert!(
        !request.records[0]
            .record
            .new_image
            .as_ref()
            .unwrap()
            .contains_key("email")
    );
}

#[test]
fn given_privacy_policy_when_backup_replayed_then_denied_fields_are_removed_from_replay() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let write = store
        .write_fixture_object_report(
            &RawBackupWriteRequest::try_new(
                OperationId::try_new("backup_replay_privacy").unwrap(),
                RawBackupObjectId::try_new("users_replay_privacy").unwrap(),
                "source_users",
                None,
                vec![backup_record("users", "user-1", "private@example.test")],
            )
            .unwrap(),
        )
        .unwrap();
    let policy = PrivacyPolicy::try_new("privacy-v2")
        .unwrap()
        .with_denied_key_name("email");

    let replay = store
        .replay_object_report_with_privacy_policy(&write.index, &policy)
        .unwrap();

    assert_eq!(replay.metrics.replayed_records, 1);
    assert_eq!(replay.metrics.policy_drops, 1);
    let new_image = replay.records[0].record.new_image.as_ref().unwrap();
    assert!(!new_image.contains_key("email"));
}

#[test]
fn given_privacy_policy_denies_replayed_record_identity_when_replaying_backup_then_replay_fails_closed()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let write = store
        .write_fixture_object_report(
            &RawBackupWriteRequest::try_new(
                OperationId::try_new("backup_replay_identity").unwrap(),
                RawBackupObjectId::try_new("users_replay_identity").unwrap(),
                "source_users",
                None,
                vec![backup_record("users", "user-1", "private@example.test")],
            )
            .unwrap(),
        )
        .unwrap();
    let policy = PrivacyPolicy::try_new("privacy-identity")
        .unwrap()
        .with_denied_key_name("user-1");

    let error = store
        .replay_object_report_with_privacy_policy(&write.index, &policy)
        .unwrap_err();

    assert!(matches!(error, RawBackupError::PrivacyIdentityDenied));
}

fn backup_record(table_name: &str, record_key: &str, email: &str) -> RawBackupRecord {
    RawBackupRecord::try_new(
        table_name,
        record_key,
        None,
        StorageStreamRecord {
            keys: storage_item([("pk", StorageValue::S(record_key.to_string()))]),
            sequence_number: record_key.to_string(),
            old_image: None,
            new_image: Some(storage_item([
                ("pk", StorageValue::S(record_key.to_string())),
                ("email", StorageValue::S(email.to_string())),
            ])),
        },
    )
    .unwrap()
}

fn write_request(operation_id: &str, object_id: &str, record_key: &str) -> RawBackupWriteRequest {
    RawBackupWriteRequest::try_new(
        OperationId::try_new(operation_id).unwrap(),
        RawBackupObjectId::try_new(object_id).unwrap(),
        "source_users",
        None,
        vec![backup_record("users", record_key, "ada@example.test")],
    )
    .unwrap()
}

fn storage_item<const N: usize>(values: [(&str, StorageValue); N]) -> StorageItem {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}
