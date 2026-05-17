use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
};

use analytics_contract::{PrivacyPolicy, StorageStreamRecord, StorageValue};

use crate::{
    FilesystemRawBackupStore, OperationId, RawBackupCompression, RawBackupEncryption,
    RawBackupError, RawBackupObjectId, RawBackupObjectLayout, RawBackupObjectStore,
    RawBackupRecord, RawBackupWriteRequest, S3CompatibleObjectClient, S3CompatibleRawBackupStore,
};

#[test]
fn given_records_when_filesystem_backup_writes_then_index_tracks_checksum_and_replay_records() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::new(
        OperationId::new("backup_1").unwrap(),
        RawBackupObjectId::new("users_0001").unwrap(),
        "source_users",
        Some("policy-v1".to_string()),
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap();

    let index = store.write_object(&request).unwrap();
    let persisted_index = store
        .read_index(&RawBackupObjectId::new("users_0001").unwrap())
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
        &OperationId::new("backup/metrics").unwrap(),
        "source users",
        7,
    )
    .unwrap();
    let request = RawBackupWriteRequest::new(
        OperationId::new("backup_metrics").unwrap(),
        object_id.clone(),
        "source_users",
        Some("policy-v1".to_string()),
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap()
    .with_policy_dropped_records(2);

    let report = store.write_object_report(&request).unwrap();
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
fn given_run_length_compression_when_backup_replays_then_records_are_restored() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::new(
        OperationId::new("backup_compressed").unwrap(),
        RawBackupObjectId::new("users_compressed").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "aaaaaaaaaaaaaaaa")],
    )
    .unwrap()
    .with_layout(RawBackupObjectLayout {
        compression: RawBackupCompression::RunLength,
        ..RawBackupObjectLayout::default()
    });

    let report = store.write_object_report(&request).unwrap();
    let replay = store.replay_object_report(&report.index).unwrap();

    assert_eq!(
        report.index.layout.compression,
        RawBackupCompression::RunLength
    );
    assert_eq!(replay.records.len(), 1);
}

#[test]
fn given_empty_external_encryption_key_when_request_built_then_it_is_rejected() {
    let request = RawBackupWriteRequest::new(
        OperationId::new("backup_encrypted").unwrap(),
        RawBackupObjectId::new("users_encrypted").unwrap(),
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
        .write_object(&request)
        .unwrap_err();

    assert!(matches!(error, RawBackupError::EmptyEncryptionKeyId));
}

#[test]
fn given_small_backup_objects_when_compacted_then_source_object_ids_are_preserved() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let first = store
        .write_object(&write_request("backup_compact", "users_1", "user-1"))
        .unwrap();
    let second = store
        .write_object(&write_request("backup_compact", "users_2", "user-2"))
        .unwrap();

    let compacted = store
        .compact_objects(
            OperationId::new("backup_compact").unwrap(),
            RawBackupObjectId::new("users_compacted").unwrap(),
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

    let report = object_store
        .write_object_report(&write_request("backup_trait", "users_trait", "user-1"))
        .unwrap();
    let replay = object_store.replay_object_report(&report.index).unwrap();

    assert_eq!(replay.records.len(), 1);
}

#[test]
fn given_s3_compatible_store_when_backup_writes_then_object_and_index_round_trip() {
    let client = InMemoryS3Client::default();
    let store = S3CompatibleRawBackupStore::new("analytics-backups", "raw/prefix", client).unwrap();
    let request = write_request("backup_s3", "users_s3", "user-1");

    let report = store.write_object_report(&request).unwrap();
    let index = store
        .read_index(&RawBackupObjectId::new("users_s3").unwrap())
        .unwrap();
    let replay = store.replay_object_report(&report.index).unwrap();

    assert_eq!(index, report.index);
    assert_eq!(
        report.index.object_path,
        "s3://analytics-backups/raw/prefix/users_s3.jsonl"
    );
    assert_eq!(
        report.index.index_path,
        "s3://analytics-backups/raw/prefix/users_s3.index.json"
    );
    assert_eq!(replay.records.len(), 1);
}

#[test]
fn given_s3_compatible_store_when_object_exists_then_collision_is_rejected() {
    let client = InMemoryS3Client::default();
    let store = S3CompatibleRawBackupStore::new("analytics-backups", "raw", client).unwrap();
    let request = write_request("backup_s3_collision", "users_s3_collision", "user-1");

    store.write_object_report(&request).unwrap();
    let error = store.write_object_report(&request).unwrap_err();

    assert!(
        matches!(error, RawBackupError::ObjectAlreadyExists(key) if key == "raw/users_s3_collision.jsonl")
    );
}

#[test]
fn given_empty_s3_bucket_when_store_created_then_it_is_rejected() {
    let error =
        S3CompatibleRawBackupStore::new(" ", "raw", InMemoryS3Client::default()).unwrap_err();

    assert!(matches!(error, RawBackupError::ObjectStore(message) if message.contains("bucket")));
}

#[test]
fn given_existing_object_when_backup_writes_again_then_collision_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::new(
        OperationId::new("backup_4").unwrap(),
        RawBackupObjectId::new("users_0004").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap();

    store.write_object(&request).unwrap();
    let error = store.write_object(&request).unwrap_err();

    assert!(matches!(
        error,
        RawBackupError::Io(error) if error.kind() == std::io::ErrorKind::AlreadyExists
    ));
}

#[derive(Debug, Default, Clone)]
struct InMemoryS3Client {
    objects: std::rc::Rc<RefCell<BTreeMap<String, Vec<u8>>>>,
}

impl S3CompatibleObjectClient for InMemoryS3Client {
    fn put_object_if_absent(&self, key: &str, bytes: &[u8]) -> crate::RawBackupResult<()> {
        let mut objects = self.objects.borrow_mut();
        if objects.contains_key(key) {
            return Err(RawBackupError::ObjectAlreadyExists(key.to_string()));
        }
        objects.insert(key.to_string(), bytes.to_vec());
        Ok(())
    }

    fn get_object(&self, key: &str) -> crate::RawBackupResult<Vec<u8>> {
        self.objects
            .borrow()
            .get(key)
            .cloned()
            .ok_or_else(|| RawBackupError::ObjectStore(format!("missing object {key}")))
    }
}

#[test]
fn given_corrupt_backup_object_when_replayed_then_checksum_failure_is_reported() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::new(
        OperationId::new("backup_2").unwrap(),
        RawBackupObjectId::new("users_0002").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap();
    let index = store.write_object(&request).unwrap();
    std::fs::write(index.object_path.as_str(), b"corrupt\n").unwrap();

    let error = store.replay_object(&index).unwrap_err();

    assert!(matches!(
        error,
        RawBackupError::ChecksumMismatch { object_id, .. } if object_id.as_str() == "users_0002"
    ));
}

#[test]
fn given_corrupt_backup_object_when_verified_then_checksum_failure_metrics_are_reported() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let request = RawBackupWriteRequest::new(
        OperationId::new("backup_verify").unwrap(),
        RawBackupObjectId::new("users_verify").unwrap(),
        "source_users",
        None,
        vec![backup_record("users", "user-1", "ada@example.test")],
    )
    .unwrap();
    let index = store.write_object(&request).unwrap();
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
    let error = RawBackupWriteRequest::new(
        OperationId::new("backup_3").unwrap(),
        RawBackupObjectId::new("users_0003").unwrap(),
        "source_users",
        None,
        Vec::new(),
    )
    .unwrap_err();

    assert!(matches!(error, RawBackupError::EmptyObject));
}

#[test]
fn given_privacy_policy_when_backup_request_filtered_then_denied_fields_are_removed() {
    let policy = PrivacyPolicy::new("privacy-v2")
        .unwrap()
        .with_denied_key_name("email");
    let request = RawBackupWriteRequest::new(
        OperationId::new("backup_privacy").unwrap(),
        RawBackupObjectId::new("users_privacy").unwrap(),
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
fn given_privacy_policy_when_backup_replayed_then_denied_fields_are_removed_from_replay() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let write = store
        .write_object_report(
            &RawBackupWriteRequest::new(
                OperationId::new("backup_replay_privacy").unwrap(),
                RawBackupObjectId::new("users_replay_privacy").unwrap(),
                "source_users",
                None,
                vec![backup_record("users", "user-1", "private@example.test")],
            )
            .unwrap(),
        )
        .unwrap();
    let policy = PrivacyPolicy::new("privacy-v2")
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

fn backup_record(table_name: &str, record_key: &str, email: &str) -> RawBackupRecord {
    RawBackupRecord::new(
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
    RawBackupWriteRequest::new(
        OperationId::new(operation_id).unwrap(),
        RawBackupObjectId::new(object_id).unwrap(),
        "source_users",
        None,
        vec![backup_record("users", record_key, "ada@example.test")],
    )
    .unwrap()
}

fn storage_item<const N: usize>(
    values: [(&str, StorageValue); N],
) -> HashMap<String, StorageValue> {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}
