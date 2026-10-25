//! Filesystem replay fixtures and calling-thread object-read accounting.

use std::cell::Cell;

use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};

use super::{
    FilesystemRawBackupStore, RawBackupCompression, RawBackupObjectId, RawBackupRecord,
    RawBackupReplayExecution, RawBackupReplayRecord, RawBackupReplayRequest,
    RawBackupReplaySummary, RawBackupWriteRequest,
};
use crate::OperationId;

thread_local! { static OBJECT_READS: Cell<usize> = const { Cell::new(0) }; }

pub(super) fn record_object_read() {
    OBJECT_READS.with(|reads| reads.set(reads.get() + 1));
}

pub(super) fn count_object_reads<T>(action: impl FnOnce() -> T) -> (T, usize) {
    let before = OBJECT_READS.with(Cell::get);
    let output = action();
    (output, OBJECT_READS.with(Cell::get) - before)
}

pub(super) struct ReplayFixture {
    pub(super) root: tempfile::TempDir,
    pub(super) store: FilesystemRawBackupStore,
    pub(super) request: RawBackupReplayRequest,
    pub(super) expected: Vec<RawBackupReplayRecord>,
}

impl ReplayFixture {
    pub(super) fn new(rows: usize, compression: RawBackupCompression, with_privacy: bool) -> Self {
        let records = (0..rows)
            .map(|index| {
                let key = format!("key-{index}");
                let record = StorageStreamRecord {
                    keys: StorageItem::from_iter([("id".into(), StorageValue::S(key.clone()))]),
                    sequence_number: index.to_string(),
                    old_image: None,
                    new_image: Some(StorageItem::from_iter([
                        ("id".into(), StorageValue::S(key.clone())),
                        ("payload".into(), StorageValue::S("quote'λ\n".repeat(512))),
                        ("secret".into(), StorageValue::S("denied".into())),
                    ])),
                };
                RawBackupRecord::try_new("entries", key, None, record).unwrap()
            })
            .collect::<Vec<_>>();
        let expected = records
            .iter()
            .map(|row| {
                let mut record = row.record.clone();
                if with_privacy {
                    record.new_image.as_mut().unwrap().remove("secret");
                }
                RawBackupReplayRecord {
                    table_name: row.table_name.clone(),
                    record_key: row.record_key.as_bytes().to_vec(),
                    source_position: None,
                    record,
                }
            })
            .collect();
        let write = RawBackupWriteRequest::try_new(
            OperationId::try_new("replay_probe").unwrap(),
            RawBackupObjectId::try_new("object").unwrap(),
            "source_entries",
            None,
            records,
        )
        .unwrap()
        .with_layout(super::RawBackupObjectLayout {
            compression,
            ..Default::default()
        });
        let root = tempfile::tempdir().unwrap();
        let store = FilesystemRawBackupStore::new(root.path());
        let index = store.write_fixture_object(&write).unwrap();
        let policy = with_privacy.then(|| {
            PrivacyPolicy::try_new("policy")
                .unwrap()
                .with_denied_key_name("secret")
        });
        let request = RawBackupReplayRequest::try_new(index.object_id, policy).unwrap();
        Self {
            root,
            store,
            request,
            expected,
        }
    }

    pub(super) fn replay_with_separate_verification(&self) -> RawBackupReplayExecution {
        self.request.validate().unwrap();
        let index = self.store.read_index(&self.request.object_id).unwrap();
        let verification = self.store.verify_object_report(&index).unwrap();
        assert!(verification.checksum_valid);
        let report = match self.request.privacy_policy.as_ref() {
            Some(policy) => self
                .store
                .replay_object_report_with_privacy_policy(&index, policy)
                .unwrap(),
            None => self.store.replay_object_report(&index).unwrap(),
        };
        RawBackupReplayExecution {
            records: report.records,
            summary: RawBackupReplaySummary {
                dry_run: true,
                object_id: self.request.object_id.clone(),
                record_count: index.record_count,
                verification,
                metrics: report.metrics,
            },
        }
    }
}
