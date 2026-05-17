use std::collections::HashMap;

use analytics_contract::{StorageStreamRecord, StorageValue};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    FilesystemRawBackupStore, OperationId, RawBackupError, RawBackupObjectId, RawBackupRecord,
    RawBackupWriteRequest,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum BackupCase {
    WriteReplay,
    EmptyRejected,
    CorruptRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum BackupResult {
    NotRun,
    ReplayComplete,
    RejectedEmpty,
    RejectedChecksum,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct BackupReport {
    result: BackupResult,
    record_count: i64,
    checksum_rejected: bool,
}

impl Default for BackupReport {
    fn default() -> Self {
        Self {
            result: BackupResult::NotRun,
            record_count: 0,
            checksum_rejected: false,
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RawBackupMbtState {
    last_report: BackupReport,
}

impl State<RawBackupDriver> for RawBackupMbtState {
    fn from_driver(driver: &RawBackupDriver) -> Result<Self> {
        Ok(Self {
            last_report: driver.last_report,
        })
    }
}

#[derive(Default)]
struct RawBackupDriver {
    last_report: BackupReport,
    counter: u64,
}

impl Driver for RawBackupDriver {
    type State = RawBackupMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            BackupScenario(backupCase: BackupCase) => {
                self.last_report = self.run_backup_case(backupCase)?;
            },
            step(backupCase: BackupCase?) => {
                if let Some(backup_case) = backupCase {
                    self.last_report = self.run_backup_case(backup_case)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/raw_backup_mbt.qnt",
    max_samples = 50,
    max_steps = 4,
    seed = "0xbacacc"
)]
fn quint_raw_backup_mbt_compares_full_state() -> impl Driver {
    RawBackupDriver::default()
}

impl RawBackupDriver {
    fn run_backup_case(&mut self, backup_case: BackupCase) -> Result<BackupReport> {
        self.counter += 1;
        let tempdir = tempfile::tempdir()?;
        let store = FilesystemRawBackupStore::new(tempdir.path());
        let operation_id = OperationId::new(format!("raw_backup_mbt_{}", self.counter))?;
        let object_id = RawBackupObjectId::new(format!("users_{:04}", self.counter))?;

        match backup_case {
            BackupCase::WriteReplay => {
                let index = store.write_object(&write_request(operation_id, object_id, 1)?)?;
                let records = store.replay_object(&index)?;
                Ok(BackupReport {
                    result: BackupResult::ReplayComplete,
                    record_count: len_to_i64(records.len()),
                    checksum_rejected: false,
                })
            }
            BackupCase::EmptyRejected => {
                let error = RawBackupWriteRequest::new(
                    operation_id,
                    object_id,
                    "source_users",
                    None,
                    Vec::new(),
                )
                .unwrap_err();
                Ok(BackupReport {
                    result: if matches!(error, RawBackupError::EmptyObject) {
                        BackupResult::RejectedEmpty
                    } else {
                        BackupResult::NotRun
                    },
                    record_count: 0,
                    checksum_rejected: false,
                })
            }
            BackupCase::CorruptRejected => {
                let index = store.write_object(&write_request(operation_id, object_id, 1)?)?;
                std::fs::write(index.object_path.as_str(), b"corrupt\n")?;
                let checksum_rejected = matches!(
                    store.replay_object(&index),
                    Err(RawBackupError::ChecksumMismatch { .. })
                );
                Ok(BackupReport {
                    result: if checksum_rejected {
                        BackupResult::RejectedChecksum
                    } else {
                        BackupResult::NotRun
                    },
                    record_count: 1,
                    checksum_rejected,
                })
            }
        }
    }
}

fn write_request(
    operation_id: OperationId,
    object_id: RawBackupObjectId,
    record_count: usize,
) -> crate::RawBackupResult<RawBackupWriteRequest> {
    RawBackupWriteRequest::new(
        operation_id,
        object_id,
        "source_users",
        Some("policy-v1".to_string()),
        (0..record_count)
            .map(|index| backup_record(format!("user-{index}").as_str()))
            .collect::<crate::RawBackupResult<Vec<_>>>()?,
    )
}

fn backup_record(record_key: &str) -> crate::RawBackupResult<RawBackupRecord> {
    RawBackupRecord::new(
        "users",
        record_key,
        None,
        StorageStreamRecord {
            keys: storage_item([("pk", StorageValue::S(record_key.to_string()))]),
            sequence_number: record_key.to_string(),
            old_image: None,
            new_image: Some(storage_item([(
                "pk",
                StorageValue::S(record_key.to_string()),
            )])),
        },
    )
}

fn storage_item<const N: usize>(
    values: [(&str, StorageValue); N],
) -> HashMap<String, StorageValue> {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn len_to_i64(value: usize) -> i64 {
    i64::try_from(value).expect("MBT record count fits in i64")
}
