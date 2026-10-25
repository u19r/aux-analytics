use std::path::Path;

use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    FilesystemRawBackupStore, MaintenanceError, MaintenanceOperations, OperationActor, OperationId,
    OperationStore, RateLimitPolicy, RawBackupDestinationKind, RawBackupError, RawBackupObjectId,
    RawBackupRecord, RawBackupReplayRequest, RawBackupWriteRequest,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum BackupCase {
    MaintenanceWriteUnsupported,
    ReplayFixture,
    VerifyFixture,
    EmptyRejected,
    CorruptRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum BackupResult {
    NotRun,
    WriteRejectedUnsupported,
    ReplayComplete,
    VerifyComplete,
    RejectedEmpty,
    RejectedChecksum,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct BackupReport {
    result: BackupResult,
    record_count: i64,
    checksum_rejected: bool,
    operation_persisted: bool,
    object_persisted: bool,
    index_persisted: bool,
    publication_marker_persisted: bool,
}

impl Default for BackupReport {
    fn default() -> Self {
        Self {
            result: BackupResult::NotRun,
            record_count: 0,
            checksum_rejected: false,
            operation_persisted: false,
            object_persisted: false,
            index_persisted: false,
            publication_marker_persisted: false,
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
fn given_quint_raw_backup_model_when_replayed_then_full_state_matches() -> impl Driver {
    RawBackupDriver::default()
}

impl RawBackupDriver {
    fn run_backup_case(&mut self, backup_case: BackupCase) -> Result<BackupReport> {
        self.counter += 1;
        let tempdir = tempfile::tempdir()?;
        let store = FilesystemRawBackupStore::new(tempdir.path());
        let operation_store = OperationStore::connect_in_memory()?;
        let operations = MaintenanceOperations::new(&operation_store);
        let actor = OperationActor::try_new("quint")?;
        let operation_id = OperationId::try_new(format!("raw_backup_mbt_{}", self.counter))?;
        let object_id = RawBackupObjectId::try_new(format!("users_{:04}", self.counter))?;

        match backup_case {
            BackupCase::MaintenanceWriteUnsupported => {
                let request = write_request(operation_id, object_id.clone(), 1)?;
                let error = operations
                    .execute_raw_backup_write(&request, &actor, RateLimitPolicy::default(), &store)
                    .expect_err("raw-backup maintenance writes must fail closed");
                ensure_raw_backup_error(
                    error,
                    RawBackupError::DestinationFenceUnavailable {
                        destination: RawBackupDestinationKind::Filesystem,
                    },
                )?;
                self.report_from_destination(
                    &operation_store,
                    tempdir.path(),
                    &object_id,
                    BackupResult::WriteRejectedUnsupported,
                    0,
                    false,
                )
            }
            BackupCase::EmptyRejected => {
                let request = empty_write_request(operation_id, object_id.clone());
                let error = operations
                    .execute_raw_backup_write(&request, &actor, RateLimitPolicy::default(), &store)
                    .expect_err("empty raw-backup requests must be rejected before effects");
                ensure_raw_backup_error(error, RawBackupError::EmptyObject)?;
                self.report_from_destination(
                    &operation_store,
                    tempdir.path(),
                    &object_id,
                    BackupResult::RejectedEmpty,
                    0,
                    false,
                )
            }
            BackupCase::ReplayFixture => {
                seed_fixture(&store, &operation_id, &object_id)?;
                let replay =
                    RawBackupReplayRequest::try_new(object_id.clone(), None)?.execute(&store)?;
                self.report_from_destination(
                    &operation_store,
                    tempdir.path(),
                    &object_id,
                    BackupResult::ReplayComplete,
                    len_to_i64(replay.summary.record_count as usize),
                    false,
                )
            }
            BackupCase::VerifyFixture => {
                seed_fixture(&store, &operation_id, &object_id)?;
                let index = store.read_index(&object_id)?;
                let verification = store.verify_object_report(&index)?;
                if !verification.checksum_valid {
                    return Err(anyhow::anyhow!(
                        "fixture verification unexpectedly rejected a valid checksum"
                    ));
                }
                self.report_from_destination(
                    &operation_store,
                    tempdir.path(),
                    &object_id,
                    BackupResult::VerifyComplete,
                    len_to_i64(index.record_count as usize),
                    false,
                )
            }
            BackupCase::CorruptRejected => {
                let index = seed_fixture(&store, &operation_id, &object_id)?;
                // This is an explicit fixture mutation, not the production
                // action under proof.
                std::fs::write(index.object_path.as_str(), b"corrupt\n")?;
                let request = RawBackupReplayRequest::try_new(object_id.clone(), None)?;
                let error = request
                    .execute(&store)
                    .expect_err("corrupt fixture must fail checksum verification");
                if !matches!(error, RawBackupError::ChecksumMismatch { .. }) {
                    return Err(anyhow::anyhow!(
                        "corrupt fixture returned an unexpected error: {error}"
                    ));
                }
                self.report_from_destination(
                    &operation_store,
                    tempdir.path(),
                    &object_id,
                    BackupResult::RejectedChecksum,
                    1,
                    true,
                )
            }
        }
    }

    fn report_from_destination(
        &self,
        operation_store: &OperationStore,
        root: &Path,
        object_id: &RawBackupObjectId,
        result: BackupResult,
        record_count: i64,
        checksum_rejected: bool,
    ) -> Result<BackupReport> {
        let (object_persisted, index_persisted, publication_marker_persisted) =
            destination_state(root, object_id);
        let operation_persisted = !operation_store.list_operations()?.is_empty();
        Ok(BackupReport {
            result,
            record_count,
            checksum_rejected,
            operation_persisted,
            object_persisted,
            index_persisted,
            publication_marker_persisted,
        })
    }
}

fn ensure_raw_backup_error(error: MaintenanceError, expected: RawBackupError) -> Result<()> {
    let matches_expected = match (error, expected) {
        (
            MaintenanceError::RawBackup(RawBackupError::DestinationFenceUnavailable {
                destination: actual,
            }),
            RawBackupError::DestinationFenceUnavailable {
                destination: expected,
            },
        ) => actual == expected,
        (MaintenanceError::RawBackup(actual), RawBackupError::EmptyObject) => {
            matches!(actual, RawBackupError::EmptyObject)
        }
        _ => false,
    };
    if matches_expected {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "raw-backup facade returned an unexpected error"
        ))
    }
}

fn seed_fixture(
    store: &FilesystemRawBackupStore,
    operation_id: &OperationId,
    object_id: &RawBackupObjectId,
) -> crate::RawBackupResult<crate::RawBackupObjectIndex> {
    // Fixture publication is deliberately outside the maintenance action under
    // proof.
    store.write_fixture_object(&write_request(operation_id.clone(), object_id.clone(), 1)?)
}

fn destination_state(root: &Path, object_id: &RawBackupObjectId) -> (bool, bool, bool) {
    (
        root.join(format!("{}.jsonl", object_id.as_str())).is_file(),
        root.join(format!("{}.index.json", object_id.as_str()))
            .is_file(),
        root.join(format!(".{}.publish.json", object_id.as_str()))
            .is_file(),
    )
}

fn write_request(
    operation_id: OperationId,
    object_id: RawBackupObjectId,
    record_count: usize,
) -> crate::RawBackupResult<RawBackupWriteRequest> {
    RawBackupWriteRequest::try_new(
        operation_id,
        object_id,
        "source_users",
        Some("policy-v1".to_string()),
        (0..record_count)
            .map(|index| backup_record(format!("user-{index}").as_str()))
            .collect::<crate::RawBackupResult<Vec<_>>>()?,
    )
}

fn empty_write_request(
    operation_id: OperationId,
    object_id: RawBackupObjectId,
) -> RawBackupWriteRequest {
    RawBackupWriteRequest {
        operation_id,
        object_id,
        source_identity: "source_users".to_string(),
        privacy_policy_version: Some("policy-v1".to_string()),
        policy_dropped_records: 0,
        layout: crate::RawBackupObjectLayout::default(),
        compacted_from_object_ids: Vec::new(),
        records: Vec::new(),
    }
}

fn backup_record(record_key: &str) -> crate::RawBackupResult<RawBackupRecord> {
    RawBackupRecord::try_new(
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

fn storage_item<const N: usize>(values: [(&str, StorageValue); N]) -> StorageItem {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn len_to_i64(value: usize) -> i64 {
    i64::try_from(value).expect("MBT record count fits in i64")
}
