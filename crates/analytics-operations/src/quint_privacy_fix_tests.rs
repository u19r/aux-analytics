use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    FilesystemRawBackupStore, MaintenanceError, MaintenanceExecutionReport, MaintenanceOperations,
    OperationActor, OperationId, OperationStore, OperationStoreError, PrivacyFixDestructiveAction,
    PrivacyFixError, PrivacyFixRequest, RateLimitPolicy, RawBackupDestinationKind,
    RawBackupObjectId, RawBackupRecord, RawBackupWriteRequest,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum FixCase {
    DryRunRawBackup,
    ApplyRawBackupRejected,
    DeleteWithoutConfirmation,
    DeleteWithConfirmationRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum FixResult {
    NotRun,
    DryRunComplete,
    DestinationFenceUnavailable,
    RejectedMissingConfirmation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct FixReport {
    result: FixResult,
    scanned_objects: i64,
    tainted_objects: i64,
    rewritten_objects: i64,
    destination_mutated: bool,
    clean_replay_verified: bool,
    operation_persisted: bool,
    report_persisted: bool,
    clean_target_mutated: bool,
    quarantine_mutated: bool,
    source_deleted: bool,
    progress_persisted: bool,
}

impl Default for FixReport {
    fn default() -> Self {
        Self {
            result: FixResult::NotRun,
            scanned_objects: 0,
            tainted_objects: 0,
            rewritten_objects: 0,
            destination_mutated: false,
            clean_replay_verified: false,
            operation_persisted: false,
            report_persisted: false,
            clean_target_mutated: false,
            quarantine_mutated: false,
            source_deleted: false,
            progress_persisted: false,
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PrivacyFixMbtState {
    last_report: FixReport,
}

impl State<PrivacyFixDriver> for PrivacyFixMbtState {
    fn from_driver(driver: &PrivacyFixDriver) -> Result<Self> {
        Ok(Self {
            last_report: driver.last_report,
        })
    }
}

#[derive(Default)]
struct PrivacyFixDriver {
    last_report: FixReport,
    counter: u64,
}

impl Driver for PrivacyFixDriver {
    type State = PrivacyFixMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            FixScenario(fixCase: FixCase) => {
                self.last_report = self.run_fix_case(fixCase)?;
            },
            step(fixCase: FixCase?) => {
                if let Some(fix_case) = fixCase {
                    self.last_report = self.run_fix_case(fix_case)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/privacy_fix_mbt.qnt",
    max_samples = 50,
    max_steps = 4,
    seed = "0xf17f1c"
)]
fn given_quint_privacy_fix_model_when_replayed_then_full_state_matches() -> impl Driver {
    PrivacyFixDriver::default()
}

impl PrivacyFixDriver {
    fn run_fix_case(&mut self, fix_case: FixCase) -> Result<FixReport> {
        self.counter += 1;
        self.run_facade_case(fix_case)
    }

    fn run_facade_case(&self, fix_case: FixCase) -> Result<FixReport> {
        let source_dir = tempfile::tempdir()?;
        let clean_dir = tempfile::tempdir()?;
        let quarantine_dir = tempfile::tempdir()?;
        let source_store = FilesystemRawBackupStore::new(source_dir.path());
        let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
        let object_id = RawBackupObjectId::try_new(format!("privacy_fix_mbt_{:04}", self.counter))?;
        let operation_id = OperationId::try_new(format!("privacy_fix_mbt_{}", self.counter))?;
        let index = seed_fixture(&source_store, &object_id, self.counter)?;
        let mut request = privacy_fix_request(
            operation_id.clone(),
            object_id,
            source_dir.path(),
            clean_dir.path(),
            quarantine_dir.path(),
            matches!(fix_case, FixCase::DryRunRawBackup),
        )?;
        if matches!(
            fix_case,
            FixCase::DeleteWithoutConfirmation | FixCase::DeleteWithConfirmationRejected
        ) {
            request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
        }
        if matches!(fix_case, FixCase::DeleteWithConfirmationRejected) {
            request.confirmation_token = Some("quint-confirmed".to_string());
        }

        let before_source = directory_snapshot(source_dir.path())?;
        let before_clean = directory_snapshot(clean_dir.path())?;
        let before_quarantine = directory_snapshot(quarantine_dir.path())?;
        let before_progress = progress_snapshot(source_dir.path())?;
        let operation_store = OperationStore::connect_in_memory()?;
        let operations = MaintenanceOperations::new(&operation_store);
        let actor = OperationActor::try_new("quint")?;
        let execution = operations.execute_privacy_fix_raw_backup(
            &request,
            &actor,
            RateLimitPolicy::default(),
            std::slice::from_ref(&index),
            &source_store,
            &clean_store,
        );
        let after_source = directory_snapshot(source_dir.path())?;
        let after_clean = directory_snapshot(clean_dir.path())?;
        let after_quarantine = directory_snapshot(quarantine_dir.path())?;
        let after_progress = progress_snapshot(source_dir.path())?;
        let clean_target_mutated = before_clean != after_clean;
        let quarantine_mutated = before_quarantine != after_quarantine;
        let source_deleted = before_source
            .keys()
            .any(|path| !after_source.contains_key(path));
        let source_mutated = before_source != after_source;
        let progress_persisted = before_progress != after_progress;
        if source_mutated && !source_deleted {
            return Err(anyhow::anyhow!(
                "privacy facade changed source fixture contents without deleting it"
            ));
        }

        let operation_persisted = operation_exists(&operation_store, &operation_id)?;
        let report_persisted = report_exists(&operation_store, &operation_id)?;
        let destination_mutated = clean_target_mutated || quarantine_mutated || source_deleted;

        match fix_case {
            FixCase::DryRunRawBackup => {
                let report = match execution {
                    Ok(MaintenanceExecutionReport::PrivacyFix(report)) => report,
                    Ok(_) => {
                        return Err(anyhow::anyhow!(
                            "privacy facade returned a non-privacy execution report"
                        ));
                    }
                    Err(error) => return Err(error.into()),
                };
                if report.operation_id != operation_id || !report.dry_run {
                    return Err(anyhow::anyhow!(
                        "privacy dry-run report did not preserve request identity"
                    ));
                }
                Ok(FixReport {
                    result: FixResult::DryRunComplete,
                    scanned_objects: u64_to_i64(report.scanned_objects),
                    tainted_objects: u64_to_i64(report.tainted_objects),
                    rewritten_objects: u64_to_i64(report.rewritten_objects),
                    destination_mutated,
                    clean_replay_verified: report.clean_replay_verified,
                    operation_persisted,
                    report_persisted,
                    clean_target_mutated,
                    quarantine_mutated,
                    source_deleted,
                    progress_persisted,
                })
            }
            FixCase::ApplyRawBackupRejected | FixCase::DeleteWithConfirmationRejected => {
                ensure_fence_rejection(execution)?;
                Ok(FixReport {
                    result: FixResult::DestinationFenceUnavailable,
                    destination_mutated,
                    operation_persisted,
                    report_persisted,
                    clean_target_mutated,
                    quarantine_mutated,
                    source_deleted,
                    progress_persisted,
                    ..FixReport::default()
                })
            }
            FixCase::DeleteWithoutConfirmation => {
                ensure_missing_confirmation(execution)?;
                Ok(FixReport {
                    result: FixResult::RejectedMissingConfirmation,
                    destination_mutated,
                    operation_persisted,
                    report_persisted,
                    clean_target_mutated,
                    quarantine_mutated,
                    source_deleted,
                    progress_persisted,
                    ..FixReport::default()
                })
            }
        }
    }
}

fn ensure_fence_rejection(
    execution: crate::MaintenanceResult<MaintenanceExecutionReport>,
) -> Result<()> {
    match execution {
        Err(MaintenanceError::PrivacyFix(PrivacyFixError::RawBackup(
            crate::RawBackupError::DestinationFenceUnavailable {
                destination: RawBackupDestinationKind::Filesystem,
            },
        ))) => Ok(()),
        Err(error) => Err(anyhow::anyhow!(
            "privacy facade returned an unexpected rejection: {error}"
        )),
        Ok(_) => Err(anyhow::anyhow!(
            "privacy apply unexpectedly produced an execution report"
        )),
    }
}

fn ensure_missing_confirmation(
    execution: crate::MaintenanceResult<MaintenanceExecutionReport>,
) -> Result<()> {
    match execution {
        Err(MaintenanceError::PrivacyFix(PrivacyFixError::MissingConfirmationToken)) => Ok(()),
        Err(error) => Err(anyhow::anyhow!(
            "privacy confirmation rejection returned an unexpected error: {error}"
        )),
        Ok(_) => Err(anyhow::anyhow!(
            "privacy request without confirmation unexpectedly executed"
        )),
    }
}

fn operation_exists(store: &OperationStore, operation_id: &OperationId) -> Result<bool> {
    Ok(store
        .list_operations()?
        .iter()
        .any(|operation| &operation.operation_id == operation_id))
}

fn report_exists(store: &OperationStore, operation_id: &OperationId) -> Result<bool> {
    match store.privacy_fix_report(operation_id) {
        Ok(_) => Ok(true),
        Err(OperationStoreError::OperationNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn seed_fixture(
    store: &FilesystemRawBackupStore,
    object_id: &RawBackupObjectId,
    counter: u64,
) -> crate::RawBackupResult<crate::RawBackupObjectIndex> {
    // Fixture publication is deliberately outside the maintenance action under
    // proof.
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new(format!("privacy_fixture_{counter}"))?,
        object_id.clone(),
        "source_users",
        None,
        vec![RawBackupRecord::try_new(
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
        )?],
    )?;
    store.write_fixture_object(&request)
}

fn privacy_fix_request(
    operation_id: OperationId,
    object_id: RawBackupObjectId,
    source_dir: &Path,
    clean_dir: &Path,
    quarantine_dir: &Path,
    dry_run: bool,
) -> crate::PrivacyFixResult<PrivacyFixRequest> {
    Ok(PrivacyFixRequest {
        operation_id,
        source_backup_prefix: source_dir.to_string_lossy().into_owned(),
        clean_target_prefix: clean_dir.to_string_lossy().into_owned(),
        quarantine_target_prefix: quarantine_dir.to_string_lossy().into_owned(),
        raw_backup_object_ids: vec![object_id],
        tables: vec!["users".to_string()],
        policy: PrivacyPolicy::try_new("privacy-v1")?.with_denied_key_name("email"),
        dry_run,
        destructive_action: PrivacyFixDestructiveAction::None,
        confirmation_token: None,
    })
}

fn directory_snapshot(path: &Path) -> Result<BTreeMap<String, Vec<u8>>> {
    if !path.is_dir() {
        return Ok(BTreeMap::new());
    }
    let mut snapshot = BTreeMap::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            snapshot.insert(
                entry.file_name().to_string_lossy().into_owned(),
                std::fs::read(entry.path())?,
            );
        }
    }
    Ok(snapshot)
}

fn progress_snapshot(path: &Path) -> Result<BTreeSet<String>> {
    Ok(directory_snapshot(path)?
        .into_keys()
        .filter(|name| name.starts_with(".privacy-cleanup-"))
        .collect())
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
