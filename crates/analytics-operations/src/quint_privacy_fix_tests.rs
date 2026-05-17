use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    FilesystemRawBackupStore, OperationId, PrivacyFixDestructiveAction, PrivacyFixError,
    PrivacyFixExecutor, PrivacyFixRequest, RawBackupObjectId, RawBackupRecord,
    RawBackupWriteRequest,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum FixCase {
    DryRunRawBackup,
    RewriteRawBackup,
    DeleteWithoutConfirmation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum FixResult {
    NotRun,
    DryRunComplete,
    CleanRewriteComplete,
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
fn quint_privacy_fix_mbt_compares_full_state() -> impl Driver {
    PrivacyFixDriver::default()
}

impl PrivacyFixDriver {
    fn run_fix_case(&mut self, fix_case: FixCase) -> Result<FixReport> {
        self.counter += 1;
        match fix_case {
            FixCase::DryRunRawBackup => self.run_raw_backup(false, true),
            FixCase::RewriteRawBackup => self.run_raw_backup(false, false),
            FixCase::DeleteWithoutConfirmation => {
                let mut request = privacy_fix_request(
                    RawBackupObjectId::new("privacy_fix_missing_confirmation")?,
                    true,
                    self.counter,
                )?;
                request.destructive_action = PrivacyFixDestructiveAction::DeleteTainted;
                let error = request.validate().unwrap_err();
                Ok(FixReport {
                    result: if matches!(error, PrivacyFixError::MissingConfirmationToken) {
                        FixResult::RejectedMissingConfirmation
                    } else {
                        FixResult::NotRun
                    },
                    ..FixReport::default()
                })
            }
        }
    }

    fn run_raw_backup(&self, _delete_after: bool, dry_run: bool) -> Result<FixReport> {
        let source_dir = tempfile::tempdir()?;
        let clean_dir = tempfile::tempdir()?;
        let source_store = FilesystemRawBackupStore::new(source_dir.path());
        let clean_store = FilesystemRawBackupStore::new(clean_dir.path());
        let object_id = RawBackupObjectId::new(format!("privacy_fix_mbt_{}", self.counter))?;
        let index = source_store.write_object(&write_request(object_id.clone(), self.counter)?)?;
        let request = privacy_fix_request(object_id, dry_run, self.counter)?;
        let report = PrivacyFixExecutor.execute_raw_backup_rewrite(
            &request,
            &source_store,
            &clean_store,
            &[index],
        )?;

        Ok(FixReport {
            result: if dry_run {
                FixResult::DryRunComplete
            } else {
                FixResult::CleanRewriteComplete
            },
            scanned_objects: u64_to_i64(report.scanned_objects),
            tainted_objects: u64_to_i64(report.tainted_objects),
            rewritten_objects: u64_to_i64(report.rewritten_objects),
            destination_mutated: !report.dry_run && report.rewritten_objects > 0,
            clean_replay_verified: report.clean_replay_verified,
        })
    }
}

fn privacy_fix_request(
    object_id: RawBackupObjectId,
    dry_run: bool,
    counter: u64,
) -> crate::PrivacyFixResult<PrivacyFixRequest> {
    Ok(PrivacyFixRequest {
        operation_id: OperationId::new(format!("privacy_fix_mbt_{counter}"))?,
        source_backup_prefix: "source".to_string(),
        clean_target_prefix: "clean".to_string(),
        raw_backup_object_ids: vec![object_id],
        tables: vec!["users".to_string()],
        policy: PrivacyPolicy::new("privacy-v1")?.with_denied_key_name("email"),
        dry_run,
        destructive_action: PrivacyFixDestructiveAction::None,
        confirmation_token: None,
    })
}

fn write_request(
    object_id: RawBackupObjectId,
    counter: u64,
) -> crate::RawBackupResult<RawBackupWriteRequest> {
    RawBackupWriteRequest::new(
        OperationId::new(format!("raw_backup_mbt_{counter}"))?,
        object_id,
        "source_users",
        None,
        vec![RawBackupRecord::new(
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
        )?],
    )
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
