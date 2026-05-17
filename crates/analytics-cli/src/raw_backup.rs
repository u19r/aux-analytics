use std::{fs, path::Path};

use analytics_contract::PrivacyPolicy;
use analytics_operations::{
    FilesystemRawBackupStore, OperationId, RawBackupCompression, RawBackupObjectId,
    RawBackupObjectLayout, RawBackupRecord, RawBackupWriteRequest,
};

use crate::{
    error::CliResult,
    raw_backup_args::{RawBackupCommand, RawBackupCompressionArg},
};

pub(crate) fn run_raw_backup_command(command: RawBackupCommand) -> CliResult<String> {
    match command {
        RawBackupCommand::Write {
            backup_root,
            operation_id,
            object_id,
            source_identity,
            records,
            privacy_policy_version,
            privacy_policy,
            compression,
            dry_run,
        } => {
            let records = read_raw_backup_records(records.as_path())?;
            let mut request = RawBackupWriteRequest::new(
                OperationId::new(operation_id)?,
                RawBackupObjectId::new(object_id)?,
                source_identity,
                privacy_policy_version,
                records,
            )?
            .with_layout(RawBackupObjectLayout {
                compression: compression.into(),
                ..RawBackupObjectLayout::default()
            });
            if let Some(path) = privacy_policy {
                let policy = read_privacy_policy(path.as_path())?;
                request = request.with_privacy_policy(&policy)?;
            }
            if dry_run {
                return Ok(serde_json::to_string_pretty(&serde_json::json!({
                    "dry_run": true,
                    "object_id": request.object_id,
                    "record_count": request.records.len(),
                    "compression": request.layout.compression,
                    "policy_drops": request.policy_dropped_records,
                    "privacy_policy_version": request.privacy_policy_version,
                }))?);
            }
            let report =
                FilesystemRawBackupStore::new(backup_root).write_object_report(&request)?;
            Ok(serde_json::to_string_pretty(&report)?)
        }
        RawBackupCommand::ReplayDryRun {
            backup_root,
            object_id,
            privacy_policy,
        } => {
            let store = FilesystemRawBackupStore::new(backup_root);
            let object_id = RawBackupObjectId::new(object_id)?;
            let index = store.read_index(&object_id)?;
            let verification = store.verify_object_report(&index)?;
            let report = if let Some(path) = privacy_policy {
                let policy = read_privacy_policy(path.as_path())?;
                store.replay_object_report_with_privacy_policy(&index, &policy)?
            } else {
                store.replay_object_report(&index)?
            };
            Ok(serde_json::to_string_pretty(&serde_json::json!({
                "dry_run": true,
                "object_id": object_id,
                "record_count": report.records.len(),
                "verification": verification,
                "metrics": report.metrics,
            }))?)
        }
    }
}

fn read_raw_backup_records(path: &Path) -> CliResult<Vec<RawBackupRecord>> {
    let contents = fs::read_to_string(path)?;
    serde_json::from_str::<Vec<RawBackupRecord>>(contents.as_str())
        .or_else(|_| {
            serde_json::from_str::<RawBackupRecord>(contents.as_str()).map(|record| vec![record])
        })
        .map_err(Into::into)
}

fn read_privacy_policy(path: &Path) -> CliResult<PrivacyPolicy> {
    let policy: PrivacyPolicy = serde_json::from_str(fs::read_to_string(path)?.as_str())?;
    policy.validate()?;
    Ok(policy)
}

impl From<RawBackupCompressionArg> for RawBackupCompression {
    fn from(value: RawBackupCompressionArg) -> Self {
        match value {
            RawBackupCompressionArg::None => Self::None,
            RawBackupCompressionArg::RunLength => Self::RunLength,
        }
    }
}
