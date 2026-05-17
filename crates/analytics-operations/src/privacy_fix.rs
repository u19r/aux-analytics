use analytics_contract::PrivacyPolicy;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    FilesystemRawBackupStore, OperationId, OperationTypeError, RawBackupError, RawBackupObjectId,
    RawBackupObjectIndex, RawBackupRecord, RawBackupWriteRequest,
};

#[derive(Debug, Error)]
pub enum PrivacyFixError {
    #[error(transparent)]
    OperationType(#[from] OperationTypeError),
    #[error(transparent)]
    RawBackup(#[from] RawBackupError),
    #[error(transparent)]
    PrivacyPolicy(#[from] analytics_contract::PrivacyPolicyError),
    #[error("privacy fix request must include at least one raw backup object")]
    EmptyRawBackupObjects,
    #[error("privacy fix destructive cleanup requires a confirmation token")]
    MissingConfirmationToken,
    #[error("raw backup index {0} was not supplied to privacy fix execution")]
    MissingRawBackupIndex(RawBackupObjectId),
}

pub type PrivacyFixResult<T> = Result<T, PrivacyFixError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivacyFixRequest {
    pub operation_id: OperationId,
    pub source_backup_prefix: String,
    pub clean_target_prefix: String,
    pub raw_backup_object_ids: Vec<RawBackupObjectId>,
    pub tables: Vec<String>,
    pub policy: PrivacyPolicy,
    pub dry_run: bool,
    pub destructive_action: PrivacyFixDestructiveAction,
    pub confirmation_token: Option<String>,
}

impl PrivacyFixRequest {
    pub fn validate(&self) -> PrivacyFixResult<()> {
        if self.raw_backup_object_ids.is_empty() {
            return Err(PrivacyFixError::EmptyRawBackupObjects);
        }
        self.policy.validate()?;
        if self.destructive_action.requires_confirmation() && self.confirmation_token.is_none() {
            return Err(PrivacyFixError::MissingConfirmationToken);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyFixDestructiveAction {
    None,
    QuarantineTainted,
    DeleteTainted,
}

impl PrivacyFixDestructiveAction {
    const fn requires_confirmation(self) -> bool {
        matches!(self, Self::QuarantineTainted | Self::DeleteTainted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivacyFixReport {
    pub operation_id: OperationId,
    pub dry_run: bool,
    pub scanned_objects: u64,
    pub tainted_objects: u64,
    pub rewritten_objects: u64,
    pub deleted_tainted_objects: u64,
    pub quarantined_tainted_objects: u64,
    pub affected_tables: Vec<String>,
    pub policy_version: String,
    pub privacy_dropped_fields: u64,
    pub clean_replay_verified: bool,
    pub destructive_action: PrivacyFixDestructiveAction,
}

impl PrivacyFixReport {
    #[must_use]
    pub fn operation_counts(&self) -> serde_json::Value {
        serde_json::json!({
            "scanned_objects": self.scanned_objects,
            "tainted_objects": self.tainted_objects,
            "rewritten_objects": self.rewritten_objects,
            "deleted_tainted_objects": self.deleted_tainted_objects,
            "quarantined_tainted_objects": self.quarantined_tainted_objects,
            "affected_tables": self.affected_tables,
            "policy_version": self.policy_version,
            "privacy_dropped_fields": self.privacy_dropped_fields,
            "clean_replay_verified": self.clean_replay_verified,
            "destructive_action": self.destructive_action,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PrivacyFixExecutor;

impl PrivacyFixExecutor {
    pub fn execute_raw_backup_rewrite(
        &self,
        request: &PrivacyFixRequest,
        source_store: &FilesystemRawBackupStore,
        clean_store: &FilesystemRawBackupStore,
        indexes: &[RawBackupObjectIndex],
    ) -> PrivacyFixResult<PrivacyFixReport> {
        request.validate()?;
        let mut affected_tables = Vec::new();
        let mut tainted_objects = 0_u64;
        let mut rewritten_objects = 0_u64;
        let mut deleted_tainted_objects = 0_u64;
        let mut quarantined_tainted_objects = 0_u64;
        let mut privacy_dropped_fields = 0_u64;
        let mut clean_replay_verified = true;

        for object_id in &request.raw_backup_object_ids {
            let index = indexes
                .iter()
                .find(|index| &index.object_id == object_id)
                .cloned()
                .map_or_else(|| source_store.read_index(object_id), Ok)
                .map_err(|error| match error {
                    RawBackupError::Io(_) => {
                        PrivacyFixError::MissingRawBackupIndex(object_id.clone())
                    }
                    error => PrivacyFixError::RawBackup(error),
                })?;
            let replay =
                source_store.replay_object_report_with_privacy_policy(&index, &request.policy)?;
            privacy_dropped_fields += replay.metrics.policy_drops;
            if replay.metrics.policy_drops > 0 {
                tainted_objects += 1;
            }
            let object_is_tainted = replay.metrics.policy_drops > 0;
            for record in &replay.records {
                push_unique(&mut affected_tables, record.table_name.as_str());
            }
            if request.dry_run {
                continue;
            }
            let write_request = RawBackupWriteRequest::new(
                request.operation_id.clone(),
                index.object_id.clone(),
                index.source_identity.clone(),
                Some(request.policy.version.clone()),
                replay
                    .records
                    .into_iter()
                    .map(|record| RawBackupRecord {
                        table_name: record.table_name,
                        record_key: String::from_utf8_lossy(record.record_key.as_slice())
                            .into_owned(),
                        source_position: record.source_position,
                        record: record.record,
                    })
                    .collect(),
            )?
            .with_layout(index.layout.clone());
            let clean_index = clean_store.write_object_report(&write_request)?.index;
            let verification = clean_store.verify_object_report(&clean_index)?;
            clean_replay_verified &= verification.checksum_valid;
            rewritten_objects += 1;
            if object_is_tainted && verification.checksum_valid {
                match request.destructive_action {
                    PrivacyFixDestructiveAction::None => {}
                    PrivacyFixDestructiveAction::QuarantineTainted => {
                        source_store.quarantine_object(&index, clean_store)?;
                        source_store.delete_object(&index)?;
                        quarantined_tainted_objects += 1;
                    }
                    PrivacyFixDestructiveAction::DeleteTainted => {
                        source_store.delete_object(&index)?;
                        deleted_tainted_objects += 1;
                    }
                }
            }
        }

        Ok(PrivacyFixReport {
            operation_id: request.operation_id.clone(),
            dry_run: request.dry_run,
            scanned_objects: request.raw_backup_object_ids.len() as u64,
            tainted_objects,
            rewritten_objects,
            deleted_tainted_objects,
            quarantined_tainted_objects,
            affected_tables,
            policy_version: request.policy.version.clone(),
            privacy_dropped_fields,
            clean_replay_verified,
            destructive_action: request.destructive_action,
        })
    }
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}
