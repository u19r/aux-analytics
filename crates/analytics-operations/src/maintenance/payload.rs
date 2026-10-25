use serde_json::json;

use super::{
    MaintenanceRequest,
    fingerprint::{calculate_request_fingerprint, calculate_value_fingerprint},
};
use crate::{PrivacyFixRequest, TableFixRequest, TrimRequest, error::MaintenanceResult};

impl MaintenanceRequest {
    pub(super) fn build_operation_payload(&self) -> MaintenanceResult<serde_json::Value> {
        match self {
            Self::TableFix(request) => request.build_operation_payload(),
            Self::PrivacyFix(request) => request.build_operation_payload(),
            Self::Trim(request) => request.build_operation_payload(),
            Self::RawBackup(request) => Ok(json!({
                "request_fingerprint": calculate_request_fingerprint(request)?,
                "object_id": request.object_id,
                "source_identity": request.source_identity,
                "privacy_policy_version": request.privacy_policy_version,
                "policy_dropped_records": request.policy_dropped_records,
                "layout": request.layout,
                "compacted_from_object_ids": request.compacted_from_object_ids,
                "record_count": request.records.len(),
            })),
        }
    }
}

impl TableFixRequest {
    fn build_operation_payload(&self) -> MaintenanceResult<serde_json::Value> {
        let mut identity = serde_json::to_value(self)?;
        if let Some(validation) = identity
            .get_mut("validation")
            .and_then(serde_json::Value::as_object_mut)
        {
            validation.remove("confirmation");
        }
        Ok(json!({
            "request_fingerprint": calculate_value_fingerprint(identity)?,
            "table_name": self.table_name,
            "dry_run": self.dry_run,
            "selection": self.selection,
            "source": self.source,
            "validation": {
                "require_post_fix_check": self.validation.require_post_fix_check,
                "max_rows": self.validation.max_rows,
                "has_confirmation": self.validation.confirmation.is_some(),
            },
        }))
    }
}

impl PrivacyFixRequest {
    pub(super) fn build_operation_payload(&self) -> MaintenanceResult<serde_json::Value> {
        Ok(json!({
            "request_fingerprint": calculate_fingerprint_without_confirmation_token(self)?,
            "source_backup_prefix": self.source_backup_prefix,
            "clean_target_prefix": self.clean_target_prefix,
            "quarantine_target_prefix": self.quarantine_target_prefix,
            "raw_backup_object_ids": self.raw_backup_object_ids,
            "tables": self.tables,
            "policy_version": self.policy.version,
            "dry_run": self.dry_run,
            "destructive_action": self.destructive_action,
        }))
    }
}

impl TrimRequest {
    fn build_operation_payload(&self) -> MaintenanceResult<serde_json::Value> {
        Ok(json!({
            "request_fingerprint": calculate_fingerprint_without_confirmation_token(self)?,
            "table_name": self.table_name,
            "dry_run": self.dry_run,
            "target": self.target,
        }))
    }
}

fn calculate_fingerprint_without_confirmation_token(
    request: &impl serde::Serialize,
) -> Result<String, serde_json::Error> {
    let mut identity = serde_json::to_value(request)?;
    if let Some(identity) = identity.as_object_mut() {
        identity.remove("confirmation_token");
    }
    calculate_value_fingerprint(identity)
}
