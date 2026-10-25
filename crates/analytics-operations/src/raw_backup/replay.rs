use analytics_contract::PrivacyPolicy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    RawBackupDestinationKind, RawBackupMetrics, RawBackupObjectId, RawBackupObjectIndex,
    RawBackupReplayRecord, RawBackupResult, apply_raw_backup_replay_privacy_policy,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackupWriteReport {
    pub index: RawBackupObjectIndex,
    pub metrics: RawBackupMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackupReplayReport {
    pub records: Vec<RawBackupReplayRecord>,
    pub metrics: RawBackupMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct RawBackupVerifyReport {
    pub object_id: RawBackupObjectId,
    pub checksum_valid: bool,
    pub expected_checksum: String,
    pub actual_checksum: String,
    pub byte_count: u64,
    pub metrics: RawBackupMetrics,
}

/// Read-only request for replaying one committed raw-backup object. Replay is
/// intentionally separate from `MaintenanceRequest`: it reads and validates
/// an existing object but never creates a durable mutating operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct RawBackupReplayRequest {
    pub object_id: RawBackupObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privacy_policy: Option<analytics_contract::PrivacyPolicy>,
}

impl RawBackupReplayRequest {
    pub fn try_new(
        object_id: RawBackupObjectId,
        privacy_policy: Option<PrivacyPolicy>,
    ) -> RawBackupResult<Self> {
        let request = Self {
            object_id,
            privacy_policy,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> RawBackupResult<()> {
        if let Some(policy) = &self.privacy_policy {
            policy.validate()?;
        }
        Ok(())
    }

    /// Verify and replay one committed object through the object-store
    /// boundary, applying the optional policy before returning records. The
    /// summary is the stable caller-facing result; records remain available
    /// to callers that own analytical ingestion.
    pub fn execute<Store: RawBackupObjectStore>(
        &self,
        store: &Store,
    ) -> RawBackupResult<RawBackupReplayExecution> {
        self.validate()?;
        let index = store.read_index(&self.object_id)?;
        let (verification, report) = store.replay_verified_object(&index)?;
        let report = match self.privacy_policy.as_ref() {
            Some(policy) => apply_raw_backup_replay_privacy_policy(report, policy)?,
            None => report,
        };
        let summary = RawBackupReplaySummary {
            dry_run: true,
            object_id: self.object_id.clone(),
            record_count: report.records.len() as u64,
            verification,
            metrics: report.metrics,
        };
        Ok(RawBackupReplayExecution {
            records: report.records,
            summary,
        })
    }
}

/// Stable read-only replay output. It intentionally does not include the
/// replayed records, which keeps CLI/API summaries bounded and prevents a
/// report from becoming an accidental data export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct RawBackupReplaySummary {
    pub dry_run: bool,
    pub object_id: RawBackupObjectId,
    pub record_count: u64,
    pub verification: RawBackupVerifyReport,
    pub metrics: RawBackupMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawBackupReplayExecution {
    pub records: Vec<RawBackupReplayRecord>,
    pub summary: RawBackupReplaySummary,
}

pub trait RawBackupObjectStore {
    fn destination_kind(&self) -> RawBackupDestinationKind {
        RawBackupDestinationKind::Generic
    }

    fn verify_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupVerifyReport>;

    /// Verify and decode the same object read. Invalid checksums return an
    /// error before any records are decoded or exposed.
    fn replay_verified_object(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<(RawBackupVerifyReport, RawBackupReplayReport)>;

    fn replay_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupReplayReport> {
        Ok(self.replay_verified_object(index)?.1)
    }

    fn replay_object_report_with_privacy_policy(
        &self,
        index: &RawBackupObjectIndex,
        policy: &PrivacyPolicy,
    ) -> RawBackupResult<RawBackupReplayReport> {
        apply_raw_backup_replay_privacy_policy(self.replay_object_report(index)?, policy)
    }

    fn read_index(&self, object_id: &RawBackupObjectId) -> RawBackupResult<RawBackupObjectIndex>;
}
