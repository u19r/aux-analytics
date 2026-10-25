use analytics_contract::{
    AnalyticsManifest, PrivacyPolicy, RetentionTimestamp, SourcePosition, StorageItem,
    StorageStreamRecord,
};
use storage_types::SystemStreamPosition;

use super::{AnalyticsEngineError, AnalyticsEngineResult, SourceBatchFailureContext};
use crate::{
    retention_effect::RetentionEffectSummary,
    source_effect::{self, SourceEffectAuthority, SourceEffectScope, SourceEffectSummary},
};

#[derive(Default)]
pub(crate) struct IngestExecution<'a> {
    pub resolved_retention: Option<&'a IngestRetention>,
    pub authority: super::source_record_authority::SourceRecordAuthority<
        &'a SourcePosition,
        &'a super::SourceRecordMutationPlan,
    >,
    pub source_record_keys: Option<StorageItem>,
    pub source_record_table_name: Option<&'a str>,
}

pub(crate) struct SourceBatchExecution<'a> {
    pub manifest: &'a AnalyticsManifest,
    pub records: Vec<SourceBatchRecord>,
    pub checkpoints: Vec<RecordCheckpoint>,
    pub policy: Option<&'a PrivacyPolicy>,
    pub fence: Option<SourceBatchFence>,
}

pub(crate) struct SourceBatchFence {
    pub authority: SourceEffectAuthority,
    pub scope: SourceEffectScope,
    pub content_digest: [u8; source_effect::CONTENT_DIGEST_BYTES],
}

pub(crate) enum SourceBatchCommit {
    Applied {
        outcome: SourceBatchIngestOutcome,
        summary: SourceEffectSummary,
    },
    AlreadyApplied {
        summary: SourceEffectSummary,
    },
    StaleFence,
}

pub(crate) struct SourceBatchTransactionContext<'a> {
    pub fence: Option<&'a SourceBatchFence>,
    pub failure_context: Option<SourceBatchFailureContext>,
}

pub(crate) enum SourceEffectDecision {
    New,
    AlreadyApplied { summary: SourceEffectSummary },
    StaleFence,
}

pub(crate) enum RetentionEffectDecision {
    New,
    AlreadyApplied { summary: RetentionEffectSummary },
    StaleFence,
}

pub(crate) struct RetentionEffectSelection {
    pub ids: Vec<String>,
    pub selected_count: usize,
    pub complete: bool,
}

impl RetentionEffectSelection {
    pub(crate) fn selected_ids(&self) -> &[String] {
        &self.ids[..self.selected_count]
    }

    pub(crate) fn summary(&self, deleted: u64) -> RetentionEffectSummary {
        RetentionEffectSummary::from_parts(
            deleted,
            (self.ids.len() > self.selected_count)
                .then(|| self.selected_ids().last().cloned())
                .flatten(),
            self.complete,
        )
    }
}

pub(crate) enum SourceFailureFenceDecision {
    New,
    StaleFence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCheckpoint {
    pub source_table_name: String,
    pub shard_id: String,
    pub position: String,
}

/// A checkpoint that is safe to persist in the durable analytics checkpoint
/// table. Iterator tokens are poller-local progress and must never cross this
/// construction boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordCheckpoint {
    source_table_name: String,
    shard_id: String,
    position: String,
}

impl RecordCheckpoint {
    pub fn try_new(
        source_table_name: impl Into<String>,
        shard_id: impl Into<String>,
        position: impl Into<String>,
    ) -> AnalyticsEngineResult<Self> {
        let shard_id = shard_id.into();
        if shard_id.starts_with("__iterator:") {
            return Err(AnalyticsEngineError::IteratorCheckpointNotDurable);
        }
        Ok(Self {
            source_table_name: source_table_name.into(),
            shard_id,
            position: position.into(),
        })
    }

    #[must_use]
    pub fn source_table_name(&self) -> &str {
        self.source_table_name.as_str()
    }

    #[must_use]
    pub fn shard_id(&self) -> &str {
        self.shard_id.as_str()
    }

    #[must_use]
    pub fn position(&self) -> &str {
        self.position.as_str()
    }
}

impl TryFrom<SourceCheckpoint> for RecordCheckpoint {
    type Error = AnalyticsEngineError;

    fn try_from(checkpoint: SourceCheckpoint) -> Result<Self, Self::Error> {
        Self::try_new(
            checkpoint.source_table_name,
            checkpoint.shard_id,
            checkpoint.position,
        )
    }
}

impl TryFrom<&SourceCheckpoint> for RecordCheckpoint {
    type Error = AnalyticsEngineError;

    fn try_from(checkpoint: &SourceCheckpoint) -> Result<Self, Self::Error> {
        Self::try_new(
            checkpoint.source_table_name.clone(),
            checkpoint.shard_id.clone(),
            checkpoint.position.clone(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRetention {
    pub period_ms: Option<u64>,
    pub timestamp: RetentionTimestamp,
    pub missing_retention: bool,
}

/// A validated policy result authorizes clearing missing-retention markers for
/// one tenant. Keeping the period behind this type prevents repair callers from
/// treating an arbitrary duration as policy evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPolicyAuthority {
    tenant_id: String,
    period_ms: u64,
}

impl RetentionPolicyAuthority {
    pub fn try_new(tenant_id: impl Into<String>, period_ms: u64) -> AnalyticsEngineResult<Self> {
        let tenant_id = tenant_id.into();
        if tenant_id.trim().is_empty() {
            return Err(AnalyticsEngineError::MissingTenant);
        }
        if period_ms == 0 {
            return Err(AnalyticsEngineError::InvalidRetentionPeriod);
        }
        Ok(Self {
            tenant_id,
            period_ms,
        })
    }

    #[must_use]
    pub fn tenant_id(&self) -> &str {
        self.tenant_id.as_str()
    }

    #[must_use]
    pub const fn period_ms(&self) -> u64 {
        self.period_ms
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestOutcome {
    Inserted,
    Updated,
    Deleted,
    Skipped,
}

#[derive(Clone, Debug)]
pub struct StreamRecordBatchItem {
    pub analytics_table_name: String,
    pub source_table_name: String,
    /// The source stream shard, when the item originated from a polled shard.
    /// Direct ingestion callers may leave this unset because they do not have
    /// a source-shard boundary to preserve.
    pub shard_id: Option<String>,
    pub record_key: Vec<u8>,
    pub record: StorageStreamRecord,
    /// Validated logical SystemStream position when this item came from the
    /// globally scoped source. Ordinary stream ingestion leaves it absent.
    pub system_position: Option<SystemStreamPosition>,
}

#[derive(Clone, Debug)]
pub struct SourceBatchRecord {
    pub item: StreamRecordBatchItem,
    pub retention: Option<IngestRetention>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SourceBatchIngestOutcome {
    pub outcomes: Vec<IngestOutcome>,
    pub dropped_fields: u64,
}

/// Durable evidence for a deterministic source-record failure. An unresolved
/// failure blocks its source batch until `repair_source_batch_failure` marks
/// the record repaired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceBatchFailure {
    pub source_table_name: String,
    pub shard_id: String,
    pub record_position: String,
    pub analytics_table_name: String,
    pub error_class: String,
    pub error_message: String,
    pub failed_at_ms: i64,
    pub repaired_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivacyIngestOutcome {
    pub outcome: IngestOutcome,
    pub dropped_fields: u64,
    pub policy_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivacyTableRemediationReport {
    pub table_name: String,
    pub dry_run: bool,
    pub mode: PrivacyTableRemediationMode,
    pub scanned_rows: u64,
    pub affected_rows: u64,
    pub projected_values_scrubbed: u64,
    pub document_values_scrubbed: u64,
    pub destination_mutated: bool,
    pub policy_version: String,
}

pub type PrivacyTableScrubReport = PrivacyTableRemediationReport;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivacyTableRemediationMode {
    Scrub,
    DeleteRows,
}

/// One registered record and its resolved batch policy.
pub(super) struct BatchRecordIngest<'a> {
    pub table: &'a analytics_contract::TableRegistration,
    pub item: &'a StreamRecordBatchItem,
    pub retention: Option<&'a IngestRetention>,
    pub source_position: Option<&'a analytics_contract::SourcePosition>,
}
