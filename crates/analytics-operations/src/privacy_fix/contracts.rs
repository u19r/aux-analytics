use std::{collections::HashMap, path::Path};

use analytics_contract::PrivacyPolicy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use super::push_unique;
use crate::{
    OperationChunkKey, OperationId, OperationStoreError, OperationTypeError, RateLimitDenied,
    RawBackupCleanupAuthority, RawBackupError, RawBackupObjectId, RawBackupObjectIndex,
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
    #[error("privacy fix request must not repeat a raw backup object")]
    DuplicateRawBackupObject,
    #[error("privacy fix request must include at least one table")]
    EmptyTables,
    #[error("privacy fix backup prefixes must be non-empty and distinct")]
    InvalidBackupPrefixes,
    #[error("privacy fix replay contained a table outside the request scope")]
    TableOutsideRequestScope,
    #[error("privacy fix source, clean, and quarantine stores must be distinct")]
    OverlappingBackupStores,
    #[error("privacy fix destructive cleanup requires a confirmation token")]
    MissingConfirmationToken,
    #[error("raw backup index {0} was not supplied to privacy fix execution")]
    MissingRawBackupIndex(RawBackupObjectId),
    #[error("privacy fix clean replay verification failed for raw backup object {0}")]
    CleanReplayVerificationFailed(RawBackupObjectId),
    #[error("privacy fix cleanup failed after {original_error}: {cleanup_error}")]
    CleanupFailed {
        original_error: String,
        cleanup_error: String,
    },
    #[error("privacy fix cleanup progress authority does not match the request and stores")]
    CleanupAuthorityMismatch,
    #[error(transparent)]
    RateLimit(#[from] RateLimitDenied),
    #[error(transparent)]
    OperationStore(#[from] OperationStoreError),
}

pub type PrivacyFixResult<T> = Result<T, PrivacyFixError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PrivacyFixRequest {
    pub operation_id: OperationId,
    pub source_backup_prefix: String,
    pub clean_target_prefix: String,
    pub quarantine_target_prefix: String,
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
        if has_duplicate_object_ids(self.raw_backup_object_ids.as_slice()) {
            return Err(PrivacyFixError::DuplicateRawBackupObject);
        }
        if self.tables.is_empty() {
            return Err(PrivacyFixError::EmptyTables);
        }
        if self.source_backup_prefix.trim().is_empty()
            || self.clean_target_prefix.trim().is_empty()
            || self.quarantine_target_prefix.trim().is_empty()
            || !Path::new(&self.source_backup_prefix).is_absolute()
            || !Path::new(&self.clean_target_prefix).is_absolute()
            || !Path::new(&self.quarantine_target_prefix).is_absolute()
            || self.source_backup_prefix == self.clean_target_prefix
            || self.source_backup_prefix == self.quarantine_target_prefix
            || self.clean_target_prefix == self.quarantine_target_prefix
        {
            return Err(PrivacyFixError::InvalidBackupPrefixes);
        }
        self.policy.validate()?;
        if self.destructive_action.requires_confirmation()
            && self
                .confirmation_token
                .as_deref()
                .is_none_or(|token| token.trim().is_empty())
        {
            return Err(PrivacyFixError::MissingConfirmationToken);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
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
pub(crate) struct PrivacyFixExecutor;

/// OperationStore lifecycle and journal bookkeeping only; it does not provide
/// atomic fencing at the destination commit boundary. Destructive production
/// apply remains disabled until the destination supplies that capability.
pub(crate) trait PrivacyFixEffectBoundary {
    fn before_effect(&mut self, key: &OperationChunkKey) -> PrivacyFixResult<()>;
    fn after_effect(&mut self, key: &OperationChunkKey) -> PrivacyFixResult<()>;
    fn abandon_effect(&mut self, key: &OperationChunkKey) -> PrivacyFixResult<()>;
}

#[cfg(test)]
/// Test-only boundary for exercising the executor kernel without the facade.
#[derive(Debug, Default)]
pub(crate) struct NoopPrivacyFixEffectBoundary;

#[cfg(test)]
impl PrivacyFixEffectBoundary for NoopPrivacyFixEffectBoundary {
    fn before_effect(&mut self, _key: &OperationChunkKey) -> PrivacyFixResult<()> {
        Ok(())
    }

    fn after_effect(&mut self, _key: &OperationChunkKey) -> PrivacyFixResult<()> {
        Ok(())
    }

    fn abandon_effect(&mut self, _key: &OperationChunkKey) -> PrivacyFixResult<()> {
        Ok(())
    }
}

pub(crate) fn same_cleanup_scope(
    actual: &RawBackupCleanupAuthority,
    expected: &RawBackupCleanupAuthority,
) -> bool {
    actual.source_backup_prefix == expected.source_backup_prefix
        && actual.clean_target_prefix == expected.clean_target_prefix
        && actual.quarantine_target_prefix == expected.quarantine_target_prefix
        && actual.source_store_root == expected.source_store_root
        && actual.clean_store_root == expected.clean_store_root
        && actual.quarantine_store_root == expected.quarantine_store_root
        && actual.requested_object_ids == expected.requested_object_ids
        && actual.tables == expected.tables
}

pub(crate) fn validate_cleanup_partition(
    progress: &crate::RawBackupCleanupProgress,
    authority: &RawBackupCleanupAuthority,
    request: &PrivacyFixRequest,
) -> PrivacyFixResult<()> {
    let expected_ids = authority.object_ids.as_slice();
    let expected_indexes = authority.expected_source_indexes.as_slice();
    if expected_ids.is_empty()
        || expected_indexes.len() != expected_ids.len()
        || has_duplicate_object_ids(expected_ids)
        || has_duplicate_object_ids(progress.pending_object_ids.as_slice())
        || has_duplicate_object_ids(progress.completed_object_ids.as_slice())
        || has_duplicate_object_ids(progress.claimed_object_ids.as_slice())
    {
        return Err(PrivacyFixError::CleanupAuthorityMismatch);
    }
    if expected_indexes
        .iter()
        .zip(expected_ids)
        .any(|(index, object_id)| &index.object_id != object_id)
        || authority.requested_object_ids.as_slice() != request.raw_backup_object_ids.as_slice()
        || expected_ids
            .iter()
            .any(|object_id| !request.raw_backup_object_ids.contains(object_id))
    {
        return Err(PrivacyFixError::CleanupAuthorityMismatch);
    }
    if progress.completed_object_ids.len() > expected_ids.len() {
        return Err(PrivacyFixError::CleanupAuthorityMismatch);
    }
    let completed_count = progress.completed_object_ids.len();
    if progress.completed_object_ids.as_slice() != &expected_ids[..completed_count]
        || progress.pending_object_ids.as_slice() != &expected_ids[completed_count..]
        || progress
            .completed_object_ids
            .iter()
            .any(|object_id| progress.pending_object_ids.contains(object_id))
        || progress.claimed_object_ids.iter().any(|object_id| {
            !progress.pending_object_ids.contains(object_id)
                || progress.completed_object_ids.contains(object_id)
        })
        || !is_ordered_subsequence(
            progress.claimed_object_ids.as_slice(),
            progress.pending_object_ids.as_slice(),
        )
    {
        return Err(PrivacyFixError::CleanupAuthorityMismatch);
    }
    Ok(())
}

fn has_duplicate_object_ids(object_ids: &[RawBackupObjectId]) -> bool {
    object_ids
        .iter()
        .enumerate()
        .any(|(position, object_id)| object_ids[..position].contains(object_id))
}

fn is_ordered_subsequence(
    values: &[RawBackupObjectId],
    ordered_values: &[RawBackupObjectId],
) -> bool {
    let mut next_position = 0;
    for value in values {
        let Some(offset) = ordered_values[next_position..]
            .iter()
            .position(|candidate| candidate == value)
        else {
            return false;
        };
        next_position += offset + 1;
    }
    true
}

pub(crate) enum ResolvedPrivacyRawBackupIndex<'a> {
    Borrowed(&'a RawBackupObjectIndex),
    Owned(Box<RawBackupObjectIndex>),
}

impl AsRef<RawBackupObjectIndex> for ResolvedPrivacyRawBackupIndex<'_> {
    fn as_ref(&self) -> &RawBackupObjectIndex {
        match self {
            Self::Borrowed(index) => index,
            Self::Owned(index) => index,
        }
    }
}

pub(crate) trait PrivacyRawBackupIndexLookup<'a> {
    fn find(&self, object_id: &RawBackupObjectId) -> Option<ResolvedPrivacyRawBackupIndex<'a>>;
}

pub(crate) struct IndexedPrivacyRawBackupIndexes<'a> {
    by_id: HashMap<&'a str, &'a RawBackupObjectIndex>,
}

impl<'a> IndexedPrivacyRawBackupIndexes<'a> {
    pub(crate) fn new(indexes: &'a [RawBackupObjectIndex]) -> Self {
        let mut by_id = HashMap::with_capacity(indexes.len());
        for index in indexes {
            by_id.entry(index.object_id.as_str()).or_insert(index);
        }
        Self { by_id }
    }
}

impl<'a> PrivacyRawBackupIndexLookup<'a> for IndexedPrivacyRawBackupIndexes<'a> {
    fn find(&self, object_id: &RawBackupObjectId) -> Option<ResolvedPrivacyRawBackupIndex<'a>> {
        self.by_id
            .get(object_id.as_str())
            .copied()
            .map(ResolvedPrivacyRawBackupIndex::Borrowed)
    }
}

pub(crate) trait PrivacyAffectedTables {
    fn observe(&mut self, table: &str);

    fn snapshot(&self) -> Vec<String>;

    fn finish(self) -> Vec<String>;
}

#[derive(Default)]
pub(crate) struct LinearPrivacyAffectedTables(Vec<String>);

impl PrivacyAffectedTables for LinearPrivacyAffectedTables {
    fn observe(&mut self, table: &str) {
        push_unique(&mut self.0, table);
    }

    fn snapshot(&self) -> Vec<String> {
        self.0.clone()
    }

    fn finish(self) -> Vec<String> {
        self.0
    }
}

pub(crate) struct PendingPrivacyCleanup<'a> {
    pub(crate) object_id: RawBackupObjectId,
    pub(crate) index: ResolvedPrivacyRawBackupIndex<'a>,
}

impl<'a> ResolvedPrivacyRawBackupIndex<'a> {
    pub(crate) fn resolve(
        source: &crate::FilesystemRawBackupStore,
        lookup: &impl PrivacyRawBackupIndexLookup<'a>,
        object_id: &RawBackupObjectId,
    ) -> PrivacyFixResult<Self> {
        if let Some(index) = lookup.find(object_id) {
            return Ok(index);
        }
        source
            .read_index(object_id)
            .map(|index| Self::Owned(Box::new(index)))
            .map_err(|error| match error {
                RawBackupError::Io(_) => PrivacyFixError::MissingRawBackupIndex(object_id.clone()),
                error => PrivacyFixError::RawBackup(error),
            })
    }
}

impl PrivacyFixRequest {
    pub(crate) fn validate_replay_scope(
        &self,
        replay: &crate::RawBackupReplayReport,
    ) -> PrivacyFixResult<()> {
        if replay
            .records
            .iter()
            .any(|record| !self.tables.contains(&record.table_name))
        {
            return Err(PrivacyFixError::TableOutsideRequestScope);
        }
        Ok(())
    }
}

impl PrivacyFixReport {
    pub(super) fn from_plan(
        request: &PrivacyFixRequest,
        affected_tables: Vec<String>,
        tainted_objects: u64,
        privacy_dropped_fields: u64,
    ) -> Self {
        Self {
            operation_id: request.operation_id.clone(),
            dry_run: true,
            scanned_objects: request.raw_backup_object_ids.len() as u64,
            tainted_objects,
            rewritten_objects: 0,
            deleted_tainted_objects: 0,
            quarantined_tainted_objects: 0,
            affected_tables,
            policy_version: request.policy.version.clone(),
            privacy_dropped_fields,
            clean_replay_verified: false,
            destructive_action: request.destructive_action,
        }
    }
}
