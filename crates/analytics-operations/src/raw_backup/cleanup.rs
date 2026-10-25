use serde::{Deserialize, Serialize};

use super::{RawBackupObjectId, RawBackupObjectIndex};
use crate::operation_contract::OperationId;

/// Durable progress for source-object cleanup after a privacy rewrite. Object
/// publication and source cleanup are separate stores, so retries resume from
/// this journal rather than guessing which destructive effects completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawBackupCleanupProgress {
    pub operation_id: OperationId,
    /// Object IDs that have not reached a completed cleanup effect yet. The
    /// privacy cleanup journal keeps this partition disjoint from
    /// `completed_object_ids` and preserves authority order in both lists.
    pub pending_object_ids: Vec<RawBackupObjectId>,
    /// Object IDs whose cleanup effects completed, in authority order.
    pub completed_object_ids: Vec<RawBackupObjectId>,
    /// Cleanup intent is persisted before the first destructive call. If the
    /// process dies after deleting an object but before saving completion,
    /// a retry can treat a missing source index as an already-completed
    /// idempotent delete instead of rereading an unavailable index.
    #[serde(default)]
    pub claimed_object_ids: Vec<RawBackupObjectId>,
    #[serde(default)]
    pub metadata: Option<RawBackupCleanupMetadata>,
    /// The request and concrete stores authorized when this journal was
    /// created. A retry must match this value exactly before it may perform
    /// any source cleanup.
    #[serde(default)]
    pub authority: Option<RawBackupCleanupAuthority>,
}

/// Exact request/store scope authorized for a privacy cleanup journal.
///
/// The request prefixes are retained alongside the concrete store roots so a
/// retry cannot reuse an operation id to redirect cleanup to a different
/// source, destination, object set, or table scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawBackupCleanupAuthority {
    pub source_backup_prefix: String,
    pub clean_target_prefix: String,
    pub quarantine_target_prefix: String,
    pub source_store_root: String,
    pub clean_store_root: String,
    pub quarantine_store_root: String,
    /// Every object selected by the request, in request order. This keeps the
    /// cleanup subset from being replayed for a reordered or expanded request.
    #[serde(default)]
    pub requested_object_ids: Vec<RawBackupObjectId>,
    pub object_ids: Vec<RawBackupObjectId>,
    /// The exact committed source indexes authorized for cleanup, in the same
    /// order as `object_ids`. Paths are deliberately retained in the index for
    /// diagnostics, while resume compares the content fingerprint fields.
    #[serde(default)]
    pub expected_source_indexes: Vec<RawBackupObjectIndex>,
    pub tables: Vec<String>,
}

/// Report fields retained with cleanup progress so a retry can finish without
/// rereading source objects whose destructive cleanup already committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawBackupCleanupMetadata {
    pub scanned_objects: u64,
    pub tainted_objects: u64,
    pub rewritten_objects: u64,
    pub affected_tables: Vec<String>,
    pub policy_version: String,
    pub destructive_action: String,
    pub privacy_dropped_fields: u64,
    pub clean_replay_verified: bool,
}

/// The on-disk envelope protects the structured cleanup state with the same
/// checksum primitive used for committed raw-backup objects. The checksum is
/// over the canonical JSON representation of `progress`, avoiding a
/// self-referential field while making truncation or accidental edits fail
/// closed at load time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawBackupCleanupProgressEnvelope {
    pub(crate) progress: RawBackupCleanupProgress,
    pub(crate) checksum: String,
}
