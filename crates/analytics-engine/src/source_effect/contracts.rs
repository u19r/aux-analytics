use std::num::NonZeroU64;

use analytics_contract::{AnalyticsManifest, PrivacyPolicy, SourcePosition};
use storage_types::{SystemStreamCheckpointToken, SystemStreamPosition, SystemStreamScope};

use super::{
    digest::manifest_digest,
    validation::{valid_scope_component, validate_record_registrations, validate_scope_contents},
};
use crate::{
    constants::{EXECUTION_ID_BYTES, MANIFEST_DIGEST_BYTES},
    engine::{
        AnalyticsEngineError, AnalyticsEngineResult, IngestOutcome, RecordCheckpoint,
        SourceBatchRecord,
    },
};

/// The execution identity is exactly the stable 16-byte logical identity from
/// the lifecycle contract. The fence is the non-zero destination epoch used
/// for authority ordering where the scope has no stronger predecessor. Table
/// streams use their durable predecessor for page ordering because each page
/// is a separate bg-jobs record and may reuse its lease fence. Operation
/// identifiers are deliberately not part of this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceEffectAuthority {
    execution_id: [u8; EXECUTION_ID_BYTES],
    fence: NonZeroU64,
}

impl SourceEffectAuthority {
    /// Constructs an authority from the opaque execution bytes and fence.
    pub fn try_new(execution_id: impl AsRef<[u8]>, fence: u64) -> AnalyticsEngineResult<Self> {
        let bytes = execution_id.as_ref();
        let execution_id =
            bytes
                .try_into()
                .map_err(|_| AnalyticsEngineError::InvalidSourceEffectExecutionId {
                    actual: bytes.len(),
                })?;
        let fence = NonZeroU64::new(fence).ok_or(AnalyticsEngineError::InvalidSourceEffectFence)?;
        Ok(Self {
            execution_id,
            fence,
        })
    }

    /// Constructs an authority once the caller already holds a fixed identity
    /// and a non-zero fence.
    #[must_use]
    pub const fn from_parts(execution_id: [u8; EXECUTION_ID_BYTES], fence: NonZeroU64) -> Self {
        Self {
            execution_id,
            fence,
        }
    }

    #[must_use]
    pub const fn execution_id(&self) -> &[u8; EXECUTION_ID_BYTES] {
        &self.execution_id
    }

    #[must_use]
    pub const fn fence(&self) -> NonZeroU64 {
        self.fence
    }
}

/// The logical source authority covered by a fenced source effect. Table
/// streams retain their table/shard scope; SystemStream carries the DB-owned
/// versioned scope and opaque issued frontier without inventing topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceEffectScope {
    Table {
        source_table_name: String,
        shard_id: String,
        /// The exact opaque source position that was durable before this
        /// page was fetched. `None` is only valid for the origin.
        previous_position: Option<String>,
    },
    System {
        source_scope: SystemStreamScope,
        /// `None` is the stream origin; `Some(token)` is a resumed page whose
        /// request must name the exact durable predecessor.
        previous_checkpoint: Option<SystemStreamCheckpointToken>,
        checkpoint: SystemStreamCheckpointToken,
    },
}

impl SourceEffectScope {
    pub fn try_new(
        source_table_name: impl Into<String>,
        shard_id: impl Into<String>,
    ) -> AnalyticsEngineResult<Self> {
        Self::try_new_with_predecessor(source_table_name, shard_id, None)
    }

    pub fn try_new_with_predecessor(
        source_table_name: impl Into<String>,
        shard_id: impl Into<String>,
        previous_position: Option<String>,
    ) -> AnalyticsEngineResult<Self> {
        let source_table_name = source_table_name.into();
        let shard_id = shard_id.into();
        if !valid_scope_component(&source_table_name) || !valid_scope_component(&shard_id) {
            return Err(AnalyticsEngineError::InvalidSourceEffectScope);
        }
        if previous_position
            .as_deref()
            .is_some_and(|position| !valid_scope_component(position))
        {
            return Err(AnalyticsEngineError::InvalidSourceEffectScope);
        }
        Ok(Self::Table {
            source_table_name,
            shard_id,
            previous_position,
        })
    }

    pub fn try_new_system(
        source_scope: SystemStreamScope,
        previous_checkpoint: Option<SystemStreamCheckpointToken>,
        checkpoint: SystemStreamCheckpointToken,
    ) -> AnalyticsEngineResult<Self> {
        Ok(Self::System {
            source_scope,
            previous_checkpoint,
            checkpoint,
        })
    }

    #[must_use]
    pub fn table_parts(&self) -> Option<(&str, &str)> {
        match self {
            Self::Table {
                source_table_name,
                shard_id,
                ..
            } => Some((source_table_name.as_str(), shard_id.as_str())),
            Self::System { .. } => None,
        }
    }

    #[must_use]
    pub fn table_previous_position(&self) -> Option<&str> {
        match self {
            Self::Table {
                previous_position, ..
            } => previous_position.as_deref(),
            Self::System { .. } => None,
        }
    }

    #[must_use]
    pub fn system_parts(&self) -> Option<(&SystemStreamScope, &SystemStreamCheckpointToken)> {
        match self {
            Self::System {
                source_scope,
                checkpoint,
                ..
            } => Some((source_scope, checkpoint)),
            Self::Table { .. } => None,
        }
    }

    #[must_use]
    pub fn is_system(&self) -> bool {
        matches!(self, Self::System { .. })
    }

    pub(crate) fn system_previous_checkpoint(&self) -> Option<&SystemStreamCheckpointToken> {
        match self {
            Self::System {
                previous_checkpoint,
                ..
            } => previous_checkpoint.as_ref(),
            Self::Table { .. } => None,
        }
    }

    /// Maps one validated logical SystemStream position to the destination's
    /// per-row source-position contract. The lane identity includes the
    /// versioned source scope, shard, and lineage so positions from distinct
    /// logical lanes remain incomparable; only the provider-issued offset is
    /// compared within one lane.
    pub(crate) fn record_source_position(
        &self,
        position: Option<&SystemStreamPosition>,
    ) -> AnalyticsEngineResult<Option<SourcePosition>> {
        match self {
            Self::Table { .. } => {
                if position.is_some() {
                    return Err(AnalyticsEngineError::MixedSourceEffectScope);
                }
                Ok(None)
            }
            Self::System { source_scope, .. } => {
                let position = position.ok_or(AnalyticsEngineError::MixedSourceEffectScope)?;
                let source_position = SourcePosition::SystemStream {
                    source_scope: source_scope.clone(),
                    position: position.clone(),
                };
                source_position.validate()?;
                Ok(Some(source_position))
            }
        }
    }
}

/// Inputs for constructing a validated fenced source batch.
pub struct SourceEffectRequestInput<'a> {
    pub manifest: &'a AnalyticsManifest,
    pub manifest_digest: [u8; MANIFEST_DIGEST_BYTES],
    pub authority: SourceEffectAuthority,
    pub scope: SourceEffectScope,
    pub records: Vec<SourceBatchRecord>,
    pub checkpoints: Vec<RecordCheckpoint>,
    pub privacy_policy: Option<&'a PrivacyPolicy>,
}

/// A validated source batch owned by one source scope and destination epoch.
pub struct SourceEffectRequest<'a> {
    pub(crate) manifest: &'a AnalyticsManifest,
    pub(crate) manifest_digest: [u8; MANIFEST_DIGEST_BYTES],
    pub(crate) authority: SourceEffectAuthority,
    pub(crate) scope: SourceEffectScope,
    pub(crate) records: Vec<SourceBatchRecord>,
    pub(crate) checkpoints: Vec<RecordCheckpoint>,
    pub(crate) privacy_policy: Option<&'a PrivacyPolicy>,
}

pub(crate) struct SourceEffectRequestParts<'a> {
    pub(crate) manifest: &'a AnalyticsManifest,
    pub(crate) manifest_digest: [u8; MANIFEST_DIGEST_BYTES],
    pub(crate) authority: SourceEffectAuthority,
    pub(crate) scope: SourceEffectScope,
    pub(crate) records: Vec<SourceBatchRecord>,
    pub(crate) checkpoints: Vec<RecordCheckpoint>,
    pub(crate) privacy_policy: Option<&'a PrivacyPolicy>,
}

impl<'a> SourceEffectRequest<'a> {
    pub fn try_new(input: SourceEffectRequestInput<'a>) -> AnalyticsEngineResult<Self> {
        input.manifest.validate()?;
        if input.manifest_digest != manifest_digest(input.manifest) {
            return Err(AnalyticsEngineError::InvalidSourceEffectManifestDigest);
        }
        if let Some(policy) = input.privacy_policy {
            policy.validate()?;
        }
        validate_scope_contents(&input.scope, &input.records, &input.checkpoints)?;
        validate_record_registrations(input.manifest, &input.records)?;
        Ok(Self {
            manifest: input.manifest,
            manifest_digest: input.manifest_digest,
            authority: input.authority,
            scope: input.scope,
            records: input.records,
            checkpoints: input.checkpoints,
            privacy_policy: input.privacy_policy,
        })
    }

    pub(crate) fn into_parts(self) -> SourceEffectRequestParts<'a> {
        SourceEffectRequestParts {
            manifest: self.manifest,
            manifest_digest: self.manifest_digest,
            authority: self.authority,
            scope: self.scope,
            records: self.records,
            checkpoints: self.checkpoints,
            privacy_policy: self.privacy_policy,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceEffectSummary {
    pub(super) inserted: u64,
    pub(super) updated: u64,
    pub(super) deleted: u64,
    pub(super) skipped: u64,
    pub(super) dropped_fields: u64,
    pub(super) checkpoint_count: u64,
}

impl SourceEffectSummary {
    pub(crate) fn from_ingest(
        outcome: &crate::engine::SourceBatchIngestOutcome,
        checkpoint_count: usize,
    ) -> Self {
        let mut summary = Self {
            inserted: 0,
            updated: 0,
            deleted: 0,
            skipped: 0,
            dropped_fields: outcome.dropped_fields,
            checkpoint_count: u64::try_from(checkpoint_count).unwrap_or(u64::MAX),
        };
        for outcome in &outcome.outcomes {
            match outcome {
                IngestOutcome::Inserted => summary.inserted = summary.inserted.saturating_add(1),
                IngestOutcome::Updated => summary.updated = summary.updated.saturating_add(1),
                IngestOutcome::Deleted => summary.deleted = summary.deleted.saturating_add(1),
                IngestOutcome::Skipped => summary.skipped = summary.skipped.saturating_add(1),
            }
        }
        summary
    }

    #[must_use]
    pub const fn inserted(&self) -> u64 {
        self.inserted
    }

    #[must_use]
    pub const fn updated(&self) -> u64 {
        self.updated
    }

    #[must_use]
    pub const fn deleted(&self) -> u64 {
        self.deleted
    }

    #[must_use]
    pub const fn skipped(&self) -> u64 {
        self.skipped
    }

    #[must_use]
    pub const fn dropped_fields(&self) -> u64 {
        self.dropped_fields
    }

    #[must_use]
    pub const fn checkpoint_count(&self) -> u64 {
        self.checkpoint_count
    }
}

/// Result of a fenced destination source transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceEffectDisposition {
    Applied { summary: SourceEffectSummary },
    AlreadyApplied { summary: SourceEffectSummary },
    StaleFence,
}
