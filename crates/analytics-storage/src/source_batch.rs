use analytics_contract::StorageStreamRecord;
use storage_types::{SystemStreamCheckpointToken, SystemStreamPosition, SystemStreamScope};

use crate::{
    AnalyticsStorageResult,
    error::{AnalyticsStorageError, AnalyticsStorageErrorKind},
};

#[derive(Debug, Clone)]
pub struct PolledRecord {
    pub source_table_name: String,
    /// The source stream shard or partition that produced this record.
    pub shard_id: String,
    /// Ordered destinations sharing this one owned source record.
    pub analytics_table_names: Vec<String>,
    pub record_key: String,
    pub record: StorageStreamRecord,
    /// Logical SystemStream position. Ordinary table streams leave this
    /// absent; SystemStream records always carry the validated tuple issued by
    /// aux-storage.
    pub system_position: Option<SystemStreamPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCheckpoint {
    pub source_table_name: String,
    pub shard_id: String,
    pub position: String,
}

impl SourceCheckpoint {
    #[must_use]
    pub fn is_iterator(&self) -> bool {
        self.shard_id.starts_with("__iterator:")
    }

    #[must_use]
    pub fn is_record(&self) -> bool {
        !self.is_iterator()
    }
}

/// The single source scope covered by a poll batch.
///
/// The storage poller derives this identity from every record and checkpoint
/// instead of storing a second, independently mutable scope field. Iterator
/// checkpoints use an internal prefix and are normalized back to their shard
/// before validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceBatchScope {
    source_table_name: String,
    shard_id: String,
}

impl SourceBatchScope {
    #[must_use]
    pub fn source_table_name(&self) -> &str {
        self.source_table_name.as_str()
    }

    #[must_use]
    pub fn shard_id(&self) -> &str {
        self.shard_id.as_str()
    }
}

#[derive(Debug, Clone)]
pub struct PollBatch {
    pub source_response_count: usize,
    pub source_nonempty_response_count: usize,
    pub source_record_count: usize,
    pub source_encoded_bytes: usize,
    pub records: Vec<PolledRecord>,
    pub checkpoints: Vec<SourceCheckpoint>,
    /// The exact durable record checkpoint observed before this page was
    /// fetched. A missing value is the durable origin for this scope. It is
    /// intentionally opaque: consumers may compare it for equality, but may
    /// not infer ordering from its representation.
    pub source_predecessor: Option<SourceCheckpoint>,
    /// Scope and opaque continuation returned by a SystemStream page. This is
    /// distinct from table-stream checkpoints and is committed only after the
    /// destination effect transaction succeeds.
    pub system_stream: Option<SystemStreamBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamBatch {
    pub source_scope: SystemStreamScope,
    /// Durable effect frontier preceding the selected records. Unrouted pages
    /// may have advanced the transient scan cursor beyond this frontier.
    /// `None` means no selected effect has committed yet.
    pub previous_checkpoint: Option<SystemStreamCheckpointToken>,
    pub checkpoint: SystemStreamCheckpointToken,
}

/// The durable SystemStream frontier used to resume a source poller.
///
/// This is intentionally distinct from [`SystemStreamBatch`], which describes
/// selected records and their durable effect predecessor. A restart only
/// supplies the persisted scope and frontier; transient scan progress replays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamResume {
    source_scope: SystemStreamScope,
    checkpoint: SystemStreamCheckpointToken,
}

impl SystemStreamResume {
    #[must_use]
    pub fn new(source_scope: SystemStreamScope, checkpoint: SystemStreamCheckpointToken) -> Self {
        Self {
            source_scope,
            checkpoint,
        }
    }

    #[must_use]
    pub fn source_scope(&self) -> &SystemStreamScope {
        &self.source_scope
    }

    #[must_use]
    pub fn checkpoint(&self) -> &SystemStreamCheckpointToken {
        &self.checkpoint
    }

    #[must_use]
    pub fn into_parts(self) -> (SystemStreamScope, SystemStreamCheckpointToken) {
        (self.source_scope, self.checkpoint)
    }
}

impl PollBatch {
    /// Returns the one source table/shard identity represented by this batch.
    ///
    /// Empty batches have no scope. Every non-empty record and checkpoint must
    /// agree on one scope; iterator checkpoint prefixes are deliberately
    /// normalized so they match the records from the same shard.
    pub fn source_scope(&self) -> AnalyticsStorageResult<Option<SourceBatchScope>> {
        let mut scope = None;
        for record in &self.records {
            merge_source_scope(&mut scope, &record.source_table_name, &record.shard_id)?;
        }
        for checkpoint in &self.checkpoints {
            let shard_id = checkpoint
                .shard_id
                .strip_prefix("__iterator:")
                .unwrap_or(checkpoint.shard_id.as_str());
            merge_source_scope(&mut scope, &checkpoint.source_table_name, shard_id)?;
        }
        Ok(scope)
    }
}

fn merge_source_scope(
    scope: &mut Option<SourceBatchScope>,
    source_table_name: &str,
    shard_id: &str,
) -> AnalyticsStorageResult<()> {
    if !valid_scope_component(source_table_name) || !valid_scope_component(shard_id) {
        return Err(AnalyticsStorageError::new(
            AnalyticsStorageErrorKind::InvalidSourceBatchScope,
        ));
    }
    if let Some(existing) = scope {
        if existing.source_table_name != source_table_name || existing.shard_id != shard_id {
            return Err(AnalyticsStorageError::new(
                AnalyticsStorageErrorKind::MixedSourceBatchScope,
            ));
        }
    } else {
        *scope = Some(SourceBatchScope {
            source_table_name: source_table_name.to_string(),
            shard_id: shard_id.to_string(),
        });
    }
    Ok(())
}

fn valid_scope_component(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotChunkCheckpoint {
    pub source_table_name: String,
    pub chunk_id: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub checkpoint: SnapshotChunkCheckpoint,
    pub records: Vec<PolledRecord>,
}
