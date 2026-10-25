//! In-memory source batches and local fixture generation.

use analytics_contract::StorageStreamRecord;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage_types::VecMap;
use utoipa::ToSchema;

use super::{
    BackfillExecutionResult, SnapshotChunkSource, StreamCatchupSource,
    accounting::{BackfillBatchAccounting, batch_accounting},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct LocalBackfillFixture {
    pub chunks: Vec<LocalBackfillChunk>,
    pub stream_updates: Vec<LocalStreamUpdate>,
}

impl LocalBackfillFixture {
    #[must_use]
    pub fn single_table(chunk_rows: &[u64], stream_updates: &[u64]) -> Self {
        Self {
            chunks: chunk_rows
                .iter()
                .enumerate()
                .map(|(index, rows)| {
                    LocalBackfillChunk::from_row_count(
                        u64::try_from(index + 1).unwrap_or(u64::MAX),
                        *rows,
                    )
                })
                .collect(),
            stream_updates: stream_updates
                .iter()
                .copied()
                .map(|cursor| LocalStreamUpdate::from_row_count(cursor, 1))
                .collect(),
        }
    }
}

impl SnapshotChunkSource for LocalBackfillFixture {
    fn snapshot_chunks(&self) -> BackfillExecutionResult<Vec<LocalBackfillChunk>> {
        Ok(self.chunks.clone())
    }
}

impl StreamCatchupSource for LocalBackfillFixture {
    fn stream_updates_after_snapshot(&self) -> BackfillExecutionResult<Vec<LocalStreamUpdate>> {
        Ok(self.stream_updates.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct LocalBackfillChunk {
    pub chunk_id: u64,
    pub records: Vec<StorageStreamRecord>,
}

impl LocalBackfillChunk {
    #[must_use]
    pub fn new(chunk_id: u64, records: Vec<StorageStreamRecord>) -> Self {
        Self { chunk_id, records }
    }

    #[must_use]
    pub fn from_row_count(chunk_id: u64, row_count: u64) -> Self {
        Self::new(
            chunk_id,
            (0..row_count)
                .map(|row| fixture_record(format!("snapshot-{chunk_id}-{row}")))
                .collect(),
        )
    }

    #[must_use]
    pub fn row_count(&self) -> u64 {
        u64::try_from(self.records.len()).unwrap_or(u64::MAX)
    }

    pub(super) fn accounting(&self) -> BackfillExecutionResult<BackfillBatchAccounting> {
        batch_accounting(&self.records)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct LocalStreamUpdate {
    pub cursor: u64,
    pub records: Vec<StorageStreamRecord>,
}

impl LocalStreamUpdate {
    #[must_use]
    pub fn new(cursor: u64, records: Vec<StorageStreamRecord>) -> Self {
        Self { cursor, records }
    }

    #[must_use]
    pub fn from_row_count(cursor: u64, row_count: u64) -> Self {
        Self::new(
            cursor,
            (0..row_count)
                .map(|row| fixture_record(format!("stream-{cursor}-{row}")))
                .collect(),
        )
    }

    #[must_use]
    pub fn row_count(&self) -> u64 {
        u64::try_from(self.records.len()).unwrap_or(u64::MAX)
    }

    pub(super) fn accounting(&self) -> BackfillExecutionResult<BackfillBatchAccounting> {
        batch_accounting(&self.records)
    }
}

fn fixture_record(sequence_number: String) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: VecMap::new(),
        sequence_number,
        old_image: None,
        new_image: None,
    }
}
