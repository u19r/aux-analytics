//! Snapshot and catch-up boundaries used by the executor.

use super::{BackfillExecutionResult, LocalBackfillChunk, LocalStreamUpdate};

pub trait SnapshotChunkSource {
    fn snapshot_chunks(&self) -> BackfillExecutionResult<Vec<LocalBackfillChunk>>;
}

pub trait StreamCatchupSource {
    fn stream_updates_after_snapshot(&self) -> BackfillExecutionResult<Vec<LocalStreamUpdate>>;
}

pub trait BackfillDestinationWriter {
    fn write_snapshot_chunk(
        &mut self,
        key: &crate::OperationChunkKey,
        chunk: &LocalBackfillChunk,
    ) -> BackfillExecutionResult<()>;

    fn write_stream_update(
        &mut self,
        key: &crate::OperationChunkKey,
        update: &LocalStreamUpdate,
    ) -> BackfillExecutionResult<()>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CountingBackfillDestinationWriter;

impl BackfillDestinationWriter for CountingBackfillDestinationWriter {
    fn write_snapshot_chunk(
        &mut self,
        _key: &crate::OperationChunkKey,
        _chunk: &LocalBackfillChunk,
    ) -> BackfillExecutionResult<()> {
        Ok(())
    }

    fn write_stream_update(
        &mut self,
        _key: &crate::OperationChunkKey,
        _update: &LocalStreamUpdate,
    ) -> BackfillExecutionResult<()> {
        Ok(())
    }
}
