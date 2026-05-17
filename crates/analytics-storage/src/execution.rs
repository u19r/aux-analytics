use std::future::Future;

use crate::{
    error::AnalyticsStorageResult,
    types::{PollBatch, SnapshotChunk, SnapshotChunkCheckpoint, SourceCheckpoint},
};

pub trait SnapshotChunkSource {
    fn next_snapshot_chunk(
        &mut self,
    ) -> impl Future<Output = AnalyticsStorageResult<Option<SnapshotChunk>>> + Send;

    fn commit_snapshot_chunk(&mut self, checkpoint: &SnapshotChunkCheckpoint);
}

pub trait StreamCatchupSource {
    fn poll_stream_catchup(
        &mut self,
    ) -> impl Future<Output = AnalyticsStorageResult<PollBatch>> + Send;

    fn commit_stream_catchup(&mut self, checkpoints: &[SourceCheckpoint]);
}

#[derive(Debug)]
pub struct BackfillExecutionInputs<TSnapshot, TStream> {
    pub snapshot_chunks: TSnapshot,
    pub stream_catchup: TStream,
}

impl<TSnapshot, TStream> BackfillExecutionInputs<TSnapshot, TStream> {
    #[must_use]
    pub fn new(snapshot_chunks: TSnapshot, stream_catchup: TStream) -> Self {
        Self {
            snapshot_chunks,
            stream_catchup,
        }
    }
}
