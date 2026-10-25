use super::*;

#[derive(Clone, Copy)]
pub(super) enum BackfillBatch<'a> {
    Snapshot(&'a LocalBackfillChunk),
    Stream(&'a LocalStreamUpdate),
}

impl BackfillBatch<'_> {
    pub(super) fn is_completed(self, state: &BackfillExecutionState) -> bool {
        match self {
            Self::Snapshot(chunk) => state.completed_chunks.contains(&chunk.chunk_id),
            Self::Stream(update) => update.cursor <= state.stream_checkpoint,
        }
    }

    pub(super) fn accounting(self) -> BackfillExecutionResult<BackfillBatchAccounting> {
        match self {
            Self::Snapshot(chunk) => chunk.accounting(),
            Self::Stream(update) => update.accounting(),
        }
    }

    pub(super) fn apply_to_state(self, state: &mut BackfillExecutionState, rows: u64) {
        match self {
            Self::Snapshot(chunk) => state.mark_chunk_completed(chunk, rows),
            Self::Stream(update) => state.apply_stream_update(update, rows),
        }
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Snapshot(_) => "chunk",
            Self::Stream(_) => "stream",
        }
    }

    pub(super) fn position(self) -> u64 {
        match self {
            Self::Snapshot(chunk) => chunk.chunk_id,
            Self::Stream(update) => update.cursor,
        }
    }

    pub(super) fn encoding_failure_message(self) -> &'static str {
        match self {
            Self::Snapshot(_) => "backfill snapshot batch encoding failed",
            Self::Stream(_) => "backfill stream batch encoding failed",
        }
    }

    pub(super) fn before_write_message(self) -> &'static str {
        match self {
            Self::Snapshot(_) => "backfill chunk lost its execution fence before destination write",
            Self::Stream(_) => "backfill stream lost its execution fence before destination write",
        }
    }

    pub(super) fn after_write_message(self) -> &'static str {
        match self {
            Self::Snapshot(_) => "backfill chunk lost its execution fence after destination write",
            Self::Stream(_) => "backfill stream lost its execution fence after destination write",
        }
    }

    pub(super) fn commit_message(self) -> &'static str {
        match self {
            Self::Snapshot(_) => "backfill chunk committed",
            Self::Stream(_) => "backfill stream update committed",
        }
    }

    pub(super) fn commit_failure_message(self) -> &'static str {
        match self {
            Self::Snapshot(_) => "backfill chunk commit lost its execution fence",
            Self::Stream(_) => "backfill stream commit lost its execution fence",
        }
    }
}
