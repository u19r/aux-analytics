//! Candidate membership and atomic destination deletion boundaries.

use super::{TrimCandidateSnapshot, TrimDeletionOutcome, TrimResult, TrimTarget};
use crate::{MaintenanceExecutionAuthority, OperationChunkKey};

pub trait TrimCandidateSource {
    fn candidate_snapshot(
        &self,
        table_name: &str,
        target: &TrimTarget,
    ) -> TrimResult<TrimCandidateSnapshot>;

    fn count_candidates(&self, table_name: &str, target: &TrimTarget) -> TrimResult<u64> {
        Ok(self
            .candidate_snapshot(table_name, target)?
            .candidate_rows())
    }
}

pub trait TrimDestination {
    /// Commit an idempotent deletion only when the destination still has
    /// exactly the planned candidate membership and revision.
    /// Implementations must compare and delete in one destination
    /// transaction; the stable key makes an uncertain retry return the prior
    /// result.
    fn delete_candidate_snapshot_with_key(
        &mut self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        target: &TrimTarget,
        expected: &TrimCandidateSnapshot,
    ) -> TrimResult<TrimDeletionOutcome>;
}
