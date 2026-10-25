use serde::{Deserialize, Serialize};

use super::{TrimError, TrimRequest, TrimResult, TrimTarget};
use crate::OperationId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrimReport {
    pub operation_id: OperationId,
    pub table_name: String,
    pub dry_run: bool,
    pub target: TrimTarget,
    pub candidate_rows: u64,
    pub rows_deleted: u64,
    pub destination_mutated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrimCandidateSnapshot {
    pub revision: String,
    pub candidate_keys: Vec<String>,
}

impl TrimCandidateSnapshot {
    #[must_use]
    pub fn candidate_rows(&self) -> u64 {
        u64::try_from(self.candidate_keys.len()).unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrimDeletionOutcome {
    pub candidate_rows: u64,
    pub rows_deleted: u64,
}

impl TrimReport {
    pub(super) fn from_dry_run(request: &TrimRequest, candidate_rows: u64) -> Self {
        Self::from_counts(request, candidate_rows, 0)
    }

    pub(super) fn try_from_deletion(
        request: &TrimRequest,
        outcome: TrimDeletionOutcome,
    ) -> TrimResult<Self> {
        if outcome.rows_deleted > outcome.candidate_rows {
            return Err(TrimError::DeleteCountExceededDryRun {
                candidate_rows: outcome.candidate_rows,
                deleted_rows: 0,
            });
        }
        Ok(Self::from_counts(
            request,
            outcome.candidate_rows,
            outcome.rows_deleted,
        ))
    }

    fn from_counts(request: &TrimRequest, candidate_rows: u64, rows_deleted: u64) -> Self {
        Self {
            operation_id: request.operation_id.clone(),
            table_name: request.table_name.clone(),
            dry_run: request.dry_run,
            target: request.target.clone(),
            candidate_rows,
            rows_deleted,
            destination_mutated: rows_deleted > 0,
        }
    }

    #[must_use]
    pub fn operation_counts(&self) -> serde_json::Value {
        serde_json::json!({
            "table_name": self.table_name,
            "dry_run": self.dry_run,
            "candidate_rows": self.candidate_rows,
            "rows_deleted": self.rows_deleted,
            "destination_mutated": self.destination_mutated,
        })
    }
}
