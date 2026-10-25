use serde::{Deserialize, Serialize};

use super::{OperationActor, OperationId, OperationKind, OperationStatus, RateLimitPolicy};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRequest {
    pub operation_id: OperationId,
    pub kind: OperationKind,
    pub actor: OperationActor,
    pub target_tables: Vec<String>,
    pub dry_run: bool,
    pub rate_limit: RateLimitPolicy,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationResult {
    pub status: OperationStatus,
    pub rows_scanned: u64,
    pub rows_written: u64,
    pub rows_deleted: u64,
    pub error_class: Option<String>,
    pub summary: serde_json::Value,
}
