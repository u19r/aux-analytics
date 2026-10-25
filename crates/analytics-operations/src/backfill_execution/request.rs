use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{BackfillExecutionError, BackfillExecutionResult};
use crate::{OperationActor, OperationId, RateLimitPolicy};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct BackfillExecutionRequest {
    pub operation_id: OperationId,
    pub actor: OperationActor,
    pub target_tables: Vec<String>,
    pub rate_limit: RateLimitPolicy,
    pub fail_once_at_chunk: Option<u64>,
}

impl BackfillExecutionRequest {
    pub fn validate(&self) -> BackfillExecutionResult<()> {
        if self.target_tables.is_empty() {
            return Err(BackfillExecutionError::EmptyTargetTables);
        }
        if self.rate_limit.max_parallel_chunks == 0 {
            return Err(BackfillExecutionError::InvalidParallelism);
        }
        Ok(())
    }
}
