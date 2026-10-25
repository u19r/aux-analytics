use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckRequest {
    pub operation_id: OperationId,
    pub actor: OperationActor,
    pub target_tables: Vec<String>,
    pub strategy: CheckComparisonStrategy,
    pub sample_limit: usize,
    pub privacy_policy_version: Option<String>,
    pub output: CheckOutputFormat,
    pub rate_limit: RateLimitPolicy,
}

impl CheckRequest {
    pub fn validate(&self) -> CheckResult<()> {
        if self.target_tables.is_empty() {
            return Err(CheckError::EmptyTargetTables);
        }
        if self.sample_limit == 0 {
            return Err(CheckError::InvalidSampleLimit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckComparisonStrategy {
    CountsOnly,
    KeySet,
    ChunkHashes,
    FullRows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckOutputFormat {
    Json,
    Text,
}
