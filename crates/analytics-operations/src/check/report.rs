use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckOutcome {
    Complete,
    Incomplete,
    Mismatched,
    Stale,
    PrivacyViolation,
}

impl CheckOutcome {
    pub(crate) fn completion_state(self) -> (OperationStatus, OperationEventKind) {
        match self {
            Self::Complete => (OperationStatus::Succeeded, OperationEventKind::Succeeded),
            Self::Incomplete | Self::Mismatched | Self::Stale | Self::PrivacyViolation => {
                (OperationStatus::Failed, OperationEventKind::Failed)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckReport {
    pub operation_id: OperationId,
    pub outcome: CheckOutcome,
    pub strategy: CheckComparisonStrategy,
    pub metrics: CheckMetrics,
    pub samples: CheckSamples,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckMetrics {
    pub source_rows: u64,
    pub destination_rows: u64,
    pub checked_rows: u64,
    pub missing_keys: u64,
    pub extra_keys: u64,
    pub mismatched_rows: u64,
    pub stale_rows: u64,
    pub privacy_violations: u64,
    pub deterministic_chunk_hash_mismatches: u64,
    pub unsupported_comparisons: u64,
    pub check_duration_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckSamples {
    pub missing_keys: Vec<String>,
    pub extra_keys: Vec<String>,
    pub mismatched_keys: Vec<String>,
    pub stale_keys: Vec<String>,
    pub privacy_violation_keys: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalCheckExecutor;
