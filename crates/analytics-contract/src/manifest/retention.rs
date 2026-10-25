use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Static retention policy for one analytical table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({
    "period_ms": 7_776_000_000_u64,
    "timestamp": {"kind": "attribute", "attribute_path": "created_at_ms"}
}))]
pub struct RetentionPolicy {
    /// Retention duration in milliseconds.
    #[schema(minimum = 1, example = 7_776_000_000_u64)]
    pub period_ms: u64,
    /// Timestamp used as the start time for retention.
    pub timestamp: RetentionTimestamp,
}

/// Timestamp basis used to compute row expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetentionTimestamp {
    /// Read a unix epoch millisecond value from a source item attribute path.
    Attribute {
        /// Dot-separated source attribute path.
        #[schema(min_length = 1, max_length = 1024, example = "created_at_ms")]
        attribute_path: String,
    },
    /// Use the aux-analytics ingestion time.
    IngestedAt,
}
