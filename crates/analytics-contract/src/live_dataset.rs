use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// A live view of one application-owned source, within its tenant namespace.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetRegistration {
    #[schema(min_length = 1, max_length = 64, pattern = "^[A-Za-z0-9_-]+$")]
    pub dataset_id: String,
    #[schema(min_length = 1, max_length = 64, pattern = "^[A-Za-z0-9_-]+$")]
    pub application_id: String,
    #[schema(min_items = 1, max_items = 32)]
    pub allowed_fields: Vec<String>,
    /// Zero creates a registration. Updates require the current version.
    pub expected_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AnalyticsDataset {
    pub tenant: String,
    #[schema(min_length = 1, max_length = 64, pattern = "^[A-Za-z0-9_-]+$")]
    pub dataset_id: String,
    #[schema(min_length = 1, max_length = 64, pattern = "^[A-Za-z0-9_-]+$")]
    pub application_id: String,
    #[schema(min_items = 1, max_items = 32)]
    pub allowed_fields: Vec<String>,
    pub source_table: String,
    pub source_index: String,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DatasetQueryMode {
    Count,
    Rows { fields: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetQuery {
    pub expected_dataset_version: u64,
    #[schema(min_length = 1, max_length = 64, pattern = "^[A-Za-z0-9_-]+$")]
    pub status: String,
    pub from_ms: u64,
    pub through_ms: u64,
    pub mode: DatasetQueryMode,
    #[schema(minimum = 1, maximum = 100)]
    pub page_size: u16,
    #[schema(nullable = true, max_length = 16384)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatasetQueryResponse {
    #[schema(min_length = 1, max_length = 64, pattern = "^[A-Za-z0-9_-]+$")]
    pub dataset_id: String,
    pub dataset_version: u64,
    pub source_watermark: String,
    pub query_fingerprint: String,
    pub from_ms: u64,
    pub through_ms: u64,
    pub count: Option<u64>,
    pub rows: Vec<serde_json::Value>,
    pub next_cursor: Option<String>,
    pub examined_rows: u32,
    pub source_pages: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetRevision {
    #[schema(min_length = 1, max_length = 64, pattern = "^[A-Za-z0-9_-]+$")]
    pub dataset_id: String,
    pub version: u64,
}
