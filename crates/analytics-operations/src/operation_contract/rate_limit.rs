use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(default, deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
#[schema(example = json!({
    "max_rows_per_second": 100,
    "max_bytes_per_second": 1000000,
    "max_source_requests_per_second": 5,
    "max_destination_writes_per_second": 5,
    "max_parallel_chunks": 1,
    "batch_size": 100,
    "pause_ms": 100,
    "retry_backoff_ms": 1000,
    "maintenance_windows_utc": []
}))]
pub struct RateLimitPolicy {
    /// Maximum rows processed per second. The default is 100 rows/s.
    #[schema(default = 100, minimum = 0)]
    pub max_rows_per_second: Option<u64>,
    /// Maximum bytes processed per second. The default is 1,000,000 bytes/s.
    #[schema(default = 1000000, minimum = 0)]
    pub max_bytes_per_second: Option<u64>,
    /// Maximum source requests per second. The default is 5 requests/s.
    #[schema(default = 5, minimum = 0)]
    pub max_source_requests_per_second: Option<u64>,
    /// Maximum destination writes per second. The default is 5 writes/s.
    #[schema(default = 5, minimum = 0)]
    pub max_destination_writes_per_second: Option<u64>,
    /// Maximum concurrent operation chunks. The default is 1 chunk.
    #[schema(default = 1, minimum = 1)]
    pub max_parallel_chunks: u16,
    /// Maximum records in one operation batch. The default is 100 records.
    #[schema(default = 100, minimum = 1)]
    pub batch_size: u64,
    /// Delay between throttled batches in milliseconds. The default is 100 ms.
    #[schema(default = 100, minimum = 0)]
    pub pause_ms: u64,
    /// Linear retry backoff in milliseconds. The default is 1,000 ms.
    #[schema(default = 1000, minimum = 0)]
    pub retry_backoff_ms: u64,
    /// UTC maintenance windows during which operation work may run.
    #[serde(default)]
    #[schema(default = json!([]))]
    pub maintenance_windows_utc: Vec<MaintenanceWindow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct MaintenanceWindow {
    /// Window start in minutes after 00:00 UTC.
    #[schema(minimum = 0, maximum = 1439, example = 60)]
    pub start_minute_utc: u16,
    /// Window end in minutes after 00:00 UTC.
    #[schema(minimum = 0, maximum = 1440, example = 120)]
    pub end_minute_utc: u16,
}

impl Default for RateLimitPolicy {
    fn default() -> Self {
        Self {
            max_rows_per_second: Some(100),
            max_bytes_per_second: Some(1_000_000),
            max_source_requests_per_second: Some(5),
            max_destination_writes_per_second: Some(5),
            max_parallel_chunks: 1,
            batch_size: 100,
            pause_ms: 100,
            retry_backoff_ms: 1_000,
            maintenance_windows_utc: Vec::new(),
        }
    }
}
