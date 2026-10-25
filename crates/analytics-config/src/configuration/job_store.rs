use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::constants::DEFAULT_JOB_STORE_REQUEST_TIMEOUT_MS;

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsOperationsConfig {
    /// DuckDB path used for durable maintenance operation state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub operation_store_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsJobStoreConfig {
    /// Unauthenticated HTTP-only endpoint for the scheduled JobStore.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub endpoint_url: Option<String>,
    #[serde(default = "default_job_store_request_timeout_ms")]
    #[schemars(default = "default_job_store_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

impl Default for AnalyticsJobStoreConfig {
    fn default() -> Self {
        Self {
            endpoint_url: None,
            request_timeout_ms: default_job_store_request_timeout_ms(),
        }
    }
}

fn default_job_store_request_timeout_ms() -> u64 {
    DEFAULT_JOB_STORE_REQUEST_TIMEOUT_MS
}
