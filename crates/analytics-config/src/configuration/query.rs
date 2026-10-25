use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::constants::DEFAULT_QUERY_MAX_READ_CONNECTIONS;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsQueryConfig {
    #[serde(default = "default_query_max_read_connections")]
    #[schemars(default = "default_query_max_read_connections")]
    pub max_read_connections: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub max_response_size_kb: Option<usize>,
    #[serde(default)]
    #[schemars(default)]
    pub lambda_response_compression: AnalyticsLambdaResponseCompression,
    #[serde(default)]
    #[schemars(default)]
    pub lambda_query_only: bool,
    #[serde(default)]
    #[schemars(default)]
    /// Disable the Lambda tenant-query deadline; explicit cancellation remains
    /// active.
    pub disable_query_deadline: bool,
}

impl Default for AnalyticsQueryConfig {
    fn default() -> Self {
        Self {
            max_read_connections: default_query_max_read_connections(),
            max_response_size_kb: None,
            lambda_response_compression: AnalyticsLambdaResponseCompression::default(),
            lambda_query_only: false,
            disable_query_deadline: false,
        }
    }
}

fn default_query_max_read_connections() -> usize {
    DEFAULT_QUERY_MAX_READ_CONNECTIONS
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsLambdaResponseCompression {
    #[default]
    None,
    GzipBase64,
}
