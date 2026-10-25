use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::catalog::RemoteCredentialsConfig;
use crate::constants::{
    DEFAULT_INGEST_PROCESSOR_ENABLED, DEFAULT_POLL_INTERVAL_MS,
    DEFAULT_POLL_MAX_RECORDS_PER_RESPONSE, DEFAULT_POLL_MAX_RESPONSES_PER_INTERVAL,
    DEFAULT_POLL_MAX_SHARDS, DEFAULT_POLL_REQUEST_TIMEOUT_MS,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsSourceConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub stream_type: Option<AnalyticsStreamType>,
    /// Stream transport endpoint override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub credentials: Option<RemoteCredentialsConfig>,
    #[serde(default = "default_poll_interval_ms")]
    #[schemars(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_poll_request_timeout_ms")]
    #[schemars(default = "default_poll_request_timeout_ms")]
    pub poll_request_timeout_ms: u64,
    #[serde(default = "default_poll_max_shards")]
    #[schemars(default = "default_poll_max_shards")]
    pub poll_max_shards: usize,
    #[serde(default = "default_poll_max_responses_per_interval")]
    #[schemars(default = "default_poll_max_responses_per_interval")]
    pub poll_max_responses_per_interval: usize,
    #[serde(default = "default_poll_max_records_per_response")]
    #[schemars(default = "default_poll_max_records_per_response")]
    pub poll_max_records_per_response: u32,
    #[serde(default)]
    #[schemars(default)]
    pub tables: Vec<AnalyticsSourceTableConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsIngestConfig {
    #[serde(default = "default_ingest_processor_enabled")]
    #[schemars(default = "default_ingest_processor_enabled")]
    pub processor_enabled: bool,
}

impl Default for AnalyticsIngestConfig {
    fn default() -> Self {
        Self {
            processor_enabled: default_ingest_processor_enabled(),
        }
    }
}

fn default_ingest_processor_enabled() -> bool {
    DEFAULT_INGEST_PROCESSOR_ENABLED
}

impl Default for AnalyticsSourceConfig {
    fn default() -> Self {
        Self {
            stream_type: None,
            endpoint_url: None,
            region: None,
            credentials: None,
            poll_interval_ms: default_poll_interval_ms(),
            poll_request_timeout_ms: default_poll_request_timeout_ms(),
            poll_max_shards: default_poll_max_shards(),
            poll_max_responses_per_interval: default_poll_max_responses_per_interval(),
            poll_max_records_per_response: default_poll_max_records_per_response(),
            tables: Vec::new(),
        }
    }
}

fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}
fn default_poll_request_timeout_ms() -> u64 {
    DEFAULT_POLL_REQUEST_TIMEOUT_MS
}
fn default_poll_max_shards() -> usize {
    DEFAULT_POLL_MAX_SHARDS
}
fn default_poll_max_responses_per_interval() -> usize {
    DEFAULT_POLL_MAX_RESPONSES_PER_INTERVAL
}
fn default_poll_max_records_per_response() -> u32 {
    DEFAULT_POLL_MAX_RECORDS_PER_RESPONSE
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsStreamType {
    StorageStream,
    AuxStorage,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsSourceTableConfig {
    pub table_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub stream_type: Option<AnalyticsStreamType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub stream_identifier: Option<String>,
}
