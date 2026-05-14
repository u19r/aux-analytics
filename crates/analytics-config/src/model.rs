use std::collections::{BTreeMap, HashMap};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::constants::{
    DEFAULT_CONFIG_VERSION, DEFAULT_HTTP_BIND_ADDR, DEFAULT_LOG_LEVEL,
    DEFAULT_OBJECT_STORAGE_SCHEME, DEFAULT_POLL_INTERVAL_MS,
    DEFAULT_POLL_MAX_RESPONSES_PER_INTERVAL, DEFAULT_POLL_MAX_SHARDS,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RootConfig {
    #[serde(rename = "$schema", default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub description: Option<String>,
    #[serde(default = "default_version")]
    #[schemars(default = "default_version")]
    pub version: String,
    #[serde(default)]
    #[schemars(default)]
    pub http: HttpConfig,
    #[serde(default)]
    #[schemars(default)]
    pub features: Features,
    #[serde(default)]
    #[schemars(default)]
    pub analytics: AnalyticsConfig,
    #[serde(default)]
    #[schemars(default)]
    pub tracing: Tracing,
}

impl Default for RootConfig {
    fn default() -> Self {
        Self {
            schema: None,
            description: None,
            version: default_version(),
            http: HttpConfig::default(),
            features: Features::default(),
            analytics: AnalyticsConfig::default(),
            tracing: Tracing::default(),
        }
    }
}

fn default_version() -> String {
    DEFAULT_CONFIG_VERSION.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct HttpConfig {
    #[serde(default = "default_bind_addr")]
    #[schemars(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default)]
    #[schemars(default)]
    pub cors: Cors,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            cors: Cors::default(),
        }
    }
}

fn default_bind_addr() -> String {
    DEFAULT_HTTP_BIND_ADDR.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct Cors {
    #[serde(default)]
    #[schemars(default)]
    pub allow_origins: Vec<String>,
}

impl Default for Cors {
    fn default() -> Self {
        Self {
            allow_origins: vec!["*".to_string()],
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct Features {
    #[serde(default)]
    #[schemars(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    #[schemars(default)]
    pub prometheus: PrometheusMetricsConfig,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prometheus: PrometheusMetricsConfig::default(),
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PrometheusMetricsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub bearer_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct Tracing {
    #[serde(default = "default_log_level_option")]
    #[schemars(default = "default_log_level_option")]
    pub log_level: Option<String>,
}

impl Default for Tracing {
    fn default() -> Self {
        Self {
            log_level: default_log_level_option(),
        }
    }
}

#[allow(clippy::unnecessary_wraps)]
fn default_log_level_option() -> Option<String> {
    Some(DEFAULT_LOG_LEVEL.to_string())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub manifest_path: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub http: AnalyticsHttpConfig,
    #[serde(default)]
    #[schemars(default)]
    pub source: AnalyticsSourceConfig,
    #[serde(default)]
    #[schemars(default)]
    pub catalog: AnalyticsCatalogConfig,
    #[serde(default)]
    #[schemars(default)]
    pub object_storage: AnalyticsObjectStorageConfig,
    #[serde(default)]
    #[schemars(default)]
    pub retention: AnalyticsRetentionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsHttpConfig {
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub ingest_endpoint_enabled: bool,
}

impl Default for AnalyticsHttpConfig {
    fn default() -> Self {
        Self {
            ingest_endpoint_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsSourceConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub stream_type: Option<AnalyticsStreamType>,
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
    #[serde(default = "default_poll_max_shards")]
    #[schemars(default = "default_poll_max_shards")]
    pub poll_max_shards: usize,
    #[serde(default = "default_poll_max_responses_per_interval")]
    #[schemars(default = "default_poll_max_responses_per_interval")]
    pub poll_max_responses_per_interval: usize,
    #[serde(default)]
    #[schemars(default)]
    pub tables: Vec<AnalyticsSourceTableConfig>,
}

impl Default for AnalyticsSourceConfig {
    fn default() -> Self {
        Self {
            stream_type: None,
            endpoint_url: None,
            region: None,
            credentials: None,
            poll_interval_ms: default_poll_interval_ms(),
            poll_max_shards: default_poll_max_shards(),
            poll_max_responses_per_interval: default_poll_max_responses_per_interval(),
            tables: Vec::new(),
        }
    }
}

fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}
fn default_poll_max_shards() -> usize {
    DEFAULT_POLL_MAX_SHARDS
}
fn default_poll_max_responses_per_interval() -> usize {
    DEFAULT_POLL_MAX_RESPONSES_PER_INTERVAL
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsRetentionConfig {
    #[serde(default)]
    #[schemars(default)]
    pub enabled: bool,
    #[serde(default = "default_retention_sweep_interval_ms")]
    #[schemars(default = "default_retention_sweep_interval_ms")]
    pub sweep_interval_ms: u64,
    #[serde(default = "default_retention_delete_batch_size")]
    #[schemars(default = "default_retention_delete_batch_size")]
    pub delete_batch_size: u64,
    #[serde(default = "default_retention_delete_batch_pause_ms")]
    #[schemars(default = "default_retention_delete_batch_pause_ms")]
    pub delete_batch_pause_ms: u64,
    #[serde(default)]
    #[schemars(default)]
    pub tables: Vec<AnalyticsRetentionTableConfig>,
}

impl Default for AnalyticsRetentionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sweep_interval_ms: default_retention_sweep_interval_ms(),
            delete_batch_size: default_retention_delete_batch_size(),
            delete_batch_pause_ms: default_retention_delete_batch_pause_ms(),
            tables: Vec::new(),
        }
    }
}

fn default_retention_sweep_interval_ms() -> u64 {
    60_000
}

fn default_retention_delete_batch_size() -> u64 {
    500
}

fn default_retention_delete_batch_pause_ms() -> u64 {
    250
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsRetentionTableConfig {
    pub analytics_table_name: String,
    pub default_period_ms: u64,
    #[serde(default)]
    #[schemars(default)]
    pub strict: bool,
    pub timestamp: RetentionTimestampConfig,
    pub tenant_policy: TenantRetentionPolicyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetentionTimestampConfig {
    Attribute { attribute_path: String },
    IngestedAt,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct TenantRetentionPolicyConfig {
    pub source: TenantRetentionPolicySource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub credentials: Option<RemoteCredentialsConfig>,
    pub request: TenantRetentionPolicyRequest,
    pub duration_selector: RetentionDurationSelector,
    #[serde(default = "default_retention_cache_ttl_ms")]
    #[schemars(default = "default_retention_cache_ttl_ms")]
    pub cache_ttl_ms: u64,
}

fn default_retention_cache_ttl_ms() -> u64 {
    300_000
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TenantRetentionPolicySource {
    AuxStorage,
    #[serde(rename = "dynamodb")]
    DynamoDb,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(deny_unknown_fields)]
pub enum TenantRetentionPolicyRequest {
    GetItem(TenantRetentionGetItemRequest),
    QueryTable(TenantRetentionQueryTableRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct TenantRetentionGetItemRequest {
    pub table_name: String,
    pub key: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub consistent_read: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub projection_expression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct TenantRetentionQueryTableRequest {
    pub table_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub index_name: Option<String>,
    pub key_condition_expression: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub filter_expression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub scan_index_forward: Option<bool>,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RetentionDurationSelector {
    pub attribute_path: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsCatalogConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub backend: Option<AnalyticsCatalogBackend>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub connection_string: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsCatalogBackend {
    Duckdb,
    DucklakeSqlite,
    DucklakePostgres,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsObjectStorageConfig {
    #[serde(default)]
    #[schemars(default)]
    pub provider: AnalyticsObjectStorageProvider,
    #[serde(default = "default_object_storage_scheme")]
    #[schemars(default = "default_object_storage_scheme")]
    pub scheme: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub endpoint_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub credentials: Option<RemoteCredentialsConfig>,
}

impl Default for AnalyticsObjectStorageConfig {
    fn default() -> Self {
        Self {
            provider: AnalyticsObjectStorageProvider::S3,
            scheme: default_object_storage_scheme(),
            bucket: None,
            path: None,
            endpoint_url: None,
            region: None,
            credentials: None,
        }
    }
}

fn default_object_storage_scheme() -> String {
    DEFAULT_OBJECT_STORAGE_SCHEME.to_string()
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsObjectStorageProvider {
    #[default]
    S3,
    R2,
    Generic,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemoteCredentialsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    #[serde(rename = "static")]
    pub r#static: Option<RemoteStaticCredentialsConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub instance_keys: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemoteStaticCredentialsConfig {
    pub access_key: String,
    pub secret_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub session_token: Option<String>,
}
