use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage_types::{AttributeValue, KeyAttributes, VecMap};

use super::catalog::RemoteCredentialsConfig;
use crate::constants::{
    DEFAULT_RETENTION_CACHE_TTL_MS, DEFAULT_RETENTION_DELETE_BATCH_SIZE,
    DEFAULT_RETENTION_SWEEP_INTERVAL_MS,
};

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
            tables: Vec::new(),
        }
    }
}

fn default_retention_sweep_interval_ms() -> u64 {
    DEFAULT_RETENTION_SWEEP_INTERVAL_MS
}

fn default_retention_delete_batch_size() -> u64 {
    DEFAULT_RETENTION_DELETE_BATCH_SIZE
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
    DEFAULT_RETENTION_CACHE_TTL_MS
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
    #[schemars(with = "BTreeMap<String, serde_json::Value>")]
    pub key: KeyAttributes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub consistent_read: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub projection_expression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    #[schemars(with = "Option<BTreeMap<String, String>>")]
    pub expression_attribute_names: Option<VecMap<String, String>>,
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
    #[schemars(with = "Option<BTreeMap<String, String>>")]
    pub expression_attribute_names: Option<VecMap<String, String>>,
    #[schemars(with = "BTreeMap<String, serde_json::Value>")]
    pub expression_attribute_values: VecMap<String, AttributeValue>,
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
