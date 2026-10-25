use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    catalog::{AnalyticsCatalogConfig, AnalyticsObjectStorageConfig},
    http::AnalyticsHttpConfig,
    job_store::{AnalyticsJobStoreConfig, AnalyticsOperationsConfig},
    privacy::AnalyticsPrivacyConfig,
    query::AnalyticsQueryConfig,
    retention::AnalyticsRetentionConfig,
    source::{AnalyticsIngestConfig, AnalyticsSourceConfig},
};

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
    pub query: AnalyticsQueryConfig,
    #[serde(default)]
    #[schemars(default)]
    pub source: AnalyticsSourceConfig,
    #[serde(default)]
    #[schemars(default)]
    pub job_store: AnalyticsJobStoreConfig,
    #[serde(default)]
    #[schemars(default)]
    pub ingest: AnalyticsIngestConfig,
    #[serde(default)]
    #[schemars(default)]
    pub catalog: AnalyticsCatalogConfig,
    #[serde(default)]
    #[schemars(default)]
    pub object_storage: AnalyticsObjectStorageConfig,
    #[serde(default)]
    #[schemars(default)]
    pub retention: AnalyticsRetentionConfig,
    #[serde(default)]
    #[schemars(default)]
    pub privacy: AnalyticsPrivacyConfig,
    #[serde(default)]
    #[schemars(default)]
    pub operations: AnalyticsOperationsConfig,
}
