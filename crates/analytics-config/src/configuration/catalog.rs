use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::constants::DEFAULT_OBJECT_STORAGE_SCHEME;

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsCatalogConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub backend: Option<AnalyticsCatalogBackend>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub connection_string: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub motherduck_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub duckdb_threads: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsCatalogBackend {
    Duckdb,
    DucklakeSqlite,
    DucklakeAuxCatalog,
    DucklakeMotherduck,
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
