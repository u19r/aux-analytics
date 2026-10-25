use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::composition::AnalyticsConfig;
use crate::constants::{DEFAULT_CONFIG_VERSION, DEFAULT_HTTP_BIND_ADDR, DEFAULT_LOG_LEVEL};

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
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct Features {
    #[serde(default)]
    #[schemars(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    #[schemars(default = "default_true")]
    pub enabled: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

pub(crate) fn default_true() -> bool {
    true
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

fn default_log_level_option() -> Option<String> {
    DEFAULT_LOG_LEVEL.to_string().into()
}
