use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::foundation::default_true;

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
