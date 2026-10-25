use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct AnalyticsPrivacyConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    pub policy_path: Option<String>,
}
