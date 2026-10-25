use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[derive(Default)]
pub enum TableScope {
    #[default]
    TenantScoped,
    GlobalReference {
        #[schema(min_length = 1, max_length = 255, example = "public_product_catalog")]
        reference_class: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct JoinPolicy {
    #[serde(default = "default_allowed_as_primary")]
    #[schema(default = true, example = true)]
    pub allowed_as_primary: bool,
    #[serde(default)]
    #[schema(default = false, example = false)]
    pub allowed_as_join: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), minimum = 1, example = 10000_u64)]
    pub max_join_rows_hint: Option<u64>,
}

impl Default for JoinPolicy {
    fn default() -> Self {
        Self {
            allowed_as_primary: true,
            allowed_as_join: false,
            max_join_rows_hint: None,
        }
    }
}

fn default_allowed_as_primary() -> bool {
    true
}
