use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::constants::DEFAULT_PARTITION_KEY_ATTRIBUTE_NAME;

/// Tenant id extraction rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TenantSelector {
    /// Do not write tenant ids.
    None,
    /// Use the source table name as tenant context.
    #[default]
    TableName,
    /// Read tenant id from a string attribute.
    Attribute {
        /// Source string attribute containing tenant id.
        #[schema(min_length = 1, max_length = 255, example = "tenant_id")]
        attribute_name: String,
    },
    /// Extract tenant id from a named regex capture group on a string
    /// attribute.
    AttributeRegex {
        /// Source string attribute to match.
        #[schema(min_length = 1, max_length = 255, example = "pk")]
        attribute_name: String,
        /// Regular expression with the configured capture group.
        #[schema(
            min_length = 1,
            max_length = 4096,
            example = "^TENANT#(?<tenant_id>[^#]+)#USER#(?<user_id>[^#]+)$"
        )]
        regex: String,
        /// Named capture group to use as tenant id.
        #[schema(min_length = 1, max_length = 255, example = "tenant_id")]
        capture: String,
    },
    /// Prefer table name when available, otherwise parse a partition-key
    /// prefix.
    TableNameOrPartitionKeyPrefix {
        /// Source partition key attribute.
        #[serde(default = "default_partition_key_attribute_name")]
        #[schema(default = "pk", min_length = 1, max_length = 255, example = "pk")]
        attribute_name: String,
    },
    /// Parse tenant id from the prefix of a partition key attribute.
    PartitionKeyPrefix {
        /// Source partition key attribute.
        #[serde(default = "default_partition_key_attribute_name")]
        #[schema(default = "pk", min_length = 1, max_length = 255, example = "pk")]
        attribute_name: String,
    },
}

/// Stable row identity extraction rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowIdentity {
    /// Use the record key supplied to the ingest endpoint.
    #[default]
    RecordKey,
    /// Hash the source stream key map.
    StreamKeys,
    /// Hash a string attribute value from the source item.
    Attribute {
        /// Source string attribute containing row identity.
        #[schema(min_length = 1, max_length = 255, example = "id")]
        attribute_name: String,
    },
    /// Hash an ordered tuple of source attributes into one stable row id.
    /// This is useful for expanded rows whose identity is a parent id plus
    /// list ordinal.
    Attributes {
        /// Ordered source attributes that together identify the row.
        #[schema(min_items = 1)]
        attribute_names: Vec<String>,
    },
    /// Extract row identity from a named regex capture group on a string
    /// attribute.
    AttributeRegex {
        /// Source string attribute to match.
        #[schema(min_length = 1, max_length = 255, example = "pk")]
        attribute_name: String,
        /// Regular expression with the configured capture group.
        #[schema(
            min_length = 1,
            max_length = 4096,
            example = "^TENANT#(?<tenant_id>[^#]+)#USER#(?<user_id>[^#]+)$"
        )]
        regex: String,
        /// Named capture group to use as row identity.
        #[schema(min_length = 1, max_length = 255, example = "user_id")]
        capture: String,
    },
}

fn default_partition_key_attribute_name() -> String {
    DEFAULT_PARTITION_KEY_ATTRIBUTE_NAME.to_string()
}
