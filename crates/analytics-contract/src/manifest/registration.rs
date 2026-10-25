use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage_types::VecMap;
use utoipa::ToSchema;

use super::{
    AnalyticsColumn, ClusteringKey, JoinPolicy, PartitionKey, ProjectionColumn, RetentionPolicy,
    RowExpansion, RowIdentity, TableScope, TenantSelector,
};
use crate::{StorageValue, constants::DEFAULT_DOCUMENT_COLUMN};

/// Top-level analytics manifest that registers source stream tables as
/// analytical tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({
    "version": 1,
    "tables": [{
        "source_table_name": "source_users",
        "analytics_table_name": "users",
        "tenant_id": "tenant-a",
        "tenant_selector": {"kind": "table_name"},
        "row_identity": {"kind": "record_key"},
        "document_column": "item",
        "skip_delete": false,
        "projection_columns": [{
            "column_name": "email",
            "attribute_path": "profile.email",
            "column_type": {"kind": "primitive", "primitive": "var_char"}
        }],
        "partition_keys": [{"column_name": "tenant_id"}],
        "clustering_keys": [{"column_name": "email", "order": "asc"}]
    }]
}))]
pub struct AnalyticsManifest {
    /// Manifest format version supported by this crate.
    #[schema(default = 1, minimum = 1, example = 1)]
    pub version: u32,
    /// Table registrations to create and maintain in the analytical catalog.
    #[schema(min_items = 1, example = json!([{
        "source_table_name": "source_users",
        "analytics_table_name": "users",
        "tenant_id": "tenant-a",
        "projection_columns": [{"column_name": "email", "attribute_path": "profile.email"}]
    }]))]
    pub tables: Vec<TableRegistration>,
}

/// Registration for one analytical table populated from one source table or
/// source table family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({
    "source_table_name": "source_users",
    "analytics_table_name": "users",
    "source_table_name_prefix": null,
    "tenant_id": "tenant-a",
    "tenant_selector": {"kind": "table_name"},
    "row_identity": {"kind": "record_key"},
    "document_column": "item",
    "skip_delete": false,
    "condition_expression": null,
    "expression_attribute_names": null,
    "expression_attribute_values": null,
    "projection_attribute_names": null,
    "projection_columns": [{
        "column_name": "email",
        "attribute_path": "profile.email",
        "column_type": {"kind": "primitive", "primitive": "var_char"}
    }],
    "columns": [],
    "partition_keys": [{"column_name": "tenant_id"}],
    "clustering_keys": [{"column_name": "email", "order": "asc"}]
}))]
pub struct TableRegistration {
    /// Source storage table name to read from.
    #[schema(min_length = 1, max_length = 255, example = "source_users")]
    pub source_table_name: String,
    /// DuckDB-compatible analytical table name to create and query.
    #[schema(
        min_length = 1,
        max_length = 255,
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$",
        example = "users"
    )]
    pub analytics_table_name: String,
    /// Optional source table prefix for families of source tables that share
    /// one registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "tenant_")]
    pub source_table_name_prefix: Option<String>,
    /// Static tenant id to write into ingested rows when the source does not
    /// carry tenant identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "tenant-a")]
    pub tenant_id: Option<String>,
    /// Rule used to derive tenant id from records when `tenant_id` is not
    /// static.
    #[serde(default)]
    #[schema(default = json!({"kind": "table_name"}), example = json!({"kind": "attribute", "attribute_name": "tenant_id"}))]
    pub tenant_selector: TenantSelector,
    /// Rule used to derive the stable row id for upserts and deletes.
    #[serde(default)]
    #[schema(default = json!({"kind": "record_key"}), example = json!({"kind": "attribute", "attribute_name": "id"}))]
    pub row_identity: RowIdentity,
    /// Optional expansion that turns one source item containing a list into
    /// one analytical source row per list element. Conditions are evaluated
    /// against the source item before expansion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null))]
    pub row_expansion: Option<RowExpansion>,
    /// JSON document column containing the full source item. Set null for a
    /// direct-ingestion table that source polling must not consume. Must not
    /// use the built-in output column names `tenant_id`, `table_name`, or
    /// `__id`.
    #[serde(default = "default_document_column")]
    #[schema(
        nullable = true,
        default = "item",
        min_length = 1,
        max_length = 255,
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$",
        example = "item"
    )]
    pub document_column: Option<String>,
    /// Whether delete events should be ignored instead of removing analytical
    /// rows.
    #[serde(default)]
    #[schema(default = false, example = false)]
    pub skip_delete: bool,
    /// Optional static retention policy for rows in this analytical table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(default)]
    #[schema(nullable = true, required = false, default = json!(null), example = json!({
        "period_ms": 7776000000_u64,
        "timestamp": {"kind": "ingested_at"}
    }))]
    pub retention: Option<RetentionPolicy>,
    /// Optional DynamoDB-compatible condition expression that filters source
    /// items before ingestion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 4096, example = "attribute_exists(profile.email)")]
    pub condition_expression: Option<String>,
    /// Expression attribute name aliases referenced by condition or projection
    /// expressions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<BTreeMap<String, String>>")]
    #[schema(value_type = Option<BTreeMap<String, String>>, nullable = true, default = json!(null), example = json!({"#email": "profile.email"}))]
    pub expression_attribute_names: Option<VecMap<String, String>>,
    /// Expression attribute values referenced by condition or projection
    /// expressions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<BTreeMap<String, StorageValue>>")]
    #[schema(value_type = Option<std::collections::BTreeMap<String, serde_json::Value>>, nullable = true, default = json!(null), example = json!({":active": {"BOOL": true}}))]
    pub expression_attribute_values: Option<VecMap<String, StorageValue>>,
    /// Simple top-level source attribute names to project as VARCHAR columns.
    /// Values must not use the built-in output column names `tenant_id`,
    /// `table_name`, or `__id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), min_items = 1, example = json!(["email", "org_id"]))]
    pub projection_attribute_names: Option<Vec<String>>,
    /// Typed projection columns from nested source attributes.
    /// Projection output names must not use the built-in output column names
    /// `tenant_id`, `table_name`, or `__id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), min_items = 1, example = json!([{
        "column_name": "email",
        "attribute_path": "profile.email",
        "column_type": {"kind": "primitive", "primitive": "var_char"}
    }]))]
    pub projection_columns: Option<Vec<ProjectionColumn>>,
    /// Explicit analytical columns. When present, these define table shape
    /// directly. Custom column names must not use the built-in output
    /// column names `tenant_id`, `table_name`, or `__id`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0, example = json!([]))]
    pub columns: Vec<AnalyticsColumn>,
    /// Tenant/reference scope used by safe structured query planning.
    #[serde(default)]
    #[schema(default = json!({"kind": "tenant_scoped"}))]
    pub table_scope: TableScope,
    /// Join participation policy for bounded structured queries.
    #[serde(default)]
    #[schema(default = json!({"allowed_as_primary": true, "allowed_as_join": false}))]
    pub join_policy: JoinPolicy,
    /// `DuckLake` partition layout columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0, example = json!([{"column_name": "tenant_id"}]))]
    pub partition_keys: Vec<PartitionKey>,
    /// `DuckLake` clustering or sorting layout columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0, example = json!([{"column_name": "email", "order": "asc"}]))]
    pub clustering_keys: Vec<ClusteringKey>,
}

fn default_document_column() -> Option<String> {
    DEFAULT_DOCUMENT_COLUMN.to_string().into()
}
