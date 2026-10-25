use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Typed projected column sourced from a nested source attribute path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({
    "column_name": "email",
    "attribute_path": "profile.email",
    "column_type": {"kind": "primitive", "primitive": "var_char"}
}))]
pub struct ProjectionColumn {
    /// Output analytical column name. Must not be one of the built-in output
    /// columns: `tenant_id`, `table_name`, or `__id`.
    #[schema(
        min_length = 1,
        max_length = 255,
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$",
        example = "email"
    )]
    pub column_name: String,
    /// Dot-separated source attribute path.
    #[schema(
        min_length = 1,
        max_length = 1024,
        pattern = r"^[A-Za-z0-9_.$-]+(\\.[A-Za-z0-9_.$-]+)*$",
        example = "profile.email"
    )]
    pub attribute_path: String,
    /// Optional output column type. Omitted values use the VARCHAR type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<std::collections::BTreeMap<String, serde_json::Value>>, nullable = true, default = json!({"kind": "primitive", "primitive": "var_char"}), example = json!({"kind": "primitive", "primitive": "var_char"}))]
    pub column_type: Option<AnalyticsColumnType>,
}

/// Analytical column definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({
    "column_name": "org_id",
    "column_type": {"kind": "primitive", "primitive": "var_char"}
}))]
pub struct AnalyticsColumn {
    /// Output analytical column name. Must not be one of the built-in output
    /// columns: `tenant_id`, `table_name`, or `__id`.
    #[schema(
        min_length = 1,
        max_length = 255,
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$",
        example = "org_id"
    )]
    pub column_name: String,
    /// DuckDB-compatible column type.
    #[schema(value_type = Object, example = json!({"kind": "primitive", "primitive": "var_char"}))]
    pub column_type: AnalyticsColumnType,
}

/// Logical analytics column type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AnalyticsColumnType {
    /// Scalar primitive value.
    Primitive {
        /// Primitive scalar type.
        primitive: PrimitiveColumnType,
    },
    /// List of primitive values.
    List {
        /// Primitive list item type.
        primitive: PrimitiveColumnType,
    },
    /// Map value stored as JSON in `DuckDB`.
    Map {
        /// Logical key type documented for callers and stored as JSON.
        key_type: String,
        /// Logical value type.
        value_type: Box<AnalyticsColumnType>,
    },
}

impl AnalyticsColumnType {
    #[must_use]
    pub fn duckdb_type(&self) -> &'static str {
        match self {
            Self::Primitive { primitive } => primitive.duckdb_type(),
            Self::List { primitive } => primitive.duckdb_list_type(),
            Self::Map { .. } => "JSON",
        }
    }
}

/// Primitive `DuckDB` column type supported by the manifest contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveColumnType {
    Boolean,
    VarChar,
    Json,
    /// Reversible tagged StorageValue JSON; numeric lexemes remain strings.
    StorageJson,
    BigInt,
    UnsignedBigInt,
    Decimal,
    Double,
    Timestamp,
}

impl PrimitiveColumnType {
    #[must_use]
    pub const fn duckdb_type(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::VarChar => "VARCHAR",
            Self::Json | Self::StorageJson => "JSON",
            Self::BigInt => "BIGINT",
            Self::UnsignedBigInt => "UBIGINT",
            Self::Decimal => "DECIMAL(38, 18)",
            Self::Double => "DOUBLE",
            Self::Timestamp => "TIMESTAMP",
        }
    }

    #[must_use]
    pub const fn duckdb_list_type(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN[]",
            Self::VarChar => "VARCHAR[]",
            Self::Json | Self::StorageJson => "JSON[]",
            Self::BigInt => "BIGINT[]",
            Self::UnsignedBigInt => "UBIGINT[]",
            Self::Decimal => "DECIMAL(38, 18)[]",
            Self::Double => "DOUBLE[]",
            Self::Timestamp => "TIMESTAMP[]",
        }
    }
}

/// `DuckLake` partition key definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({"column_name": "tenant_id"}))]
pub struct PartitionKey {
    /// Output column used for partitioning.
    #[schema(min_length = 1, max_length = 255, example = "tenant_id")]
    pub column_name: String,
    /// Optional partition bucketing transform.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(inline, nullable = true, default = json!(null), example = json!({"kind": "hash", "buckets": 32}))]
    pub bucket: Option<PartitionBucket>,
}

/// Partition bucketing transform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PartitionBucket {
    /// Hash bucket a column into a fixed number of buckets.
    Hash { buckets: u32 },
    /// Time bucket a timestamp column.
    Time { unit: TimeBucketUnit, size: u32 },
}

/// Time bucket unit for partitioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TimeBucketUnit {
    Minute,
    Hour,
    Day,
    Month,
}

/// `DuckLake` clustering or sorting key definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({"column_name": "email", "order": "asc"}))]
pub struct ClusteringKey {
    /// Output column used for clustering.
    #[schema(min_length = 1, max_length = 255, example = "email")]
    pub column_name: String,
    /// Sort direction. Defaults to ascending when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(inline, nullable = true, default = "asc", example = "asc")]
    pub order: Option<SortOrder>,
}

/// Sort direction for clustering and structured order clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    Asc,
    Desc,
}
