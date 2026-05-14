use std::collections::{BTreeMap, HashMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    ManifestValidationError, StorageValue,
    constants::{
        BUILTIN_ROW_ID_COLUMN, BUILTIN_TABLE_NAME_COLUMN, BUILTIN_TENANT_ID_COLUMN,
        DEFAULT_DOCUMENT_COLUMN, DEFAULT_PARTITION_KEY_ATTRIBUTE_NAME, INTERNAL_EXPIRY_COLUMN,
        INTERNAL_INGESTED_AT_COLUMN, INTERNAL_MISSING_RETENTION_COLUMN, MANIFEST_VERSION,
    },
};

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

impl AnalyticsManifest {
    #[must_use]
    pub fn new(tables: Vec<TableRegistration>) -> Self {
        Self {
            version: MANIFEST_VERSION,
            tables,
        }
    }

    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        if self.version != MANIFEST_VERSION {
            return Err(ManifestValidationError::UnsupportedVersion {
                actual: self.version,
                expected: MANIFEST_VERSION,
            });
        }

        let mut table_names = HashSet::new();
        for table in &self.tables {
            table.validate()?;
            if !table_names.insert(table.analytics_table_name.as_str()) {
                return Err(ManifestValidationError::DuplicateAnalyticsTableName(
                    table.analytics_table_name.clone(),
                ));
            }
        }

        Ok(())
    }
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
    /// JSON document column containing the full source item. Set null to omit
    /// the full document. Must not use the built-in output column names
    /// `tenant_id`, `table_name`, or `__id`.
    #[serde(
        default = "default_document_column",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[schema(nullable = true, default = json!(null), example = json!({"#email": "profile.email"}))]
    pub expression_attribute_names: Option<HashMap<String, String>>,
    /// Expression attribute values referenced by condition or projection
    /// expressions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object, nullable = true, default = json!(null), example = json!({":active": {"BOOL": true}}))]
    pub expression_attribute_values: Option<BTreeMap<String, StorageValue>>,
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
    /// `DuckLake` partition layout columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0, example = json!([{"column_name": "tenant_id"}]))]
    pub partition_keys: Vec<PartitionKey>,
    /// `DuckLake` clustering or sorting layout columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0, example = json!([{"column_name": "email", "order": "asc"}]))]
    pub clustering_keys: Vec<ClusteringKey>,
}

impl TableRegistration {
    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        validate_non_empty("source_table_name", self.source_table_name.as_str())?;
        validate_non_empty("analytics_table_name", self.analytics_table_name.as_str())?;
        if let Some(document_column) = self.document_column.as_deref() {
            validate_non_empty("document_column", document_column)?;
            validate_custom_output_column(self.analytics_table_name.as_str(), document_column)?;
        }
        validate_tenant_selector(&self.tenant_selector)?;
        validate_row_identity(&self.row_identity)?;
        if let Some(retention) = self.retention.as_ref() {
            retention.validate(self.analytics_table_name.as_str())?;
        }

        let mut output_column_names = HashSet::new();
        if let Some(document_column) = self.document_column.as_deref() {
            insert_output_column_or_duplicate_error(
                self.analytics_table_name.as_str(),
                &mut output_column_names,
                document_column,
            )?;
        }
        for column in &self.columns {
            column.validate()?;
            validate_custom_output_column(
                self.analytics_table_name.as_str(),
                column.column_name.as_str(),
            )?;
            insert_output_column_or_duplicate_error(
                self.analytics_table_name.as_str(),
                &mut output_column_names,
                column.column_name.as_str(),
            )?;
        }

        if let Some(projection_columns) = self.projection_columns.as_deref() {
            let mut projection_column_names = HashSet::new();
            for column in projection_columns {
                validate_non_empty("projection column_name", column.column_name.as_str())?;
                validate_non_empty("projection attribute_path", column.attribute_path.as_str())?;
                validate_custom_output_column(
                    self.analytics_table_name.as_str(),
                    column.column_name.as_str(),
                )?;
                if !projection_column_names.insert(column.column_name.as_str()) {
                    return Err(ManifestValidationError::DuplicateColumnName {
                        table: self.analytics_table_name.clone(),
                        column: column.column_name.clone(),
                    });
                }
                insert_output_column_or_duplicate_error(
                    self.analytics_table_name.as_str(),
                    &mut output_column_names,
                    column.column_name.as_str(),
                )?;
            }
        }

        if let Some(attribute_names) = self.projection_attribute_names.as_deref() {
            let mut projection_attribute_names = HashSet::new();
            for column_name in attribute_names {
                validate_non_empty("projection attribute name", column_name.as_str())?;
                validate_custom_output_column(
                    self.analytics_table_name.as_str(),
                    column_name.as_str(),
                )?;
                if !projection_attribute_names.insert(column_name.as_str()) {
                    return Err(ManifestValidationError::DuplicateColumnName {
                        table: self.analytics_table_name.clone(),
                        column: column_name.clone(),
                    });
                }
                insert_output_column_or_duplicate_error(
                    self.analytics_table_name.as_str(),
                    &mut output_column_names,
                    column_name.as_str(),
                )?;
            }
        }

        for partition_key in &self.partition_keys {
            validate_non_empty("partition column_name", partition_key.column_name.as_str())?;
            validate_declared_output_column(
                self.analytics_table_name.as_str(),
                partition_key.column_name.as_str(),
                &output_column_names,
            )?;
        }

        for clustering_key in &self.clustering_keys {
            validate_non_empty(
                "clustering column_name",
                clustering_key.column_name.as_str(),
            )?;
            validate_declared_output_column(
                self.analytics_table_name.as_str(),
                clustering_key.column_name.as_str(),
                &output_column_names,
            )?;
        }

        Ok(())
    }
}

fn validate_tenant_selector(selector: &TenantSelector) -> Result<(), ManifestValidationError> {
    match selector {
        TenantSelector::None | TenantSelector::TableName => Ok(()),
        TenantSelector::Attribute { attribute_name }
        | TenantSelector::TableNameOrPartitionKeyPrefix { attribute_name }
        | TenantSelector::PartitionKeyPrefix { attribute_name } => {
            validate_non_empty("tenant_selector attribute_name", attribute_name)
        }
        TenantSelector::AttributeRegex {
            attribute_name,
            regex,
            capture,
        } => {
            validate_non_empty("tenant_selector attribute_name", attribute_name)?;
            validate_non_empty("tenant_selector regex", regex)?;
            validate_non_empty("tenant_selector capture", capture)
        }
    }
}

fn validate_row_identity(identity: &RowIdentity) -> Result<(), ManifestValidationError> {
    match identity {
        RowIdentity::RecordKey | RowIdentity::StreamKeys => Ok(()),
        RowIdentity::Attribute { attribute_name } => {
            validate_non_empty("row_identity attribute_name", attribute_name)
        }
        RowIdentity::AttributeRegex {
            attribute_name,
            regex,
            capture,
        } => {
            validate_non_empty("row_identity attribute_name", attribute_name)?;
            validate_non_empty("row_identity regex", regex)?;
            validate_non_empty("row_identity capture", capture)
        }
    }
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), ManifestValidationError> {
    if value.trim().is_empty() {
        return Err(ManifestValidationError::EmptyField(field));
    }
    Ok(())
}

fn validate_declared_output_column(
    table: &str,
    column: &str,
    output_column_names: &HashSet<&str>,
) -> Result<(), ManifestValidationError> {
    if output_column_names.contains(column) || is_builtin_column(column) {
        return Ok(());
    }
    Err(ManifestValidationError::UnknownLayoutColumn {
        table: table.to_string(),
        column: column.to_string(),
    })
}

fn insert_output_column_or_duplicate_error<'a>(
    table: &str,
    output_column_names: &mut HashSet<&'a str>,
    column: &'a str,
) -> Result<(), ManifestValidationError> {
    if output_column_names.insert(column) {
        return Ok(());
    }
    Err(ManifestValidationError::DuplicateColumnName {
        table: table.to_string(),
        column: column.to_string(),
    })
}

fn validate_custom_output_column(table: &str, column: &str) -> Result<(), ManifestValidationError> {
    if is_builtin_column(column) {
        return Err(ManifestValidationError::ReservedOutputColumn {
            table: table.to_string(),
            column: column.to_string(),
        });
    }
    Ok(())
}

fn is_builtin_column(column: &str) -> bool {
    matches!(
        column,
        BUILTIN_TENANT_ID_COLUMN
            | BUILTIN_ROW_ID_COLUMN
            | BUILTIN_TABLE_NAME_COLUMN
            | INTERNAL_INGESTED_AT_COLUMN
            | INTERNAL_EXPIRY_COLUMN
            | INTERNAL_MISSING_RETENTION_COLUMN
    )
}

#[allow(clippy::unnecessary_wraps)]
fn default_document_column() -> Option<String> {
    Some(DEFAULT_DOCUMENT_COLUMN.to_string())
}

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

/// Static retention policy for one analytical table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({
    "period_ms": 7_776_000_000_u64,
    "timestamp": {"kind": "attribute", "attribute_path": "created_at_ms"}
}))]
pub struct RetentionPolicy {
    /// Retention duration in milliseconds.
    #[schema(minimum = 1, example = 7_776_000_000_u64)]
    pub period_ms: u64,
    /// Timestamp used as the start time for retention.
    pub timestamp: RetentionTimestamp,
}

impl RetentionPolicy {
    fn validate(&self, table: &str) -> Result<(), ManifestValidationError> {
        if self.period_ms == 0 {
            return Err(ManifestValidationError::InvalidRetentionPeriod {
                table: table.to_string(),
            });
        }
        validate_retention_timestamp(&self.timestamp)
    }
}

/// Timestamp basis used to compute row expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetentionTimestamp {
    /// Read a unix epoch millisecond value from a source item attribute path.
    Attribute {
        /// Dot-separated source attribute path.
        #[schema(min_length = 1, max_length = 1024, example = "created_at_ms")]
        attribute_path: String,
    },
    /// Use the aux-analytics ingestion time.
    IngestedAt,
}

fn validate_retention_timestamp(
    timestamp: &RetentionTimestamp,
) -> Result<(), ManifestValidationError> {
    match timestamp {
        RetentionTimestamp::Attribute { attribute_path } => {
            validate_non_empty("retention timestamp attribute_path", attribute_path)
        }
        RetentionTimestamp::IngestedAt => Ok(()),
    }
}

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
    /// Optional output column type. Defaults to VARCHAR when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object, nullable = true, default = json!(null), example = json!({"kind": "primitive", "primitive": "var_char"}))]
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

impl AnalyticsColumn {
    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        validate_non_empty("column_name", self.column_name.as_str())
    }
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
        /// Logical key type. Currently documented for callers and stored as
        /// JSON.
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
    BigInt,
    Decimal,
    Timestamp,
}

impl PrimitiveColumnType {
    #[must_use]
    pub const fn duckdb_type(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::VarChar => "VARCHAR",
            Self::Json => "JSON",
            Self::BigInt => "BIGINT",
            Self::Decimal => "DECIMAL(38, 18)",
            Self::Timestamp => "TIMESTAMP",
        }
    }

    #[must_use]
    pub const fn duckdb_list_type(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN[]",
            Self::VarChar => "VARCHAR[]",
            Self::Json => "JSON[]",
            Self::BigInt => "BIGINT[]",
            Self::Decimal => "DECIMAL(38, 18)[]",
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
    #[schema(nullable = true, default = json!(null), example = json!({"kind": "hash", "buckets": 32}))]
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
    #[schema(nullable = true, default = "asc", example = "asc")]
    pub order: Option<SortOrder>,
}

/// Sort direction for clustering and structured order clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    Asc,
    Desc,
}
