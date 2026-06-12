use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use crate::{SortOrder, StructuredQueryValidationError};

const MAX_STRUCTURED_QUERY_JOINS: usize = 2;
const MAX_QUERY_IDENTIFIER_LEN: usize = 255;

/// Structured query tree compiled to safe SQL by the analytics engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({
    "analytics_table_name": "users",
    "select": [{"kind": "column", "column_name": "email", "alias": null}],
    "filters": [{
        "kind": "eq",
        "expression": {"kind": "column", "column_name": "org_id"},
        "value": "org-a"
    }],
    "group_by": [],
    "order_by": [{"expression": {"kind": "column", "column_name": "email"}, "direction": "asc"}],
    "limit": 100
}))]
pub struct StructuredQuery {
    /// Registered analytical table name to query.
    #[schema(
        min_length = 1,
        max_length = 255,
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$",
        example = "users"
    )]
    pub analytics_table_name: String,
    /// Optional alias for the primary table. Defaults to the table name when
    /// omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "u")]
    pub table_alias: Option<String>,
    /// Optional bounded joins.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0, max_items = 2)]
    pub joins: Vec<QueryJoin>,
    /// Select list. At least one select item is required.
    #[schema(min_items = 1, example = json!([{"kind": "column", "column_name": "email", "alias": null}]))]
    pub select: Vec<QuerySelect>,
    /// Optional filter predicates joined with AND.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0, example = json!([{
        "kind": "eq",
        "expression": {"kind": "column", "column_name": "org_id"},
        "value": "org-a"
    }]))]
    pub filters: Vec<QueryPredicate>,
    /// Optional GROUP BY expressions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0, example = json!([]))]
    pub group_by: Vec<QueryExpression>,
    /// Optional ORDER BY clauses.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0, example = json!([{
        "expression": {"kind": "column", "column_name": "email"},
        "direction": "asc"
    }]))]
    pub order_by: Vec<QueryOrder>,
    /// Optional row limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), minimum = 1, maximum = 100_000, example = 100)]
    pub limit: Option<u32>,
}

impl StructuredQuery {
    pub fn canonical_json(&self) -> Result<String, serde_json::Error> {
        let value = serde_json::to_value(self)?;
        serde_json::to_string(&canonicalize_json_value(value))
    }

    pub fn query_hash(&self) -> Result<String, serde_json::Error> {
        let canonical_json = self.canonical_json()?;
        let digest = Sha256::digest(canonical_json.as_bytes());
        Ok(format!("sha256:{}", hex_lower(digest.as_ref())))
    }

    pub fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        validate_query_identifier("analytics_table_name", self.analytics_table_name.as_str())?;
        if let Some(table_alias) = self.table_alias.as_deref() {
            validate_query_identifier("table_alias", table_alias)?;
        }
        validate_join_count(self.joins.len())?;
        let mut aliases = std::collections::HashSet::new();
        let primary_alias = self
            .table_alias
            .as_deref()
            .unwrap_or(self.analytics_table_name.as_str());
        insert_unique_alias(&mut aliases, primary_alias)?;
        for join in &self.joins {
            join.validate_shape()?;
            insert_unique_alias(&mut aliases, join.table_alias.as_str())?;
        }
        if self.select.is_empty() {
            return Err(StructuredQueryValidationError::EmptySelect);
        }
        let mut output_aliases = std::collections::HashSet::new();
        for select in &self.select {
            select.validate_shape()?;
            insert_unique_alias(&mut output_aliases, select.output_alias().as_str())?;
        }
        for filter in &self.filters {
            filter.validate_shape()?;
        }
        for expression in &self.group_by {
            expression.validate_shape()?;
        }
        for order in &self.order_by {
            order.validate_shape()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryJoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryJoin {
    pub kind: QueryJoinKind,
    #[schema(min_length = 1, max_length = 255, example = "orgs")]
    pub analytics_table_name: String,
    #[schema(min_length = 1, max_length = 255, example = "o")]
    pub table_alias: String,
    #[serde(default)]
    #[schema(min_items = 1)]
    pub on: Vec<QueryJoinPredicate>,
}

impl QueryJoin {
    fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        validate_query_identifier(
            "join analytics_table_name",
            self.analytics_table_name.as_str(),
        )?;
        validate_query_identifier("join table_alias", self.table_alias.as_str())?;
        if self.on.is_empty() {
            return Err(StructuredQueryValidationError::MissingJoinPredicate(
                self.table_alias.clone(),
            ));
        }
        for predicate in &self.on {
            predicate.validate_shape()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryJoinPredicate {
    pub left: QueryExpression,
    pub right: QueryExpression,
}

impl QueryJoinPredicate {
    fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        self.left.validate_shape()?;
        self.right.validate_shape()
    }
}

/// Structured select expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuerySelect {
    /// Select a registered analytical column.
    Column {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "u")]
        table_alias: Option<String>,
        /// Registered column name.
        #[schema(min_length = 1, max_length = 255, example = "email")]
        column_name: String,
        /// Optional output alias. Defaults to the column name when omitted.
        #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "user_email")]
        alias: Option<String>,
    },
    /// Select a path from the configured JSON document column.
    DocumentPath {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "u")]
        table_alias: Option<String>,
        /// Registered document column name.
        #[schema(min_length = 1, max_length = 255, example = "item")]
        document_column: String,
        /// Dot-separated JSON path relative to the document root.
        #[schema(min_length = 1, max_length = 1024, example = "profile.email")]
        path: String,
        /// Required output alias for the extracted value.
        #[schema(min_length = 1, max_length = 255, example = "email")]
        alias: String,
    },
    /// Select count(*).
    Count {
        /// Required output alias for the count column.
        #[schema(min_length = 1, max_length = 255, example = "count")]
        alias: String,
    },
    Sum {
        expression: QueryExpression,
        #[schema(min_length = 1, max_length = 255, example = "total")]
        alias: String,
    },
    Min {
        expression: QueryExpression,
        #[schema(min_length = 1, max_length = 255, example = "minimum")]
        alias: String,
    },
    Max {
        expression: QueryExpression,
        #[schema(min_length = 1, max_length = 255, example = "maximum")]
        alias: String,
    },
    Avg {
        expression: QueryExpression,
        #[schema(min_length = 1, max_length = 255, example = "average")]
        alias: String,
    },
    CountDistinct {
        expression: QueryExpression,
        #[schema(min_length = 1, max_length = 255, example = "distinct_count")]
        alias: String,
    },
}

impl QuerySelect {
    #[must_use]
    pub fn output_alias(&self) -> String {
        match self {
            Self::Column {
                column_name, alias, ..
            } => alias.clone().unwrap_or_else(|| column_name.clone()),
            Self::DocumentPath { alias, .. }
            | Self::Count { alias }
            | Self::Sum { alias, .. }
            | Self::Min { alias, .. }
            | Self::Max { alias, .. }
            | Self::Avg { alias, .. }
            | Self::CountDistinct { alias, .. } => alias.clone(),
        }
    }

    fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        match self {
            Self::Column {
                table_alias,
                column_name,
                alias,
            } => {
                validate_optional_query_identifier("select table_alias", table_alias.as_deref())?;
                validate_query_identifier("select column_name", column_name)?;
                if let Some(alias) = alias.as_deref() {
                    validate_query_identifier("select alias", alias)?;
                }
            }
            Self::DocumentPath {
                table_alias,
                document_column,
                path,
                alias,
            } => {
                validate_optional_query_identifier("select table_alias", table_alias.as_deref())?;
                validate_query_identifier("select document_column", document_column)?;
                validate_query_path(path)?;
                validate_query_identifier("select alias", alias)?;
            }
            Self::Count { alias } => validate_query_identifier("select alias", alias)?,
            Self::Sum { expression, alias }
            | Self::Min { expression, alias }
            | Self::Max { expression, alias }
            | Self::Avg { expression, alias }
            | Self::CountDistinct { expression, alias } => {
                expression.validate_shape()?;
                validate_query_identifier("select alias", alias)?;
            }
        }
        Ok(())
    }
}

/// Structured expression used by filters, grouping, and ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryExpression {
    /// Registered analytical column expression.
    Column {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "u")]
        table_alias: Option<String>,
        /// Registered column name.
        #[schema(min_length = 1, max_length = 255, example = "org_id")]
        column_name: String,
    },
    /// JSON document path expression.
    DocumentPath {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "u")]
        table_alias: Option<String>,
        /// Registered document column name.
        #[schema(min_length = 1, max_length = 255, example = "item")]
        document_column: String,
        /// Dot-separated JSON path relative to the document root.
        #[schema(min_length = 1, max_length = 1024, example = "profile.email")]
        path: String,
    },
}

impl QueryExpression {
    fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        match self {
            Self::Column {
                table_alias,
                column_name,
            } => {
                validate_optional_query_identifier(
                    "expression table_alias",
                    table_alias.as_deref(),
                )?;
                validate_query_identifier("expression column_name", column_name)
            }
            Self::DocumentPath {
                table_alias,
                document_column,
                path,
            } => {
                validate_optional_query_identifier(
                    "expression table_alias",
                    table_alias.as_deref(),
                )?;
                validate_query_identifier("expression document_column", document_column)?;
                validate_query_path(path)
            }
        }
    }
}

/// Structured predicate used in WHERE clauses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryPredicate {
    /// Equality predicate.
    Eq {
        /// Left-hand expression.
        expression: QueryExpression,
        /// Right-hand literal JSON value.
        #[schema(value_type = serde_json::Value, example = "org-a")]
        value: serde_json::Value,
    },
    /// Inequality predicate.
    NotEq {
        /// Left-hand expression.
        expression: QueryExpression,
        /// Right-hand literal JSON value.
        #[schema(value_type = serde_json::Value, example = "org-a")]
        value: serde_json::Value,
    },
    /// Greater-than predicate.
    Gt {
        /// Left-hand expression.
        expression: QueryExpression,
        /// Right-hand literal JSON value.
        #[schema(value_type = serde_json::Value, example = 100)]
        value: serde_json::Value,
    },
    /// Greater-than-or-equal predicate.
    Gte {
        /// Left-hand expression.
        expression: QueryExpression,
        /// Right-hand literal JSON value.
        #[schema(value_type = serde_json::Value, example = 100)]
        value: serde_json::Value,
    },
    /// Less-than predicate.
    Lt {
        /// Left-hand expression.
        expression: QueryExpression,
        /// Right-hand literal JSON value.
        #[schema(value_type = serde_json::Value, example = 100)]
        value: serde_json::Value,
    },
    /// Less-than-or-equal predicate.
    Lte {
        /// Left-hand expression.
        expression: QueryExpression,
        /// Right-hand literal JSON value.
        #[schema(value_type = serde_json::Value, example = 100)]
        value: serde_json::Value,
    },
    /// IS NULL predicate.
    IsNull {
        /// Expression to test.
        expression: QueryExpression,
    },
    /// IS NOT NULL predicate.
    IsNotNull {
        /// Expression to test.
        expression: QueryExpression,
    },
}

impl QueryPredicate {
    fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        match self {
            Self::Eq { expression, .. }
            | Self::NotEq { expression, .. }
            | Self::Gt { expression, .. }
            | Self::Gte { expression, .. }
            | Self::Lt { expression, .. }
            | Self::Lte { expression, .. }
            | Self::IsNull { expression }
            | Self::IsNotNull { expression } => expression.validate_shape(),
        }
    }
}

/// Structured ORDER BY clause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(example = json!({
    "expression": {"kind": "column", "column_name": "email"},
    "direction": "asc"
}))]
pub struct QueryOrder {
    /// Expression to order by.
    pub expression: QueryExpression,
    /// Sort direction. Defaults to ascending when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = "asc", example = "asc")]
    pub direction: Option<SortOrder>,
}

impl QueryOrder {
    fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        self.expression.validate_shape()
    }
}

fn validate_query_non_empty(
    field: &'static str,
    value: &str,
) -> Result<(), StructuredQueryValidationError> {
    if value.trim().is_empty() {
        return Err(StructuredQueryValidationError::EmptyField(field));
    }
    Ok(())
}

fn validate_optional_query_identifier(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), StructuredQueryValidationError> {
    if let Some(value) = value {
        validate_query_identifier(field, value)?;
    }
    Ok(())
}

fn validate_query_identifier(
    field: &'static str,
    value: &str,
) -> Result<(), StructuredQueryValidationError> {
    validate_query_non_empty(field, value)?;
    if value.len() > MAX_QUERY_IDENTIFIER_LEN {
        return Err(StructuredQueryValidationError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(StructuredQueryValidationError::EmptyField(field));
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || chars.any(|character| !(character == '_' || character.is_ascii_alphanumeric()))
    {
        return Err(StructuredQueryValidationError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    Ok(())
}

fn validate_join_count(count: usize) -> Result<(), StructuredQueryValidationError> {
    if count > MAX_STRUCTURED_QUERY_JOINS {
        return Err(StructuredQueryValidationError::TooManyJoins {
            actual: count,
            maximum: MAX_STRUCTURED_QUERY_JOINS,
        });
    }
    Ok(())
}

fn insert_unique_alias(
    aliases: &mut std::collections::HashSet<String>,
    alias: &str,
) -> Result<(), StructuredQueryValidationError> {
    validate_query_identifier("alias", alias)?;
    if aliases.insert(alias.to_string()) {
        return Ok(());
    }
    Err(StructuredQueryValidationError::DuplicateAlias(
        alias.to_string(),
    ))
}

fn validate_query_path(path: &str) -> Result<(), StructuredQueryValidationError> {
    validate_query_non_empty("document path", path)?;
    if path.split('.').any(|segment| segment.trim().is_empty()) {
        return Err(StructuredQueryValidationError::InvalidPath(
            path.to_string(),
        ));
    }
    Ok(())
}

fn canonicalize_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .into_iter()
                .map(canonicalize_json_value)
                .collect::<Vec<_>>(),
        ),
        serde_json::Value::Object(fields) => {
            let mut sorted = serde_json::Map::new();
            let mut field_entries = fields.into_iter().collect::<Vec<_>>();
            field_entries.sort_by(|left, right| left.0.cmp(&right.0));
            for (key, value) in field_entries {
                sorted.insert(key, canonicalize_json_value(value));
            }
            serde_json::Value::Object(sorted)
        }
        value => value,
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
