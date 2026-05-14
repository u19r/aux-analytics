use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{SortOrder, StructuredQueryValidationError};

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
    pub fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        validate_query_non_empty("analytics_table_name", self.analytics_table_name.as_str())?;
        if self.select.is_empty() {
            return Err(StructuredQueryValidationError::EmptySelect);
        }
        for select in &self.select {
            select.validate_shape()?;
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

/// Structured select expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuerySelect {
    /// Select a registered analytical column.
    Column {
        /// Registered column name.
        #[schema(min_length = 1, max_length = 255, example = "email")]
        column_name: String,
        /// Optional output alias. Defaults to the column name when omitted.
        #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "user_email")]
        alias: Option<String>,
    },
    /// Select a path from the configured JSON document column.
    DocumentPath {
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
}

impl QuerySelect {
    fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        match self {
            Self::Column { column_name, alias } => {
                validate_query_non_empty("select column_name", column_name)?;
                if let Some(alias) = alias.as_deref() {
                    validate_query_non_empty("select alias", alias)?;
                }
            }
            Self::DocumentPath {
                document_column,
                path,
                alias,
            } => {
                validate_query_non_empty("select document_column", document_column)?;
                validate_query_path(path)?;
                validate_query_non_empty("select alias", alias)?;
            }
            Self::Count { alias } => validate_query_non_empty("select alias", alias)?,
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
        /// Registered column name.
        #[schema(min_length = 1, max_length = 255, example = "org_id")]
        column_name: String,
    },
    /// JSON document path expression.
    DocumentPath {
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
            Self::Column { column_name } => {
                validate_query_non_empty("expression column_name", column_name)
            }
            Self::DocumentPath {
                document_column,
                path,
            } => {
                validate_query_non_empty("expression document_column", document_column)?;
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

fn validate_query_path(path: &str) -> Result<(), StructuredQueryValidationError> {
    validate_query_non_empty("document path", path)?;
    if path.split('.').any(|segment| segment.trim().is_empty()) {
        return Err(StructuredQueryValidationError::InvalidPath(
            path.to_string(),
        ));
    }
    Ok(())
}
