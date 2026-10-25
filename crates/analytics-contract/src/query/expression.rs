use super::*;

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
    #[schema(default = json!([]), min_items = 1)]
    pub on: Vec<QueryJoinPredicate>,
}

impl QueryJoin {
    pub(super) fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
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
    pub(super) fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
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
        /// Optional source table alias. Omitted values resolve against the
        /// registered table name when the column is unambiguous.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "u")]
        table_alias: Option<String>,
        /// Registered column name.
        #[schema(min_length = 1, max_length = 255, example = "email")]
        column_name: String,
        /// Optional output alias. Omitted values use the selected column name.
        #[schema(
            nullable = true,
            default = json!(null),
            min_length = 1,
            max_length = 255,
            example = "user_email"
        )]
        alias: Option<String>,
    },
    /// Select a path from the configured JSON document column.
    DocumentPath {
        /// Optional source table alias. Omitted values resolve against the
        /// registered table name when the document column is unambiguous.
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
    /// Select any supported structured expression.
    Expression {
        expression: QueryExpression,
        /// Required output alias for the expression.
        #[schema(min_length = 1, max_length = 255, example = "classification")]
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
            | Self::Expression { alias, .. }
            | Self::Count { alias }
            | Self::Sum { alias, .. }
            | Self::Min { alias, .. }
            | Self::Max { alias, .. }
            | Self::Avg { alias, .. }
            | Self::CountDistinct { alias, .. } => alias.clone(),
        }
    }

    pub(super) fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
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
            Self::Expression { expression, alias } => {
                expression.validate_shape()?;
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
        /// Optional source table alias. Omitted values resolve against the
        /// registered table name when the column is unambiguous.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "u")]
        table_alias: Option<String>,
        /// Registered column name.
        #[schema(min_length = 1, max_length = 255, example = "org_id")]
        column_name: String,
    },
    /// JSON document path expression.
    DocumentPath {
        /// Optional source table alias. Omitted values resolve against the
        /// registered table name when the document column is unambiguous.
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
    /// Unicode lowercase normalization for case-insensitive ordering.
    Lower {
        #[schema(value_type = Object)]
        expression: Box<QueryExpression>,
    },
    /// Ordered CASE-style classification with literal results. Each branch
    /// combines its comparisons with AND; the first matching branch wins.
    Conditional {
        #[schemars(length(min = 1, max = 32))]
        #[schema(min_items = 1, max_items = 32)]
        branches: Vec<QueryConditionalBranch>,
        /// Scalar result used when no branch matches.
        #[schema(value_type = serde_json::Value, example = "standard")]
        else_value: serde_json::Value,
    },
}

impl QueryExpression {
    pub(super) fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
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
            Self::Lower { expression } => expression.validate_shape(),
            Self::Conditional {
                branches,
                else_value,
            } => validate_conditional_expression(branches, else_value),
        }
    }
}

fn validate_conditional_expression(
    branches: &[QueryConditionalBranch],
    else_value: &serde_json::Value,
) -> Result<(), StructuredQueryValidationError> {
    if branches.is_empty() {
        return Err(StructuredQueryValidationError::EmptyConditionalExpression);
    }
    if branches.len() > MAX_CONDITIONAL_BRANCHES {
        return Err(StructuredQueryValidationError::TooManyConditionalBranches {
            actual: branches.len(),
            maximum: MAX_CONDITIONAL_BRANCHES,
        });
    }
    let result_kind = conditional_literal_kind(else_value)?;
    for branch in branches {
        branch.validate_shape(result_kind)?;
    }
    Ok(())
}

/// One ordered branch of a conditional structured expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryConditionalBranch {
    /// Comparisons combined with AND.
    #[schemars(length(min = 1, max = 16))]
    #[schema(min_items = 1, max_items = 16)]
    pub all: Vec<QueryColumnComparison>,
    /// Scalar result returned when all comparisons match.
    #[schema(value_type = serde_json::Value, example = "late_arrival")]
    pub then_value: serde_json::Value,
}

impl QueryConditionalBranch {
    fn validate_shape(
        &self,
        result_kind: ConditionalLiteralKind,
    ) -> Result<(), StructuredQueryValidationError> {
        if self.all.is_empty() {
            return Err(StructuredQueryValidationError::EmptyConditionalBranch);
        }
        if self.all.len() > MAX_CONDITIONAL_COMPARISONS {
            return Err(
                StructuredQueryValidationError::TooManyConditionalComparisons {
                    actual: self.all.len(),
                    maximum: MAX_CONDITIONAL_COMPARISONS,
                },
            );
        }
        if conditional_literal_kind(&self.then_value)? != result_kind {
            return Err(StructuredQueryValidationError::IncompatibleConditionalLiteral);
        }
        for comparison in &self.all {
            comparison.validate_shape()?;
        }
        Ok(())
    }
}

/// Safe comparison between a registered column and a JSON literal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryColumnComparison {
    /// Optional source table alias. Omitted values resolve against the
    /// registered table name when the column is unambiguous.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "m")]
    pub table_alias: Option<String>,
    #[schema(min_length = 1, max_length = 255, example = "ingested_at_ms")]
    pub column_name: String,
    pub operator: QueryComparisonOperator,
    #[schema(value_type = serde_json::Value, example = 1782864000000_i64)]
    pub value: serde_json::Value,
}

impl QueryColumnComparison {
    fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        validate_optional_query_identifier("conditional table_alias", self.table_alias.as_deref())?;
        validate_query_identifier("conditional column_name", self.column_name.as_str())
    }
}

/// Operator supported by a conditional column comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryComparisonOperator {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConditionalLiteralKind {
    Boolean,
    Number,
    String,
}

fn conditional_literal_kind(
    value: &serde_json::Value,
) -> Result<ConditionalLiteralKind, StructuredQueryValidationError> {
    match value {
        serde_json::Value::Bool(_) => Ok(ConditionalLiteralKind::Boolean),
        serde_json::Value::Number(_) => Ok(ConditionalLiteralKind::Number),
        serde_json::Value::String(_) => Ok(ConditionalLiteralKind::String),
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(StructuredQueryValidationError::UnsupportedConditionalLiteral)
        }
    }
}
