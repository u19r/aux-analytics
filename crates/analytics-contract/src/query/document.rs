use super::*;

/// String comparison supported inside a registered JSON document predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryStringOperator {
    Eq,
    NotEq,
    Contains,
    StartsWith,
    EndsWith,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Bounded predicate evaluated relative to a registered JSON document or one
/// element selected by an array-any predicate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryDocumentPredicate {
    Constant {
        value: bool,
    },
    Exists {
        /// Optional path relative to the document root. Omitted values test
        /// the document root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null))]
        path: Option<String>,
    },
    NonEmpty {
        /// Optional path relative to the document root. Omitted values test
        /// the document root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null))]
        path: Option<String>,
    },
    Compare {
        /// Optional path relative to the document root. Omitted values test
        /// the document root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null))]
        path: Option<String>,
        operator: QueryComparisonOperator,
        #[schema(value_type = serde_json::Value, example = true)]
        value: serde_json::Value,
    },
    String {
        /// Optional path relative to the document root. Omitted values test
        /// the document root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null))]
        path: Option<String>,
        operator: QueryStringOperator,
        value: String,
        /// Whether string matching is case-sensitive. The default is false.
        #[serde(default)]
        #[schema(default = false)]
        case_sensitive: bool,
    },
    TimestampString {
        /// Optional path relative to the document root. Omitted values test
        /// the document root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null))]
        path: Option<String>,
        operator: QueryStringOperator,
        value: String,
    },
    AffixedString {
        /// Optional path relative to the document root. Omitted values test
        /// the document root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null))]
        path: Option<String>,
        operator: QueryStringOperator,
        value: String,
        prefix: String,
        suffix: String,
        /// Whether string matching is case-sensitive. The default is false.
        #[serde(default)]
        #[schema(default = false)]
        case_sensitive: bool,
    },
    All {
        #[schemars(length(min = 1, max = 32))]
        #[schema(min_items = 1, max_items = 32)]
        #[schema(value_type = Vec<Object>)]
        predicates: Vec<QueryDocumentPredicate>,
    },
    Any {
        #[schemars(length(min = 1, max = 32))]
        #[schema(min_items = 1, max_items = 32)]
        #[schema(value_type = Vec<Object>)]
        predicates: Vec<QueryDocumentPredicate>,
    },
    Not {
        #[schema(value_type = Object)]
        predicate: Box<QueryDocumentPredicate>,
    },
    ArrayAny {
        /// Optional path relative to the document root. Omitted values test
        /// the document root.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null))]
        path: Option<String>,
        #[schema(value_type = Object)]
        predicate: Box<QueryDocumentPredicate>,
    },
}

impl QueryDocumentPredicate {
    fn validate_shape(
        &self,
        depth: usize,
        nodes: &mut usize,
    ) -> Result<(), StructuredQueryValidationError> {
        if depth >= MAX_DOCUMENT_PREDICATE_DEPTH {
            return Err(StructuredQueryValidationError::DocumentPredicateTooDeep {
                maximum: MAX_DOCUMENT_PREDICATE_DEPTH,
            });
        }
        *nodes = nodes.saturating_add(1);
        if *nodes > MAX_DOCUMENT_PREDICATE_NODES {
            return Err(
                StructuredQueryValidationError::TooManyDocumentPredicateNodes {
                    actual: *nodes,
                    maximum: MAX_DOCUMENT_PREDICATE_NODES,
                },
            );
        }
        match self {
            Self::Constant { .. } => Ok(()),
            Self::Exists { path }
            | Self::NonEmpty { path }
            | Self::Compare { path, .. }
            | Self::String { path, .. }
            | Self::TimestampString { path, .. }
            | Self::AffixedString { path, .. } => validate_optional_document_path(path.as_deref()),
            Self::All { predicates } | Self::Any { predicates } => {
                validate_document_predicate_branches(predicates)?;
                for predicate in predicates {
                    predicate.validate_shape(depth + 1, nodes)?;
                }
                Ok(())
            }
            Self::Not { predicate } | Self::ArrayAny { predicate, .. } => {
                if let Self::ArrayAny { path, .. } = self {
                    validate_optional_document_path(path.as_deref())?;
                }
                predicate.validate_shape(depth + 1, nodes)
            }
        }
    }
}

fn validate_document_predicate_branches(
    predicates: &[QueryDocumentPredicate],
) -> Result<(), StructuredQueryValidationError> {
    if predicates.is_empty() {
        return Err(StructuredQueryValidationError::EmptyDocumentPredicate);
    }
    if predicates.len() > MAX_DOCUMENT_PREDICATE_BRANCHES {
        return Err(
            StructuredQueryValidationError::TooManyDocumentPredicateBranches {
                actual: predicates.len(),
                maximum: MAX_DOCUMENT_PREDICATE_BRANCHES,
            },
        );
    }
    Ok(())
}

fn validate_optional_document_path(
    path: Option<&str>,
) -> Result<(), StructuredQueryValidationError> {
    path.map_or(Ok(()), validate_query_path)
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
    /// Predicate over one registered JSON document column.
    DocumentMatches {
        /// Optional source table alias. Omitted values resolve against the
        /// registered table name when the document column is unambiguous.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255, example = "u")]
        table_alias: Option<String>,
        document_column: String,
        #[schema(no_recursion)]
        predicate: QueryDocumentPredicate,
    },
}

impl QueryPredicate {
    pub(super) fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        match self {
            Self::Eq { expression, .. }
            | Self::NotEq { expression, .. }
            | Self::Gt { expression, .. }
            | Self::Gte { expression, .. }
            | Self::Lt { expression, .. }
            | Self::Lte { expression, .. }
            | Self::IsNull { expression }
            | Self::IsNotNull { expression } => expression.validate_shape(),
            Self::DocumentMatches {
                table_alias,
                document_column,
                predicate,
            } => {
                validate_optional_query_identifier(
                    "document predicate table_alias",
                    table_alias.as_deref(),
                )?;
                validate_query_identifier("document predicate document_column", document_column)?;
                let mut nodes = 0;
                predicate.validate_shape(0, &mut nodes)
            }
        }
    }
}
