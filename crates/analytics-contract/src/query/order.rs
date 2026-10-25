use super::*;
use crate::SortOrder;

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
    #[schema(inline, nullable = true, default = "asc", example = "asc")]
    pub direction: Option<SortOrder>,
}

impl QueryOrder {
    pub(super) fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        self.expression.validate_shape()
    }
}
