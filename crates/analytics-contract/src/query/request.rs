use super::*;

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
    /// Optional table alias. Omitted values use the registered table name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, min_length = 1, max_length = 255, example = "users")]
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
    /// Optional row limit in rows. Omitted values return up to 1,000 rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(
        nullable = true,
        default = 1000,
        minimum = 1,
        maximum = 100_000,
        example = 100
    )]
    pub limit: Option<u32>,
    /// Optional number of matching rows to skip before applying `limit`. An
    /// omitted offset starts at the first matching row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(
        nullable = true,
        default = 0,
        minimum = 0,
        maximum = 100_000,
        example = 0
    )]
    pub offset: Option<u32>,
}
