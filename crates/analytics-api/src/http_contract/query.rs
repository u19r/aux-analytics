use analytics_contract::StructuredQuery;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request shape for the raw-SQL route. The HTTP API accepts tenant-scoped
/// structured queries for tenant-isolated reads.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "sql": "select email, org_id from users where org_id = 'org-a' order by email"
}))]
pub(crate) struct UnscopedSqlQueryRequest {
    /// Raw SQL text supplied to the route.
    #[schema(
        min_length = 1,
        max_length = 20000,
        example = "select email, org_id from users where org_id = 'org-a' order by email"
    )]
    pub sql: String,
}

/// Explicitly unscoped structured query request.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "query": {
        "analytics_table_name": "legacy_items",
        "select": [{"kind": "document_path", "document_column": "item", "path": "profile.email", "alias": "email"}],
        "filters": [],
        "group_by": [],
        "order_by": [],
        "limit": 100
    }
}))]
pub(crate) struct UnscopedStructuredQueryRequest {
    /// Structured query tree for explicitly unscoped data. The API does not
    /// inject a tenant predicate into this query.
    pub query: StructuredQuery,
}

/// Tenant-scoped structured query request for callers that should not
/// construct SQL.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "target_tenant_id": "tenant_01",
    "query": {
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
    }
}))]
pub(crate) struct TenantQueryRequest {
    /// Tenant id that the query is authorized to read. The API injects this as
    /// an engine-owned tenant predicate; callers must not rely on their own
    /// tenant filters for isolation.
    #[schema(min_length = 1, max_length = 255, example = "tenant_01")]
    pub target_tenant_id: String,
    /// Structured query tree compiled by the analytics engine.
    pub query: StructuredQuery,
}

/// Named tenant-scoped structured query for batch execution.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "name": "total_users",
    "query": {
        "analytics_table_name": "users",
        "select": [{"kind": "count", "alias": "count"}],
        "filters": [],
        "group_by": [],
        "order_by": [],
        "limit": 1
    }
}))]
pub(crate) struct TenantQueryBatchItem {
    /// Caller-owned stable name used to correlate results.
    #[schema(min_length = 1, max_length = 255, example = "total_users")]
    pub name: String,
    /// Structured query tree compiled by the analytics engine.
    pub query: StructuredQuery,
}

/// Tenant-scoped structured query batch request.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "target_tenant_id": "tenant_01",
    "queries": [{
        "name": "total_users",
        "query": {
            "analytics_table_name": "users",
            "select": [{"kind": "count", "alias": "count"}],
            "filters": [],
            "group_by": [],
            "order_by": [],
            "limit": 1
        }
    }]
}))]
pub(crate) struct TenantQueryBatchRequest {
    /// Tenant id that each query is authorized to read. The API injects this as
    /// an engine-owned tenant predicate.
    #[schema(min_length = 1, max_length = 255, example = "tenant_01")]
    pub target_tenant_id: String,
    /// Named structured queries to execute against the same tenant scope.
    #[schema(min_items = 1)]
    pub queries: Vec<TenantQueryBatchItem>,
}

/// Query result rows as JSON objects.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "rows": [{"email": "ada@example.com", "org_id": "org-a"}],
    "columns": [{"name": "email", "value_type": "string", "nullable": false}],
    "execution": {
        "query_hash": "sha256:example",
        "row_count": 1,
        "truncated": false,
        "tables": ["users"],
        "elapsed_ms": 7,
        "plan_shape": {
            "logical_source_count": 1,
            "join_count": 0,
            "filter_count": 1,
            "group_expression_count": 0,
            "order_expression_count": 0,
            "aggregate_count": 0,
            "conditional_expression_count": 0
        }
    },
    "source_watermark": {"max_occurred_at_ms": null, "max_ingested_at_ms": null}
}))]
pub struct QueryResponse {
    /// Result rows returned by `DuckDB`.
    #[schema(min_items = 0, example = json!([{"email": "ada@example.com", "org_id": "org-a"}]))]
    pub rows: Vec<serde_json::Value>,
    pub columns: Vec<QueryResultColumn>,
    pub execution: QueryExecutionMetadata,
    pub source_watermark: QuerySourceWatermark,
}

/// Named query result rows.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "name": "total_users",
    "rows": [{"count": 42}],
    "columns": [{"name": "count", "value_type": "i64", "nullable": false}],
    "execution": {
        "query_hash": "sha256:example",
        "row_count": 1,
        "truncated": false,
        "tables": ["users"],
        "elapsed_ms": 7,
        "plan_shape": {
            "logical_source_count": 1,
            "join_count": 0,
            "filter_count": 0,
            "group_expression_count": 0,
            "order_expression_count": 0,
            "aggregate_count": 1,
            "conditional_expression_count": 0
        }
    },
    "source_watermark": {"max_occurred_at_ms": null, "max_ingested_at_ms": null}
}))]
pub struct QueryBatchResult {
    /// Caller-owned stable name from the batch request.
    #[schema(example = "total_users")]
    pub name: String,
    /// Dynamic JSON rows returned by `DuckDB`.
    ///
    /// Query projections are selected at runtime, so a statically typed row
    /// would either lose arbitrary columns and nested values or change the
    /// serialized response contract.
    #[schema(min_items = 0, example = json!([{"count": 42}]))]
    pub rows: Vec<serde_json::Value>,
    pub columns: Vec<QueryResultColumn>,
    pub execution: QueryExecutionMetadata,
    pub source_watermark: QuerySourceWatermark,
}

#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryResultColumn {
    pub name: String,
    pub value_type: QueryResultValueType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryResultValueType {
    Boolean,
    String,
    I64,
    U64,
    Decimal,
    F64,
    Json,
    TimestampMs,
}

#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryExecutionMetadata {
    pub query_hash: String,
    pub row_count: u64,
    pub truncated: bool,
    pub tables: Vec<String>,
    pub elapsed_ms: u64,
    /// Sanitized logical shape derived from the validated structured query.
    /// This does not claim physical row-scan counts, which are backend and
    /// optimizer dependent.
    pub plan_shape: QueryPlanShape,
}

/// Bounded, literal-free logical query shape for performance evidence.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryPlanShape {
    pub logical_source_count: u32,
    pub join_count: u32,
    pub filter_count: u32,
    pub group_expression_count: u32,
    pub order_expression_count: u32,
    pub aggregate_count: u32,
    pub conditional_expression_count: u32,
}

#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QuerySourceWatermark {
    pub max_occurred_at_ms: Option<i64>,
    pub max_ingested_at_ms: Option<i64>,
}

/// Batch query result rows as JSON objects.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "results": [{"name": "total_users", "rows": [{"count": 42}]}]
}))]
pub(crate) struct QueryBatchResponse {
    /// Results in request order.
    #[schema(min_items = 0)]
    pub results: Vec<QueryBatchResult>,
}
