use utoipa::OpenApi;

#[allow(clippy::missing_panics_doc)]
#[must_use]
pub fn build() -> utoipa::openapi::OpenApi {
    #[derive(OpenApi)]
    #[openapi(
        paths(
            crate::router::up,
            crate::router::ready,
            crate::router::health_status,
            crate::router::diagnostics,
            crate::router::manifest,
            crate::router::unscoped_sql_query,
            crate::router::unscoped_structured_query,
            crate::router::tenant_query,
            crate::router::ingest_stream_record,
            crate::router::openapi_json,
        ),
        components(schemas(
            analytics_contract::AnalyticsColumn,
            analytics_contract::AnalyticsManifest,
            analytics_contract::ClusteringKey,
            analytics_contract::PartitionBucket,
            analytics_contract::PartitionKey,
            analytics_contract::PrimitiveColumnType,
            analytics_contract::ProjectionColumn,
            analytics_contract::QueryExpression,
            analytics_contract::QueryOrder,
            analytics_contract::QueryPredicate,
            analytics_contract::QuerySelect,
            analytics_contract::RetentionPolicy,
            analytics_contract::RetentionTimestamp,
            analytics_contract::RowIdentity,
            analytics_contract::SortOrder,
            analytics_contract::StorageStreamRecord,
            analytics_contract::StructuredQuery,
            analytics_contract::TableRegistration,
            analytics_contract::TenantSelector,
            crate::types::QueryResponse,
            crate::types::TenantQueryRequest,
            crate::types::UnscopedSqlQueryRequest,
            crate::types::UnscopedStructuredQueryRequest,
            crate::types::IngestResponse,
            crate::types::IngestStreamRecordRequest,
            crate::types::IngestStreamRecordPayload,
            crate::types::StandardStreamEventRecord,
            crate::types::StandardStreamStorageRecord,
            crate::types::DiagnosticsResponse,
            crate::types::SourceHealth,
            crate::types::SourceHealthStatus,
            crate::types::RetentionHealth,
            crate::types::RetentionHealthStatus,
            crate::types::CheckpointHealth,
            crate::types::HealthResponse,
            crate::types::ReadyResponse,
            crate::types::ErrorResponse,
        )),
        tags(
            (name = "Health", description = "Service health and readiness"),
            (name = "Analytics", description = "Analytics manifest, ingestion, and query API"),
            (name = "OpenAPI", description = "OpenAPI document")
        )
    )]
    struct AnalyticsApi;

    AnalyticsApi::openapi()
}

#[must_use]
pub fn build_json() -> serde_json::Value {
    let mut value = serde_json::to_value(build()).unwrap_or_else(|_| serde_json::json!({}));
    enrich_erased_object_fields(&mut value);
    value
}

#[allow(clippy::too_many_lines)]
fn enrich_erased_object_fields(spec: &mut serde_json::Value) {
    set_property_metadata(
        spec,
        "ProjectionColumn",
        "column_type",
        "Optional output column type. Defaults to VARCHAR when omitted.",
        serde_json::json!(null),
        serde_json::json!({"kind": "primitive", "primitive": "var_char"}),
    );
    set_property_metadata(
        spec,
        "TableRegistration",
        "expression_attribute_values",
        "Expression attribute values referenced by condition or projection expressions.",
        serde_json::json!(null),
        serde_json::json!({":active": {"BOOL": true}}),
    );
    set_property_metadata(
        spec,
        "TableRegistration",
        "tenant_selector",
        "Rule used to derive tenant id from records when tenant_id is not static.",
        serde_json::json!({"kind": "table_name"}),
        serde_json::json!({"kind": "attribute", "attribute_name": "tenant_id"}),
    );
    set_property_metadata(
        spec,
        "TableRegistration",
        "row_identity",
        "Rule used to derive the stable row id for upserts and deletes.",
        serde_json::json!({"kind": "record_key"}),
        serde_json::json!({"kind": "attribute", "attribute_name": "id"}),
    );
    set_property_metadata(
        spec,
        "StorageStreamRecord",
        "OldImage",
        "Previous source image for update and delete events.",
        serde_json::json!(null),
        serde_json::json!({
            "pk": {"S": "USER#user-1"},
            "profile": {"M": {"email": {"S": "old@example.com"}}}
        }),
    );
    set_property_metadata(
        spec,
        "StorageStreamRecord",
        "NewImage",
        "New source image for insert and update events.",
        serde_json::json!(null),
        serde_json::json!({
            "pk": {"S": "USER#user-1"},
            "profile": {"M": {"email": {"S": "ada@example.com"}}},
            "org_id": {"S": "org-a"}
        }),
    );
    set_property_metadata(
        spec,
        "StandardStreamStorageRecord",
        "OldImage",
        "Previous record image for update and delete events.",
        serde_json::json!(null),
        serde_json::json!({
            "pk": {"S": "USER#user-1"},
            "profile": {"M": {"email": {"S": "old@example.com"}}}
        }),
    );
    set_property_metadata(
        spec,
        "StandardStreamStorageRecord",
        "NewImage",
        "New record image for insert and update events.",
        serde_json::json!(null),
        serde_json::json!({
            "pk": {"S": "USER#user-1"},
            "profile": {"M": {"email": {"S": "ada@example.com"}}}
        }),
    );
    set_property_metadata(
        spec,
        "PartitionKey",
        "bucket",
        "Optional partition bucketing transform.",
        serde_json::json!(null),
        serde_json::json!({"kind": "hash", "buckets": 32}),
    );
    set_property_metadata(
        spec,
        "ClusteringKey",
        "order",
        "Sort direction. Defaults to ascending when omitted.",
        serde_json::json!("asc"),
        serde_json::json!("asc"),
    );
    set_property_metadata(
        spec,
        "QueryOrder",
        "direction",
        "Sort direction. Defaults to ascending when omitted.",
        serde_json::json!("asc"),
        serde_json::json!("asc"),
    );
}

fn set_property_metadata(
    spec: &mut serde_json::Value,
    schema: &str,
    property: &str,
    description: &str,
    default: serde_json::Value,
    example: serde_json::Value,
) {
    let Some(property_schema) = spec
        .pointer_mut(&format!(
            "/components/schemas/{schema}/properties/{property}"
        ))
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    property_schema
        .entry("description")
        .or_insert_with(|| serde_json::Value::String(description.to_string()));
    property_schema.entry("default").or_insert(default);
    property_schema.entry("example").or_insert(example);
}
