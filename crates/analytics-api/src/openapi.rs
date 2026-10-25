use utoipa::OpenApi;

#[must_use]
pub fn build() -> utoipa::openapi::OpenApi {
    #[derive(OpenApi)]
    #[openapi(
        paths(
            crate::router::up,
            crate::router::ready,
            crate::router::health_status,
            crate::router::diagnostics,
            crate::router::list_operations,
            crate::router::submit_operation,
            crate::router::apply_tenant_range_purge,
            crate::router::register_range_purge_claim,
            crate::router::operation_status,
            crate::router::operation_audit,
            crate::router::cancel_operation,
            crate::router::manifest,
            crate::router::register_table,
            crate::router::register_tables,
            crate::router::unscoped_sql_query,
            crate::router::unscoped_structured_query,
            crate::router::tenant_query,
            crate::router::tenant_query_batch,
            crate::router::ingest_stream_record,
            crate::router::ingest_stream_record_batch,
            crate::router::openapi_json,
        ),
        components(schemas(
            analytics_contract::AnalyticsColumn,
            analytics_contract::AnalyticsManifest,
            analytics_contract::TenantRangePurgeEffectHttpRequest,
            analytics_contract::RangePurgeClaimHttpRequest,
            analytics_contract::TenantRangePurgeRequest,
            analytics_contract::TenantRangePurgeResponse,
            analytics_contract::ClusteringKey,
            analytics_contract::PartitionBucket,
            analytics_contract::PartitionKey,
            analytics_contract::PrimitiveColumnType,
            analytics_contract::ProjectionColumn,
            analytics_contract::QueryColumnComparison,
            analytics_contract::QueryComparisonOperator,
            analytics_contract::QueryConditionalBranch,
            analytics_contract::QueryDocumentPredicate,
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
            crate::http_contract::QueryBatchResponse,
            crate::http_contract::QueryBatchResult,
            crate::http_contract::QueryPlanShape,
            crate::http_contract::QueryResponse,
            crate::http_contract::TenantQueryBatchItem,
            crate::http_contract::TenantQueryBatchRequest,
            crate::http_contract::TenantQueryRequest,
            crate::http_contract::UnscopedSqlQueryRequest,
            crate::http_contract::UnscopedStructuredQueryRequest,
            crate::http_contract::IngestResponse,
            crate::http_contract::IngestBatchResponse,
            crate::http_contract::IngestStreamRecordBatchRequest,
            crate::http_contract::IngestStreamRecordRequest,
            crate::http_contract::IngestStreamRecordPayload,
            crate::http_contract::OperationAuditEventResponse,
            crate::http_contract::OperationAuditResponse,
            crate::http_contract::OperationCancelResponse,
            crate::http_contract::OperationCursorResponse,
            crate::http_contract::OperationListResponse,
            crate::http_contract::OperationStatusResponse,
            analytics_operations::MaintenanceSubmission,
            analytics_operations::MaintenanceSubmissionOutcome,
            analytics_operations::RateLimitPolicy,
            crate::http_contract::HttpMaintenanceRequest,
            crate::http_contract::StandardStreamEventRecord,
            crate::http_contract::StandardStreamStorageRecord,
            crate::http_contract::DiagnosticsResponse,
            crate::http_contract::SourceHealth,
            crate::http_contract::SourceHealthStatus,
            crate::http_contract::RetentionHealth,
            crate::http_contract::RetentionHealthStatus,
            crate::http_contract::CheckpointHealth,
            crate::http_contract::HealthResponse,
            crate::http_contract::ReadyResponse,
            crate::http_contract::ErrorResponse,
        )),
        tags(
            (name = "Health", description = "Service health and readiness"),
            (name = "Analytics", description = "Analytics manifest, ingestion, and query API"),
            (name = "Operations", description = "Durable operation status, audit, and control API"),
            (name = "OpenAPI", description = "OpenAPI document")
        )
    )]
    struct AnalyticsApi;

    AnalyticsApi::openapi()
}

#[must_use]
pub fn build_json() -> serde_json::Value {
    serde_json::to_value(build()).expect("OpenAPI document serializes")
}
