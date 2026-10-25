use std::{fmt, sync::Arc, time::Instant};

use analytics_contract::{AnalyticsManifest, TableRegistration};
use analytics_engine::{
    AnalyticsEngineError, IngestOutcome, StreamRecordBatchItem, prepare_tenant_structured_query,
    prepare_unscoped_structured_query,
};
use analytics_operations::{
    MaintenanceOperations, OperationActor, OperationId, OperationStatus, OperationStore,
};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::Value;

use crate::{
    AnalyticsEngineAccess, AnalyticsEngineAccessError,
    constants::{
        INGEST_ERROR_RECORD_CONTRACT, INGEST_ERROR_RETRYABLE, INGEST_LATENCY_METRIC,
        INGEST_PRIVACY_DROPS_METRIC, INGEST_REQUESTS_METRIC, MAX_TENANT_QUERY_BATCH_ITEMS,
        QUERY_LATENCY_METRIC, QUERY_REQUESTS_METRIC,
    },
    http_contract::{
        AppState, DiagnosticsResponse, ErrorResponse, HealthResponse, HttpMaintenanceRequest,
        IngestBatchResponse, IngestResponse, IngestStreamRecordBatchRequest,
        IngestStreamRecordRequest, OperationAuditEventResponse, OperationAuditResponse,
        OperationCancelResponse, OperationDiagnostics, OperationListResponse,
        OperationStatusResponse, QueryBatchResponse, QueryBatchResult, QueryResponse,
        ReadyResponse, TenantQueryBatchItem, TenantQueryBatchRequest, TenantQueryRequest,
        UnscopedSqlQueryRequest, UnscopedStructuredQueryRequest,
    },
    request_validation::{RequestValidators, ValidatedJson},
};

mod health;
mod ingest_execution;
mod ingestion;
mod operations;
mod query;
mod range_purge;
#[cfg(test)]
mod range_purge_tests;
mod response;

pub(crate) use health::{
    __path_diagnostics, __path_health_status, __path_ready, __path_up, diagnostics, health_status,
    ready, up,
};
pub(crate) use ingestion::{
    __path_ingest_stream_record, __path_ingest_stream_record_batch, __path_manifest,
    __path_register_table, __path_register_tables, ingest_stream_record,
    ingest_stream_record_batch, manifest, register_table, register_tables,
};
pub(crate) use operations::{
    __path_cancel_operation, __path_list_operations, __path_operation_audit,
    __path_operation_status, __path_submit_operation, cancel_operation, list_operations,
    operation_audit, operation_status, submit_operation,
};
pub(crate) use query::{
    __path_tenant_query, __path_tenant_query_batch, __path_unscoped_sql_query,
    __path_unscoped_structured_query, tenant_query, tenant_query_batch, unscoped_sql_query,
    unscoped_structured_query,
};
pub(crate) use response::{
    __path_openapi_json, OperationApiError, error_response, error_response_with_code, openapi_json,
    operation_response_for_id, operation_store_response, prometheus_metrics_router,
    query_error_response, record_ingest_metrics, record_query_metrics,
};

mod routing;
pub(crate) use range_purge::{
    __path_apply_tenant_range_purge, __path_register_range_purge_claim, apply_tenant_range_purge,
    register_range_purge_claim,
};
pub use routing::{
    EndpointConfig, MetricsEndpointConfig, router, router_with_config, server_router,
    server_router_with_config, server_router_with_metrics,
};
