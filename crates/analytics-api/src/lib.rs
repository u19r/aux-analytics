mod engine_access;
mod openapi;
mod query_response;
mod request_validation;
pub mod retention;
mod router;
mod types;

#[cfg(test)]
mod engine_access_tests;

pub use engine_access::{AnalyticsEngineAccess, AnalyticsEngineAccessError};
pub use openapi::{build as build_openapi, build_json as build_openapi_json};
pub use query_response::{QueryResponseBuildError, build_query_batch_result, build_query_response};
pub use router::{
    EndpointConfig, MetricsEndpointConfig, PrometheusMetricsEndpointConfig, router,
    router_with_config, server_router, server_router_with_config, server_router_with_metrics,
};
pub use types::{
    AppState, CheckpointHealth, IngestStreamRecordRequest, QueryBatchResult, QueryPlanShape,
    QueryResponse, RetentionHealth, RetentionHealthStatus, SourceHealth, SourceHealthStatus,
    SourcePollingPhase, TableLagHealth,
};
