pub(crate) mod background_jobs;
mod cli;
mod clock;
mod constants;
mod engine_access;
mod error;
mod http_contract;
mod openapi;
mod query_response;
mod request_validation;
pub mod retention;
mod router;
mod run;
mod runtime_config;
mod server;
mod source_polling;

#[cfg(test)]
mod background_jobs_runtime_tests;
#[cfg(test)]
mod engine_access_tests;
#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod openapi_tests;
#[cfg(test)]
mod query_batch_tests;
#[cfg(test)]
mod query_response_tests;
#[cfg(test)]
mod quint_query_security_tests;
#[cfg(test)]
mod quint_source_polling_tests;
#[cfg(test)]
mod request_validation_tests;
#[cfg(test)]
mod retention_tests;
#[cfg(test)]
mod router_tests;
#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod server_tests;
#[cfg(test)]
mod source_health_performance_tests;
#[cfg(test)]
mod source_polling_tests;

pub use engine_access::{AnalyticsEngineAccess, AnalyticsEngineAccessError};
pub use error::{ApiError, ApiResult};
pub use http_contract::{
    AppState, CheckpointHealth, IngestStreamRecordRequest, QueryBatchResult, QueryPlanShape,
    QueryResponse, RetentionHealth, RetentionHealthStatus, SourceHealth, SourceHealthStatus,
    SourcePollingPhase, TableLagHealth,
};
pub use openapi::{build as build_openapi, build_json as build_openapi_json};
pub use router::{
    EndpointConfig, MetricsEndpointConfig, router, router_with_config, server_router,
    server_router_with_config, server_router_with_metrics,
};
pub use run::run;

#[cfg(feature = "bdd-test-support")]
#[doc(hidden)]
pub mod bdd_test_support;
