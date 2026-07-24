mod cli;
mod engine_access;
mod error;
mod openapi;
mod query_response;
mod request_validation;
pub mod retention;
mod router;
mod runtime_config;
mod server;
mod source_polling;
mod types;

#[cfg(test)]
mod engine_access_tests;
#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod performance_tests;
#[cfg(test)]
mod quint_query_security_tests;
#[cfg(test)]
mod quint_source_polling_tests;
#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod server_tests;
#[cfg(test)]
mod source_polling_tests;

use clap::Parser as _;
pub use engine_access::{AnalyticsEngineAccess, AnalyticsEngineAccessError};
pub use error::{ApiError, ApiResult};
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

pub fn run() -> ApiResult<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(async_worker_threads())
        .enable_all()
        .build()?
        .block_on(server::serve(cli::ApiCli::parse()))
}

fn async_worker_threads() -> usize {
    async_worker_threads_for(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(2),
    )
}

fn async_worker_threads_for(available: usize) -> usize {
    available.max(2)
}
