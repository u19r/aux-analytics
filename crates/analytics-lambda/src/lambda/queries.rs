use std::{sync::Arc, time::Instant};

use analytics_api::{AnalyticsEngineAccess, QueryBatchResult, QueryResponse};
use analytics_engine::{
    AnalyticsEngineError, prepare_tenant_structured_query, prepare_unscoped_structured_query,
};
use serde_json::value::RawValue;

use super::{AnalyticsLambdaError, AnalyticsLambdaHandler, TenantQueryBatchItem};

impl AnalyticsLambdaHandler {
    pub(super) async fn query_unscoped(
        &self,
        query: analytics_contract::StructuredQuery,
    ) -> Result<Box<RawValue>, AnalyticsLambdaError> {
        let started = Instant::now();
        let manifest = Arc::clone(&self.manifest);
        let (rows, prepared) = self
            .engine
            .with_read(move |engine| {
                let prepared = prepare_unscoped_structured_query(manifest.as_ref(), &query)?;
                let rows = engine.query_prepared_structured_json(&prepared)?;
                Ok::<_, AnalyticsEngineError>((rows, prepared))
            })
            .await??;
        self.query_response(QueryResponse::new(rows, prepared.metadata(), started))
    }

    pub(super) async fn query_tenant(
        &self,
        target_tenant_id: String,
        query: analytics_contract::StructuredQuery,
    ) -> Result<Box<RawValue>, AnalyticsLambdaError> {
        let started = Instant::now();
        let response_config = self.response_config;
        let manifest = Arc::clone(&self.manifest);
        let (rows, prepared) = self
            .engine
            .with_read(move |engine| {
                let prepared = prepare_tenant_structured_query(
                    manifest.as_ref(),
                    &query,
                    target_tenant_id.as_str(),
                )?;
                let rows = if response_config.disable_query_deadline {
                    engine.query_prepared_structured_json_without_timeout(&prepared)?
                } else {
                    engine.query_prepared_structured_json(&prepared)?
                };
                Ok::<_, AnalyticsEngineError>((rows, prepared))
            })
            .await??;
        self.query_response(QueryResponse::new(rows, prepared.metadata(), started))
    }

    pub(super) async fn query_tenant_batch(
        &self,
        target_tenant_id: String,
        queries: Vec<TenantQueryBatchItem>,
    ) -> Result<Vec<QueryBatchResult>, AnalyticsLambdaError> {
        AnalyticsEngineAccess::execute_query_batch(queries, |query, cancellation| {
            let engine = self.engine.clone();
            let manifest = Arc::clone(&self.manifest);
            let target_tenant_id = target_tenant_id.clone();
            let response_config = self.response_config;
            async move {
                let started = Instant::now();
                let (rows, prepared) = engine
                    .with_cancellable_read(cancellation, move |engine| {
                        let prepared = prepare_tenant_structured_query(
                            manifest.as_ref(),
                            &query.query,
                            target_tenant_id.as_str(),
                        )?;
                        let rows = if response_config.disable_query_deadline {
                            engine.query_prepared_structured_json_without_timeout(&prepared)?
                        } else {
                            engine.query_prepared_structured_json(&prepared)?
                        };
                        Ok::<_, AnalyticsEngineError>((rows, prepared))
                    })
                    .await??;
                let response =
                    QueryBatchResult::new(query.name, rows, prepared.metadata(), started);
                Ok::<_, AnalyticsLambdaError>(response)
            }
        })
        .await
    }
}
