use std::sync::Arc;

use analytics_api::IngestStreamRecordRequest;
use analytics_engine::IngestOutcome;

use super::{
    AnalyticsLambdaError, AnalyticsLambdaHandler, IngestBatchLambdaResponse, serialize_response,
};

impl AnalyticsLambdaHandler {
    pub(super) async fn ingest_one(
        &self,
        analytics_table_name: &str,
        request: IngestStreamRecordRequest,
    ) -> Result<IngestOutcome, AnalyticsLambdaError> {
        let (record_key, record) = request.into_contract_record();
        if let Some(policy) = self.privacy_policy.as_ref() {
            let manifest = Arc::clone(&self.manifest);
            let policy = Arc::clone(policy);
            let analytics_table_name = analytics_table_name.to_owned();
            return self
                .engine
                .with_write(move |engine| {
                    engine.ingest_stream_record_with_privacy_policy(
                        manifest.as_ref(),
                        analytics_table_name.as_str(),
                        record_key.as_bytes(),
                        record,
                        &policy,
                    )
                })
                .await
                .map(|outcome| outcome.outcome)
                .map_err(AnalyticsLambdaError::ingest);
        }
        let manifest = Arc::clone(&self.manifest);
        let analytics_table_name = analytics_table_name.to_owned();
        self.engine
            .with_write(move |engine| {
                engine.ingest_stream_record(
                    manifest.as_ref(),
                    analytics_table_name.as_str(),
                    record_key.as_bytes(),
                    record,
                )
            })
            .await
            .map_err(AnalyticsLambdaError::ingest)
    }
}

impl AnalyticsLambdaHandler {
    pub(super) fn ensure_mutation_allowed(&self) -> Result<(), AnalyticsLambdaError> {
        if self.response_config.query_only {
            return Err(AnalyticsLambdaError::validation(
                "mutation operations are disabled when analytics.query.lambda_query_only is true",
            ));
        }
        Ok(())
    }

    pub(super) fn ensure_ingest_allowed(&self) -> Result<(), AnalyticsLambdaError> {
        self.ensure_mutation_allowed()?;
        if !self.ingest_enabled {
            return Err(AnalyticsLambdaError::validation(
                "ingest operations are disabled by analytics ingest route configuration",
            ));
        }
        Ok(())
    }

    pub(super) async fn ingest_batch(
        &self,
        analytics_table_name: String,
        records: Vec<analytics_api::IngestStreamRecordRequest>,
    ) -> Result<Box<serde_json::value::RawValue>, AnalyticsLambdaError> {
        self.ensure_ingest_allowed()?;
        let mut outcomes = Vec::with_capacity(records.len());
        for request in records {
            let outcome = self
                .ingest_one(analytics_table_name.as_str(), request)
                .await?;
            outcomes.push(super::ingest_outcome_name(outcome));
        }
        serialize_response(IngestBatchLambdaResponse {
            processed: outcomes.len(),
            outcomes,
        })
    }

    pub(super) async fn ingest_response(
        &self,
        table_name: &str,
        request: analytics_api::IngestStreamRecordRequest,
    ) -> Result<Box<serde_json::value::RawValue>, AnalyticsLambdaError> {
        self.ensure_ingest_allowed()?;
        let outcome = self.ingest_one(table_name, request).await?;
        super::serialize_response(super::IngestLambdaResponse {
            outcome: super::ingest_outcome_name(outcome),
        })
    }
}
