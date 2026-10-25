use super::*;

impl AnalyticsLambdaHandler {
    pub async fn handle_event(&self, payload: Value) -> Result<Value, AnalyticsLambdaError> {
        let response = self.handle_serialized_event(payload).await?;
        Ok(serde_json::from_str(response.get())?)
    }

    pub async fn handle_serialized_event(
        &self,
        payload: Value,
    ) -> Result<Box<RawValue>, AnalyticsLambdaError> {
        let event = validate_event(payload)?;
        match event {
            AnalyticsLambdaEvent::Init => serialize_response(InitResponse {
                initialized: true,
                table_count: self.manifest.tables.len(),
            }),
            AnalyticsLambdaEvent::UnscopedStructuredQuery { query } => {
                self.query_unscoped(query).await
            }
            AnalyticsLambdaEvent::TenantQuery {
                target_tenant_id,
                query,
            } => self.query_tenant(target_tenant_id, query).await,
            AnalyticsLambdaEvent::TenantQueryBatch {
                target_tenant_id,
                queries,
            } => {
                let results = self.query_tenant_batch(target_tenant_id, queries).await?;
                self.query_response(QueryBatchLambdaResponse { results })
            }
            AnalyticsLambdaEvent::MaintenanceSubmit { request, actor } => {
                self.submit_maintenance(request, actor)
            }
            AnalyticsLambdaEvent::Ingest {
                analytics_table_name,
                request,
            } => self.ingest_response(&analytics_table_name, *request).await,
            AnalyticsLambdaEvent::IngestBatch {
                analytics_table_name,
                records,
            } => self.ingest_batch(analytics_table_name, records).await,
        }
    }
}
