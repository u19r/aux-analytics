use analytics_contract::StorageStreamRecord;
use analytics_engine::IngestRetention;

use super::*;

impl AppState {
    pub(super) async fn ingest_record_into_table(
        &self,
        analytics_table_name: &str,
        request: IngestStreamRecordRequest,
    ) -> Result<IngestResponse, AnalyticsEngineError> {
        let (record_key, record) = request.into_contract_record();
        let manifest = Arc::clone(&*self.manifest.read().await);
        let retention = self
            .find_ingest_retention(&manifest, analytics_table_name, &record)
            .await;
        let table_name = analytics_table_name.to_owned();
        let policy = self.privacy_policy.clone();
        let response = self
            .engine
            .with_write(move |engine| {
                if let Some(policy) = policy {
                    engine
                        .ingest_stream_record_with_privacy_policy_and_retention(
                            &manifest,
                            &table_name,
                            record_key.as_bytes(),
                            record,
                            policy.as_ref(),
                            retention.as_ref(),
                        )
                        .map(IngestResponse::from)
                } else {
                    engine
                        .ingest_stream_record_with_retention(
                            &manifest,
                            &table_name,
                            record_key.as_bytes(),
                            record,
                            retention.as_ref(),
                        )
                        .map(IngestResponse::from)
                }
            })
            .await?;
        response.record_privacy_drops();
        Ok(response)
    }

    async fn find_ingest_retention(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record: &StorageStreamRecord,
    ) -> Option<IngestRetention> {
        let runtime = self.retention.as_ref()?;
        let source_table_name = manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == analytics_table_name)
            .map_or("", |table| table.source_table_name.as_str());
        runtime
            .retention_for_record(manifest, analytics_table_name, source_table_name, record)
            .await
    }

    pub(super) fn validate_ingest_batch(
        &self,
        request: &IngestStreamRecordBatchRequest,
        started: Instant,
    ) -> Result<(), &'static str> {
        if request.records.is_empty() {
            record_ingest_metrics("validation_error", started);
            return Err("batch ingestion requires at least one record");
        }
        if self.privacy_policy.is_some() || self.retention.is_some() {
            record_ingest_metrics("unsupported", started);
            return Err(
                "batch ingestion is only supported when privacy filtering and retention lookup \
                 are disabled",
            );
        }
        Ok(())
    }

    pub(super) async fn ingest_record_batch_into_table(
        &self,
        analytics_table_name: &str,
        request: IngestStreamRecordBatchRequest,
    ) -> Result<IngestBatchResponse, AnalyticsEngineError> {
        let manifest = Arc::clone(&*self.manifest.read().await);
        let source_table_name = manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == analytics_table_name)
            .map(|table| table.source_table_name.clone())
            .unwrap_or_else(|| analytics_table_name.to_owned());
        let records = request
            .records
            .into_iter()
            .map(|request| {
                let (record_key, record) = request.into_contract_record();
                StreamRecordBatchItem {
                    analytics_table_name: analytics_table_name.to_owned(),
                    source_table_name: source_table_name.clone(),
                    shard_id: None,
                    record_key: record_key.into_bytes(),
                    record,
                    system_position: None,
                }
            })
            .collect();
        let outcomes = self
            .engine
            .with_write(move |engine| engine.ingest_stream_record_batch(&manifest, records))
            .await?;
        Ok(IngestBatchResponse {
            record_count: outcomes.len(),
            outcomes: outcomes.into_iter().map(IngestResponse::from).collect(),
        })
    }
}
