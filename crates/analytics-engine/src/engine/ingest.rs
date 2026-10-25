use analytics_contract::{AnalyticsManifest, StorageStreamRecord};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, IngestExecution, IngestOutcome,
    SourceBatchCommit, SourceBatchExecution, SourceBatchRecord, StreamRecordBatchItem,
    immediate_ingest::StreamRecordIngest, transaction::EngineTransaction,
};

impl AnalyticsEngine {
    pub fn ingest_stream_record(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.ingest_stream_record_with_retention(
            manifest,
            analytics_table_name,
            record_key,
            record,
            None,
        )
    }

    /// Exercises the real ingestion path for a representative serialized
    /// record without retaining the resulting row.
    ///
    /// Unlike normal ingestion, a record excluded by its registration
    /// condition is a contract error. This lets source adapters table-drive
    /// tests over every emitted analytics table and catch registration drift
    /// before deployment.
    pub fn validate_ingest_contract(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
    ) -> AnalyticsEngineResult<()> {
        let mut transaction = EngineTransaction::begin(self)?;
        let result = transaction.ingest_record(StreamRecordIngest {
            manifest,
            analytics_table_name,
            record_key,
            record,
            execution: IngestExecution::default(),
        });
        transaction.rollback()?;
        match result? {
            IngestOutcome::Skipped => Err(AnalyticsEngineError::RecordDoesNotMatchRegistration(
                analytics_table_name.to_string(),
            )),
            IngestOutcome::Inserted | IngestOutcome::Updated | IngestOutcome::Deleted => Ok(()),
        }
    }

    /// Ingests a direct HTTP/Lambda batch without source-polling checkpoint
    /// state. Background source jobs must use
    /// [`Self::ingest_fenced_source_batch`].
    pub fn ingest_stream_record_batch(
        &self,
        manifest: &AnalyticsManifest,
        records: Vec<StreamRecordBatchItem>,
    ) -> AnalyticsEngineResult<Vec<IngestOutcome>> {
        let source_records = records
            .into_iter()
            .map(|item| SourceBatchRecord {
                item,
                retention: None,
            })
            .collect::<Vec<_>>();
        match self.ingest_source_batch_core(SourceBatchExecution {
            manifest,
            records: source_records,
            checkpoints: Vec::new(),
            policy: None,
            fence: None,
        })? {
            SourceBatchCommit::Applied { outcome, .. } => Ok(outcome.outcomes),
            SourceBatchCommit::AlreadyApplied { .. } | SourceBatchCommit::StaleFence => {
                Err(AnalyticsEngineError::IncompleteSourceEffectState)
            }
        }
    }
}
