use analytics_contract::{AnalyticsManifest, PrivacyPolicy, SourcePosition, StorageStreamRecord};

use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestExecution, IngestOutcome, IngestRetention,
    PrivacyIngestOutcome, filter_stream_record, immediate_ingest::StreamRecordIngest,
    source_record_authority::SourceRecordAuthority,
};

impl AnalyticsEngine {
    pub fn ingest_stream_record_with_privacy_policy(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        policy: &PrivacyPolicy,
    ) -> AnalyticsEngineResult<PrivacyIngestOutcome> {
        self.ingest_stream_record_with_privacy_policy_and_retention(
            manifest,
            analytics_table_name,
            record_key,
            record,
            policy,
            None,
        )
    }

    pub fn ingest_stream_record_with_privacy_policy_and_retention(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        policy: &PrivacyPolicy,
        resolved_retention: Option<&IngestRetention>,
    ) -> AnalyticsEngineResult<PrivacyIngestOutcome> {
        let source_record_keys = record.keys.clone();
        let matcher = policy.validated()?;
        let filtered = filter_stream_record(record, &matcher);
        let outcome = self.ingest_record_atomically(StreamRecordIngest {
            manifest,
            analytics_table_name,
            record_key,
            record: filtered.record,
            execution: IngestExecution {
                resolved_retention,
                source_record_keys: Some(source_record_keys),
                ..IngestExecution::default()
            },
        })?;
        Ok(PrivacyIngestOutcome {
            outcome,
            dropped_fields: filtered.dropped_fields,
            policy_version: policy.version.clone(),
        })
    }

    pub fn ingest_stream_record_at_source_position(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        source_position: &SourcePosition,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.ingest_record_atomically(StreamRecordIngest {
            manifest,
            analytics_table_name,
            record_key,
            record,
            execution: IngestExecution {
                authority: SourceRecordAuthority::Positioned(source_position),
                ..IngestExecution::default()
            },
        })
    }

    pub fn ingest_stream_record_with_retention(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        resolved_retention: Option<&IngestRetention>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.ingest_record_atomically(StreamRecordIngest {
            manifest,
            analytics_table_name,
            record_key,
            record,
            execution: IngestExecution {
                resolved_retention,
                ..IngestExecution::default()
            },
        })
    }
}
