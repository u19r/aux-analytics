use analytics_contract::{AnalyticsManifest, StorageStreamRecord};

use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestExecution, IngestOutcome, IngestRetention,
    current_time_millis, source_record_authority::SourceRecordAuthority, static_retention,
};
use crate::row::RowSource;

#[derive(Clone, Copy)]
pub(super) struct ImmediateIngestRequest<'a> {
    pub table: &'a analytics_contract::TableRegistration,
    pub source: RowSource<'a>,
    pub ingested_at_ms: i64,
    pub effective_retention: Option<&'a IngestRetention>,
    pub authority: SourceRecordAuthority<
        &'a analytics_contract::SourcePosition,
        &'a super::SourceRecordMutationPlan,
    >,
}

pub(super) struct StreamRecordIngest<'a> {
    pub manifest: &'a AnalyticsManifest,
    pub analytics_table_name: &'a str,
    pub record_key: &'a [u8],
    pub record: StorageStreamRecord,
    pub execution: IngestExecution<'a>,
}

impl AnalyticsEngine {
    pub(super) fn apply_registered_stream_record(
        &self,
        table: &analytics_contract::TableRegistration,
        request: StreamRecordIngest<'_>,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        if table.row_expansion.is_some() {
            return self.ingest_expanded_registered_record(table, request, writes);
        }
        let StreamRecordIngest {
            record_key,
            record,
            execution,
            ..
        } = request;
        let IngestExecution {
            resolved_retention,
            authority,
            source_record_keys,
            source_record_table_name,
        } = execution;
        let ingested_at_ms = current_time_millis();
        let manifest_retention: Option<IngestRetention>;
        let effective_retention = if resolved_retention.is_some() {
            resolved_retention
        } else {
            manifest_retention = static_retention(table);
            manifest_retention.as_ref()
        };
        let owner_source_table_name =
            source_record_table_name.unwrap_or(table.source_table_name.as_str());
        let source = RowSource {
            stream_key_digest: authority
                .owner()
                .map(|(_, plan)| plan.source_key_digest.as_str()),
            source_table_name: owner_source_table_name,
            record_key,
            record_keys: source_record_keys.as_ref().unwrap_or(&record.keys),
        };
        let request = ImmediateIngestRequest {
            table,
            source,
            ingested_at_ms,
            effective_retention,
            authority,
        };
        match (record.old_image, record.new_image) {
            (None, Some(new_image)) => self.ingest_insert_image(request, new_image, writes),
            (Some(old_image), Some(new_image)) => {
                self.ingest_update_images(request, old_image, new_image, writes)
            }
            (Some(old_image), None) => self.ingest_delete_image(request, old_image, writes),
            (None, None) => self.advance_empty_record(request),
        }
    }
    fn ingest_expanded_registered_record(
        &self,
        table: &analytics_contract::TableRegistration,
        request: StreamRecordIngest<'_>,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let StreamRecordIngest {
            record_key,
            record,
            execution,
            ..
        } = request;
        let IngestExecution {
            resolved_retention: effective_retention,
            authority,
            source_record_keys,
            source_record_table_name,
        } = execution;
        let manifest_retention = static_retention(table);
        let effective_retention = effective_retention.or(manifest_retention.as_ref());
        let owner_source_table_name = source_record_table_name.unwrap_or(&table.source_table_name);
        self.apply_expanded_stream_record(
            &record,
            super::expanded_ingest::ExpandedIngestRequest {
                table,
                source: RowSource {
                    source_table_name: owner_source_table_name,
                    record_key,
                    record_keys: source_record_keys.as_ref().unwrap_or(&record.keys),
                    stream_key_digest: authority
                        .owner()
                        .map(|(_, plan)| plan.source_key_digest.as_str()),
                },
                retention: effective_retention,
                authority,
            },
            writes,
        )
    }
}
