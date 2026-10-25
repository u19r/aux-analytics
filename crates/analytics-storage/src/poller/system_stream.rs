use storage_types::{GetStreamRecordsResponse, StreamRecord, SystemStreamCheckpointToken};

use super::state::{AuxStorageGlobalPoller, empty_poll_batch, logical_record_key};
use crate::{
    error::{AnalyticsStorageError, AnalyticsStorageErrorKind, AnalyticsStorageResult},
    source_batch::{PollBatch, PolledRecord, SystemStreamBatch},
};

impl AuxStorageGlobalPoller {
    pub(crate) async fn poll_once(
        &mut self,
        max_responses: usize,
    ) -> AnalyticsStorageResult<PollBatch> {
        if max_responses == 0 {
            return Ok(empty_poll_batch());
        }
        let response = self
            .client
            .get_system_stream_records(
                self.source_scope.clone(),
                self.scan_checkpoint.clone(),
                self.max_records_per_response,
            )
            .await?;
        let Some(response_checkpoint) = self.validate_response_checkpoint(&response)? else {
            return Ok(empty_poll_batch());
        };
        self.serialization_buffer.clear();
        serde_json::to_writer(&mut self.serialization_buffer, response.records())?;
        let source_encoded_bytes = self.serialization_buffer.len();
        let source_record_count = response.records().len();
        let (source_records, _) = response.into_parts();
        let records = self.route_system_records(source_records)?;
        let system_stream = if records.is_empty() {
            self.scan_checkpoint = Some(response_checkpoint);
            None
        } else {
            Some(SystemStreamBatch {
                source_scope: self.source_scope.clone(),
                previous_checkpoint: self.checkpoint.clone(),
                checkpoint: response_checkpoint,
            })
        };
        Ok(PollBatch {
            source_response_count: 1,
            source_nonempty_response_count: usize::from(source_record_count != 0),
            source_record_count,
            source_encoded_bytes,
            records,
            checkpoints: Vec::new(),
            source_predecessor: None,
            system_stream,
        })
    }

    fn validate_response_checkpoint(
        &self,
        response: &GetStreamRecordsResponse,
    ) -> AnalyticsStorageResult<Option<SystemStreamCheckpointToken>> {
        let Some(response_scope) = response.source_scope() else {
            return Err(AnalyticsStorageError::new(
                AnalyticsStorageErrorKind::MissingResponseScope,
            ));
        };
        if response_scope != &self.source_scope {
            return Err(AnalyticsStorageError::new(
                AnalyticsStorageErrorKind::ResponseScopeMismatch,
            ));
        }
        let Some(response_checkpoint) = response.checkpoint().cloned() else {
            if response.records().is_empty() {
                return Ok(None);
            }
            return Err(AnalyticsStorageError::new(
                AnalyticsStorageErrorKind::MissingResponseCheckpoint,
            ));
        };
        Ok(Some(response_checkpoint))
    }

    fn route_system_records(
        &self,
        source_records: Vec<StreamRecord>,
    ) -> AnalyticsStorageResult<Vec<PolledRecord>> {
        let mut records = Vec::new();
        for mut source_record in source_records {
            let source_table_name = source_record.source_table_name.take().ok_or_else(|| {
                AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingRecordSourceTable)
            })?;
            let system_position = source_record.system_position.take().ok_or_else(|| {
                AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingRecordSystemPosition)
            })?;
            let analytics_table_names = self
                .routes
                .analytics_tables_for_source(source_table_name.as_ref())?;
            if analytics_table_names.is_empty() {
                continue;
            }
            records.push(PolledRecord {
                source_table_name: source_table_name.to_string(),
                shard_id: system_position.shard_id().as_str().to_string(),
                analytics_table_names: analytics_table_names.to_vec(),
                record_key: logical_record_key(&system_position),
                record: crate::stream_conversion::convert_stream_record(source_record),
                system_position: Some(system_position),
            });
        }
        Ok(records)
    }
}
