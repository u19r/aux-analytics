use std::time::Instant;

use storage_types::{GetStreamRecordsResponse, StreamRecord, SystemStreamCheckpointToken};

use super::{
    state::{AuxStorageGlobalPoller, empty_poll_batch, logical_record_key},
    system_stream_batch::{SystemStreamPage, SystemStreamReadBatch},
};
use crate::{
    error::{AnalyticsStorageError, AnalyticsStorageErrorKind, AnalyticsStorageResult},
    source_batch::{PollBatch, PolledRecord},
};

impl AuxStorageGlobalPoller {
    pub(crate) async fn poll_once(
        &mut self,
        max_responses: usize,
    ) -> AnalyticsStorageResult<PollBatch> {
        if max_responses == 0 {
            return Ok(empty_poll_batch());
        }
        if let Some(started) = self.pending_effect {
            if started.elapsed() < crate::constants::PENDING_EFFECT_TIMEOUT {
                return Ok(empty_poll_batch());
            }
            self.pending_effect = None;
            self.scan_checkpoint = self.checkpoint.clone();
        }

        let batch = self.fetch_batch(max_responses).await?;
        if batch.system_stream.is_some() {
            self.pending_effect = Some(Instant::now());
        }
        Ok(batch)
    }

    async fn fetch_batch(&mut self, max_responses: usize) -> AnalyticsStorageResult<PollBatch> {
        let mut read = SystemStreamReadBatch::new(
            self.scan_checkpoint.clone(),
            self.max_records_per_batch,
            self.max_batch_bytes,
            self.routes.max_fanout(),
        );
        for _ in 0..max_responses {
            let page_limit = read.calculate_page_limit(self.max_records_per_response);
            if page_limit == 0 {
                break;
            }
            let response = self
                .client
                .get_system_stream_records(
                    self.source_scope.clone(),
                    read.cursor.clone(),
                    page_limit,
                )
                .await?;
            read.record_response();
            let Some(checkpoint) = self.validate_response_checkpoint(&response)? else {
                break;
            };
            let page = self.route_page(response, checkpoint)?;
            self.resize_oversized_page(page_limit, page.job_bytes);
            if !read.admit_page(page)? {
                break;
            }
        }
        let (cursor, batch) =
            read.into_poll_batch(self.source_scope.clone(), self.checkpoint.clone());
        self.scan_checkpoint = cursor;
        Ok(batch)
    }

    fn route_page(
        &mut self,
        response: GetStreamRecordsResponse,
        checkpoint: SystemStreamCheckpointToken,
    ) -> AnalyticsStorageResult<SystemStreamPage> {
        let source_records = response.records().len();
        let source_bytes = if source_records == 0 {
            0
        } else {
            self.measure_serialized_bytes(response.records())?
        };
        let (records, _) = response.into_parts();
        let records = self.route_system_records(records)?;
        let job_bytes = if records.is_empty() {
            0
        } else {
            self.measure_serialized_bytes(&records)?
        };
        Ok(SystemStreamPage {
            checkpoint,
            source_records,
            source_bytes,
            job_bytes,
            records,
        })
    }

    fn measure_serialized_bytes<T: serde::Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> AnalyticsStorageResult<usize> {
        self.serialization_buffer.clear();
        serde_json::to_writer(&mut self.serialization_buffer, value)?;
        Ok(self.serialization_buffer.len())
    }

    fn resize_oversized_page(&mut self, page_limit: u32, page_bytes: usize) {
        if page_limit <= 1 || page_bytes <= self.max_batch_bytes {
            return;
        }
        let scaled = u64::from(page_limit)
            .saturating_mul(self.max_batch_bytes as u64)
            .saturating_div(page_bytes as u64)
            .max(1);
        self.max_records_per_response = u32::try_from(scaled)
            .unwrap_or(u32::MAX)
            .min(page_limit - 1);
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
