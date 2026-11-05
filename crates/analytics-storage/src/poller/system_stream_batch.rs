use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};

use super::state::empty_poll_batch;
use crate::{
    error::{AnalyticsStorageError, AnalyticsStorageErrorKind, AnalyticsStorageResult},
    source_batch::{PollBatch, PolledRecord, SystemStreamBatch},
};

/// Owns the uncommitted cursor and admission budget for one ordered read.
/// Dropping a rejected or cancelled read cannot publish partial progress.
pub(super) struct SystemStreamReadBatch {
    pub(super) cursor: Option<SystemStreamCheckpointToken>,
    batch: PollBatch,
    routed_records: usize,
    encoded_job_bytes: usize,
    max_records: usize,
    max_bytes: usize,
    max_fanout: usize,
}

pub(super) struct SystemStreamPage {
    pub(super) checkpoint: SystemStreamCheckpointToken,
    pub(super) source_records: usize,
    pub(super) source_bytes: usize,
    pub(super) job_bytes: usize,
    pub(super) records: Vec<PolledRecord>,
}

impl SystemStreamReadBatch {
    pub(super) fn new(
        cursor: Option<SystemStreamCheckpointToken>,
        max_records: usize,
        max_bytes: usize,
        max_fanout: usize,
    ) -> Self {
        Self {
            cursor,
            batch: empty_poll_batch(),
            routed_records: 0,
            encoded_job_bytes: 0,
            max_records,
            max_bytes,
            max_fanout: max_fanout.max(1),
        }
    }

    pub(super) fn calculate_page_limit(&self, response_limit: u32) -> u32 {
        let remaining = self.max_records.saturating_sub(self.routed_records);
        let bytes = self.max_bytes.saturating_sub(self.encoded_job_bytes);
        if bytes == 0 {
            return 0;
        }
        // This estimate only sizes the next read. Exact serialization below
        // owns admission; source bytes cannot guarantee catalog mutation size.
        let bytes_per_record = self
            .encoded_job_bytes
            .checked_div(self.batch.source_record_count)
            .unwrap_or(1_024)
            .max(1);
        response_limit
            .min(u32::try_from(remaining / self.max_fanout).unwrap_or(u32::MAX))
            .min(u32::try_from((bytes / bytes_per_record).max(1)).unwrap_or(u32::MAX))
    }

    pub(super) fn record_response(&mut self) {
        self.batch.source_response_count += 1;
    }

    pub(super) fn admit_page(&mut self, page: SystemStreamPage) -> AnalyticsStorageResult<bool> {
        let routed_count = page
            .records
            .iter()
            .map(|r| r.analytics_table_names.len())
            .sum::<usize>();
        if self.routed_records.saturating_add(routed_count) > self.max_records
            || self.encoded_job_bytes.saturating_add(page.job_bytes) > self.max_bytes
        {
            if self.batch.records.is_empty() {
                return Err(AnalyticsStorageError::new(
                    AnalyticsStorageErrorKind::SourceBatchExceedsBudget,
                ));
            }
            return Ok(false);
        }
        self.batch.source_record_count += page.source_records;
        self.batch.source_nonempty_response_count += usize::from(page.source_records != 0);
        self.batch.source_encoded_bytes += page.source_bytes;
        self.routed_records += routed_count;
        self.encoded_job_bytes += page.job_bytes;
        self.cursor = Some(page.checkpoint);
        self.batch.records.extend(page.records);
        Ok(page.source_records != 0 && !self.batch.records.is_empty())
    }

    pub(super) fn into_poll_batch(
        mut self,
        scope: SystemStreamScope,
        previous_checkpoint: Option<SystemStreamCheckpointToken>,
    ) -> (Option<SystemStreamCheckpointToken>, PollBatch) {
        if !self.batch.records.is_empty() {
            self.batch.system_stream = self.cursor.clone().map(|checkpoint| SystemStreamBatch {
                source_scope: scope,
                previous_checkpoint,
                checkpoint,
            });
        }
        (self.cursor, self.batch)
    }
}
