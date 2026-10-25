use analytics_contract::{SourceMutation, SourcePosition, StorageItem, TableRegistration};

use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestOutcome, IngestRetention, current_time_millis,
};
use crate::{
    batch_writer::{BatchSqlExecutor, BatchUpsertOutcome},
    row::RowSource,
};

#[derive(Clone, Copy)]
struct BatchIngestRequest<'a> {
    table: &'a TableRegistration,
    source: RowSource<'a>,
    ingested_at_ms: i64,
    retention: Option<&'a IngestRetention>,
    source_position: Option<&'a SourcePosition>,
}

impl AnalyticsEngine {
    pub(super) fn ingest_single_row_stream_record(
        &self,
        request: super::execution::BatchRecordIngest<'_>,
        batch: &mut BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let super::execution::BatchRecordIngest {
            table,
            item,
            retention: resolved_retention,
            source_position,
        } = request;
        let source = RowSource {
            source_table_name: &item.source_table_name,
            record_key: &item.record_key,
            record_keys: &item.record.keys,
            stream_key_digest: None,
        };
        let ingested_at_ms = current_time_millis();
        let request = BatchIngestRequest {
            table,
            source,
            ingested_at_ms,
            retention: resolved_retention,
            source_position,
        };
        match (&item.record.old_image, &item.record.new_image) {
            (None, Some(image)) => self.stage_insert_image(request, image, batch),
            (Some(old), Some(new)) => self.stage_update_images(request, (old, new), batch),
            (Some(image), None) => self.stage_delete_image(request, image, batch),
            (None, None) => Ok(IngestOutcome::Skipped),
        }
    }

    fn stage_insert_image(
        &self,
        request: BatchIngestRequest<'_>,
        new_image: &StorageItem,
        batch: &mut BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let BatchIngestRequest {
            table,
            source,
            ingested_at_ms,
            retention,
            source_position,
        } = request;
        if !self.item_matches_registration(table, Some(new_image))? {
            return Ok(IngestOutcome::Skipped);
        }
        let row = self.batch_row_from_item(table, source, new_image, ingested_at_ms, retention)?;
        batch.apply_upsert(table, row, BatchUpsertOutcome::Inserted, source_position)
    }

    fn stage_update_images(
        &self,
        request: BatchIngestRequest<'_>,
        images: (&StorageItem, &StorageItem),
        batch: &mut BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let BatchIngestRequest {
            table,
            source,
            ingested_at_ms,
            retention,
            source_position,
        } = request;
        let (old_image, new_image) = images;
        let old_matches = self.item_matches_registration(table, Some(old_image))?;
        let new_matches = self.item_matches_registration(table, Some(new_image))?;
        if !old_matches && !new_matches {
            if let Some(source_position) = source_position
                && let Ok(row) =
                    self.batch_row_from_item(table, source, old_image, ingested_at_ms, None)
            {
                self.apply_filtered_source_position(table, row, source_position)?;
            }
            return Ok(IngestOutcome::Skipped);
        }
        if old_matches && !new_matches {
            return self.stage_filtered_delete(request, old_image, batch);
        }
        let new_row =
            self.batch_row_from_item(table, source, new_image, ingested_at_ms, retention)?;
        if !old_matches {
            return batch.apply_upsert(
                table,
                new_row,
                BatchUpsertOutcome::Inserted,
                source_position,
            );
        }
        let old_row = self.batch_row_from_item(table, source, old_image, ingested_at_ms, None)?;
        if old_row.same_identity(&new_row) {
            return batch.apply_upsert(
                table,
                new_row,
                BatchUpsertOutcome::Updated,
                source_position,
            );
        }
        self.stage_row_relocation(request, (old_row, new_row), batch)
    }

    fn stage_filtered_delete(
        &self,
        request: BatchIngestRequest<'_>,
        old_image: &StorageItem,
        batch: &mut BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let BatchIngestRequest {
            table,
            source,
            ingested_at_ms,
            source_position,
            ..
        } = request;
        if table.skip_delete {
            if let Some(source_position) = source_position
                && let Ok(row) =
                    self.batch_row_from_item(table, source, old_image, ingested_at_ms, None)
            {
                return self.advance_source_position(table, row, source_position, true);
            }
            return Ok(IngestOutcome::Skipped);
        }
        let row = self.batch_row_from_item(table, source, old_image, ingested_at_ms, None)?;
        batch.apply_delete(table, row, source_position)
    }

    fn stage_row_relocation(
        &self,
        request: BatchIngestRequest<'_>,
        rows: (crate::row::batch::BatchRow, crate::row::batch::BatchRow),
        batch: &mut BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let (old_row, new_row) = rows;
        let BatchIngestRequest {
            table,
            source_position,
            ..
        } = request;
        let old_plan = source_position
            .map(|position| {
                self.source_mutation_plan_for(table, &old_row, position, SourceMutation::Delete)
            })
            .transpose()?;
        let new_plan = source_position
            .map(|position| {
                self.source_mutation_plan_for(table, &new_row, position, SourceMutation::Update)
            })
            .transpose()?;
        if old_plan
            .is_some_and(|plan| !Self::source_mutation_plan_allows(plan, SourceMutation::Delete))
            || new_plan.is_some_and(|plan| {
                !Self::source_mutation_plan_allows(plan, SourceMutation::Update)
            })
        {
            return Ok(IngestOutcome::Skipped);
        }
        // Tenant or row-identity changes move the row to a new merge key. Stage
        // an explicit delete for the old key before the replacement upsert so
        // the old grain cannot remain readable.
        batch.apply_delete(table, old_row, source_position)?;
        batch.apply_upsert(table, new_row, BatchUpsertOutcome::Updated, source_position)
    }

    fn stage_delete_image(
        &self,
        request: BatchIngestRequest<'_>,
        old_image: &StorageItem,
        batch: &mut BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let BatchIngestRequest {
            table,
            source,
            ingested_at_ms,
            retention: _,
            source_position,
        } = request;
        if table.skip_delete {
            let old_matches = crate::condition::item_matches_registration(
                table,
                old_image,
                &mut self.caches.borrow_mut(),
            )?;
            let row = self.batch_row_from_item(table, source, old_image, ingested_at_ms, None)?;
            let Some(source_position) = source_position else {
                return Ok(IngestOutcome::Skipped);
            };
            if old_matches {
                return self.advance_source_position(table, row, source_position, true);
            }
            return self.apply_filtered_source_position(table, row, source_position);
        }
        if !self.item_matches_registration(table, Some(old_image))? {
            if let Some(source_position) = source_position
                && let Ok(row) =
                    self.batch_row_from_item(table, source, old_image, ingested_at_ms, None)
            {
                self.apply_filtered_source_position(table, row, source_position)?;
            }
            return Ok(IngestOutcome::Skipped);
        }
        let row = self.batch_row_from_item(table, source, old_image, ingested_at_ms, None)?;
        batch.apply_delete(table, row, source_position)
    }
}
