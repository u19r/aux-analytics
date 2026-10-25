use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestOutcome, immediate_ingest::ImmediateIngestRequest,
};

impl AnalyticsEngine {
    pub(super) fn ingest_insert_image(
        &self,
        request: ImmediateIngestRequest<'_>,
        new_image: analytics_contract::StorageItem,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let ImmediateIngestRequest {
            table,
            source,
            ingested_at_ms,
            effective_retention,
            authority,
            ..
        } = request;

        if !crate::condition::item_matches_registration(
            table,
            &new_image,
            &mut self.caches.borrow_mut(),
        )? {
            if let Some((source_position, plan)) = authority.owner() {
                self.apply_filtered_source_record_state(
                    table,
                    source.source_table_name,
                    plan,
                    source_position,
                )?;
            }
            return Ok(IngestOutcome::Skipped);
        }
        let row = self.batch_row_from_item(
            table,
            source,
            &new_image,
            ingested_at_ms,
            effective_retention,
        )?;
        self.upsert_immediate_row(request, row, writes)
    }

    pub(super) fn ingest_update_images(
        &self,
        request: ImmediateIngestRequest<'_>,
        old_image: analytics_contract::StorageItem,
        new_image: analytics_contract::StorageItem,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let ImmediateIngestRequest {
            table,
            source,
            ingested_at_ms,
            effective_retention,
            authority,
            ..
        } = request;

        let old_matches = crate::condition::item_matches_registration(
            table,
            &old_image,
            &mut self.caches.borrow_mut(),
        )?;
        let new_matches = crate::condition::item_matches_registration(
            table,
            &new_image,
            &mut self.caches.borrow_mut(),
        )?;
        if !old_matches && !new_matches {
            return self.skip_unmatched_update(request, old_image);
        }
        if old_matches && !new_matches {
            return self.delete_previous_image(request, old_image, writes);
        }
        let new_row = self.batch_row_from_item(
            table,
            source,
            &new_image,
            ingested_at_ms,
            effective_retention,
        )?;
        if !old_matches {
            self.remove_previous_children(request, &new_row, writes)?;
            return self.upsert_immediate_row(request, new_row, writes);
        }
        let old_row = self.batch_row_from_item(table, source, &old_image, ingested_at_ms, None)?;
        if old_row.same_identity(&new_row) {
            return self.upsert_immediate_row(request, new_row, writes);
        }
        if authority.owner().is_some() {
            return self.relocate_owned_rows(request, old_row, new_row, writes);
        }
        self.relocate_immediate_rows(request, old_row, new_row)
    }

    pub(super) fn ingest_delete_image(
        &self,
        request: ImmediateIngestRequest<'_>,
        old_image: analytics_contract::StorageItem,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let ImmediateIngestRequest {
            table,
            source,
            ingested_at_ms,
            authority,
            ..
        } = request;
        let source_position = authority.position();

        if !crate::condition::item_matches_registration(
            table,
            &old_image,
            &mut self.caches.borrow_mut(),
        )? {
            let Some(source_position) = source_position else {
                return Ok(IngestOutcome::Skipped);
            };
            if let Some((source_position, plan)) = authority.owner() {
                self.apply_filtered_source_record_state(
                    table,
                    source.source_table_name,
                    plan,
                    source_position,
                )?;
            } else if let Ok(row) =
                self.batch_row_from_item(table, source, &old_image, ingested_at_ms, None)
            {
                self.apply_filtered_source_position(table, row, source_position)?;
            }
            return Ok(IngestOutcome::Skipped);
        }
        if table.skip_delete {
            return self.retain_deleted_image(request, old_image);
        }
        let row = self.batch_row_from_item(table, source, &old_image, ingested_at_ms, None)?;
        self.delete_immediate_row(request, row, writes)
    }

    pub(super) fn advance_empty_record(
        &self,
        request: ImmediateIngestRequest<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let ImmediateIngestRequest {
            table,
            authority,
            source,
            ..
        } = request;

        if let Some((source_position, plan)) = authority.owner() {
            let identities = plan
                .previous
                .as_ref()
                .map_or(&[][..], |previous| previous.identities.as_slice());
            self.replace_source_record_state(
                table,
                source.source_table_name,
                plan,
                source_position,
                identities,
            )?;
        }
        Ok(IngestOutcome::Skipped)
    }

    pub(super) fn skip_unmatched_update(
        &self,
        request: ImmediateIngestRequest<'_>,
        old_image: analytics_contract::StorageItem,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let ImmediateIngestRequest {
            table,
            source,
            ingested_at_ms,
            authority,
            ..
        } = request;
        let source_position = authority.position();

        let Some(source_position) = source_position else {
            return Ok(IngestOutcome::Skipped);
        };
        if let Some((source_position, plan)) = authority.owner() {
            self.apply_filtered_source_record_state(
                table,
                source.source_table_name,
                plan,
                source_position,
            )?;
        }
        if authority.owner().is_none()
            && let Ok(row) =
                self.batch_row_from_item(table, source, &old_image, ingested_at_ms, None)
        {
            self.apply_filtered_source_position(table, row, source_position)?;
        }
        Ok(IngestOutcome::Skipped)
    }

    pub(super) fn delete_previous_image(
        &self,
        request: ImmediateIngestRequest<'_>,
        old_image: analytics_contract::StorageItem,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let ImmediateIngestRequest {
            table,
            source,
            ingested_at_ms,
            authority,
            ..
        } = request;
        let source_position = authority.position();

        if table.skip_delete {
            let Some(source_position) = source_position else {
                return Ok(IngestOutcome::Skipped);
            };
            if let Some((source_position, plan)) = authority.owner() {
                self.apply_filtered_source_record_state(
                    table,
                    source.source_table_name,
                    plan,
                    source_position,
                )?;
                return Ok(IngestOutcome::Skipped);
            }
            let Ok(row) = self.batch_row_from_item(table, source, &old_image, ingested_at_ms, None)
            else {
                return Ok(IngestOutcome::Skipped);
            };
            return self.advance_source_position(table, row, source_position, true);
        }
        let row = self.batch_row_from_item(table, source, &old_image, ingested_at_ms, None)?;
        self.delete_immediate_row(request, row, writes)
    }

    pub(super) fn retain_deleted_image(
        &self,
        request: ImmediateIngestRequest<'_>,
        old_image: analytics_contract::StorageItem,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let ImmediateIngestRequest {
            table,
            source,
            ingested_at_ms,
            authority,
            ..
        } = request;
        let source_position = authority.position();

        let row = self.batch_row_from_item(table, source, &old_image, ingested_at_ms, None)?;
        let Some(source_position) = source_position else {
            return Ok(IngestOutcome::Skipped);
        };
        if let Some((source_position, plan)) = authority.owner() {
            let initial_identity;
            let identities = if let Some(previous) = plan.previous.as_ref() {
                previous.identities.as_slice()
            } else {
                initial_identity = [Self::source_record_identity_for_row(&row, true)?];
                &initial_identity
            };
            self.replace_source_record_state(
                table,
                source.source_table_name,
                plan,
                source_position,
                identities,
            )?;
            return Ok(IngestOutcome::Skipped);
        }
        self.advance_source_position(table, row, source_position, true)
    }
}
