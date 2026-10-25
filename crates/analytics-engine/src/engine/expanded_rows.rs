use std::collections::BTreeMap;

use analytics_contract::{
    SourceMutation, SourcePosition, StorageItem, StorageStreamRecord, TableRegistration,
};

use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestOutcome, IngestRetention, SourceMutationPlan,
    current_time_millis,
    repair_contract::{
        ExpandedStreamRows, combine_expanded_outcomes, expanded_row_identity,
        unique_expanded_row_identities,
    },
};
use crate::{
    batch_writer::{BatchSqlExecutor, BatchUpsertOutcome},
    row::RowSource,
};

pub(super) type ExpandedSourcePlans = BTreeMap<(String, String, String), SourceMutationPlan>;

impl AnalyticsEngine {
    pub(super) fn ingest_expanded_stream_record(
        &self,
        request: super::execution::BatchRecordIngest<'_>,
        batch: &mut BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let super::execution::BatchRecordIngest {
            table,
            item,
            retention,
            source_position,
        } = request;
        let Some(rows) = self.materialize_expanded_stream_record(
            table,
            &item.record,
            retention,
            RowSource {
                source_table_name: &item.source_table_name,
                record_key: &item.record_key,
                record_keys: &item.record.keys,
                stream_key_digest: None,
            },
        )?
        else {
            return Ok(IngestOutcome::Skipped);
        };
        let source_plans =
            self.preflight_expanded_source_mutations(table, &rows, source_position)?;
        if source_position.is_some() && source_plans.is_none() {
            return Ok(IngestOutcome::Skipped);
        }
        let mut outcomes = Vec::with_capacity(rows.old.len() + rows.new.len());
        let upsert_outcome = if rows.old_matched {
            BatchUpsertOutcome::Updated
        } else {
            BatchUpsertOutcome::Inserted
        };
        let new_identities = &rows.new_identities;
        for row in rows.new {
            outcomes.push(batch.apply_upsert(table, row, upsert_outcome, source_position)?);
        }
        for row in rows.old {
            if !new_identities.contains(&expanded_row_identity(&row)?) {
                outcomes.push(batch.apply_delete(table, row, source_position)?);
            }
        }
        if let Some(source_position) = source_position {
            for row in rows.ignored {
                let identity = expanded_row_identity(&row)?;
                if !new_identities.contains(&identity) {
                    let plan = source_plans
                        .as_ref()
                        .and_then(|plans| plans.get(&identity).copied())
                        .expect("expanded ignored row has a source mutation plan");
                    self.advance_source_position_with_plan(
                        table,
                        row,
                        source_position,
                        true,
                        plan,
                    )?;
                }
            }
        }
        Ok(combine_expanded_outcomes(&outcomes))
    }

    pub(super) fn materialize_expanded_stream_record(
        &self,
        table: &TableRegistration,
        record: &StorageStreamRecord,
        retention: Option<&IngestRetention>,
        source: RowSource<'_>,
    ) -> AnalyticsEngineResult<Option<ExpandedStreamRows>> {
        let expansion = table
            .row_expansion
            .as_ref()
            .expect("expanded ingestion requires row_expansion");
        let old_matches = self.item_matches_registration(table, record.old_image.as_ref())?;
        let new_matches = self.item_matches_registration(table, record.new_image.as_ref())?;
        if !old_matches && !new_matches && record.old_image.is_none() && record.new_image.is_none()
        {
            return Ok(None);
        }

        let ingested_at_ms = current_time_millis();
        let old_rows = if old_matches && !table.skip_delete {
            self.expanded_rows_from_item(
                table,
                expansion,
                source,
                record.old_image.as_ref().expect("old image matched"),
                ingested_at_ms,
                None,
            )?
        } else {
            Vec::new()
        };
        let new_rows = if new_matches {
            self.expanded_rows_from_item(
                table,
                expansion,
                source,
                record.new_image.as_ref().expect("new image matched"),
                ingested_at_ms,
                retention,
            )?
        } else {
            Vec::new()
        };
        let ignored_rows = if old_matches && table.skip_delete {
            self.expanded_rows_from_item(
                table,
                expansion,
                source,
                record.old_image.as_ref().expect("old image matched"),
                ingested_at_ms,
                None,
            )?
        } else {
            Vec::new()
        };
        unique_expanded_row_identities(&old_rows)?;
        let new_identities = unique_expanded_row_identities(&new_rows)?;
        unique_expanded_row_identities(&ignored_rows)?;
        Ok(Some(ExpandedStreamRows {
            old: old_rows,
            new: new_rows,
            ignored: ignored_rows,
            new_identities,
            old_matched: old_matches,
        }))
    }

    pub(super) fn preflight_expanded_source_mutations(
        &self,
        table: &TableRegistration,
        rows: &ExpandedStreamRows,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<Option<ExpandedSourcePlans>> {
        let Some(source_position) = source_position else {
            return Ok(None);
        };
        let mut plans = BTreeMap::new();
        for row in &rows.old {
            let identity = expanded_row_identity(row)?;
            if !rows.new_identities.contains(&identity) {
                let plan = self.source_mutation_plan_for(
                    table,
                    row,
                    source_position,
                    SourceMutation::Delete,
                )?;
                if !Self::source_mutation_plan_allows(plan, SourceMutation::Delete) {
                    return Ok(None);
                }
                plans.insert(identity, plan);
            }
        }
        for row in rows.ignored.iter() {
            let identity = expanded_row_identity(row)?;
            if rows.new_identities.contains(&identity) {
                continue;
            }
            let plan =
                self.source_mutation_plan_for(table, row, source_position, SourceMutation::Update)?;
            if !Self::source_mutation_plan_allows(plan, SourceMutation::Update) {
                return Ok(None);
            }
            plans.insert(identity, plan);
        }
        for row in &rows.new {
            let identity = expanded_row_identity(row)?;
            let plan =
                self.source_mutation_plan_for(table, row, source_position, SourceMutation::Update)?;
            if !Self::source_mutation_plan_allows(plan, SourceMutation::Update) {
                return Ok(None);
            }
            plans.insert(identity, plan);
        }
        Ok(Some(plans))
    }

    pub(super) fn item_matches_registration(
        &self,
        table: &TableRegistration,
        item: Option<&StorageItem>,
    ) -> AnalyticsEngineResult<bool> {
        item.map_or(Ok(false), |item| {
            crate::condition::item_matches_registration(table, item, &mut self.caches.borrow_mut())
        })
    }

    pub(super) fn expanded_rows_from_item(
        &self,
        table: &TableRegistration,
        expansion: &analytics_contract::RowExpansion,
        source: RowSource<'_>,
        image: &StorageItem,
        ingested_at_ms: i64,
        retention: Option<&IngestRetention>,
    ) -> AnalyticsEngineResult<Vec<crate::row::batch::BatchRow>> {
        crate::row_expansion::expand_source_item(image, expansion)?
            .into_iter()
            .map(|expanded_item| {
                self.batch_row_from_item(table, source, &expanded_item, ingested_at_ms, retention)
            })
            .collect()
    }
}
