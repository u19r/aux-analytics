use std::collections::BTreeSet;

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, IngestExecution, IngestOutcome,
    RecordCheckpoint, SourceBatchExecution, SourceBatchFailureContext, SourceBatchRecord,
    StreamRecordBatchItem, filter_stream_record, immediate_ingest::StreamRecordIngest,
    source_batch_failure_context, source_record_authority::SourceRecordAuthority, static_retention,
    transaction::EngineTransaction,
};
use crate::batch_writer::BatchSqlExecutor;

struct SourceRecordExecution {
    item: StreamRecordBatchItem,
    retention: Option<super::IngestRetention>,
    authority:
        SourceRecordAuthority<analytics_contract::SourcePosition, super::SourceRecordMutationPlan>,
}

impl AnalyticsEngine {
    pub(super) fn apply_source_batch_records(
        &self,
        execution: &mut SourceBatchExecution<'_>,
        failure_context: &mut Option<SourceBatchFailureContext>,
        system_owner_authorized: bool,
        transaction: &mut EngineTransaction<'_>,
    ) -> AnalyticsEngineResult<(Vec<IngestOutcome>, u64)> {
        let matcher = execution
            .policy
            .map(analytics_contract::PrivacyPolicy::validated)
            .transpose()?;
        let records = std::mem::take(&mut execution.records);
        let mut outcomes = Vec::with_capacity(records.len());
        let mut dropped_fields = 0_u64;
        let manifest_retentions = execution
            .manifest
            .tables
            .iter()
            .map(static_retention)
            .collect::<Vec<_>>();
        let system_effect = execution
            .fence
            .as_ref()
            .is_some_and(|fence| fence.scope.is_system());
        let mut system_owner_keys_seen = BTreeSet::new();
        let mut batch = BatchSqlExecutor::new(&self.conn);
        for source_record in records {
            let SourceBatchRecord {
                mut item,
                retention,
            } = source_record;
            *failure_context =
                source_batch_failure_context(&item, execution.checkpoints.as_slice())?;
            let source_position = execution
                .fence
                .as_ref()
                .map(|fence| {
                    fence
                        .scope
                        .record_source_position(item.system_position.as_ref())
                })
                .transpose()?
                .flatten();
            let authority = self.build_source_record_authority(
                execution,
                &item,
                source_position,
                system_owner_authorized.then_some(&mut system_owner_keys_seen),
            )?;
            if let Some(matcher) = matcher.as_ref() {
                let filtered = filter_stream_record(item.record, matcher);
                dropped_fields = dropped_fields.saturating_add(filtered.dropped_fields);
                item.record = filtered.record;
            }
            let record = SourceRecordExecution {
                item,
                retention,
                authority,
            };
            let outcome = if system_effect {
                self.ingest_system_source_record(execution, record, transaction)?
            } else {
                self.ingest_source_batch_record(
                    execution,
                    record,
                    &manifest_retentions,
                    &mut batch,
                )?
            };
            outcomes.push(outcome);
        }
        if !system_effect {
            batch.flush()?;
        }
        Ok((outcomes, dropped_fields))
    }

    fn build_source_record_authority(
        &self,
        execution: &SourceBatchExecution<'_>,
        item: &StreamRecordBatchItem,
        source_position: Option<analytics_contract::SourcePosition>,
        owner_keys_seen: Option<&mut BTreeSet<String>>,
    ) -> AnalyticsEngineResult<
        SourceRecordAuthority<analytics_contract::SourcePosition, super::SourceRecordMutationPlan>,
    > {
        let Some(position) = source_position else {
            return Ok(SourceRecordAuthority::Unpositioned);
        };
        let Some(system_owner_keys_seen) = owner_keys_seen else {
            return Ok(SourceRecordAuthority::Positioned(position));
        };
        let table = execution
            .manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == item.analytics_table_name)
            .ok_or_else(|| {
                AnalyticsEngineError::TableNotRegistered(item.analytics_table_name.clone())
            })?;
        let digest = super::source_record_key::source_key_digest(
            &item.analytics_table_name,
            &item.source_table_name,
            &position,
            &item.record.keys,
        )?;
        let allow_cross_lane = system_owner_keys_seen.insert(digest.clone());
        let plan = self.source_record_mutation_plan_for_record(
            table,
            &item.source_table_name,
            Some(&position),
            &digest,
            allow_cross_lane,
        )?;
        Ok(match plan {
            Some(plan) => SourceRecordAuthority::Owned { position, plan },
            None => SourceRecordAuthority::Positioned(position),
        })
    }

    fn ingest_source_batch_record(
        &self,
        execution: &SourceBatchExecution<'_>,
        record: SourceRecordExecution,
        manifest_retentions: &[Option<super::IngestRetention>],
        batch: &mut BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let SourceRecordExecution {
            item,
            retention,
            authority,
        } = record;
        let (table_index, table) = execution
            .manifest
            .tables
            .iter()
            .enumerate()
            .find(|(_, table)| table.analytics_table_name == item.analytics_table_name)
            .ok_or_else(|| {
                AnalyticsEngineError::TableNotRegistered(item.analytics_table_name.clone())
            })?;
        let retention = retention.as_ref().or_else(|| {
            manifest_retentions
                .get(table_index)
                .and_then(Option::as_ref)
        });
        let request = super::execution::BatchRecordIngest {
            table,
            item: &item,
            retention,
            source_position: authority.borrow().position(),
        };
        if table.row_expansion.is_some() {
            self.ingest_expanded_stream_record(request, batch)
        } else {
            self.ingest_single_row_stream_record(request, batch)
        }
    }

    fn ingest_system_source_record(
        &self,
        execution: &SourceBatchExecution<'_>,
        source_record: SourceRecordExecution,
        transaction: &mut EngineTransaction<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let SourceRecordExecution {
            item,
            retention,
            authority,
        } = source_record;
        transaction.ingest_record(StreamRecordIngest {
            manifest: execution.manifest,
            analytics_table_name: &item.analytics_table_name,
            record_key: &item.record_key,
            record: item.record,
            execution: IngestExecution {
                resolved_retention: retention.as_ref(),
                authority: authority.borrow(),
                source_record_keys: None,
                source_record_table_name: Some(&item.source_table_name),
            },
        })
    }

    pub(super) fn write_source_batch_checkpoints(
        &self,
        checkpoints: &[RecordCheckpoint],
    ) -> AnalyticsEngineResult<()> {
        for checkpoint in checkpoints {
            self.write_record_checkpoint(checkpoint)?;
        }

        Ok(())
    }
}
