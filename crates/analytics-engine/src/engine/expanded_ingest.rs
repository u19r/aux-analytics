use analytics_contract::{StorageStreamRecord, TableRegistration};

use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestOutcome, IngestRetention, SourceRecordIdentity,
    SourceRecordMutationPlan, combine_expanded_outcomes, expanded_row_identity,
    expanded_rows::ExpandedSourcePlans, repair_contract::ExpandedStreamRows,
    source_record_authority::SourceRecordAuthority,
};
use crate::row::{RowSource, batch::BatchRow};

#[derive(Clone, Copy)]
pub(super) struct ExpandedIngestRequest<'a> {
    pub table: &'a TableRegistration,
    pub source: RowSource<'a>,
    pub retention: Option<&'a IngestRetention>,
    pub authority:
        SourceRecordAuthority<&'a analytics_contract::SourcePosition, &'a SourceRecordMutationPlan>,
}

impl ExpandedStreamRows {
    fn owner_identities(
        &self,
        owner: &SourceRecordMutationPlan,
        skip_delete: bool,
    ) -> AnalyticsEngineResult<Vec<SourceRecordIdentity>> {
        let (rows, visible) = if !self.new.is_empty() {
            (&self.new, true)
        } else if !self.ignored.is_empty() {
            (&self.ignored, true)
        } else if !self.old.is_empty() {
            (&self.old, false)
        } else {
            let mut identities = owner
                .previous
                .as_ref()
                .map_or_else(Vec::new, |previous| previous.identities.clone());
            if !self.old_matched && !skip_delete {
                for identity in &mut identities {
                    identity.row_visible = false;
                }
            }
            return Ok(identities);
        };
        rows.iter()
            .map(|row| SourceRecordIdentity::from_expanded_row(row, visible))
            .collect()
    }
}

impl SourceRecordIdentity {
    fn from_expanded_row(row: &BatchRow, row_visible: bool) -> AnalyticsEngineResult<Self> {
        let (table_name, tenant_id, row_id) = expanded_row_identity(row)?;
        Ok(Self {
            table_name,
            tenant_id,
            row_id,
            row_visible,
        })
    }
}

impl AnalyticsEngine {
    pub(super) fn apply_expanded_stream_record(
        &self,
        record: &StorageStreamRecord,
        request: ExpandedIngestRequest<'_>,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let table = request.table;
        let Some(mut rows) = self.materialize_expanded_stream_record(
            table,
            record,
            request.retention,
            request.source,
        )?
        else {
            self.advance_unmaterialized_expanded_owner(table, request)?;
            return Ok(IngestOutcome::Skipped);
        };
        let source_plans = if request.authority.owner().is_some() {
            None
        } else {
            self.preflight_expanded_source_mutations(table, &rows, request.authority.position())?
        };
        if request.authority.owner().is_none()
            && request.authority.position().is_some()
            && source_plans.is_none()
        {
            return Ok(IngestOutcome::Skipped);
        }
        let owner_identities = request
            .authority
            .owner()
            .map(|(_, owner)| rows.owner_identities(owner, table.skip_delete))
            .transpose()?;
        let mut outcomes = self.upsert_expanded_rows(&mut rows, request, &source_plans, writes)?;
        outcomes.extend(self.delete_expanded_rows(&mut rows, request, &source_plans, writes)?);
        self.advance_expanded_positions(table, rows, request, &source_plans)?;
        if let (Some((position, owner)), Some(identities)) =
            (request.authority.owner(), owner_identities.as_deref())
        {
            self.replace_source_record_state(
                table,
                request.source.source_table_name,
                owner,
                position,
                identities,
            )?;
        }
        Ok(combine_expanded_outcomes(&outcomes))
    }

    fn advance_unmaterialized_expanded_owner(
        &self,
        table: &TableRegistration,
        request: ExpandedIngestRequest<'_>,
    ) -> AnalyticsEngineResult<()> {
        if let Some((position, owner)) = request.authority.owner() {
            let identities = owner
                .previous
                .as_ref()
                .map_or(&[][..], |previous| previous.identities.as_slice());
            self.replace_source_record_state(
                table,
                request.source.source_table_name,
                owner,
                position,
                identities,
            )?;
        }
        Ok(())
    }

    fn upsert_expanded_rows(
        &self,
        rows: &mut ExpandedStreamRows,
        request: ExpandedIngestRequest<'_>,
        plans: &Option<ExpandedSourcePlans>,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<Vec<IngestOutcome>> {
        let table = request.table;
        let mut outcomes = Vec::new();
        for row in std::mem::take(&mut rows.new) {
            let outcome = if let Some((position, owner)) = request.authority.owner() {
                self.apply_source_record_upsert(table, row, (position, owner), writes)?
            } else {
                let identity = expanded_row_identity(&row)?;
                let plan = plans
                    .as_ref()
                    .and_then(|plans| plans.get(&identity).copied());
                self.apply_upsert_with_plan(table, row, request.authority.position(), plan)?
            };
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }

    fn delete_expanded_rows(
        &self,
        rows: &mut ExpandedStreamRows,
        request: ExpandedIngestRequest<'_>,
        plans: &Option<ExpandedSourcePlans>,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<Vec<IngestOutcome>> {
        let table = request.table;
        let mut outcomes = Vec::new();
        if let Some((_, owner)) = request.authority.owner() {
            self.delete_expanded_owner_rows(table, rows, owner, writes)?;
            return Ok(outcomes);
        }
        for row in std::mem::take(&mut rows.old) {
            let identity = expanded_row_identity(&row)?;
            if rows.new_identities.contains(&identity) {
                continue;
            }
            let plan = plans
                .as_ref()
                .and_then(|plans| plans.get(&identity).copied());
            outcomes.push(self.apply_delete_with_plan(
                table,
                row,
                request.authority.position(),
                plan,
            )?);
        }
        Ok(outcomes)
    }

    fn delete_expanded_owner_rows(
        &self,
        table: &TableRegistration,
        rows: &ExpandedStreamRows,
        owner: &SourceRecordMutationPlan,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<()> {
        if !rows.new_identities.is_empty() || !rows.old.is_empty() {
            if let Some(previous) = owner.previous.as_ref() {
                for identity in &previous.identities {
                    let key = (
                        identity.table_name.clone(),
                        identity.tenant_id.clone(),
                        identity.row_id.clone(),
                    );
                    if !rows.new_identities.contains(&key) {
                        self.delete_source_record_identity(table, identity, writes)?;
                    }
                }
            }
            for row in &rows.old {
                if !rows.new_identities.contains(&expanded_row_identity(row)?) {
                    self.delete_source_record_identity(
                        table,
                        &SourceRecordIdentity::from_expanded_row(row, false)?,
                        writes,
                    )?;
                }
            }
        } else if !rows.old_matched
            && !table.skip_delete
            && let Some(previous) = owner.previous.as_ref()
        {
            for identity in &previous.identities {
                self.delete_source_record_identity(table, identity, writes)?;
            }
        }
        Ok(())
    }

    fn advance_expanded_positions(
        &self,
        table: &TableRegistration,
        rows: ExpandedStreamRows,
        request: ExpandedIngestRequest<'_>,
        plans: &Option<ExpandedSourcePlans>,
    ) -> AnalyticsEngineResult<()> {
        if request.authority.owner().is_some() {
            return Ok(());
        }
        let Some(position) = request.authority.position() else {
            return Ok(());
        };
        for row in rows.ignored {
            let identity = expanded_row_identity(&row)?;
            if rows.new_identities.contains(&identity) {
                continue;
            }
            let plan = plans
                .as_ref()
                .and_then(|plans| plans.get(&identity).copied())
                .expect("expanded ignored row has a source mutation plan");
            self.advance_source_position_with_plan(table, row, position, true, plan)?;
        }
        Ok(())
    }
}
