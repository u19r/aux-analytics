use analytics_contract::SourceMutation;

use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestOutcome, immediate_ingest::ImmediateIngestRequest,
};

impl AnalyticsEngine {
    pub(super) fn upsert_immediate_row(
        &self,
        request: ImmediateIngestRequest<'_>,
        row: crate::row::batch::BatchRow,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let Some((position, plan)) = request.authority.owner() else {
            return self.apply_upsert(request.table, row, request.authority.position());
        };
        let identity = Self::source_record_identity_for_row(&row, true)?;
        let outcome =
            self.apply_source_record_upsert(request.table, row, (position, plan), writes)?;
        self.replace_source_record_state(
            request.table,
            request.source.source_table_name,
            plan,
            position,
            &[identity],
        )?;
        Ok(outcome)
    }

    pub(super) fn remove_previous_children(
        &self,
        request: ImmediateIngestRequest<'_>,
        row: &crate::row::batch::BatchRow,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<()> {
        if let Some((_, plan)) = request.authority.owner() {
            let identity = Self::source_record_identity_for_row(row, true)?;
            self.delete_source_record_children_except(
                request.table,
                plan,
                Some(&identity),
                writes,
            )?;
        }
        Ok(())
    }

    pub(super) fn relocate_immediate_rows(
        &self,
        request: ImmediateIngestRequest<'_>,
        old_row: crate::row::batch::BatchRow,
        new_row: crate::row::batch::BatchRow,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let ImmediateIngestRequest {
            table, authority, ..
        } = request;
        let source_position = authority.position();
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
        // A changed tenant or row identity is a relocation, not an in-place
        // update. Keep deletion and replacement atomic so the old key cannot
        // survive a successful ingest.
        self.apply_row_relocation(request, old_row, new_row, old_plan.zip(new_plan))
    }

    pub(super) fn delete_immediate_row(
        &self,
        request: ImmediateIngestRequest<'_>,
        row: crate::row::batch::BatchRow,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let Some((position, plan)) = request.authority.owner() else {
            return self.apply_delete(request.table, row, request.authority.position());
        };
        let identities = Self::source_record_tombstone_identities(plan, &row)?;
        self.delete_source_record_children_except(request.table, plan, None, writes)?;
        let outcome = if plan.previous.is_some() {
            IngestOutcome::Deleted
        } else {
            writes.apply_delete(request.table, row, None)?
        };
        self.replace_source_record_state(
            request.table,
            request.source.source_table_name,
            plan,
            position,
            &identities,
        )?;
        Ok(outcome)
    }

    pub(super) fn relocate_owned_rows(
        &self,
        request: ImmediateIngestRequest<'_>,
        old_row: crate::row::batch::BatchRow,
        new_row: crate::row::batch::BatchRow,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let table = request.table;

        self.remove_previous_children(request, &new_row, writes)?;
        writes.apply_delete(table, old_row, None)?;
        let outcome = self.upsert_immediate_row(request, new_row, writes)?;
        Ok(if outcome == IngestOutcome::Skipped {
            IngestOutcome::Deleted
        } else {
            IngestOutcome::Updated
        })
    }

    pub(super) fn apply_row_relocation(
        &self,
        request: ImmediateIngestRequest<'_>,
        old_row: crate::row::batch::BatchRow,
        new_row: crate::row::batch::BatchRow,
        plans: Option<(super::SourceMutationPlan, super::SourceMutationPlan)>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let delete_outcome = self.apply_delete_with_plan(
            request.table,
            old_row,
            request.authority.position(),
            plans.map(|(old, _)| old),
        )?;
        let upsert_outcome = self.apply_upsert_with_plan(
            request.table,
            new_row,
            request.authority.position(),
            plans.map(|(_, new)| new),
        )?;
        Ok(if upsert_outcome == IngestOutcome::Skipped {
            delete_outcome
        } else {
            IngestOutcome::Updated
        })
    }
}
