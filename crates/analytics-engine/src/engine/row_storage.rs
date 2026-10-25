use analytics_contract::{
    MergeDecision, SourceMutation, SourcePosition, TableRegistration, merge_decision,
};
use duckdb::{OptionalExt, types::ToSql};

use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestOutcome, PrivacyRowIdentity,
    assignment_set_clause, current_time_millis, placeholder_list,
};
use crate::{
    batch_writer::batch_columns,
    row::batch::{BatchRow, BatchRowIdentity},
    sql,
};

#[derive(Clone, Copy)]
pub(crate) struct SourceMutationPlan {
    pub(super) decision: MergeDecision,
    pub(super) row_visible: bool,
}

impl AnalyticsEngine {
    pub(super) fn apply_upsert(
        &self,
        table: &TableRegistration,
        row: BatchRow,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.apply_upsert_with_plan(table, row, source_position, None)
    }

    pub(super) fn apply_upsert_with_plan(
        &self,
        table: &TableRegistration,
        mut row: BatchRow,
        source_position: Option<&SourcePosition>,
        plan: Option<SourceMutationPlan>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        row.set_source_position(source_position)?;
        let identity = row.identity()?;
        let columns = batch_columns(table, row.has_retention_columns());
        let Some(source_position) = source_position else {
            let values = row.values();
            let outcome = if self.update_typed_row(table, &columns, values)? {
                IngestOutcome::Updated
            } else {
                self.insert_typed_row(table, &columns, values)?;
                IngestOutcome::Inserted
            };
            return Ok(outcome);
        };

        let decision = match plan {
            Some(plan) => plan.decision,
            None => {
                self.source_mutation_plan_for(table, &row, source_position, SourceMutation::Update)?
                    .decision
            }
        };
        let outcome = match decision {
            MergeDecision::Insert | MergeDecision::Append => {
                let values = row.values();
                self.insert_typed_row(table, &columns, values)?;
                self.save_typed_row_source_position(table, &identity, source_position, true)?;
                IngestOutcome::Inserted
            }
            MergeDecision::Replace => {
                let values = row.values();
                if self.update_typed_row(table, &columns, values)? {
                    self.save_typed_row_source_position(table, &identity, source_position, true)?;
                    IngestOutcome::Updated
                } else {
                    self.insert_typed_row(table, &columns, values)?;
                    self.save_typed_row_source_position(table, &identity, source_position, true)?;
                    IngestOutcome::Inserted
                }
            }
            MergeDecision::IgnoreDuplicate
            | MergeDecision::IgnoreStale
            | MergeDecision::RequiresValidation
            | MergeDecision::Delete => IngestOutcome::Skipped,
        };
        Ok(outcome)
    }

    pub(super) fn apply_delete(
        &self,
        table: &TableRegistration,
        row: BatchRow,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.apply_delete_with_plan(table, row, source_position, None)
    }

    pub(super) fn apply_delete_with_plan(
        &self,
        table: &TableRegistration,
        row: BatchRow,
        source_position: Option<&SourcePosition>,
        plan: Option<SourceMutationPlan>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let identity = row.identity()?;
        let Some(source_position) = source_position else {
            self.delete_typed_row(table.analytics_table_name.as_str(), &identity)?;
            return Ok(IngestOutcome::Deleted);
        };

        let decision = match plan {
            Some(plan) => plan.decision,
            None => {
                self.source_mutation_plan_for(table, &row, source_position, SourceMutation::Delete)?
                    .decision
            }
        };
        let outcome = match decision {
            MergeDecision::Delete => {
                self.delete_typed_row(table.analytics_table_name.as_str(), &identity)?;
                self.save_typed_row_source_position(table, &identity, source_position, false)?;
                IngestOutcome::Deleted
            }
            MergeDecision::IgnoreDuplicate
            | MergeDecision::IgnoreStale
            | MergeDecision::RequiresValidation
            | MergeDecision::Insert
            | MergeDecision::Replace
            | MergeDecision::Append => IngestOutcome::Skipped,
        };
        Ok(outcome)
    }

    pub(super) fn apply_filtered_source_position(
        &self,
        table: &TableRegistration,
        row: BatchRow,
        source_position: &SourcePosition,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let plan = self.source_mutation_plan(table, &row, source_position)?;
        self.apply_filtered_source_position_with_plan(table, row, source_position, plan)
    }

    pub(super) fn apply_filtered_source_position_with_plan(
        &self,
        table: &TableRegistration,
        row: BatchRow,
        source_position: &SourcePosition,
        plan: SourceMutationPlan,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let identity = row.identity()?;
        let decision = plan.decision;
        if matches!(decision, MergeDecision::Replace | MergeDecision::Append) {
            self.save_typed_row_source_position(
                table,
                &identity,
                source_position,
                plan.row_visible,
            )?;
        }
        Ok(IngestOutcome::Skipped)
    }

    pub(super) fn advance_source_position(
        &self,
        table: &TableRegistration,
        row: BatchRow,
        source_position: &SourcePosition,
        row_visible: bool,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let plan = self.source_mutation_plan(table, &row, source_position)?;
        self.advance_source_position_with_plan(table, row, source_position, row_visible, plan)
    }

    pub(super) fn advance_source_position_with_plan(
        &self,
        table: &TableRegistration,
        row: BatchRow,
        source_position: &SourcePosition,
        row_visible: bool,
        plan: SourceMutationPlan,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let identity = row.identity()?;
        let decision = plan.decision;
        if matches!(
            decision,
            MergeDecision::Insert | MergeDecision::Replace | MergeDecision::Append
        ) {
            self.save_typed_row_source_position(table, &identity, source_position, row_visible)?;
        }
        Ok(IngestOutcome::Skipped)
    }

    pub(super) fn source_mutation_plan(
        &self,
        table: &TableRegistration,
        row: &BatchRow,
        source_position: &SourcePosition,
    ) -> AnalyticsEngineResult<SourceMutationPlan> {
        self.source_mutation_plan_for(table, row, source_position, SourceMutation::Update)
    }

    pub(super) fn source_mutation_plan_for(
        &self,
        table: &TableRegistration,
        row: &BatchRow,
        source_position: &SourcePosition,
        mutation: SourceMutation,
    ) -> AnalyticsEngineResult<SourceMutationPlan> {
        let identity = row.identity()?;
        self.source_mutation_plan_for_identity(table, &identity, source_position, mutation)
    }

    pub(super) fn source_mutation_plan_for_identity(
        &self,
        table: &TableRegistration,
        identity: &BatchRowIdentity<'_>,
        source_position: &SourcePosition,
        mutation: SourceMutation,
    ) -> AnalyticsEngineResult<SourceMutationPlan> {
        let metadata =
            self.load_typed_row_source_metadata(table.analytics_table_name.as_str(), identity)?;
        let existing_position = metadata.as_ref().map(|(position, _)| position);
        let row_visible = metadata
            .as_ref()
            .is_none_or(|(_, row_visible)| *row_visible);
        let decision = merge_decision(
            existing_position,
            source_position,
            mutation,
            table.skip_delete,
        )?;
        Ok(SourceMutationPlan {
            decision,
            row_visible,
        })
    }

    pub(super) fn source_mutation_plan_allows(
        plan: SourceMutationPlan,
        mutation: SourceMutation,
    ) -> bool {
        match mutation {
            SourceMutation::Delete => matches!(plan.decision, MergeDecision::Delete),
            SourceMutation::Insert | SourceMutation::Update => matches!(
                plan.decision,
                MergeDecision::Insert | MergeDecision::Replace | MergeDecision::Append
            ),
        }
    }

    fn load_typed_row_source_metadata(
        &self,
        table_name: &str,
        identity: &BatchRowIdentity<'_>,
    ) -> AnalyticsEngineResult<Option<(SourcePosition, bool)>> {
        let metadata = self
            .conn
            .prepare(
                "SELECT source_position_json, row_visible
                 FROM __analytics_row_source_positions
                 WHERE analytics_table_name = ?
                   AND source_table_name = ?
                   AND tenant_id = ?
                   AND source_row_id = ?
                 LIMIT 1",
            )?
            .query_row(
                duckdb::params![
                    table_name,
                    identity.table_name,
                    identity.tenant_id,
                    identity.row_id,
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
            )
            .optional()?;
        if let Some((value, row_visible)) = metadata {
            return Ok(Some((serde_json::from_str(value.as_str())?, row_visible)));
        }
        let sql = format!(
            "SELECT {} FROM {} WHERE table_name = ? AND tenant_id = ? AND __id = ? LIMIT 1",
            sql::quote_identifier(crate::constants::SOURCE_POSITION_COLUMN),
            sql::quote_identifier(table_name)
        );
        let value = self
            .conn
            .prepare(sql.as_str())?
            .query_row(
                duckdb::params![identity.table_name, identity.tenant_id, identity.row_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        value
            .map(|value| serde_json::from_str(value.as_str()))
            .transpose()
            .map(|position| position.map(|position| (position, true)))
            .map_err(Into::into)
    }

    fn save_typed_row_source_position(
        &self,
        table: &TableRegistration,
        identity: &BatchRowIdentity<'_>,
        source_position: &SourcePosition,
        row_visible: bool,
    ) -> AnalyticsEngineResult<()> {
        self.save_row_source_position_values(
            table.analytics_table_name.as_str(),
            identity.table_name,
            identity.tenant_id,
            identity.row_id,
            source_position,
            row_visible,
        )
    }

    pub(super) fn insert_typed_row(
        &self,
        table: &TableRegistration,
        columns: &[String],
        values: &[duckdb::types::Value],
    ) -> AnalyticsEngineResult<()> {
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            sql::quote_identifier(table.analytics_table_name.as_str()),
            columns
                .iter()
                .map(|column| sql::quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            placeholder_list(columns.len()),
        );
        self.conn
            .prepare(sql.as_str())?
            .execute(duckdb::params_from_iter(values.iter()))?;
        Ok(())
    }

    pub(super) fn update_typed_row(
        &self,
        table: &TableRegistration,
        columns: &[String],
        values: &[duckdb::types::Value],
    ) -> AnalyticsEngineResult<bool> {
        if columns.len() <= 3 {
            return Ok(false);
        }
        let set_clause = assignment_set_clause(columns.iter().skip(3).map(String::as_str));
        let sql = format!(
            "UPDATE {} SET {set_clause} WHERE table_name = ? AND tenant_id = ? AND __id = ?",
            sql::quote_identifier(table.analytics_table_name.as_str())
        );
        let parameters = values[3..].iter().chain(values[..3].iter());
        let changed = self
            .conn
            .prepare(sql.as_str())?
            .execute(duckdb::params_from_iter(parameters))?;
        Ok(changed > 0)
    }

    pub(super) fn delete_typed_row(
        &self,
        table_name: &str,
        identity: &BatchRowIdentity<'_>,
    ) -> AnalyticsEngineResult<()> {
        let sql = format!(
            "DELETE FROM {} WHERE table_name = ? AND tenant_id = ? AND __id = ?",
            sql::quote_identifier(table_name)
        );
        self.conn.prepare(sql.as_str())?.execute(duckdb::params![
            identity.table_name,
            identity.tenant_id,
            identity.row_id
        ])?;
        Ok(())
    }

    fn save_row_source_position_values(
        &self,
        analytics_table_name: &str,
        source_table_name: &str,
        tenant_id: &str,
        row_id: &str,
        source_position: &SourcePosition,
        row_visible: bool,
    ) -> AnalyticsEngineResult<()> {
        self.conn.execute(
            "MERGE INTO __analytics_row_source_positions AS target
             USING (
                 SELECT ? AS analytics_table_name,
                        ? AS source_table_name,
                        ? AS tenant_id,
                        ? AS source_row_id,
                        CAST(? AS JSON) AS source_position_json,
                        ? AS row_visible,
                        ? AS updated_at_ms
             ) AS incoming
             ON target.analytics_table_name = incoming.analytics_table_name
                AND target.source_table_name = incoming.source_table_name
                AND target.tenant_id = incoming.tenant_id
                AND target.source_row_id = incoming.source_row_id
             WHEN MATCHED THEN UPDATE SET
                 source_position_json = incoming.source_position_json,
                 row_visible = incoming.row_visible,
                 updated_at_ms = incoming.updated_at_ms
             WHEN NOT MATCHED THEN INSERT
                 (analytics_table_name, source_table_name, tenant_id, source_row_id,
                  source_position_json, row_visible, updated_at_ms)
                 VALUES (incoming.analytics_table_name, incoming.source_table_name,
                         incoming.tenant_id, incoming.source_row_id,
                         incoming.source_position_json, incoming.row_visible,
                         incoming.updated_at_ms)",
            duckdb::params![
                analytics_table_name,
                source_table_name,
                tenant_id,
                row_id,
                serde_json::to_string(source_position)?,
                row_visible,
                current_time_millis(),
            ],
        )?;
        Ok(())
    }

    pub(super) fn update_privacy_scrubbed_row(
        &self,
        table_name: &str,
        identity: PrivacyRowIdentity<'_>,
        assignments: storage_types::VecMap<&str, duckdb::types::Value>,
    ) -> AnalyticsEngineResult<()> {
        let set_clause = assignment_set_clause(assignments.keys().copied());
        let sql = format!(
            "UPDATE {} SET {set_clause} WHERE table_name = ? AND tenant_id = ? AND __id = ?",
            sql::quote_identifier(table_name),
        );
        let identity_values = [identity.table_name, identity.tenant_id, identity.row_id];
        let values = assignments
            .values()
            .map(|value| value as &dyn ToSql)
            .chain(identity_values.iter().map(|value| value as &dyn ToSql));
        self.conn
            .prepare(sql.as_str())?
            .execute(duckdb::params_from_iter(values))?;
        Ok(())
    }

    pub(super) fn delete_privacy_violating_row(
        &self,
        table_name: &str,
        identity: PrivacyRowIdentity<'_>,
    ) -> AnalyticsEngineResult<()> {
        let sql = format!(
            "DELETE FROM {} WHERE table_name = ? AND tenant_id = ? AND __id = ?",
            sql::quote_identifier(table_name),
        );
        self.conn.prepare(sql.as_str())?.execute(duckdb::params![
            identity.table_name,
            identity.tenant_id,
            identity.row_id
        ])?;
        Ok(())
    }
}
