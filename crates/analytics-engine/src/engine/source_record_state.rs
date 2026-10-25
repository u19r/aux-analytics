use analytics_contract::{
    MergeDecision, SourceMutation, SourcePosition, TableRegistration, merge_decision,
};
use serde::{Deserialize, Serialize};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, IngestOutcome,
    current_time_millis,
};
use crate::row::batch::{BatchRow, BatchRowIdentity};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SourceRecordIdentity {
    pub(super) table_name: String,
    pub(super) tenant_id: String,
    pub(super) row_id: String,
    pub(super) row_visible: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct SourceRecordState {
    pub(super) source_position: SourcePosition,
    pub(super) identities: Vec<SourceRecordIdentity>,
}

impl SourceRecordState {
    fn parse(
        position: &str,
        identities: &str,
        source_table_name: &str,
    ) -> AnalyticsEngineResult<Self> {
        let source_position = serde_json::from_str(position).map_err(|error| {
            AnalyticsEngineError::CorruptSystemSourceRecordState(format!(
                "invalid source position: {error}"
            ))
        })?;
        let mut identities: Vec<SourceRecordIdentity> =
            serde_json::from_str(identities).map_err(|error| {
                AnalyticsEngineError::CorruptSystemSourceRecordState(format!(
                    "invalid child identities: {error}"
                ))
            })?;
        identities.sort_unstable_by(|left, right| {
            (&left.table_name, &left.tenant_id, &left.row_id).cmp(&(
                &right.table_name,
                &right.tenant_id,
                &right.row_id,
            ))
        });
        if identities
            .iter()
            .any(|identity| identity.table_name != source_table_name)
            || identities.windows(2).any(|pair| {
                pair[0].table_name == pair[1].table_name
                    && pair[0].tenant_id == pair[1].tenant_id
                    && pair[0].row_id == pair[1].row_id
            })
        {
            return Err(AnalyticsEngineError::CorruptSystemSourceRecordState(
                "duplicate or wrong-table child identity".to_string(),
            ));
        }
        Ok(Self {
            source_position,
            identities,
        })
    }

    fn validate_scope(&self, incoming: &SourcePosition) -> AnalyticsEngineResult<()> {
        if let (
            SourcePosition::SystemStream {
                source_scope: stored,
                ..
            },
            SourcePosition::SystemStream {
                source_scope: requested,
                ..
            },
        ) = (&self.source_position, incoming)
            && stored == requested
        {
            return Ok(());
        }
        Err(AnalyticsEngineError::CorruptSystemSourceRecordState(
            "source owner position has a different kind or scope".to_string(),
        ))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SourceRecordMutationPlan {
    pub(super) source_key_digest: String,
    pub(super) previous: Option<SourceRecordState>,
    pub(super) decision: MergeDecision,
}

struct SourceRecordStateWrite<'a> {
    analytics_table_name: &'a str,
    source_table_name: &'a str,
    plan: &'a SourceRecordMutationPlan,
    source_position: &'a SourcePosition,
    identities: &'a [SourceRecordIdentity],
}

impl SourceRecordStateWrite<'_> {
    fn write_owner_state(self, connection: &duckdb::Connection) -> AnalyticsEngineResult<()> {
        // The fenced transaction loaded and validated this owner before
        // applying its plan.
        let position = serde_json::to_string(self.source_position)?;
        let identities = serde_json::to_string(self.identities)?;
        let updated_at_ms = current_time_millis();
        let removed = connection
            .prepare_cached(
                "DELETE FROM temp.main.__analytics_source_record_states WHERE source_key_digest = \
                 ?",
            )?
            .execute([&self.plan.source_key_digest])?;
        if removed != usize::from(self.plan.previous.is_some()) {
            return Err(AnalyticsEngineError::CorruptSystemSourceRecordState(
                "source owner replacement did not remove the expected previous row".to_string(),
            ));
        }
        let changed = connection
            .prepare_cached(
                "INSERT INTO temp.main.__analytics_source_record_states

             (source_position_json, child_identities_json, updated_at_ms,
              analytics_table_name, source_table_name, source_key_digest, __source_owner_dirty)
             VALUES (CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?, TRUE)",
            )?
            .execute(duckdb::params![
                position,
                identities,
                updated_at_ms,
                self.analytics_table_name,
                self.source_table_name,
                self.plan.source_key_digest,
            ])?;

        if changed != 1 {
            return Err(AnalyticsEngineError::CorruptSystemSourceRecordState(
                "source owner write did not affect exactly one row".to_string(),
            ));
        }
        Ok(())
    }
}

impl AnalyticsEngine {
    pub(super) fn source_record_mutation_plan_for_record(
        &self,
        table: &TableRegistration,
        source_table_name: &str,
        source_position: Option<&SourcePosition>,
        source_key_digest: &str,
        allow_cross_lane: bool,
    ) -> AnalyticsEngineResult<Option<SourceRecordMutationPlan>> {
        let Some(source_position) = source_position else {
            return Ok(None);
        };
        if !matches!(source_position, SourcePosition::SystemStream { .. }) {
            return Ok(None);
        }
        let previous = self.load_source_record_state(source_table_name, source_key_digest)?;
        if let Some(previous) = &previous {
            previous.validate_scope(source_position)?;
        }
        let mut decision = merge_decision(
            previous.as_ref().map(|state| &state.source_position),
            source_position,
            SourceMutation::Update,
            table.skip_delete,
        )?;
        if allow_cross_lane && previous.is_some() && decision == MergeDecision::RequiresValidation {
            decision = MergeDecision::Replace;
        }
        Ok(Some(SourceRecordMutationPlan {
            source_key_digest: source_key_digest.to_string(),
            previous,
            decision,
        }))
    }

    pub(super) fn apply_filtered_source_record_state(
        &self,
        table: &TableRegistration,
        source_table_name: &str,
        plan: &SourceRecordMutationPlan,
        source_position: &SourcePosition,
    ) -> AnalyticsEngineResult<()> {
        if !source_record_plan_allows(plan.decision) {
            return Ok(());
        }
        let identities = plan
            .previous
            .as_ref()
            .map_or(&[][..], |previous| previous.identities.as_slice());
        SourceRecordStateWrite {
            analytics_table_name: table.analytics_table_name.as_str(),
            source_table_name,
            plan,
            source_position,
            identities,
        }
        .write_owner_state(&self.conn)?;
        Ok(())
    }

    pub(super) fn replace_source_record_state(
        &self,
        table: &TableRegistration,
        source_table_name: &str,
        plan: &SourceRecordMutationPlan,
        source_position: &SourcePosition,
        identities: &[SourceRecordIdentity],
    ) -> AnalyticsEngineResult<()> {
        if !source_record_plan_allows(plan.decision) {
            return Ok(());
        }
        SourceRecordStateWrite {
            analytics_table_name: table.analytics_table_name.as_str(),
            source_table_name,
            plan,
            source_position,
            identities,
        }
        .write_owner_state(&self.conn)
    }

    pub(super) fn delete_source_record_identity(
        &self,
        table: &TableRegistration,
        identity: &SourceRecordIdentity,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<()> {
        writes.apply_delete_identity(
            table,
            BatchRowIdentity {
                table_name: &identity.table_name,
                tenant_id: &identity.tenant_id,
                row_id: &identity.row_id,
            },
        )
    }

    pub(super) fn source_record_identity_for_row(
        row: &BatchRow,
        row_visible: bool,
    ) -> AnalyticsEngineResult<SourceRecordIdentity> {
        let identity = row.identity()?;
        Ok(SourceRecordIdentity {
            table_name: identity.table_name.to_string(),
            tenant_id: identity.tenant_id.to_string(),
            row_id: identity.row_id.to_string(),
            row_visible,
        })
    }

    pub(super) fn delete_source_record_children_except(
        &self,
        table: &TableRegistration,
        plan: &SourceRecordMutationPlan,
        keep: Option<&SourceRecordIdentity>,
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<()> {
        if let Some(previous) = plan.previous.as_ref() {
            for identity in &previous.identities {
                if keep.is_some_and(|keep| {
                    keep.table_name == identity.table_name
                        && keep.tenant_id == identity.tenant_id
                        && keep.row_id == identity.row_id
                }) {
                    continue;
                }
                self.delete_source_record_identity(table, identity, writes)?;
            }
        }
        Ok(())
    }

    pub(super) fn source_record_tombstone_identities(
        plan: &SourceRecordMutationPlan,
        row: &BatchRow,
    ) -> AnalyticsEngineResult<Vec<SourceRecordIdentity>> {
        if let Some(previous) = plan.previous.as_ref() {
            Ok(previous
                .identities
                .iter()
                .cloned()
                .map(|mut identity| {
                    identity.row_visible = false;
                    identity
                })
                .collect())
        } else {
            Ok(vec![Self::source_record_identity_for_row(row, false)?])
        }
    }

    pub(super) fn apply_source_record_upsert(
        &self,
        table: &TableRegistration,
        row: BatchRow,
        owner: (&SourcePosition, &SourceRecordMutationPlan),
        writes: &mut crate::batch_writer::BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        use crate::batch_writer::BatchUpsertOutcome;
        let (position, plan) = owner;
        let identity = row.identity()?;
        // The transaction accepted this owner transition before
        // materialization.
        let is_update = plan.decision == MergeDecision::Replace
            && plan.previous.as_ref().is_some_and(|previous| {
                previous.identities.iter().any(|prior| {
                    prior.row_visible
                        && (
                            prior.table_name.as_str(),
                            prior.tenant_id.as_str(),
                            prior.row_id.as_str(),
                        ) == (identity.table_name, identity.tenant_id, identity.row_id)
                })
            });
        let outcome = if is_update {
            BatchUpsertOutcome::Updated
        } else {
            BatchUpsertOutcome::Inserted
        };
        writes.apply_upsert(table, row, outcome, Some(position))
    }

    fn load_source_record_state(
        &self,
        source_table_name: &str,
        source_key_digest: &str,
    ) -> AnalyticsEngineResult<Option<SourceRecordState>> {
        // The source transaction stages this bounded working set in native
        // DuckDB memory with an index, including each earlier in-batch write.
        let mut statement = self.conn.prepare_cached(
            "SELECT source_position_json, child_identities_json
             FROM temp.main.__analytics_source_record_states
             WHERE source_key_digest = ?
             LIMIT 2",
        )?;
        let mut rows = statement.query_map([source_key_digest], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let Some((position, identities)) = rows.next().transpose()? else {
            return Ok(None);
        };
        if rows.next().transpose()?.is_some() {
            return Err(AnalyticsEngineError::CorruptSystemSourceRecordState(
                "duplicate source owner state".to_string(),
            ));
        }
        SourceRecordState::parse(&position, &identities, source_table_name).map(Some)
    }
}

fn source_record_plan_allows(decision: MergeDecision) -> bool {
    matches!(
        decision,
        MergeDecision::Insert | MergeDecision::Replace | MergeDecision::Append
    )
}

impl SourceRecordMutationPlan {
    pub(super) fn allows_ingest(&self) -> AnalyticsEngineResult<bool> {
        if self.decision == MergeDecision::RequiresValidation {
            return Err(AnalyticsEngineError::SystemSourceRecordRequiresValidation);
        }
        Ok(source_record_plan_allows(self.decision))
    }
}
