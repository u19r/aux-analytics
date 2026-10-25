use std::collections::BTreeSet;

use duckdb::Connection;

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, SourceBatchExecution,
    SourceBatchFailureContext, placeholder_list, source_batch_failure_context,
    source_record_key::source_key_digest,
};

/// One bounded owner working set inside the source transaction. Native DuckLake
/// has no point index, so read and publish the persistent owners once per
/// batch.
pub(super) struct SourceRecordStateBatch<'a> {
    connection: &'a Connection,
    target: String,
}

impl<'a> SourceRecordStateBatch<'a> {
    pub(super) fn try_new(
        engine: &'a AnalyticsEngine,
        execution: &SourceBatchExecution<'_>,
        failure_context: &mut Option<SourceBatchFailureContext>,
    ) -> AnalyticsEngineResult<Option<Self>> {
        let Some(fence) = execution
            .fence
            .as_ref()
            .filter(|fence| fence.scope.is_system())
        else {
            return Ok(None);
        };
        let keys = collect_owner_keys(execution, &fence.scope, failure_context)?;
        if keys.is_empty() {
            return Ok(None);
        }
        let (database, schema): (String, String) =
            engine
                .conn
                .query_row("SELECT current_database(), current_schema()", [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;
        let target = format!(
            "{}.{}.__analytics_source_record_states",
            crate::sql::quote_identifier(&database),
            crate::sql::quote_identifier(&schema)
        );
        Self::stage_owners(&engine.conn, target, &keys).map(Some)
    }

    fn stage_owners(
        connection: &'a Connection,
        target: String,
        keys: &BTreeSet<String>,
    ) -> AnalyticsEngineResult<Self> {
        connection.execute(
            &format!(
                "CREATE TEMP TABLE __analytics_source_record_states AS
                 SELECT *, FALSE AS __source_owner_dirty FROM {}
                 WHERE source_key_digest IN ({})",
                target,
                placeholder_list(keys.len()),
            ),
            duckdb::params_from_iter(keys),
        )?;
        let batch = Self { connection, target };
        connection.execute_batch(
            "CREATE INDEX __analytics_source_record_states_batch_digest
             ON temp.main.__analytics_source_record_states (source_key_digest)",
        )?;
        Ok(batch)
    }

    pub(super) fn flush(self) -> AnalyticsEngineResult<()> {
        let expected: usize = self.connection.query_row(
            "SELECT count(*) FROM temp.main.__analytics_source_record_states WHERE \
             __source_owner_dirty",
            [],
            |row| row.get(0),
        )?;
        let changed = self.connection.execute(
            &format!(
                "MERGE INTO {} AS target
                 USING (SELECT * FROM temp.main.__analytics_source_record_states WHERE \
                 __source_owner_dirty) AS source
                 ON target.source_key_digest = source.source_key_digest
                 WHEN MATCHED THEN UPDATE SET
                   source_position_json = source.source_position_json,
                   child_identities_json = source.child_identities_json,
                   updated_at_ms = source.updated_at_ms
                 WHEN NOT MATCHED THEN INSERT
                   (analytics_table_name, source_table_name, source_key_digest,
                    source_position_json, child_identities_json, updated_at_ms)
                 VALUES (source.analytics_table_name, source.source_table_name, \
                 source.source_key_digest,
                         source.source_position_json, source.child_identities_json, \
                 source.updated_at_ms)",
                self.target,
            ),
            [],
        )?;
        if changed != expected {
            return Err(AnalyticsEngineError::CorruptSystemSourceRecordState(
                "source owner batch did not affect exactly one row per changed key".into(),
            ));
        }
        self.connection
            .execute_batch("DROP TABLE temp.main.__analytics_source_record_states")?;
        Ok(())
    }
}

impl Drop for SourceRecordStateBatch<'_> {
    fn drop(&mut self) {
        let _ = self
            .connection
            .execute_batch("DROP TABLE IF EXISTS temp.main.__analytics_source_record_states");
    }
}

fn collect_owner_keys(
    execution: &SourceBatchExecution<'_>,
    scope: &crate::SourceEffectScope,
    failure_context: &mut Option<SourceBatchFailureContext>,
) -> AnalyticsEngineResult<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for record in &execution.records {
        let item = &record.item;
        *failure_context = source_batch_failure_context(item, &execution.checkpoints)?;
        let position = scope
            .record_source_position(item.system_position.as_ref())?
            .ok_or(AnalyticsEngineError::IncompleteSourceEffectState)?;
        keys.insert(source_key_digest(
            &item.analytics_table_name,
            &item.source_table_name,
            &position,
            &item.record.keys,
        )?);
    }
    *failure_context = None;
    Ok(keys)
}
