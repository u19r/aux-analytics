use std::collections::{BTreeMap, BTreeSet};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, SourceBatchExecution,
    SourceBatchFailureContext, source_batch_failure_context,
    source_record_key::source_key_digest,
    source_record_state::{DirtyOwnerState, OwnerStateCache, SourceRecordState},
};

/// One bounded owner working set inside the source transaction.
///
/// The batch loads the persistent owners once, applies merge decisions in
/// memory, and publishes only changed rows in one MERGE. Per-record SQL
/// statements made up most of the source transaction time.
pub(super) struct SourceRecordStateBatch<'a> {
    engine: &'a AnalyticsEngine,
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
        let target = load_owner_state_target(&engine.conn)?;
        let states = load_owner_states(&engine.conn, &target, &keys)?;
        *engine.owner_state_cache.borrow_mut() = Some(OwnerStateCache::new(states));
        Ok(Some(Self { engine, target }))
    }

    pub(super) fn flush(self) -> AnalyticsEngineResult<()> {
        let Some(mut cache) = self.engine.owner_state_cache.borrow_mut().take() else {
            return Ok(());
        };
        let dirty = std::mem::take(&mut cache.dirty);
        if dirty.is_empty() {
            return Ok(());
        }
        self.stage_dirty_rows(&dirty)?;
        self.merge_dirty_rows(dirty.len())
    }

    fn stage_dirty_rows(
        &self,
        dirty: &BTreeMap<String, DirtyOwnerState>,
    ) -> AnalyticsEngineResult<()> {
        let connection = &self.engine.conn;
        connection.execute_batch(
            "CREATE TEMP TABLE __analytics_source_record_states (
                 analytics_table_name VARCHAR,
                 source_table_name VARCHAR,
                 source_key_digest VARCHAR,
                 source_position_json VARCHAR,
                 child_identities_json VARCHAR,
                 updated_at_ms BIGINT
             )",
        )?;
        {
            let mut appender = connection.appender("__analytics_source_record_states")?;
            for (digest, row) in dirty {
                appender.append_row((
                    row.analytics_table_name.as_str(),
                    row.source_table_name.as_str(),
                    digest.as_str(),
                    row.source_position_json.as_str(),
                    row.child_identities_json.as_str(),
                    row.updated_at_ms,
                ))?;
            }
            appender.flush()?;
        }
        Ok(())
    }

    fn merge_dirty_rows(&self, expected: usize) -> AnalyticsEngineResult<()> {
        let connection = &self.engine.conn;
        let changed = connection.execute(
            &format!(
                "MERGE INTO {} AS target
                 USING (SELECT * FROM temp.main.__analytics_source_record_states) AS source
                 ON target.source_key_digest = source.source_key_digest
                 WHEN MATCHED THEN UPDATE SET
                   source_position_json = CAST(source.source_position_json AS JSON),
                   child_identities_json = CAST(source.child_identities_json AS JSON),
                   updated_at_ms = source.updated_at_ms
                 WHEN NOT MATCHED THEN INSERT
                   (analytics_table_name, source_table_name, source_key_digest,
                    source_position_json, child_identities_json, updated_at_ms)
                 VALUES (source.analytics_table_name, source.source_table_name, \
                 source.source_key_digest,
                         CAST(source.source_position_json AS JSON), \
                 CAST(source.child_identities_json AS JSON), source.updated_at_ms)",
                self.target,
            ),
            [],
        )?;
        if changed != expected {
            return Err(AnalyticsEngineError::CorruptSystemSourceRecordState(
                "source owner batch did not affect exactly one row per changed key".into(),
            ));
        }
        connection.execute_batch("DROP TABLE temp.main.__analytics_source_record_states")?;
        Ok(())
    }
}

impl Drop for SourceRecordStateBatch<'_> {
    fn drop(&mut self) {
        // The transaction rolls back staging DDL on error. This guard owns
        // only the in-memory working set, not existing caller tables.
        self.engine.owner_state_cache.borrow_mut().take();
    }
}

fn load_owner_states(
    connection: &duckdb::Connection,
    target: &str,
    keys: &BTreeSet<String>,
) -> AnalyticsEngineResult<BTreeMap<String, SourceRecordState>> {
    // Stage the digests through an appender instead of a generated
    // `IN (?, ?, ...)` list. A 1,000-key IN list dominates batch planning.
    connection.execute_batch(
        "CREATE TEMP TABLE __analytics_source_record_state_keys (source_key_digest VARCHAR)",
    )?;
    {
        let mut appender = connection.appender("__analytics_source_record_state_keys")?;
        for key in keys {
            appender.append_row([key.as_str()])?;
        }
        appender.flush()?;
    }
    let states = load_matching_owner_states(connection, target)?;
    connection.execute_batch("DROP TABLE temp.main.__analytics_source_record_state_keys")?;
    Ok(states)
}

fn load_matching_owner_states(
    connection: &duckdb::Connection,
    target: &str,
) -> AnalyticsEngineResult<BTreeMap<String, SourceRecordState>> {
    let mut statement = connection.prepare(&format!(
        "SELECT target.source_key_digest, target.source_position_json,
                target.child_identities_json, target.source_table_name
         FROM {target} AS target
         WHERE target.source_key_digest IN (
             SELECT source_key_digest FROM temp.main.__analytics_source_record_state_keys
         )"
    ))?;
    let mut states = BTreeMap::new();
    let mut rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    for row in &mut rows {
        let (digest, position, identities, source_table_name) = row?;
        let state = SourceRecordState::parse(&position, &identities, &source_table_name)?;
        if states.insert(digest, state).is_some() {
            return Err(AnalyticsEngineError::CorruptSystemSourceRecordState(
                "duplicate source owner state".to_string(),
            ));
        }
    }
    Ok(states)
}

fn load_owner_state_target(connection: &duckdb::Connection) -> AnalyticsEngineResult<String> {
    let (database, schema): (String, String) =
        connection.query_row("SELECT current_database(), current_schema()", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
    Ok(format!(
        "{}.{}.__analytics_source_record_states",
        crate::sql::quote_identifier(&database),
        crate::sql::quote_identifier(&schema)
    ))
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
