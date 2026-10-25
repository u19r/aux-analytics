use std::collections::{BTreeMap, BTreeSet};

use duckdb::OptionalExt;
use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, RecordCheckpoint,
    SourceBatchFailure, SourceCheckpoint, StreamRecordBatchItem, transaction::EngineTransaction,
};
use crate::source_effect;

impl AnalyticsEngine {
    pub fn load_source_checkpoints(&self) -> AnalyticsEngineResult<Vec<RecordCheckpoint>> {
        self.refresh_object_storage_credentials_if_needed()?;
        let mut statement = self.conn.prepare(
            "SELECT source_table_name, shard_id, position
             FROM analytics_checkpoints
             ORDER BY updated_at_ms ASC",
        )?;
        let mut rows = statement.query([])?;
        let mut checkpoints = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let checkpoint = SourceCheckpoint {
                source_table_name: row.get(0)?,
                shard_id: row.get(1)?,
                position: row.get(2)?,
            };
            let Ok(checkpoint) = RecordCheckpoint::try_from(checkpoint) else {
                continue;
            };
            checkpoints.insert(
                (
                    checkpoint.source_table_name().to_string(),
                    checkpoint.shard_id().to_string(),
                ),
                checkpoint,
            );
        }
        Ok(checkpoints.into_values().collect())
    }

    /// Loads the single DB-owned SystemStream scope frontier. The checkpoint
    /// token remains opaque at this boundary and is only resumed by the
    /// storage poller that received it from aux-storage.
    pub fn load_system_stream_checkpoint(
        &self,
    ) -> AnalyticsEngineResult<Option<(SystemStreamScope, SystemStreamCheckpointToken)>> {
        self.refresh_object_storage_credentials_if_needed()?;
        source_effect::load_system_checkpoint(&self.conn)
    }

    /// Returns every recorded source-batch failure, including failures that an
    /// operator has already repaired. The durable history makes a fail-closed
    /// source stop auditable without retaining the source record payload.
    pub fn load_source_batch_failures(&self) -> AnalyticsEngineResult<Vec<SourceBatchFailure>> {
        self.refresh_object_storage_credentials_if_needed()?;
        let mut statement = self.conn.prepare(
            "SELECT source_table_name, shard_id, record_position,
                    analytics_table_name, error_class, error_message,
                    failed_at_ms, repaired_at_ms
             FROM analytics_source_batch_failures
             ORDER BY failed_at_ms ASC, source_table_name ASC, shard_id ASC,
                      record_position ASC",
        )?;
        let mut rows = statement.query([])?;
        let mut failures = Vec::new();
        while let Some(row) = rows.next()? {
            failures.push(source_batch_failure_from_row(row)?);
        }
        Ok(failures)
    }

    /// Releases one blocked source batch after an operator has repaired the
    /// malformed source record. The source cursor is intentionally untouched;
    /// the next poll must still replay the repaired batch through ingestion.
    pub fn repair_source_batch_failure(
        &self,
        source_table_name: &str,
        shard_id: &str,
        record_position: &str,
    ) -> AnalyticsEngineResult<bool> {
        self.refresh_object_storage_credentials_if_needed()?;
        let transaction = EngineTransaction::begin(self)?;
        let updated = self.conn.execute(
            "UPDATE analytics_source_batch_failures
             SET repaired_at_ms = ?
             WHERE source_table_name = ?
               AND shard_id = ?
               AND record_position = ?
               AND repaired_at_ms IS NULL",
            duckdb::params![
                current_time_millis(),
                source_table_name,
                shard_id,
                record_position
            ],
        );
        transaction.finish(updated.map(|updated| updated > 0).map_err(Into::into))
    }

    pub(super) fn reject_blocked_source_batch<'records>(
        &self,
        records: impl Iterator<Item = &'records StreamRecordBatchItem>,
        checkpoints: &[RecordCheckpoint],
    ) -> AnalyticsEngineResult<()> {
        let mut source_scopes = BTreeSet::new();
        for item in records {
            let failure_context = source_batch_failure_context(item, checkpoints)?;
            source_scopes.insert(match failure_context {
                Some(context) => (context.source_table_name, Some(context.shard_id)),
                None => (bounded_failure_text(item.source_table_name.as_str()), None),
            });
        }
        source_scopes.extend(checkpoints.iter().map(|checkpoint| {
            (
                bounded_failure_text(checkpoint.source_table_name()),
                Some(bounded_failure_text(checkpoint.shard_id())),
            )
        }));
        for (source_table_name, shard_id) in source_scopes {
            let failure = if let Some(shard_id) = shard_id {
                self.conn
                    .query_row(
                        "SELECT source_table_name, shard_id, record_position,
                                analytics_table_name, error_class, error_message,
                                failed_at_ms, repaired_at_ms
                         FROM analytics_source_batch_failures
                         WHERE source_table_name = ?
                           AND shard_id = ?
                           AND repaired_at_ms IS NULL
                         ORDER BY failed_at_ms DESC
                         LIMIT 1",
                        duckdb::params![source_table_name, shard_id],
                        source_batch_failure_from_row,
                    )
                    .optional()?
            } else {
                self.conn
                    .query_row(
                        "SELECT source_table_name, shard_id, record_position,
                                analytics_table_name, error_class, error_message,
                                failed_at_ms, repaired_at_ms
                         FROM analytics_source_batch_failures
                         WHERE source_table_name = ?
                           AND repaired_at_ms IS NULL
                         ORDER BY failed_at_ms DESC
                         LIMIT 1",
                        duckdb::params![source_table_name],
                        source_batch_failure_from_row,
                    )
                    .optional()?
            };
            if let Some(failure) = failure {
                return Err(AnalyticsEngineError::SourceBatchBlocked {
                    source_table_name: failure.source_table_name,
                    shard_id: failure.shard_id,
                    record_position: failure.record_position,
                });
            }
        }
        Ok(())
    }

    pub(super) fn record_source_batch_error<T>(
        &self,
        error: AnalyticsEngineError,
        failure_context: Option<&SourceBatchFailureContext>,
    ) -> AnalyticsEngineResult<T> {
        if error.is_record_contract_failure()
            && let Some(context) = failure_context
            && let Err(record_error) = self.persist_source_batch_failure(context, &error)
        {
            return Err(AnalyticsEngineError::SourceBatchFailureNotRecorded {
                message: record_error.to_string(),
            });
        }
        Err(error)
    }

    pub(super) fn persist_source_batch_failure(
        &self,
        context: &SourceBatchFailureContext,
        error: &AnalyticsEngineError,
    ) -> AnalyticsEngineResult<()> {
        let transaction = EngineTransaction::begin(self)?;
        let result: AnalyticsEngineResult<()> = (|| {
            self.conn.execute(
                "DELETE FROM analytics_source_batch_failures
                 WHERE source_table_name = ?
                   AND shard_id = ?
                   AND record_position = ?
                   AND repaired_at_ms IS NULL",
                duckdb::params![
                    context.source_table_name,
                    context.shard_id,
                    context.record_position
                ],
            )?;
            self.conn.execute(
                "INSERT INTO analytics_source_batch_failures
                    (source_table_name, shard_id, record_position,
                     analytics_table_name, error_class, error_message,
                     failed_at_ms, repaired_at_ms)
                 VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
                duckdb::params![
                    context.source_table_name,
                    context.shard_id,
                    context.record_position,
                    context.analytics_table_name,
                    source_batch_error_class(error),
                    bounded_failure_text(error.to_string().as_str()),
                    current_time_millis()
                ],
            )?;
            Ok(())
        })();
        transaction.finish(result)
    }

    pub fn save_source_checkpoints(
        &self,
        checkpoints: &[RecordCheckpoint],
    ) -> AnalyticsEngineResult<()> {
        self.refresh_object_storage_credentials_if_needed()?;
        if checkpoints.is_empty() {
            return Ok(());
        }
        self.initialize_runtime_schema()?;
        let transaction = EngineTransaction::begin(self)?;
        for checkpoint in checkpoints {
            self.write_record_checkpoint(checkpoint)?;
        }
        transaction.commit()
    }

    /// Converts untrusted source progress into record checkpoints before any
    /// runtime-table setup or transaction begins. Iterator tokens therefore
    /// cannot create or mutate durable checkpoint state.
    pub fn save_source_checkpoints_from_source(
        &self,
        checkpoints: &[SourceCheckpoint],
    ) -> AnalyticsEngineResult<()> {
        let checkpoints = checkpoints
            .iter()
            .map(RecordCheckpoint::try_from)
            .collect::<AnalyticsEngineResult<Vec<_>>>()?;
        self.save_source_checkpoints(&checkpoints)
    }

    pub(super) fn write_record_checkpoint(
        &self,
        checkpoint: &RecordCheckpoint,
    ) -> AnalyticsEngineResult<()> {
        self.conn.execute(
            "DELETE FROM analytics_checkpoints
             WHERE source_table_name = ? AND shard_id = ?",
            duckdb::params![checkpoint.source_table_name(), checkpoint.shard_id()],
        )?;
        self.conn.execute(
            "INSERT INTO analytics_checkpoints
                (source_table_name, shard_id, position, updated_at_ms)
             VALUES (?, ?, ?, ?)",
            duckdb::params![
                checkpoint.source_table_name(),
                checkpoint.shard_id(),
                checkpoint.position(),
                current_time_millis()
            ],
        )?;
        Ok(())
    }

    pub(super) fn write_system_stream_checkpoint(
        &self,
        scope: &SystemStreamScope,
        checkpoint: &SystemStreamCheckpointToken,
    ) -> AnalyticsEngineResult<()> {
        let generation = i64::try_from(scope.generation())
            .map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
        self.conn.execute(
            "DELETE FROM analytics_system_stream_checkpoints
             WHERE scope_version = ? AND scope_id = ? AND generation = ?",
            duckdb::params![
                i32::from(scope.version()),
                scope.scope_id().as_str(),
                generation,
            ],
        )?;
        self.conn.execute(
            "INSERT INTO analytics_system_stream_checkpoints
                (scope_version, scope_id, generation, checkpoint, updated_at_ms)
             VALUES (?, ?, ?, ?, ?)",
            duckdb::params![
                i32::from(scope.version()),
                scope.scope_id().as_str(),
                generation,
                checkpoint.as_str(),
                current_time_millis(),
            ],
        )?;
        Ok(())
    }
}

pub(crate) fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

pub(crate) fn parse_source_effect_fence(
    value: &str,
) -> AnalyticsEngineResult<std::num::NonZeroU64> {
    let fence = value
        .parse::<u64>()
        .ok()
        .and_then(std::num::NonZeroU64::new)
        .ok_or(AnalyticsEngineError::InvalidSourceEffectFence)?;
    if fence.get().to_string() != value {
        return Err(AnalyticsEngineError::InvalidSourceEffectFence);
    }
    Ok(fence)
}

pub(crate) fn source_batch_failure_from_row(
    row: &duckdb::Row<'_>,
) -> duckdb::Result<SourceBatchFailure> {
    Ok(SourceBatchFailure {
        source_table_name: row.get(0)?,
        shard_id: row.get(1)?,
        record_position: row.get(2)?,
        analytics_table_name: row.get(3)?,
        error_class: row.get(4)?,
        error_message: row.get(5)?,
        failed_at_ms: row.get(6)?,
        repaired_at_ms: row.get(7)?,
    })
}

pub(crate) struct SourceBatchFailureContext {
    pub(crate) source_table_name: String,
    pub(crate) shard_id: String,
    pub(crate) record_position: String,
    pub(crate) analytics_table_name: String,
}

pub(crate) fn source_batch_failure_context(
    item: &StreamRecordBatchItem,
    checkpoints: &[RecordCheckpoint],
) -> AnalyticsEngineResult<Option<SourceBatchFailureContext>> {
    let checkpoint = if item.shard_id.is_none() {
        let mut matching = checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.source_table_name() == item.source_table_name);
        let checkpoint = matching.next();
        if checkpoint.is_some() && matching.next().is_some() {
            return Err(AnalyticsEngineError::AmbiguousSourceShard {
                source_table_name: bounded_failure_text(item.source_table_name.as_str()),
            });
        }
        checkpoint
    } else {
        None
    };
    let Some(shard_id) = item
        .shard_id
        .as_deref()
        .or_else(|| checkpoint.map(RecordCheckpoint::shard_id))
    else {
        return Ok(None);
    };
    Ok(Some(SourceBatchFailureContext {
        source_table_name: bounded_failure_text(item.source_table_name.as_str()),
        shard_id: bounded_failure_text(shard_id),
        record_position: bounded_failure_text(item.record.sequence_number.as_str()),
        analytics_table_name: bounded_failure_text(item.analytics_table_name.as_str()),
    }))
}

pub(crate) fn bounded_failure_text(value: &str) -> String {
    value.chars().take(256).collect()
}

pub(crate) const fn source_batch_error_class(error: &AnalyticsEngineError) -> &'static str {
    match error {
        AnalyticsEngineError::RecordDoesNotMatchRegistration(_) => "condition_mismatch",
        AnalyticsEngineError::MissingTenant => "missing_tenant",
        AnalyticsEngineError::MissingAttribute(_) => "missing_attribute",
        AnalyticsEngineError::RegexNoMatch { .. } => "regex_no_match",
        AnalyticsEngineError::MissingIdentifier(_) => "missing_identifier",
        AnalyticsEngineError::AttributeConversion(_) => "attribute_conversion",
        AnalyticsEngineError::InvalidRetentionTimestamp(_) => "invalid_retention_timestamp",
        AnalyticsEngineError::InvalidRowExpansionRecord(_) => "invalid_row_expansion_record",
        AnalyticsEngineError::MissingRowExpansionPath => "missing_row_expansion_path",
        AnalyticsEngineError::DuplicateExpandedRowIdentity => "duplicate_row_identity",
        AnalyticsEngineError::InvalidSystemSourceKey(_) => "invalid_system_source_key",
        _ => "record_contract",
    }
}
