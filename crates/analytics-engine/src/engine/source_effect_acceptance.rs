use duckdb::OptionalExt;

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, SourceBatchFailureContext,
    SourceEffectDecision, SourceFailureFenceDecision, bounded_failure_text, current_time_millis,
    parse_source_effect_fence, source_batch_error_class, transaction::EngineTransaction,
};
use crate::source_effect::{self, SourceEffectAuthority, SourceEffectScope};

impl AnalyticsEngine {
    pub(super) fn source_effect_decision(
        &self,
        scope: &SourceEffectScope,
        authority: &SourceEffectAuthority,
        content_digest: &[u8; source_effect::CONTENT_DIGEST_BYTES],
    ) -> AnalyticsEngineResult<SourceEffectDecision> {
        // Load authority first so duplicate durable rows fail closed even when
        // a matching receipt would otherwise make this invocation a replay.
        let accepted = source_effect::load_authority(&self.conn, scope)?;
        let receipt = if scope.is_system() {
            source_effect::load_system_receipt(&self.conn, authority.execution_id())?
        } else {
            source_effect::load_receipt(&self.conn, authority.execution_id())?
        };
        if let Some(receipt) = receipt {
            receipt.validate_replay(scope, authority, content_digest, accepted.as_ref())?;
            return Ok(SourceEffectDecision::AlreadyApplied {
                summary: receipt.summary,
            });
        }

        let failure_decision = if scope.is_system() {
            SourceFailureFenceDecision::New
        } else {
            self.source_failure_fence_decision(scope, authority)?
        };
        if matches!(failure_decision, SourceFailureFenceDecision::StaleFence) {
            return Ok(SourceEffectDecision::StaleFence);
        }
        if scope.is_system()
            && !source_effect::system_checkpoint_matches(
                &self.conn,
                scope,
                scope.system_previous_checkpoint(),
            )?
        {
            return Ok(SourceEffectDecision::StaleFence);
        }
        // Each stream is ordered by its durable predecessor. Lease fences
        // belong to individual jobs: the next job can have a lower fence.
        // Receipt identity and the predecessor checks reject conflicting
        // replays and workers that attempt to advance an obsolete checkpoint.
        if !scope.is_system() && !self.table_source_predecessor_matches(scope)? {
            return Ok(SourceEffectDecision::StaleFence);
        }
        let Some(accepted) = accepted else {
            return Ok(SourceEffectDecision::New);
        };
        if accepted.execution_id == *authority.execution_id() {
            return Err(AnalyticsEngineError::IncompleteSourceEffectState);
        }
        Ok(SourceEffectDecision::New)
    }

    fn table_source_predecessor_matches(
        &self,
        scope: &SourceEffectScope,
    ) -> AnalyticsEngineResult<bool> {
        let Some((source_table_name, shard_id)) = scope.table_parts() else {
            return Err(AnalyticsEngineError::InvalidSourceEffectScope);
        };
        let requested = scope.table_previous_position();
        let mut statement = self.conn.prepare(
            "SELECT position
             FROM analytics_checkpoints
             WHERE source_table_name = ? AND shard_id = ?
             LIMIT 2",
        )?;
        let rows = statement.query_map(duckdb::params![source_table_name, shard_id], |row| {
            row.get::<_, String>(0)
        })?;
        let mut positions = Vec::with_capacity(2);
        for row in rows {
            positions.push(row?);
        }
        if positions.len() > 1 {
            return Err(AnalyticsEngineError::IncompleteSourceEffectState);
        }
        Ok(match (requested, positions.first()) {
            (None, None) => true,
            (Some(expected), Some(actual)) => expected == actual,
            _ => false,
        })
    }

    fn source_failure_fence_decision(
        &self,
        scope: &SourceEffectScope,
        authority: &SourceEffectAuthority,
    ) -> AnalyticsEngineResult<SourceFailureFenceDecision> {
        if self.source_failure_fence_is_stale(scope, authority)? {
            Ok(SourceFailureFenceDecision::StaleFence)
        } else {
            Ok(SourceFailureFenceDecision::New)
        }
    }

    fn source_failure_fence_is_stale(
        &self,
        scope: &SourceEffectScope,
        authority: &SourceEffectAuthority,
    ) -> AnalyticsEngineResult<bool> {
        let Some(highest_fence) = self.highest_source_failure_fence(scope)? else {
            return Ok(false);
        };
        if highest_fence != authority.fence() {
            return Ok(highest_fence > authority.fence());
        }

        if !self.has_source_failure_execution(scope, highest_fence, authority)? {
            return Ok(true);
        }
        self.has_conflicting_source_failure_execution(scope, highest_fence, authority)
    }

    fn highest_source_failure_fence(
        &self,
        scope: &SourceEffectScope,
    ) -> AnalyticsEngineResult<Option<std::num::NonZeroU64>> {
        let Some((source_table_name, shard_id)) = scope.table_parts() else {
            return Ok(None);
        };
        let fence = self
            .conn
            .query_row(
                "SELECT fence
                 FROM analytics_source_batch_failures
                 WHERE source_table_name = ?
                   AND shard_id = ?
                   AND fence IS NOT NULL
                 ORDER BY LENGTH(fence) DESC, fence DESC
                 LIMIT 1",
                duckdb::params![source_table_name, shard_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        fence
            .map(|fence| parse_source_effect_fence(fence.as_str()))
            .transpose()
    }

    fn has_source_failure_execution(
        &self,
        scope: &SourceEffectScope,
        fence: std::num::NonZeroU64,
        authority: &SourceEffectAuthority,
    ) -> AnalyticsEngineResult<bool> {
        let Some((source_table_name, shard_id)) = scope.table_parts() else {
            return Ok(false);
        };
        Ok(self
            .conn
            .query_row(
                "SELECT 1
             FROM analytics_source_batch_failures
             WHERE source_table_name = ?
               AND shard_id = ?
               AND fence = ?
               AND execution_id = ?
             LIMIT 1",
                duckdb::params![
                    source_table_name,
                    shard_id,
                    fence.get().to_string(),
                    duckdb::types::Value::Blob(authority.execution_id().to_vec()),
                ],
                |row| row.get::<_, i32>(0),
            )
            .optional()?
            .is_some())
    }

    fn has_conflicting_source_failure_execution(
        &self,
        scope: &SourceEffectScope,
        fence: std::num::NonZeroU64,
        authority: &SourceEffectAuthority,
    ) -> AnalyticsEngineResult<bool> {
        let Some((source_table_name, shard_id)) = scope.table_parts() else {
            return Ok(false);
        };
        Ok(self
            .conn
            .query_row(
                "SELECT 1
                 FROM analytics_source_batch_failures
                 WHERE source_table_name = ?
                   AND shard_id = ?
                   AND fence = ?
                   AND (execution_id IS NULL OR execution_id <> ?)
                 LIMIT 1",
                duckdb::params![
                    source_table_name,
                    shard_id,
                    fence.get().to_string(),
                    duckdb::types::Value::Blob(authority.execution_id().to_vec()),
                ],
                |row| row.get::<_, i32>(0),
            )
            .optional()?
            .is_some())
    }

    pub(super) fn persist_fenced_source_batch_failure(
        &self,
        scope: &SourceEffectScope,
        authority: &SourceEffectAuthority,
        context: &SourceBatchFailureContext,
        error: &AnalyticsEngineError,
    ) -> AnalyticsEngineResult<bool> {
        let transaction = EngineTransaction::begin_fenced(self)?;
        let stale = self.source_batch_failure_is_stale(scope, authority)?;
        let result = self.write_fenced_source_batch_failure(stale, authority, context, error);
        transaction.finish(result.map(|()| !stale))
    }

    fn source_batch_failure_is_stale(
        &self,
        scope: &SourceEffectScope,
        authority: &SourceEffectAuthority,
    ) -> AnalyticsEngineResult<bool> {
        if !scope.is_system() && !self.table_source_predecessor_matches(scope)? {
            return Ok(true);
        }
        self.source_failure_fence_is_stale(scope, authority)
    }

    fn write_fenced_source_batch_failure(
        &self,
        stale: bool,
        authority: &SourceEffectAuthority,
        context: &SourceBatchFailureContext,
        error: &AnalyticsEngineError,
    ) -> AnalyticsEngineResult<()> {
        // Preserve the deterministic-record audit even when a newer owner won
        // the destination fence. Marking this stale observation repaired keeps
        // it historical rather than creating a quarantine for the accepted
        // scope.
        if !stale {
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
        }
        self.conn.execute(
            "INSERT INTO analytics_source_batch_failures
                (source_table_name, shard_id, record_position,
                 analytics_table_name, error_class, error_message,
                 failed_at_ms, repaired_at_ms, fence, execution_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                context.source_table_name,
                context.shard_id,
                context.record_position,
                context.analytics_table_name,
                source_batch_error_class(error),
                bounded_failure_text(error.to_string().as_str()),
                current_time_millis(),
                stale.then(current_time_millis),
                authority.fence().get().to_string(),
                duckdb::types::Value::Blob(authority.execution_id().to_vec()),
            ],
        )?;
        Ok(())
    }
}
