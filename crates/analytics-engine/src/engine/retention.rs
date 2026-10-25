use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, RetentionEffectDecision,
    RetentionEffectSelection, transaction::EngineTransaction,
};
use crate::retention_effect::{
    self, RetentionEffectDisposition, RetentionEffectRequest, RetentionEffectScope,
    RetentionEffectSummary,
};

impl AnalyticsEngine {
    /// Persists the scheduler's trusted time and occurrence before a job is
    /// enqueued. A later effect must match this row exactly before it can
    /// touch analytics data.
    pub fn authorize_retention_schedule_occurrence(
        &self,
        table_name: &str,
        cutoff_ms: i64,
        schedule_at_ms: i64,
        server_time_ms: u64,
    ) -> AnalyticsEngineResult<()> {
        if !self.destination_fence.supports_sources() {
            return Err(AnalyticsEngineError::RetentionEffectFenceUnavailable);
        }
        let scope = RetentionEffectScope::try_new(table_name.to_owned())?;
        RetentionEffectRequest::validate_schedule(cutoff_ms, schedule_at_ms, server_time_ms)?;
        self.refresh_object_storage_credentials_if_needed()?;
        let transaction = EngineTransaction::begin_fenced(self)?;
        let result = retention_effect::authorize_schedule_occurrence(
            &self.conn,
            &scope,
            cutoff_ms,
            schedule_at_ms,
            server_time_ms,
        );
        transaction.finish(result)
    }

    /// Returns durable unfinished work, including an occurrence authorized
    /// before the scheduler could enqueue its first job.
    pub fn find_pending_retention_sweep(
        &self,
        table_name: &str,
    ) -> AnalyticsEngineResult<Option<crate::RetentionScheduleOccurrence>> {
        let scope = RetentionEffectScope::try_new(table_name.to_owned())?;
        self.refresh_object_storage_credentials_if_needed()?;
        retention_effect::find_pending_schedule_occurrence(&self.conn, &scope)
    }

    /// Deletes one deterministic retention batch under a destination fence.
    /// The selected rows, domain cursor, accepted authority, and destination
    /// receipt commit together; a replay never deletes a second batch.
    pub fn delete_expired_rows_fenced(
        &self,
        request: RetentionEffectRequest,
    ) -> AnalyticsEngineResult<RetentionEffectDisposition> {
        if !self.destination_fence.supports_sources() {
            return Err(AnalyticsEngineError::RetentionEffectFenceUnavailable);
        }
        self.refresh_object_storage_credentials_if_needed()?;
        let transaction = EngineTransaction::begin_fenced(self)?;
        let result = self.apply_retention_effect_transaction(&request);
        match result {
            Ok(RetentionEffectDisposition::AlreadyApplied { .. })
            | Ok(RetentionEffectDisposition::StaleFence) => {
                let _ = transaction.rollback();
                result
            }
            result => transaction.finish(result),
        }
    }

    fn apply_retention_effect_transaction(
        &self,
        request: &RetentionEffectRequest,
    ) -> AnalyticsEngineResult<RetentionEffectDisposition> {
        match self.retention_effect_decision(request)? {
            RetentionEffectDecision::AlreadyApplied { summary } => {
                Ok(RetentionEffectDisposition::AlreadyApplied { summary })
            }
            RetentionEffectDecision::StaleFence => Ok(RetentionEffectDisposition::StaleFence),
            RetentionEffectDecision::New => self.apply_new_retention_effect(request),
        }
    }

    fn apply_new_retention_effect(
        &self,
        request: &RetentionEffectRequest,
    ) -> AnalyticsEngineResult<RetentionEffectDisposition> {
        let selection = self.select_retention_effect_batch(request)?;
        self.validate_retention_effect_selection(&selection)?;
        let deleted = self.delete_retention_effect_batch(request, &selection)?;
        let summary = selection.summary(deleted);
        self.persist_retention_effect(request, &summary)?;
        Ok(RetentionEffectDisposition::Applied { summary })
    }

    fn select_retention_effect_batch(
        &self,
        request: &RetentionEffectRequest,
    ) -> AnalyticsEngineResult<RetentionEffectSelection> {
        let ids = retention_effect::select_expired_ids(
            &self.conn,
            &request.scope,
            request.cutoff_ms,
            request.cursor.as_deref(),
            request.batch_size.saturating_add(1),
        )?;
        #[cfg(test)]
        if let Some((ready, release)) = &self.retention_effect_barriers {
            ready.wait();
            release.wait();
        }

        let selected_count = ids.len().min(request.batch_size as usize);
        let complete = ids.len() <= selected_count;
        Ok(RetentionEffectSelection {
            ids,
            selected_count,
            complete,
        })
    }

    fn validate_retention_effect_selection(
        &self,
        selection: &RetentionEffectSelection,
    ) -> AnalyticsEngineResult<()> {
        let selected_ids = selection.selected_ids();
        if selected_ids
            .iter()
            .enumerate()
            .any(|(index, id)| selected_ids[..index].contains(id))
        {
            return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
        }
        Ok(())
    }

    fn delete_retention_effect_batch(
        &self,
        request: &RetentionEffectRequest,
        selection: &RetentionEffectSelection,
    ) -> AnalyticsEngineResult<u64> {
        let deleted = retention_effect::delete_expired_ids(
            &self.conn,
            &request.scope,
            request.cutoff_ms,
            selection.selected_ids(),
        )?;
        if deleted != selection.selected_count as u64 {
            return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
        }
        Ok(deleted)
    }

    fn persist_retention_effect(
        &self,
        request: &RetentionEffectRequest,
        summary: &RetentionEffectSummary,
    ) -> AnalyticsEngineResult<()> {
        retention_effect::insert_receipt(
            &self.conn,
            &request.scope,
            &request.authority,
            request,
            request.cursor.as_deref(),
            summary,
        )?;
        retention_effect::replace_authority(
            &self.conn,
            &request.scope,
            &request.authority,
            summary.cursor(),
        )?;
        retention_effect::advance_schedule_occurrence(&self.conn, request, summary)?;
        Ok(())
    }

    fn retention_effect_decision(
        &self,
        request: &RetentionEffectRequest,
    ) -> AnalyticsEngineResult<RetentionEffectDecision> {
        let accepted = retention_effect::load_authority(&self.conn, &request.scope)?;
        let receipt = retention_effect::load_receipt(&self.conn, request.authority.execution_id())?;
        let occurrence = retention_effect::load_schedule_occurrence(
            &self.conn,
            &request.scope,
            request.schedule_at_ms,
        )?
        .ok_or(AnalyticsEngineError::RetentionEffectScheduleUnavailable)?;
        if occurrence.cutoff_ms != request.cutoff_ms
            || occurrence.server_time_ms != request.server_time_ms
        {
            return Err(AnalyticsEngineError::ConflictingRetentionScheduleOccurrence);
        }
        if let Some(receipt) = receipt {
            if accepted.as_ref().is_some_and(|authority| {
                authority.execution_id == *request.authority.execution_id()
            }) && (occurrence.cursor.as_deref() != receipt.summary.cursor()
                || occurrence.complete != receipt.summary.complete())
            {
                return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
            }
            return self.reconcile_retention_receipt(request, accepted, receipt);
        }
        if accepted
            .as_ref()
            .is_some_and(|accepted| accepted.execution_id == *request.authority.execution_id())
        {
            return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
        }
        if occurrence.complete || (request.cursor.is_none() && occurrence.cursor.is_some()) {
            return Ok(RetentionEffectDecision::StaleFence);
        }
        if occurrence.cursor.as_deref() != request.cursor.as_deref() {
            return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
        }
        if accepted
            .as_ref()
            .and_then(|authority| authority.cursor.as_deref())
            != request.cursor.as_deref()
        {
            return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
        }
        Ok(RetentionEffectDecision::New)
    }

    fn reconcile_retention_receipt(
        &self,
        request: &RetentionEffectRequest,
        accepted: Option<retention_effect::StoredAuthority>,
        receipt: retention_effect::StoredReceipt,
    ) -> AnalyticsEngineResult<RetentionEffectDecision> {
        if receipt.table_name != request.scope.table_name()
            || receipt.cutoff_ms != request.cutoff_ms
            || receipt.schedule_at_ms != request.schedule_at_ms
            || receipt.server_time_ms != request.server_time_ms
            || receipt.batch_size != request.batch_size
            || receipt.cursor_before != request.cursor
        {
            return Err(AnalyticsEngineError::ConflictingRetentionEffectReceipt);
        }
        if request.authority.fence() < receipt.fence {
            return Ok(RetentionEffectDecision::StaleFence);
        }
        let Some(accepted) = accepted else {
            return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
        };
        if accepted.execution_id != *request.authority.execution_id() {
            return Ok(RetentionEffectDecision::StaleFence);
        }
        if accepted.fence != receipt.fence {
            return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
        }
        if accepted.cursor.as_deref() != receipt.summary.cursor() {
            return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
        }
        Ok(RetentionEffectDecision::AlreadyApplied {
            summary: receipt.summary,
        })
    }
}
