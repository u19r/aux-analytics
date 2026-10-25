use std::collections::BTreeMap;

use analytics_contract::{StorageStreamRecord, TableRegistration};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, IngestExecution, IngestOutcome,
    RepairReceipt, immediate_ingest::StreamRecordIngest, repair_json_values_equal,
    transaction::EngineTransaction, validate_repair_receipt_inputs,
};
use crate::maintenance_effect::{
    MaintenanceEffectApplyRequest, MaintenanceEffectDisposition, MaintenanceEffectVerifyRequest,
};

struct RepairRows {
    old: Option<BTreeMap<String, serde_json::Value>>,
    new: Option<BTreeMap<String, serde_json::Value>>,
}

impl RepairRows {
    fn into_receipt(
        self,
        repair_key: &str,
        table_name: &str,
        source_revision: &str,
        outcome: IngestOutcome,
    ) -> AnalyticsEngineResult<RepairReceipt> {
        let (row, visible) = match outcome {
            IngestOutcome::Inserted | IngestOutcome::Updated => (self.new, true),
            IngestOutcome::Deleted => (self.old.or(self.new), false),
            IngestOutcome::Skipped => (None, false),
        };
        let row =
            row.ok_or_else(|| AnalyticsEngineError::RepairRecordSkipped(table_name.to_string()))?;
        RepairReceipt::from_row(repair_key, table_name, source_revision, &row, visible)
    }
    fn matches_receipt(&self, receipt: &RepairReceipt, table_name: &str, revision: &str) -> bool {
        if receipt.source_revision != revision || receipt.analytics_table_name != table_name {
            return false;
        }
        let row = if receipt.row_visible {
            self.new.as_ref()
        } else {
            self.old.as_ref()
        };
        row.is_some_and(|row| receipt.matches_row(row))
    }
}

impl AnalyticsEngine {
    pub fn apply_stream_record_with_receipt(
        &self,
        request: MaintenanceEffectApplyRequest<'_>,
    ) -> AnalyticsEngineResult<MaintenanceEffectDisposition> {
        validate_repair_receipt_inputs(request.repair_key, request.source_revision)?;
        if !self.destination_fence.supports_repairs() {
            return Err(AnalyticsEngineError::RepairReceiptFenceUnavailable);
        }
        let table = self.repair_table(request.manifest, request.analytics_table_name)?;

        #[cfg(test)]
        if let Some(barrier) = &self.repair_receipt_barrier {
            barrier.wait();
        }

        let _repair_receipt_guard = self.lock_repair_receipts()?;
        let _repair_receipt_file_guard = self.lock_repair_receipt_file()?;

        let mut transaction = EngineTransaction::begin_fenced(self)?;
        let rows = self.canonical_repair_rows(table, request.record_key, &request.record)?;
        let result = self.apply_repair_transaction(request, rows, &mut transaction);
        transaction.finish(result)
    }

    /// Verifies both the canonical destination row and the durable receipt
    /// written by [`Self::apply_stream_record_with_receipt`].  Missing,
    /// conflicting, or stale receipts return `Ok(false)` so a caller can
    /// never accept a count-only or otherwise unproven repair. Once content
    /// and source revision match, the exact receipt row is consumed in the
    /// same transaction; a failed verification leaves it available for retry.
    pub fn verify_stream_record_with_receipt(
        &self,
        request: MaintenanceEffectVerifyRequest<'_>,
    ) -> AnalyticsEngineResult<bool> {
        validate_repair_receipt_inputs(request.repair_key, request.source_revision)?;
        if !self.destination_fence.supports_repairs() {
            return Err(AnalyticsEngineError::RepairReceiptFenceUnavailable);
        }
        let table = self.repair_table(request.manifest, request.analytics_table_name)?;

        let _repair_receipt_guard = self.lock_repair_receipts()?;
        let _repair_receipt_file_guard = self.lock_repair_receipt_file()?;

        self.refresh_object_storage_credentials_if_needed()?;
        let transaction = EngineTransaction::begin_fenced(self)?;
        let rows = self.canonical_repair_rows(table, request.record_key, request.expected)?;
        let result = self.verify_repair_transaction(&request, &rows);
        if matches!(result, Ok(false)) {
            let _ = transaction.rollback();
            return Ok(false);
        }
        transaction.finish(result)
    }
    fn apply_repair_transaction(
        &self,
        request: MaintenanceEffectApplyRequest<'_>,
        rows: RepairRows,
        transaction: &mut EngineTransaction<'_>,
    ) -> AnalyticsEngineResult<MaintenanceEffectDisposition> {
        let MaintenanceEffectApplyRequest {
            manifest,
            analytics_table_name,
            record_key,
            record,
            repair_key,
            source_revision,
            authority,
        } = request;
        self.advance_repair_authority(authority)?;
        if let Some(existing_effect) = self.load_repair_effect(repair_key)? {
            if existing_effect.execution_id != authority.execution_id()
                || !existing_effect.matches_request(
                    analytics_table_name,
                    source_revision,
                    rows.old.as_ref(),
                    rows.new.as_ref(),
                )
            {
                return Err(AnalyticsEngineError::ConflictingMaintenanceEffectReceipt(
                    repair_key.to_string(),
                ));
            }
            if existing_effect.fence != authority.fence().get().to_string() {
                self.update_repair_effect_fence(authority, repair_key)?;
            }
            return Ok(MaintenanceEffectDisposition::AlreadyApplied);
        }
        if self.load_repair_receipt(repair_key)?.is_some() {
            return Err(AnalyticsEngineError::IncompleteMaintenanceEffectState);
        }

        let outcome = transaction.ingest_record(StreamRecordIngest {
            manifest,
            analytics_table_name,
            record_key,
            record,
            execution: IngestExecution::default(),
        })?;
        let receipt =
            rows.into_receipt(repair_key, analytics_table_name, source_revision, outcome)?;
        self.replace_repair_receipt(&receipt)?;
        self.replace_repair_effect(authority, &receipt, false)?;
        Ok(MaintenanceEffectDisposition::Applied)
    }

    fn verify_repair_transaction(
        &self,
        request: &MaintenanceEffectVerifyRequest<'_>,
        rows: &RepairRows,
    ) -> AnalyticsEngineResult<bool> {
        let MaintenanceEffectVerifyRequest {
            analytics_table_name,
            repair_key,
            source_revision,
            authority,
            ..
        } = *request;
        self.advance_repair_authority(authority)?;
        let effect = self.load_repair_effect(repair_key)?;
        if let Some(effect) = effect.as_ref() {
            effect.validate_authority(authority)?;
            if !effect.matches_request(
                analytics_table_name,
                source_revision,
                rows.old.as_ref(),
                rows.new.as_ref(),
            ) {
                return Ok(false);
            }
            if effect.consumed {
                let receipt = effect.as_receipt()?;
                return self.repair_destination_matches(&receipt);
            }
        }
        self.verify_repair_receipt(request, rows, effect.as_ref())
    }

    fn verify_repair_receipt(
        &self,
        request: &MaintenanceEffectVerifyRequest<'_>,
        rows: &RepairRows,
        effect: Option<&super::RepairEffect>,
    ) -> AnalyticsEngineResult<bool> {
        let MaintenanceEffectVerifyRequest {
            analytics_table_name,
            repair_key,
            source_revision,
            authority,
            ..
        } = *request;
        let Some(receipt) = self.load_repair_receipt(repair_key)? else {
            return Ok(false);
        };
        if !rows.matches_receipt(&receipt, analytics_table_name, source_revision) {
            return Ok(false);
        }
        if let Some(effect) = effect
            && (effect.row_visible != receipt.row_visible
                || !repair_json_values_equal(&effect.expected_row_json, &receipt.expected_row_json))
        {
            return Ok(false);
        }

        if !self.repair_destination_matches(&receipt)? {
            return Ok(false);
        }

        self.consume_repair_receipt(&receipt)?;
        self.mark_repair_effect_consumed(authority, &receipt)?;
        Ok(true)
    }

    fn lock_repair_receipts(&self) -> AnalyticsEngineResult<Option<std::sync::MutexGuard<'_, ()>>> {
        self.repair_receipt_lock
            .as_ref()
            .map(|lock| {
                lock.lock().map_err(|_| {
                    AnalyticsEngineError::Io(std::io::Error::other(
                        "repair receipt lock is poisoned",
                    ))
                })
            })
            .transpose()
    }

    fn canonical_repair_rows(
        &self,
        table: &TableRegistration,
        record_key: &[u8],
        record: &StorageStreamRecord,
    ) -> AnalyticsEngineResult<RepairRows> {
        let convert = |item| self.canonical_repair_row(table, record_key, &record.keys, item);
        Ok(RepairRows {
            old: record.old_image.as_ref().map(convert).transpose()?,
            new: record.new_image.as_ref().map(convert).transpose()?,
        })
    }
}
