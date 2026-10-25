use std::{cell::Cell, collections::BTreeMap};

use analytics_contract::{AnalyticsManifest, SourcePosition, TableRegistration};
use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};

use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestOutcome, SourceBatchRecord,
    StreamRecordBatchItem, update_row_values_for_test,
};
use crate::{
    batch_writer::{BatchSqlExecutor, BatchUpsertOutcome},
    row::RowSource,
    source_effect::{
        SourceEffectAuthority, SourceEffectDisposition, SourceEffectRequest,
        SourceEffectRequestInput, SourceEffectScope, manifest_digest,
    },
};

thread_local! {
    static JSON_BRIDGE_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

struct JsonBridgeGuard(bool);

impl JsonBridgeGuard {
    fn select(direct_values: bool) -> Self {
        Self(JSON_BRIDGE_ACTIVE.replace(!direct_values))
    }
}

impl Drop for JsonBridgeGuard {
    fn drop(&mut self) {
        JSON_BRIDGE_ACTIVE.set(self.0);
    }
}

pub(crate) fn is_json_bridge_active() -> bool {
    JSON_BRIDGE_ACTIVE.get()
}

impl AnalyticsEngine {
    #[cfg(test)]
    pub(crate) fn performance_batch_ingest_for_test(
        &self,
        table: &TableRegistration,
        records: &[StreamRecordBatchItem],
        direct_values: bool,
    ) -> AnalyticsEngineResult<()> {
        self.conn.execute_batch(
            format!(
                "DELETE FROM {}",
                crate::sql::quote_identifier(table.analytics_table_name.as_str())
            )
            .as_str(),
        )?;
        let mut batch = BatchSqlExecutor::new(&self.conn);
        for item in records {
            let source = RowSource {
                stream_key_digest: None,
                source_table_name: item.source_table_name.as_str(),
                record_key: item.record_key.as_slice(),
                record_keys: &item.record.keys,
            };
            let image = item
                .record
                .new_image
                .as_ref()
                .expect("performance fixture has a new image");
            if direct_values {
                let row = self.batch_row_from_item(table, source, image, 1_900_000_000, None)?;
                batch.apply_upsert(table, row, BatchUpsertOutcome::Inserted, None)?;
            } else {
                let row = self.row_from_item(table, source, image, 1_900_000_000, None)?;
                let row = self.batch_row_from_json(table, &row)?;
                batch.apply_upsert(table, row, BatchUpsertOutcome::Inserted, None)?;
            }
        }
        batch.flush()
    }

    #[cfg(test)]
    pub(crate) fn performance_reset_source_destination_for_test(
        &self,
        table_name: &str,
    ) -> AnalyticsEngineResult<()> {
        for statement in [
            format!("DELETE FROM {}", crate::sql::quote_identifier(table_name)),
            "DELETE FROM __analytics_row_source_positions".to_string(),
            "DELETE FROM __analytics_source_record_states".to_string(),
            "DELETE FROM analytics_system_stream_checkpoints".to_string(),
            "DELETE FROM __analytics_system_source_effect_authority".to_string(),
            "DELETE FROM __analytics_system_source_effect_receipts".to_string(),
            "DELETE FROM __analytics_source_effect_authority".to_string(),
            "DELETE FROM __analytics_source_effect_receipts".to_string(),
            "DELETE FROM analytics_source_batch_failures".to_string(),
        ] {
            self.conn.execute_batch(statement.as_str())?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn performance_update_values(
        table: &TableRegistration,
        row: &BTreeMap<String, serde_json::Value>,
        update_columns: &[&str],
    ) -> Vec<duckdb::types::Value> {
        update_row_values_for_test(table, row, update_columns)
    }

    pub(crate) fn performance_immediate_ingest_for_test(
        &self,
        manifest: &AnalyticsManifest,
        item: &StreamRecordBatchItem,
        source_position: Option<&SourcePosition>,
        direct_values: bool,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let _bridge = JsonBridgeGuard::select(direct_values);
        match source_position {
            Some(position) => self.ingest_stream_record_at_source_position(
                manifest,
                &item.analytics_table_name,
                &item.record_key,
                item.record.clone(),
                &position.clone(),
            ),
            None => self.ingest_stream_record(
                manifest,
                &item.analytics_table_name,
                &item.record_key,
                item.record.clone(),
            ),
        }
    }

    pub(crate) fn performance_system_ingest_for_test(
        &self,
        manifest: &AnalyticsManifest,
        records: &[SourceBatchRecord],
        source_scope: &SystemStreamScope,
        checkpoint: &SystemStreamCheckpointToken,
        direct_values: bool,
    ) -> AnalyticsEngineResult<SourceEffectDisposition> {
        let _bridge = JsonBridgeGuard::select(direct_values);
        let request = SourceEffectRequest::try_new(SourceEffectRequestInput {
            manifest,
            manifest_digest: manifest_digest(manifest),
            authority: SourceEffectAuthority::try_new([42; 16], 1)?,
            scope: SourceEffectScope::try_new_system(
                source_scope.clone(),
                None,
                checkpoint.clone(),
            )?,
            records: records.to_vec(),
            checkpoints: Vec::new(),
            privacy_policy: None,
        })?;
        self.ingest_fenced_source_batch(request)
    }
}
