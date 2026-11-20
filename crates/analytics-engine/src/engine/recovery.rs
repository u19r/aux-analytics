use std::collections::HashMap;

use analytics_contract::{AnalyticsManifest, RowIdentity, StorageStreamRecord};
use duckdb::types::Value;
use storage_types::SystemStreamScope;

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, RepairReceipt,
    columns_for_registration, repair_row_matches_json,
    source_record_key::source_key_digest_for_scope, transaction::EngineTransaction,
};
use crate::row::RowSource;

impl AnalyticsEngine {
    /// Write and verify up to 256 source images in a new inspection database.
    /// This does not install source checkpoints or reproduce event history.
    pub fn recover_inspection_page(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        source_table_name: &str,
        scope: Option<&SystemStreamScope>,
        records: Vec<StorageStreamRecord>,
    ) -> AnalyticsEngineResult<usize> {
        let table = self.repair_table(manifest, analytics_table_name)?;
        if records.is_empty()
            || records.len() > 256
            || matches!(table.row_identity, RowIdentity::RecordKey)
            || !table
                .source_table_name_prefix
                .as_deref()
                .map_or(table.source_table_name == source_table_name, |prefix| {
                    source_table_name.starts_with(prefix)
                })
            || (matches!(table.row_identity, RowIdentity::StreamKeys) && scope.is_none())
            || table.retention.is_some()
            || table.skip_delete
        {
            return Err(AnalyticsEngineError::RecoveryPageMismatch);
        }
        let mut expected = HashMap::with_capacity(records.len());
        let mut predicates = Vec::with_capacity(records.len());
        let mut parameters = Vec::with_capacity(records.len() * 3);
        let mut rows = Vec::with_capacity(records.len());
        for record in &records {
            let image = record
                .new_image
                .as_ref()
                .ok_or(AnalyticsEngineError::RecoveryPageMismatch)?;
            if record.old_image.is_some() {
                return Err(AnalyticsEngineError::RecoveryPageMismatch);
            }
            if !self.item_matches_registration(table, Some(image))? {
                continue;
            }
            let digest = if matches!(table.row_identity, RowIdentity::StreamKeys) {
                Some(source_key_digest_for_scope(
                    analytics_table_name,
                    source_table_name,
                    scope.ok_or(AnalyticsEngineError::RecoveryPageMismatch)?,
                    &record.keys,
                )?)
            } else {
                None
            };
            let source = RowSource {
                source_table_name,
                record_key: b"",
                record_keys: &record.keys,
                stream_key_digest: digest.as_deref(),
            };
            let mut projected = self.row_from_item(table, source, image, 0, None)?;
            for column in columns_for_registration(table) {
                projected
                    .entry(column.column_name)
                    .or_insert(serde_json::Value::Null);
            }
            let receipt = RepairReceipt::from_row(
                "inspection",
                analytics_table_name,
                "inspection",
                &projected,
                true,
            )?;
            let identity = (
                receipt.source_table_name,
                receipt.tenant_id,
                receipt.source_row_id,
            );
            parameters.extend([
                Value::Text(identity.0.clone()),
                Value::Text(identity.1.clone()),
                Value::Text(identity.2.clone()),
            ]);
            predicates.push("(table_name = ? AND tenant_id = ? AND __id = ?)");
            if expected
                .insert(identity, receipt.expected_row_json)
                .is_some()
            {
                return Err(AnalyticsEngineError::RecoveryPageMismatch);
            }
            rows.push(self.batch_row_from_item(table, source, image, 0, None)?);
        }
        if rows.is_empty() {
            return Ok(0);
        }
        let mut transaction = EngineTransaction::begin(self)?;
        for row in rows {
            transaction.stage_inspection_row(table, row)?;
        }
        transaction.commit()?;
        let sql = format!(
            "SELECT * FROM {} WHERE {}",
            crate::sql::quote_identifier(analytics_table_name),
            predicates.join(" OR ")
        );
        let actual = self.query_sql_json_with_optional_timeout(
            &sql,
            &parameters,
            None,
            Some(expected.len()),
        )?;
        if actual.len() != expected.len() {
            return Err(AnalyticsEngineError::RecoveryPageMismatch);
        }
        let matched = expected.len();
        for row in actual {
            let identity = (
                row.get("table_name").and_then(serde_json::Value::as_str),
                row.get("tenant_id").and_then(serde_json::Value::as_str),
                row.get("__id").and_then(serde_json::Value::as_str),
            );
            let (Some(source), Some(tenant), Some(id)) = identity else {
                return Err(AnalyticsEngineError::RecoveryPageMismatch);
            };
            let key = (source.to_owned(), tenant.to_owned(), id.to_owned());
            let Some(expected_row) = expected.remove(&key) else {
                return Err(AnalyticsEngineError::RecoveryPageMismatch);
            };
            if !repair_row_matches_json(&expected_row, &row) {
                return Err(AnalyticsEngineError::RecoveryPageMismatch);
            }
        }
        if !expected.is_empty() {
            return Err(AnalyticsEngineError::RecoveryPageMismatch);
        }
        Ok(matched)
    }
}
