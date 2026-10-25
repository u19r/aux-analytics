use std::collections::BTreeMap;

use analytics_contract::{AnalyticsManifest, StorageItem, TableRegistration};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, RepairEffect, RepairReceipt,
    columns_for_registration, current_time_millis, repair_row_matches_json,
};
use crate::{maintenance_effect::MaintenanceEffectAuthority, row::RowSource, sql};

impl AnalyticsEngine {
    pub(super) fn repair_table<'manifest>(
        &self,
        manifest: &'manifest AnalyticsManifest,
        analytics_table_name: &str,
    ) -> AnalyticsEngineResult<&'manifest TableRegistration> {
        let table = self.registered_table(manifest, analytics_table_name)?;
        if table.row_expansion.is_some() {
            return Err(AnalyticsEngineError::UnsupportedRepairReceiptExpansion(
                analytics_table_name.to_string(),
            ));
        }
        Ok(table)
    }

    pub(super) fn canonical_repair_row(
        &self,
        table: &TableRegistration,
        record_key: &[u8],
        record_keys: &StorageItem,
        item: &StorageItem,
    ) -> AnalyticsEngineResult<BTreeMap<String, serde_json::Value>> {
        let mut row = self.row_from_item(
            table,
            RowSource {
                stream_key_digest: None,
                source_table_name: table.source_table_name.as_str(),
                record_key,
                record_keys,
            },
            item,
            0,
            None,
        )?;
        for column in columns_for_registration(table) {
            row.entry(column.column_name)
                .or_insert(serde_json::Value::Null);
        }
        Ok(row)
    }

    pub(super) fn load_repair_receipt(
        &self,
        repair_key: &str,
    ) -> AnalyticsEngineResult<Option<RepairReceipt>> {
        let mut statement = self.conn.prepare(
            "SELECT analytics_table_name, source_table_name, tenant_id,
                    source_row_id, source_revision, expected_row_json,
                    row_visible
             FROM __analytics_row_repair_receipts
             WHERE repair_key = ?
             LIMIT 2",
        )?;
        let rows = statement.query_map(duckdb::params![repair_key], |row| {
            Ok(RepairReceipt {
                repair_key: repair_key.to_string(),
                analytics_table_name: row.get(0)?,
                source_table_name: row.get(1)?,
                tenant_id: row.get(2)?,
                source_row_id: row.get(3)?,
                source_revision: row.get(4)?,
                expected_row_json: row.get(5)?,
                row_visible: row.get(6)?,
            })
        })?;
        let mut receipts = Vec::with_capacity(2);
        for row in rows {
            receipts.push(row?);
        }
        match receipts.len() {
            0 => Ok(None),
            1 => Ok(receipts.pop()),
            _ => Err(AnalyticsEngineError::AmbiguousRepairReceipt(
                repair_key.to_string(),
            )),
        }
    }

    pub(super) fn load_repair_effect(
        &self,
        repair_key: &str,
    ) -> AnalyticsEngineResult<Option<RepairEffect>> {
        let mut statement = self.conn.prepare(
            "SELECT execution_id, fence, analytics_table_name, source_revision,
                    expected_row_json, row_visible, consumed
             FROM __analytics_row_repair_effects
             WHERE repair_key = ?
             LIMIT 2",
        )?;
        let rows = statement.query_map(duckdb::params![repair_key], |row| {
            Ok(RepairEffect {
                execution_id: row.get(0)?,
                fence: row.get(1)?,
                repair_key: repair_key.to_string(),
                analytics_table_name: row.get(2)?,
                source_revision: row.get(3)?,
                expected_row_json: row.get(4)?,
                row_visible: row.get(5)?,
                consumed: row.get(6)?,
            })
        })?;
        let effects = rows.collect::<Result<Vec<_>, _>>()?;
        match effects.len() {
            0 => Ok(None),
            1 => Ok(effects.into_iter().next()),
            _ => Err(AnalyticsEngineError::AmbiguousMaintenanceEffectReceipt(
                repair_key.to_string(),
            )),
        }
    }

    pub(super) fn replace_repair_effect(
        &self,
        authority: &MaintenanceEffectAuthority,
        receipt: &RepairReceipt,
        consumed: bool,
    ) -> AnalyticsEngineResult<()> {
        self.conn.execute(
            "DELETE FROM __analytics_row_repair_effects WHERE repair_key = ?",
            duckdb::params![receipt.repair_key.as_str()],
        )?;
        self.conn.execute(
            "INSERT INTO __analytics_row_repair_effects
                (execution_id, fence, repair_key, analytics_table_name, source_revision,
                 expected_row_json, row_visible, consumed, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                authority.execution_id(),
                authority.fence().get().to_string(),
                receipt.repair_key.as_str(),
                receipt.analytics_table_name.as_str(),
                receipt.source_revision.as_str(),
                receipt.expected_row_json.as_str(),
                receipt.row_visible,
                consumed,
                current_time_millis(),
            ],
        )?;
        Ok(())
    }

    pub(super) fn update_repair_effect_fence(
        &self,
        authority: &MaintenanceEffectAuthority,
        repair_key: &str,
    ) -> AnalyticsEngineResult<()> {
        let changed = self.conn.execute(
            "UPDATE __analytics_row_repair_effects
             SET fence = ?, updated_at_ms = ?
             WHERE execution_id = ? AND repair_key = ?",
            duckdb::params![
                authority.fence().get().to_string(),
                current_time_millis(),
                authority.execution_id(),
                repair_key,
            ],
        )?;
        if changed == 1 {
            Ok(())
        } else {
            Err(AnalyticsEngineError::AmbiguousMaintenanceEffectReceipt(
                repair_key.to_string(),
            ))
        }
    }

    pub(super) fn mark_repair_effect_consumed(
        &self,
        authority: &MaintenanceEffectAuthority,
        receipt: &RepairReceipt,
    ) -> AnalyticsEngineResult<()> {
        let changed = self.conn.execute(
            "UPDATE __analytics_row_repair_effects
             SET consumed = TRUE, updated_at_ms = ?
             WHERE repair_key = ? AND execution_id = ? AND fence = ?",
            duckdb::params![
                current_time_millis(),
                receipt.repair_key.as_str(),
                authority.execution_id(),
                authority.fence().get().to_string(),
            ],
        )?;
        if changed == 1 {
            return Ok(());
        }
        self.replace_repair_effect(authority, receipt, true)
    }

    pub(super) fn advance_repair_authority(
        &self,
        authority: &MaintenanceEffectAuthority,
    ) -> AnalyticsEngineResult<()> {
        let mut statement = self.conn.prepare(
            "SELECT fence FROM __analytics_row_repair_authority
             WHERE execution_id = ?
             LIMIT 2",
        )?;
        let fences = statement
            .query_map(duckdb::params![authority.execution_id()], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if fences.len() > 1 {
            return Err(AnalyticsEngineError::AmbiguousMaintenanceEffectAuthority);
        }
        let current = fences
            .first()
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| AnalyticsEngineError::AmbiguousMaintenanceEffectAuthority)?;
        if current.is_some_and(|fence| fence > authority.fence().get()) {
            return Err(AnalyticsEngineError::StaleMaintenanceEffectFence(
                authority.execution_id().to_string(),
            ));
        }
        if current.is_none_or(|fence| fence < authority.fence().get()) {
            self.conn.execute(
                "DELETE FROM __analytics_row_repair_authority WHERE execution_id = ?",
                duckdb::params![authority.execution_id()],
            )?;
            self.conn.execute(
                "INSERT INTO __analytics_row_repair_authority
                    (execution_id, fence, updated_at_ms)
                 VALUES (?, ?, ?)",
                duckdb::params![
                    authority.execution_id(),
                    authority.fence().get().to_string(),
                    current_time_millis(),
                ],
            )?;
        }
        Ok(())
    }

    pub(super) fn consume_repair_receipt(
        &self,
        receipt: &RepairReceipt,
    ) -> AnalyticsEngineResult<()> {
        // `load_repair_receipt` used LIMIT 2 and the caller validated every
        // receipt field before reaching this delete. DuckLake normalizes JSON
        // predicates differently from DuckDB, so the exact durable key is the
        // safe deletion predicate after that fenced, unique load.
        let consumed = self.conn.execute(
            "DELETE FROM __analytics_row_repair_receipts
             WHERE repair_key = ?",
            duckdb::params![receipt.repair_key.as_str()],
        )?;
        if consumed == 1 {
            Ok(())
        } else {
            Err(AnalyticsEngineError::RepairReceiptNotConsumed(
                receipt.repair_key.clone(),
            ))
        }
    }

    pub(super) fn replace_repair_receipt(
        &self,
        receipt: &RepairReceipt,
    ) -> AnalyticsEngineResult<()> {
        // DuckLake does not support primary-key constraints. Delete and insert
        // in the caller's fenced transaction to retain idempotent replacement
        // semantics on both DuckDB and DuckLake catalogs. The caller has
        // already checked the existing receipt while holding the fence.
        self.conn.execute(
            "DELETE FROM __analytics_row_repair_receipts WHERE repair_key = ?",
            duckdb::params![receipt.repair_key.as_str()],
        )?;
        self.conn.execute(
            "INSERT INTO __analytics_row_repair_receipts
                (repair_key, analytics_table_name, source_table_name, tenant_id,
                 source_row_id, source_revision, expected_row_json, row_visible,
                 updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                receipt.repair_key.as_str(),
                receipt.analytics_table_name.as_str(),
                receipt.source_table_name.as_str(),
                receipt.tenant_id.as_str(),
                receipt.source_row_id.as_str(),
                receipt.source_revision.as_str(),
                receipt.expected_row_json.as_str(),
                receipt.row_visible,
                current_time_millis(),
            ],
        )?;
        Ok(())
    }

    fn query_repair_rows(
        &self,
        receipt: &RepairReceipt,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        let query = format!(
            "SELECT * FROM {} WHERE table_name = ? AND tenant_id = ? AND __id = ? LIMIT 2",
            sql::quote_identifier(receipt.analytics_table_name.as_str())
        );
        self.query_sql_json_with_optional_timeout(
            query.as_str(),
            &[
                duckdb::types::Value::Text(receipt.source_table_name.clone()),
                duckdb::types::Value::Text(receipt.tenant_id.clone()),
                duckdb::types::Value::Text(receipt.source_row_id.clone()),
            ],
            None,
            Some(2),
        )
    }

    pub(super) fn repair_destination_matches(
        &self,
        receipt: &RepairReceipt,
    ) -> AnalyticsEngineResult<bool> {
        let actual_rows = self.query_repair_rows(receipt)?;
        Ok(if receipt.row_visible {
            actual_rows.len() == 1
                && repair_row_matches_json(&receipt.expected_row_json, &actual_rows[0])
        } else {
            actual_rows.is_empty()
        })
    }
}
