use analytics_contract::{TenantRangePurgeRequest, TenantRangePurgeResponse};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, RetentionPolicyAuthority,
    current_time_millis, placeholder_list,
};
use crate::{
    maintenance_effect::{TenantRangePurgeEffectDisposition, TenantRangePurgeEffectRequest},
    sql,
};

impl AnalyticsEngine {
    /// Counts rows still visible to queries and eligible for expiry deletion.
    pub fn expired_retention_count(
        &self,
        table_name: &str,
        now_ms: i64,
    ) -> AnalyticsEngineResult<u64> {
        self.refresh_object_storage_credentials_if_needed()?;
        let sql = sql::expired_retention_count(table_name);
        let count: i64 = self
            .conn
            .query_row(sql.as_str(), [now_ms], |row| row.get(0))?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    /// Deletes one bounded tenant-scoped range under destination authority.
    ///
    /// The authority fence, deleted rows, and exact request/response receipt
    /// commit in one destination transaction. A retry of the same execution
    /// returns the retained response without deleting another chunk.
    pub fn apply_tenant_range_purge(
        &self,
        effect: TenantRangePurgeEffectRequest<'_>,
    ) -> AnalyticsEngineResult<TenantRangePurgeEffectDisposition> {
        effect
            .request
            .validate()
            .map_err(|error| AnalyticsEngineError::InvalidRangePurge(error.to_string()))?;
        crate::maintenance_effect::validate_execution_id(effect.request_id)?;
        let request_json = serde_json::to_string(effect.request)?;
        self.with_range_purge_transaction(|| {
            self.apply_range_purge_transaction(effect, &request_json)
        })
    }

    fn apply_range_purge_transaction(
        &self,
        effect: TenantRangePurgeEffectRequest<'_>,
        request_json: &str,
    ) -> AnalyticsEngineResult<TenantRangePurgeEffectDisposition> {
        self.validate_range_purge_claim(effect.authority)?;
        if let Some(response) = self.load_range_purge_response(effect, request_json)? {
            return Ok(TenantRangePurgeEffectDisposition::AlreadyApplied(response));
        }

        let response = self.purge_tenant_range_chunk(effect.request)?;
        let response_json = serde_json::to_string(&response)?;
        self.conn.execute(
            "INSERT INTO __analytics_range_purge_receipts
             (execution_key, request_id, request_json, response_json, committed_at_ms)
             VALUES (?, ?, ?, ?, ?)",
            duckdb::params![
                range_purge_execution_key(effect.authority),
                effect.request_id,
                request_json,
                response_json,
                current_time_millis(),
            ],
        )?;
        Ok(TenantRangePurgeEffectDisposition::Applied(response))
    }

    fn load_range_purge_response(
        &self,
        effect: TenantRangePurgeEffectRequest<'_>,
        request_json: &str,
    ) -> AnalyticsEngineResult<Option<TenantRangePurgeResponse>> {
        let mut statement = self.conn.prepare(
            "SELECT request_json, response_json
             FROM __analytics_range_purge_receipts
             WHERE execution_key = ? AND request_id = ? LIMIT 2",
        )?;
        let receipts = statement
            .query_map(
                duckdb::params![
                    range_purge_execution_key(effect.authority),
                    effect.request_id
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        if receipts.len() > 1 {
            return Err(AnalyticsEngineError::AmbiguousMaintenanceEffectReceipt(
                effect.request_id.to_string(),
            ));
        }
        if let Some((stored_request, stored_response)) = receipts.first() {
            if stored_request != request_json {
                return Err(AnalyticsEngineError::ConflictingMaintenanceEffectReceipt(
                    effect.request_id.to_string(),
                ));
            }
            let response = serde_json::from_str(stored_response)?;
            return Ok(Some(response));
        }

        Ok(None)
    }

    /// Deletes one bounded, tenant-scoped range and returns an opaque `__id`
    /// checkpoint for the next chunk. The table and predicate columns are
    /// validated by the contract before being interpolated into SQL; values
    /// remain bound parameters. `complete` is true only when this scan found
    /// no further matching rows after the supplied cursor.
    fn purge_tenant_range_chunk(
        &self,
        request: &TenantRangePurgeRequest,
    ) -> AnalyticsEngineResult<TenantRangePurgeResponse> {
        let table = sql::quote_identifier(request.table_name.as_str());
        let ids = self.select_tenant_range_ids(request)?;
        if ids.is_empty() {
            return Ok(TenantRangePurgeResponse {
                rows_deleted: 0,
                cursor: request.cursor.clone(),
                complete: true,
            });
        }

        // Delete the selected chunk in one statement. This keeps the worker
        // bounded by `limit` while avoiding one round trip per analytics row.
        let placeholders = placeholder_list(ids.len());
        let delete_sql =
            format!("DELETE FROM {table} WHERE tenant_id = ? AND __id IN ({placeholders})");
        let mut delete_values = Vec::with_capacity(ids.len().saturating_add(1));
        delete_values.push(duckdb::types::Value::Text(request.tenant_id.clone()));
        delete_values.extend(ids.iter().cloned().map(duckdb::types::Value::Text));
        let rows_deleted = self
            .conn
            .prepare(delete_sql.as_str())?
            .execute(duckdb::params_from_iter(delete_values))? as u64;
        let cursor = ids.last().cloned();
        Ok(TenantRangePurgeResponse {
            rows_deleted,
            cursor,
            complete: false,
        })
    }

    fn select_tenant_range_ids(
        &self,
        request: &TenantRangePurgeRequest,
    ) -> AnalyticsEngineResult<Vec<String>> {
        let table = sql::quote_identifier(request.table_name.as_str());
        let mut predicate = TenantRangePredicate::from_request(request);
        let select_sql = format!(
            "SELECT __id FROM {table} WHERE {} ORDER BY __id ASC LIMIT ?",
            predicate.sql
        );
        predicate
            .values
            .push(duckdb::types::Value::BigInt(i64::from(request.limit)));
        self.conn
            .prepare(select_sql.as_str())?
            .query_map(duckdb::params_from_iter(predicate.values), |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn missing_retention_count(&self, table_name: &str) -> AnalyticsEngineResult<u64> {
        self.refresh_object_storage_credentials_if_needed()?;
        let sql = sql::missing_retention_count(table_name);
        let count: i64 = self.conn.query_row(sql.as_str(), [], |row| row.get(0))?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    pub fn repair_missing_retention(
        &self,
        table_name: &str,
        tenant_id: &str,
        period_ms: u64,
        batch_size: u64,
    ) -> AnalyticsEngineResult<u64> {
        let authority = RetentionPolicyAuthority::try_new(tenant_id, period_ms)?;
        self.repair_missing_retention_with_authority(table_name, &authority, batch_size)
    }

    pub fn repair_missing_retention_with_authority(
        &self,
        table_name: &str,
        authority: &RetentionPolicyAuthority,
        batch_size: u64,
    ) -> AnalyticsEngineResult<u64> {
        self.refresh_object_storage_credentials_if_needed()?;
        let sql = sql::repair_missing_retention(table_name);
        let changed = self.conn.prepare(sql.as_str())?.execute(duckdb::params![
            i64::try_from(authority.period_ms()).unwrap_or(i64::MAX),
            authority.tenant_id(),
            i64::try_from(batch_size).unwrap_or(i64::MAX)
        ])?;
        Ok(changed as u64)
    }
}

fn range_purge_execution_key(claim: &bg_jobs::RemoteJobClaim) -> Vec<u8> {
    let mut identity = Vec::new();
    claim.bind_request_identity(&mut identity);
    identity
}

struct TenantRangePredicate {
    sql: String,
    values: Vec<duckdb::types::Value>,
}

impl TenantRangePredicate {
    fn from_request(request: &TenantRangePurgeRequest) -> Self {
        let timestamp = sql::quote_identifier(request.timestamp_column.as_str());
        // `request.table_name` names the physical analytics table selected in
        // the FROM clause. Each row's `table_name` column is source metadata
        // and may differ (for example, `metric_points_v1_stream`), so using
        // the request value as a row predicate would silently purge nothing.
        // The physical table plus tenant predicate is the ownership boundary.
        let mut predicates = vec![
            "tenant_id = ?".to_string(),
            format!("{timestamp} >= ?"),
            format!("{timestamp} < ?"),
        ];
        let mut values = vec![
            duckdb::types::Value::Text(request.tenant_id.clone()),
            duckdb::types::Value::BigInt(request.start_ms),
            duckdb::types::Value::BigInt(request.end_ms),
        ];
        for (column, value) in &request.equals {
            let column = sql::quote_identifier(column.as_str());
            match value {
                Some(value) => {
                    predicates.push(format!("{column} = ?"));
                    values.push(duckdb::types::Value::Text(value.clone()));
                }
                None => predicates.push(format!("{column} IS NULL")),
            }
        }
        if let Some(cursor) = request.cursor.as_deref() {
            predicates.push("__id > ?".to_string());
            values.push(duckdb::types::Value::Text(cursor.to_string()));
        }
        Self {
            sql: predicates.join(" AND "),
            values,
        }
    }
}
