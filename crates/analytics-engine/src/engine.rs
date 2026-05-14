//! Domain-agnostic analytics execution engine.
//!
//! This crate accepts only `analytics-contract` manifests and contract stream
//! records. Do not add storage polling, storage subscriptions, aux-fn entity
//! types, or storage facade dependencies here. Put those integrations in
//! `analytics-storage` or another adapter crate and convert into contract
//! records before calling the engine.

use std::{
    cell::RefCell,
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use analytics_contract::{
    AnalyticsColumn, AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn,
    RetentionPolicy, RetentionTimestamp, StorageStreamRecord, StructuredQuery, TableRegistration,
};
use config::StorageBackend;
use duckdb::Connection;
use thiserror::Error;

use crate::{
    cache::EngineCaches,
    sql,
    structured_query::{structured_query_sql, tenant_scoped_structured_query_sql},
};

const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum AnalyticsEngineError {
    #[error("manifest version {actual} is not supported; expected {expected}")]
    UnsupportedManifestVersion { actual: u32, expected: u32 },
    #[error(transparent)]
    InvalidManifest(#[from] analytics_contract::ManifestValidationError),
    #[error("table registration not found for analytics table {0}")]
    TableNotRegistered(String),
    #[error(transparent)]
    Projection(#[from] crate::projection::ProjectionError),
    #[error("record does not include a tenant id and registration has no tenant id")]
    MissingTenant,
    #[error("record is missing required attribute {0}")]
    MissingAttribute(String),
    #[error("attribute {attribute_name} value does not match regex")]
    RegexNoMatch { attribute_name: String },
    #[error("regex for attribute {attribute_name} does not define capture group {capture}")]
    RegexMissingCapture {
        attribute_name: String,
        capture: String,
    },
    #[error("record is missing required identifier column {0}")]
    MissingIdentifier(&'static str),
    #[error("query must be a single read-only SELECT statement")]
    InvalidQuery,
    #[error("query exceeded {timeout_ms}ms execution timeout")]
    QueryTimeout { timeout_ms: u128 },
    #[error("structured query is invalid: {0}")]
    InvalidStructuredQuery(String),
    #[error("condition expression is invalid: {0}")]
    InvalidConditionExpression(String),
    #[error("regex is invalid for attribute {attribute_name}: {message}")]
    InvalidRegex {
        attribute_name: String,
        message: String,
    },
    #[error("storage item conversion failed for attribute {0}")]
    AttributeConversion(String),
    #[error("retention timestamp is missing or invalid: {0}")]
    InvalidRetentionTimestamp(String),
    #[error("retention period must be greater than zero")]
    InvalidRetentionPeriod,
    #[error("retention period overflowed unix epoch milliseconds")]
    RetentionOverflow,
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type AnalyticsEngineResult<T> = Result<T, AnalyticsEngineError>;

pub struct AnalyticsEngine {
    conn: Connection,
    supports_table_layout: bool,
    pub(crate) caches: RefCell<EngineCaches>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCheckpoint {
    pub source_table_name: String,
    pub shard_id: String,
    pub position: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRetention {
    pub period_ms: Option<u64>,
    pub timestamp: RetentionTimestamp,
    pub missing_retention: bool,
}

impl AnalyticsEngine {
    pub fn connect(backend: &StorageBackend) -> AnalyticsEngineResult<Self> {
        let conn = match backend {
            StorageBackend::DuckDb { path } => Connection::open(path)?,
            StorageBackend::DuckLake { .. } => Connection::open_in_memory()?,
        };
        sql::configure_connection(&conn, backend)?;
        Ok(Self {
            conn,
            supports_table_layout: matches!(backend, StorageBackend::DuckLake { .. }),
            caches: RefCell::new(EngineCaches::new()),
        })
    }

    pub fn connect_duckdb(path: impl AsRef<Path>) -> AnalyticsEngineResult<Self> {
        Self::connect(&StorageBackend::DuckDb {
            path: path.as_ref().to_string_lossy().to_string(),
        })
    }

    #[must_use]
    pub const fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn ensure_manifest(&self, manifest: &AnalyticsManifest) -> AnalyticsEngineResult<()> {
        manifest.validate()?;
        self.ensure_runtime_tables()?;

        for table in &manifest.tables {
            crate::condition::validate_condition(table, &mut self.caches.borrow_mut())?;
            self.ensure_table(table)?;
        }
        Ok(())
    }

    pub fn ensure_runtime_tables(&self) -> AnalyticsEngineResult<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS __analytics_source_checkpoints (
                source_table_name VARCHAR NOT NULL,
                shard_id VARCHAR NOT NULL,
                position VARCHAR NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (source_table_name, shard_id)
            );",
            [],
        )?;
        Ok(())
    }

    pub fn load_source_checkpoints(&self) -> AnalyticsEngineResult<Vec<SourceCheckpoint>> {
        self.ensure_runtime_tables()?;
        let mut statement = self.conn.prepare(
            "SELECT source_table_name, shard_id, position FROM __analytics_source_checkpoints",
        )?;
        let mut rows = statement.query([])?;
        let mut checkpoints = Vec::new();
        while let Some(row) = rows.next()? {
            checkpoints.push(SourceCheckpoint {
                source_table_name: row.get(0)?,
                shard_id: row.get(1)?,
                position: row.get(2)?,
            });
        }
        Ok(checkpoints)
    }

    pub fn save_source_checkpoint(
        &self,
        checkpoint: &SourceCheckpoint,
    ) -> AnalyticsEngineResult<()> {
        self.ensure_runtime_tables()?;
        self.conn.execute(
            "INSERT OR REPLACE INTO __analytics_source_checkpoints
                (source_table_name, shard_id, position, updated_at_ms)
             VALUES (?, ?, ?, ?)",
            duckdb::params![
                checkpoint.source_table_name,
                checkpoint.shard_id,
                checkpoint.position,
                current_time_millis()
            ],
        )?;
        Ok(())
    }

    pub fn ensure_table(&self, table: &TableRegistration) -> AnalyticsEngineResult<()> {
        let columns = columns_for_registration(table);
        self.conn.execute(
            sql::create_table(table.analytics_table_name.as_str(), &columns).as_str(),
            [],
        )?;
        if table.retention.is_some() {
            self.ensure_retention_columns(table.analytics_table_name.as_str())?;
        }
        if self.supports_table_layout {
            if let Some(statement) = sql::alter_table_partitioned_by(
                table.analytics_table_name.as_str(),
                &table.partition_keys,
            ) {
                self.conn.execute(statement.as_str(), [])?;
            }
            if let Some(statement) = sql::alter_table_sorted_by(
                table.analytics_table_name.as_str(),
                &table.clustering_keys,
            ) {
                self.conn.execute(statement.as_str(), [])?;
            }
        }
        Ok(())
    }

    pub fn ensure_retention_columns(&self, table_name: &str) -> AnalyticsEngineResult<()> {
        for statement in sql::retention_column_statements(table_name) {
            self.conn.execute(statement.as_str(), [])?;
        }
        Ok(())
    }

    pub fn ingest_stream_record(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.ingest_stream_record_with_retention(
            manifest,
            analytics_table_name,
            record_key,
            record,
            None,
        )
    }

    #[allow(clippy::too_many_lines)]
    pub fn ingest_stream_record_with_retention(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        resolved_retention: Option<&IngestRetention>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let table = manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == analytics_table_name)
            .ok_or_else(|| {
                AnalyticsEngineError::TableNotRegistered(analytics_table_name.to_string())
            })?;

        let record_keys = record.keys;
        let ingested_at_ms = current_time_millis();
        let manifest_retention: Option<IngestRetention>;
        let effective_retention = if resolved_retention.is_some() {
            resolved_retention
        } else {
            manifest_retention = static_retention(table);
            manifest_retention.as_ref()
        };
        match (record.old_image, record.new_image) {
            (None, Some(new_image)) => {
                if !crate::condition::item_matches_registration(
                    table,
                    &new_image,
                    &mut self.caches.borrow_mut(),
                )? {
                    return Ok(IngestOutcome::Skipped);
                }
                let row = self.row_from_item(
                    table,
                    record_key,
                    &record_keys,
                    new_image,
                    ingested_at_ms,
                    effective_retention,
                )?;
                if self.update_row(table.analytics_table_name.as_str(), &row)? {
                    return Ok(IngestOutcome::Updated);
                }
                self.insert_row(table.analytics_table_name.as_str(), &row)?;
                Ok(IngestOutcome::Inserted)
            }
            (Some(old_image), Some(new_image)) => {
                let old_matches = crate::condition::item_matches_registration(
                    table,
                    &old_image,
                    &mut self.caches.borrow_mut(),
                )?;
                let new_matches = crate::condition::item_matches_registration(
                    table,
                    &new_image,
                    &mut self.caches.borrow_mut(),
                )?;
                if !old_matches && !new_matches {
                    return Ok(IngestOutcome::Skipped);
                }
                if old_matches && !new_matches {
                    if table.skip_delete {
                        return Ok(IngestOutcome::Skipped);
                    }
                    let row = self.row_from_item(
                        table,
                        record_key,
                        &record_keys,
                        old_image,
                        ingested_at_ms,
                        None,
                    )?;
                    self.delete_row(table.analytics_table_name.as_str(), &row)?;
                    return Ok(IngestOutcome::Deleted);
                }
                let row = self.row_from_item(
                    table,
                    record_key,
                    &record_keys,
                    new_image,
                    ingested_at_ms,
                    effective_retention,
                )?;
                let updated = self.update_row(table.analytics_table_name.as_str(), &row)?;
                if updated {
                    Ok(IngestOutcome::Updated)
                } else {
                    self.insert_row(table.analytics_table_name.as_str(), &row)?;
                    Ok(IngestOutcome::Inserted)
                }
            }
            (Some(old_image), None) => {
                if table.skip_delete {
                    return Ok(IngestOutcome::Skipped);
                }
                if !crate::condition::item_matches_registration(
                    table,
                    &old_image,
                    &mut self.caches.borrow_mut(),
                )? {
                    return Ok(IngestOutcome::Skipped);
                }
                let row = self.row_from_item(
                    table,
                    record_key,
                    &record_keys,
                    old_image,
                    ingested_at_ms,
                    None,
                )?;
                self.delete_row(table.analytics_table_name.as_str(), &row)?;
                Ok(IngestOutcome::Deleted)
            }
            (None, None) => Ok(IngestOutcome::Skipped),
        }
    }

    pub fn delete_expired_rows(
        &self,
        table_name: &str,
        now_ms: i64,
        batch_size: u64,
    ) -> AnalyticsEngineResult<u64> {
        let sql = sql::delete_expired_rows(table_name);
        let changed = self.conn.prepare(sql.as_str())?.execute(duckdb::params![
            now_ms,
            i64::try_from(batch_size).unwrap_or(i64::MAX)
        ])?;
        Ok(changed as u64)
    }

    pub fn missing_retention_count(&self, table_name: &str) -> AnalyticsEngineResult<u64> {
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
        if period_ms == 0 {
            return Err(AnalyticsEngineError::InvalidRetentionPeriod);
        }
        let sql = sql::repair_missing_retention(table_name);
        let changed = self.conn.prepare(sql.as_str())?.execute(duckdb::params![
            i64::try_from(period_ms).unwrap_or(i64::MAX),
            tenant_id,
            i64::try_from(batch_size).unwrap_or(i64::MAX)
        ])?;
        Ok(changed as u64)
    }

    pub fn query_unscoped_sql_json(
        &self,
        sql: &str,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_unscoped_sql_json_with_timeout(sql, DEFAULT_QUERY_TIMEOUT)
    }

    pub fn query_unscoped_sql_json_with_timeout(
        &self,
        sql: &str,
        timeout: Duration,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        validate_read_only_query(sql)?;
        let wrapped = format!(
            "SELECT to_json(result) FROM ({}) AS result",
            sql.trim_end_matches(';')
        );
        let timed_out = query_timeout_watchdog(&self.conn, timeout);
        let mut stmt = self.conn.prepare(wrapped.as_str()).map_err(|err| {
            if timed_out.timed_out() {
                AnalyticsEngineError::QueryTimeout {
                    timeout_ms: timeout.as_millis(),
                }
            } else {
                err.into()
            }
        })?;
        let rows = stmt
            .query_map([], |row| {
                let text: String = row.get(0)?;
                serde_json::from_str::<serde_json::Value>(&text)
                    .map_err(|_| duckdb::Error::InvalidColumnIndex(0))
            })
            .map_err(|err| {
                if timed_out.timed_out() {
                    AnalyticsEngineError::QueryTimeout {
                        timeout_ms: timeout.as_millis(),
                    }
                } else {
                    err.into()
                }
            })?;

        let mut values = Vec::new();
        for row in rows {
            match row {
                Ok(value) => values.push(value),
                Err(_) if timed_out.timed_out() => {
                    return Err(AnalyticsEngineError::QueryTimeout {
                        timeout_ms: timeout.as_millis(),
                    });
                }
                Err(err) => return Err(err.into()),
            }
        }
        if timed_out.timed_out() {
            return Err(AnalyticsEngineError::QueryTimeout {
                timeout_ms: timeout.as_millis(),
            });
        }
        Ok(values)
    }

    pub fn query_unscoped_structured_json(
        &self,
        manifest: &AnalyticsManifest,
        query: &StructuredQuery,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        query
            .validate_shape()
            .map_err(|err| AnalyticsEngineError::InvalidStructuredQuery(err.to_string()))?;
        let table = manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == query.analytics_table_name)
            .ok_or_else(|| {
                AnalyticsEngineError::TableNotRegistered(query.analytics_table_name.clone())
            })?;
        let sql = structured_query_sql(table, query)?;
        self.query_unscoped_sql_json(sql.as_str())
    }

    pub fn query_tenant_structured_json(
        &self,
        manifest: &AnalyticsManifest,
        query: &StructuredQuery,
        target_tenant_id: &str,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        query
            .validate_shape()
            .map_err(|err| AnalyticsEngineError::InvalidStructuredQuery(err.to_string()))?;
        let table = manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == query.analytics_table_name)
            .ok_or_else(|| {
                AnalyticsEngineError::TableNotRegistered(query.analytics_table_name.clone())
            })?;
        let sql = tenant_scoped_structured_query_sql(table, query, target_tenant_id)?;
        self.query_unscoped_sql_json(sql.as_str())
    }

    fn insert_row(
        &self,
        table_name: &str,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<()> {
        let columns = row.keys().cloned().collect::<Vec<_>>();
        let placeholders = vec!["?"; columns.len()].join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})",
            sql::quote_identifier(table_name),
            columns
                .iter()
                .map(|column| sql::quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let values = columns
            .iter()
            .map(|column| {
                json_value_to_duckdb_value(row.get(column).unwrap_or(&serde_json::Value::Null))
            })
            .collect::<Vec<_>>();
        self.conn
            .prepare(sql.as_str())?
            .execute(duckdb::params_from_iter(values))?;
        Ok(())
    }

    fn update_row(
        &self,
        table_name: &str,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<bool> {
        let tenant_id = row
            .get("tenant_id")
            .ok_or(AnalyticsEngineError::MissingIdentifier("tenant_id"))?;
        let row_id = row
            .get("__id")
            .ok_or(AnalyticsEngineError::MissingIdentifier("__id"))?;
        let table_value = row
            .get("table_name")
            .ok_or(AnalyticsEngineError::MissingIdentifier("table_name"))?;

        let update_columns = row
            .keys()
            .filter(|column| !matches!(column.as_str(), "tenant_id" | "__id" | "table_name"))
            .cloned()
            .collect::<Vec<_>>();
        if update_columns.is_empty() {
            return Ok(false);
        }

        let set_clause = update_columns
            .iter()
            .map(|column| format!("{} = ?", sql::quote_identifier(column)))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "UPDATE {} SET {set_clause} WHERE table_name = ? AND tenant_id = ? AND __id = ?",
            sql::quote_identifier(table_name)
        );
        let mut values = update_columns
            .iter()
            .map(|column| {
                json_value_to_duckdb_value(row.get(column).unwrap_or(&serde_json::Value::Null))
            })
            .collect::<Vec<_>>();
        values.push(json_value_to_duckdb_value(table_value));
        values.push(json_value_to_duckdb_value(tenant_id));
        values.push(json_value_to_duckdb_value(row_id));
        let changed = self
            .conn
            .prepare(sql.as_str())?
            .execute(duckdb::params_from_iter(values))?;
        Ok(changed > 0)
    }

    fn delete_row(
        &self,
        table_name: &str,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<()> {
        let tenant_id = row
            .get("tenant_id")
            .ok_or(AnalyticsEngineError::MissingIdentifier("tenant_id"))?;
        let row_id = row
            .get("__id")
            .ok_or(AnalyticsEngineError::MissingIdentifier("__id"))?;
        let table_value = row
            .get("table_name")
            .ok_or(AnalyticsEngineError::MissingIdentifier("table_name"))?;
        let sql = format!(
            "DELETE FROM {} WHERE table_name = ? AND tenant_id = ? AND __id = ?",
            sql::quote_identifier(table_name)
        );
        self.conn
            .prepare(sql.as_str())?
            .execute(duckdb::params_from_iter(vec![
                json_value_to_duckdb_value(table_value),
                json_value_to_duckdb_value(tenant_id),
                json_value_to_duckdb_value(row_id),
            ]))?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestOutcome {
    Inserted,
    Updated,
    Deleted,
    Skipped,
}

fn validate_read_only_query(sql: &str) -> AnalyticsEngineResult<()> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(AnalyticsEngineError::InvalidQuery);
    }
    let statement = trimmed.trim_end_matches(';').trim_end();
    if statement.contains(';') {
        return Err(AnalyticsEngineError::InvalidQuery);
    }
    let normalized = statement.to_ascii_lowercase();
    if normalized.starts_with("select ") || normalized.starts_with("with ") {
        return Ok(());
    }
    Err(AnalyticsEngineError::InvalidQuery)
}

struct QueryTimeoutWatchdog {
    cancel: Option<mpsc::Sender<()>>,
    timed_out: Arc<AtomicBool>,
}

impl QueryTimeoutWatchdog {
    fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::SeqCst)
    }
}

impl Drop for QueryTimeoutWatchdog {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

fn query_timeout_watchdog(conn: &Connection, timeout: Duration) -> QueryTimeoutWatchdog {
    let interrupt_handle = conn.interrupt_handle();
    let (cancel, cancelled) = mpsc::channel();
    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_for_thread = Arc::clone(&timed_out);
    thread::spawn(move || {
        if cancelled.recv_timeout(timeout).is_err() {
            timed_out_for_thread.store(true, Ordering::SeqCst);
            interrupt_handle.interrupt();
        }
    });
    QueryTimeoutWatchdog {
        cancel: Some(cancel),
        timed_out,
    }
}

fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

pub(crate) fn columns_for_registration(table: &TableRegistration) -> Vec<AnalyticsColumn> {
    let mut columns = Vec::new();
    if let Some(document_column) = table.document_column.as_ref() {
        columns.push(AnalyticsColumn {
            column_name: document_column.clone(),
            column_type: AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::Json,
            },
        });
    }
    if !table.columns.is_empty() {
        columns.extend(table.columns.clone());
        return columns;
    }
    if let Some(projection_columns) = table.projection_columns.as_deref() {
        columns.extend(projection_columns.iter().map(|column| AnalyticsColumn {
            column_name: column.column_name.clone(),
            column_type: projection_column_type(column),
        }));
        return columns;
    }
    columns.extend(
        table
            .projection_attribute_names
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|column_name| AnalyticsColumn {
                column_name: column_name.clone(),
                column_type: default_column_type(),
            }),
    );
    columns
}

fn projection_column_type(column: &ProjectionColumn) -> AnalyticsColumnType {
    column
        .column_type
        .clone()
        .unwrap_or_else(default_column_type)
}

fn default_column_type() -> AnalyticsColumnType {
    AnalyticsColumnType::Primitive {
        primitive: PrimitiveColumnType::VarChar,
    }
}

pub(crate) fn static_retention(table: &TableRegistration) -> Option<IngestRetention> {
    table.retention.as_ref().map(retention_from_policy)
}

fn retention_from_policy(policy: &RetentionPolicy) -> IngestRetention {
    IngestRetention {
        period_ms: Some(policy.period_ms),
        timestamp: policy.timestamp.clone(),
        missing_retention: false,
    }
}

fn json_value_to_duckdb_value(value: &serde_json::Value) -> duckdb::types::Value {
    match value {
        serde_json::Value::String(text) => duckdb::types::Value::Text(text.clone()),
        serde_json::Value::Number(number) => duckdb::types::Value::Text(number.to_string()),
        serde_json::Value::Bool(value) => duckdb::types::Value::Boolean(*value),
        serde_json::Value::Null => duckdb::types::Value::Null,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            duckdb::types::Value::Text(value.to_string())
        }
    }
}
