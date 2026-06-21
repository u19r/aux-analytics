//! Domain-agnostic analytics execution engine.
//!
//! This crate accepts only `analytics-contract` manifests and contract stream
//! records. Do not add storage polling, storage subscriptions, application
//! entity types, or storage facade dependencies here. Put those integrations in
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
    AnalyticsColumn, AnalyticsColumnType, AnalyticsManifest, MergeDecision, PrimitiveColumnType,
    PrivacyPolicy, ProjectionColumn, RetentionPolicy, RetentionTimestamp, SourceMutation,
    SourcePosition, SourcePositionError, StorageStreamRecord, StructuredQuery, TableRegistration,
    merge_decision,
};
use aws_credentials::{
    AwsResolvedCredentials, AwsStaticCredentials, resolve_default_chain_credentials_with_expiry,
};
use config::{
    AnalyticsObjectStorageConfig, CatalogType, DuckLakeCatalogSettings, RemoteCredentialsConfig,
    RemoteStaticCredentialsConfig, StorageBackend,
};
use duckdb::{Config as DuckDbConfig, Connection, OptionalExt, types::TimeUnit};
use thiserror::Error;

use crate::{
    cache::EngineCaches,
    sql,
    structured_query::{
        structured_query_sql_for_manifest, tenant_scoped_structured_query_sql_for_manifest,
    },
};

const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum AnalyticsEngineError {
    #[error("manifest version {actual} is not supported; expected {expected}")]
    UnsupportedManifestVersion { actual: u32, expected: u32 },
    #[error(transparent)]
    InvalidManifest(#[from] analytics_contract::ManifestValidationError),
    #[error(transparent)]
    PrivacyPolicy(#[from] analytics_contract::PrivacyPolicyError),
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
    #[error(transparent)]
    SourcePosition(#[from] SourcePositionError),
    #[error("AWS credential resolution failed: {0}")]
    AwsCredentials(#[from] aws_credentials::CredentialsError),
}

pub type AnalyticsEngineResult<T> = Result<T, AnalyticsEngineError>;

pub struct AnalyticsEngine {
    conn: Connection,
    supports_table_layout: bool,
    supports_persistent_checkpoints: bool,
    object_storage_credentials: RefCell<Option<DuckLakeObjectStorageCredentials>>,
    pub(crate) caches: RefCell<EngineCaches>,
}

#[derive(Debug, Clone)]
struct DuckLakeObjectStorageCredentials {
    object_storage: AnalyticsObjectStorageConfig,
    resolved: AwsResolvedCredentials,
}

impl DuckLakeObjectStorageCredentials {
    fn needs_refresh(&self) -> bool {
        self.resolved.needs_refresh()
    }
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

fn configure_ducklake_object_storage_credentials(
    backend: &StorageBackend,
) -> AnalyticsEngineResult<(StorageBackend, Option<DuckLakeObjectStorageCredentials>)> {
    let StorageBackend::DuckLake {
        object_storage: Some(object_storage),
        ..
    } = backend
    else {
        return Ok((backend.clone(), None));
    };
    if !object_storage_needs_resolved_credentials(object_storage) {
        return Ok((backend.clone(), None));
    }

    let resolved = resolve_default_chain_credentials_with_expiry()?;
    let mut configured_backend = backend.clone();
    if let StorageBackend::DuckLake {
        object_storage: Some(configured_object_storage),
        ..
    } = &mut configured_backend
    {
        configured_object_storage.credentials =
            Some(remote_credentials_config(&resolved.credentials));
    }
    Ok((
        configured_backend,
        Some(DuckLakeObjectStorageCredentials {
            object_storage: (**object_storage).clone(),
            resolved,
        }),
    ))
}

fn object_storage_needs_resolved_credentials(
    object_storage: &AnalyticsObjectStorageConfig,
) -> bool {
    if object_storage.bucket.is_none() {
        return false;
    }
    let Some(credentials) = object_storage.credentials.as_ref() else {
        return true;
    };
    credentials.r#static.is_none() && credentials.instance_keys.unwrap_or(false)
}

fn remote_credentials_config(credentials: &AwsStaticCredentials) -> RemoteCredentialsConfig {
    RemoteCredentialsConfig {
        r#static: Some(RemoteStaticCredentialsConfig {
            access_key: credentials.access_key_id.clone(),
            secret_key: credentials.secret_access_key.clone(),
            session_token: credentials.session_token.clone(),
        }),
        instance_keys: None,
    }
}

impl AnalyticsEngine {
    pub fn connect(backend: &StorageBackend) -> AnalyticsEngineResult<Self> {
        let conn = match backend {
            StorageBackend::DuckDb { path } => Connection::open(path)?,
            StorageBackend::DuckLake {
                catalog: CatalogType::Sqlite | CatalogType::Postgres,
                ..
            } => Connection::open_in_memory()?,
            StorageBackend::DuckLake {
                catalog: CatalogType::MotherDuck,
                catalog_settings,
                ..
            } => Connection::open_in_memory_with_flags(motherduck_config(catalog_settings)?)?,
        };
        let (configured_backend, object_storage_credentials) =
            configure_ducklake_object_storage_credentials(backend)?;
        sql::configure_connection(&conn, &configured_backend)?;
        Ok(Self {
            conn,
            supports_table_layout: matches!(backend, StorageBackend::DuckLake { .. }),
            supports_persistent_checkpoints: true,
            object_storage_credentials: RefCell::new(object_storage_credentials),
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

    fn refresh_object_storage_credentials_if_needed(&self) -> AnalyticsEngineResult<()> {
        let mut state = self.object_storage_credentials.borrow_mut();
        let Some(state) = state.as_mut() else {
            return Ok(());
        };
        if !state.needs_refresh() {
            return Ok(());
        }

        let resolved = resolve_default_chain_credentials_with_expiry()?;
        let mut object_storage = state.object_storage.clone();
        object_storage.credentials = Some(remote_credentials_config(&resolved.credentials));
        sql::configure_object_storage(&self.conn, &object_storage)?;
        state.resolved = resolved;
        Ok(())
    }

    pub fn ensure_manifest(&self, manifest: &AnalyticsManifest) -> AnalyticsEngineResult<()> {
        self.refresh_object_storage_credentials_if_needed()?;
        manifest.validate()?;
        if self.supports_persistent_checkpoints {
            self.ensure_runtime_tables()?;
        }

        for table in &manifest.tables {
            crate::condition::validate_condition(table, &mut self.caches.borrow_mut())?;
            self.ensure_table(table)?;
        }
        Ok(())
    }

    pub fn ensure_runtime_tables(&self) -> AnalyticsEngineResult<()> {
        self.refresh_object_storage_credentials_if_needed()?;
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS analytics_checkpoints (
                source_table_name VARCHAR NOT NULL,
                shard_id VARCHAR NOT NULL,
                position VARCHAR NOT NULL,
                updated_at_ms BIGINT NOT NULL
            );
             CREATE TABLE IF NOT EXISTS __analytics_row_source_positions (
                analytics_table_name VARCHAR NOT NULL,
                source_table_name VARCHAR NOT NULL,
                tenant_id VARCHAR NOT NULL,
                source_row_id VARCHAR NOT NULL,
                source_position_json JSON NOT NULL,
                row_visible BOOLEAN NOT NULL,
                updated_at_ms BIGINT NOT NULL
            );",
            [],
        )?;
        Ok(())
    }

    pub fn load_source_checkpoints(&self) -> AnalyticsEngineResult<Vec<SourceCheckpoint>> {
        self.refresh_object_storage_credentials_if_needed()?;
        if !self.supports_persistent_checkpoints {
            return Ok(Vec::new());
        }
        self.ensure_runtime_tables()?;
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
            checkpoints.insert(
                (
                    checkpoint.source_table_name.clone(),
                    checkpoint.shard_id.clone(),
                ),
                checkpoint,
            );
        }
        Ok(checkpoints.into_values().collect())
    }

    pub fn save_source_checkpoint(
        &self,
        checkpoint: &SourceCheckpoint,
    ) -> AnalyticsEngineResult<()> {
        self.refresh_object_storage_credentials_if_needed()?;
        if !self.supports_persistent_checkpoints {
            return Ok(());
        }
        self.ensure_runtime_tables()?;
        self.conn.execute(
            "DELETE FROM analytics_checkpoints
             WHERE source_table_name = ? AND shard_id = ?",
            duckdb::params![checkpoint.source_table_name, checkpoint.shard_id],
        )?;
        self.conn.execute(
            "INSERT INTO analytics_checkpoints
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
        self.refresh_object_storage_credentials_if_needed()?;
        let columns = columns_for_registration(table);
        execute_convergent_schema_statement(
            &self.conn,
            sql::create_table(table.analytics_table_name.as_str(), &columns).as_str(),
        )?;
        for statement in
            sql::manifest_column_statements(table.analytics_table_name.as_str(), &columns)
        {
            self.conn.execute(statement.as_str(), [])?;
        }
        self.conn.execute(
            sql::source_position_column_statement(table.analytics_table_name.as_str()).as_str(),
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
        self.refresh_object_storage_credentials_if_needed()?;
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

    pub fn ingest_stream_record_batch(
        &self,
        manifest: &AnalyticsManifest,
        records: Vec<StreamRecordBatchItem>,
    ) -> AnalyticsEngineResult<Vec<IngestOutcome>> {
        self.refresh_object_storage_credentials_if_needed()?;
        self.conn.execute_batch("BEGIN TRANSACTION")?;
        let mut outcomes = Vec::with_capacity(records.len());
        let mut batch = BatchSqlExecutor::new(&self.conn);
        for item in records {
            match self.ingest_stream_record_batch_item(manifest, item, &mut batch) {
                Ok(outcome) => outcomes.push(outcome),
                Err(error) => return rollback_batch(&self.conn, error),
            }
        }
        self.conn.execute_batch("COMMIT")?;
        Ok(outcomes)
    }

    #[allow(clippy::too_many_lines)]
    fn ingest_stream_record_batch_item(
        &self,
        manifest: &AnalyticsManifest,
        item: StreamRecordBatchItem,
        batch: &mut BatchSqlExecutor<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let table = manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == item.analytics_table_name)
            .ok_or_else(|| {
                AnalyticsEngineError::TableNotRegistered(item.analytics_table_name.clone())
            })?;

        let record_keys = item.record.keys;
        let ingested_at_ms = current_time_millis();
        let manifest_retention = static_retention(table);
        match (item.record.old_image, item.record.new_image) {
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
                    item.record_key.as_slice(),
                    &record_keys,
                    new_image,
                    ingested_at_ms,
                    manifest_retention.as_ref(),
                )?;
                batch.apply_upsert(table, &row)
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
                        item.record_key.as_slice(),
                        &record_keys,
                        old_image,
                        ingested_at_ms,
                        None,
                    )?;
                    return batch.apply_delete(table.analytics_table_name.as_str(), &row);
                }
                let row = self.row_from_item(
                    table,
                    item.record_key.as_slice(),
                    &record_keys,
                    new_image,
                    ingested_at_ms,
                    manifest_retention.as_ref(),
                )?;
                batch.apply_upsert(table, &row)
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
                    item.record_key.as_slice(),
                    &record_keys,
                    old_image,
                    ingested_at_ms,
                    None,
                )?;
                batch.apply_delete(table.analytics_table_name.as_str(), &row)
            }
            (None, None) => Ok(IngestOutcome::Skipped),
        }
    }

    pub fn ingest_stream_record_with_privacy_policy(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        policy: &PrivacyPolicy,
    ) -> AnalyticsEngineResult<PrivacyIngestOutcome> {
        self.ingest_stream_record_with_privacy_policy_and_retention(
            manifest,
            analytics_table_name,
            record_key,
            record,
            policy,
            None,
        )
    }

    pub fn ingest_stream_record_with_privacy_policy_and_retention(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        policy: &PrivacyPolicy,
        resolved_retention: Option<&IngestRetention>,
    ) -> AnalyticsEngineResult<PrivacyIngestOutcome> {
        let filtered = filter_stream_record(record, policy)?;
        let outcome = self.ingest_stream_record_with_retention(
            manifest,
            analytics_table_name,
            record_key,
            filtered.record,
            resolved_retention,
        )?;
        Ok(PrivacyIngestOutcome {
            outcome,
            dropped_fields: filtered.dropped_fields,
            policy_version: policy.version.clone(),
        })
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn ingest_stream_record_at_source_position(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        source_position: SourcePosition,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.ingest_stream_record_internal(
            manifest,
            analytics_table_name,
            record_key,
            record,
            None,
            Some(&source_position),
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
        self.ingest_stream_record_internal(
            manifest,
            analytics_table_name,
            record_key,
            record,
            resolved_retention,
            None,
        )
    }

    #[allow(clippy::too_many_lines)]
    fn ingest_stream_record_internal(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        resolved_retention: Option<&IngestRetention>,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.refresh_object_storage_credentials_if_needed()?;
        if let Some(source_position) = source_position {
            source_position.validate()?;
        }
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
                self.apply_upsert(table, row, source_position)
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
                    return self.apply_delete(table, &row, source_position);
                }
                let row = self.row_from_item(
                    table,
                    record_key,
                    &record_keys,
                    new_image,
                    ingested_at_ms,
                    effective_retention,
                )?;
                self.apply_upsert(table, row, source_position)
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
                self.apply_delete(table, &row, source_position)
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
        self.refresh_object_storage_credentials_if_needed()?;
        let sql = sql::delete_expired_rows(table_name);
        let changed = self.conn.prepare(sql.as_str())?.execute(duckdb::params![
            now_ms,
            i64::try_from(batch_size).unwrap_or(i64::MAX)
        ])?;
        Ok(changed as u64)
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
        self.refresh_object_storage_credentials_if_needed()?;
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

    pub fn query_unscoped_sql_json_without_timeout(
        &self,
        sql: &str,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_unscoped_sql_json_with_optional_timeout(sql, None)
    }

    pub fn query_unscoped_sql_json_with_timeout(
        &self,
        sql: &str,
        timeout: Duration,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_unscoped_sql_json_with_optional_timeout(sql, Some(timeout))
    }

    fn query_unscoped_sql_json_with_optional_timeout(
        &self,
        sql: &str,
        timeout: Option<Duration>,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.refresh_object_storage_credentials_if_needed()?;
        validate_read_only_query(sql)?;
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(AnalyticsEngineError::QueryTimeout { timeout_ms: 0 });
        }
        let wrapped = format!(
            "SELECT to_json(result) FROM ({}) AS result",
            sql.trim_end_matches(';')
        );
        let timed_out = timeout.map(|timeout| query_timeout_watchdog(&self.conn, timeout));
        let mut stmt = self.conn.prepare(wrapped.as_str()).map_err(|err| {
            if timed_out
                .as_ref()
                .is_some_and(QueryTimeoutWatchdog::timed_out)
            {
                AnalyticsEngineError::QueryTimeout {
                    timeout_ms: timeout.map_or(0, |timeout| timeout.as_millis()),
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
                if timed_out
                    .as_ref()
                    .is_some_and(QueryTimeoutWatchdog::timed_out)
                {
                    AnalyticsEngineError::QueryTimeout {
                        timeout_ms: timeout.map_or(0, |timeout| timeout.as_millis()),
                    }
                } else {
                    err.into()
                }
            })?;

        let mut values = Vec::new();
        for row in rows {
            match row {
                Ok(value) => values.push(value),
                Err(_)
                    if timed_out
                        .as_ref()
                        .is_some_and(QueryTimeoutWatchdog::timed_out) =>
                {
                    return Err(AnalyticsEngineError::QueryTimeout {
                        timeout_ms: timeout.map_or(0, |timeout| timeout.as_millis()),
                    });
                }
                Err(err) => return Err(err.into()),
            }
        }
        if timed_out
            .as_ref()
            .is_some_and(QueryTimeoutWatchdog::timed_out)
        {
            return Err(AnalyticsEngineError::QueryTimeout {
                timeout_ms: timeout.map_or(0, |timeout| timeout.as_millis()),
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
        let sql = structured_query_sql_for_manifest(manifest, query)?;
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
        let sql =
            tenant_scoped_structured_query_sql_for_manifest(manifest, query, target_tenant_id)?;
        self.query_unscoped_sql_json(sql.as_str())
    }

    pub fn query_tenant_structured_json_without_timeout(
        &self,
        manifest: &AnalyticsManifest,
        query: &StructuredQuery,
        target_tenant_id: &str,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        query
            .validate_shape()
            .map_err(|err| AnalyticsEngineError::InvalidStructuredQuery(err.to_string()))?;
        let sql =
            tenant_scoped_structured_query_sql_for_manifest(manifest, query, target_tenant_id)?;
        self.query_unscoped_sql_json_without_timeout(sql.as_str())
    }

    pub fn scrub_table_with_privacy_policy(
        &self,
        table: &TableRegistration,
        policy: &PrivacyPolicy,
        dry_run: bool,
    ) -> AnalyticsEngineResult<PrivacyTableRemediationReport> {
        self.remediate_table_with_privacy_policy(
            table,
            policy,
            dry_run,
            PrivacyTableRemediationMode::Scrub,
        )
    }

    pub fn remediate_table_with_privacy_policy(
        &self,
        table: &TableRegistration,
        policy: &PrivacyPolicy,
        dry_run: bool,
        mode: PrivacyTableRemediationMode,
    ) -> AnalyticsEngineResult<PrivacyTableRemediationReport> {
        self.refresh_object_storage_credentials_if_needed()?;
        policy.validate()?;
        let projection_columns = privacy_scrubbable_projection_columns(table, policy)?;
        let mut selected_columns = vec!["__id".to_string()];
        for column in &projection_columns {
            selected_columns.push(column.clone());
        }
        if let Some(document_column) = table.document_column.as_ref() {
            selected_columns.push(document_column.clone());
        }
        let sql = privacy_remediation_scan_sql(
            table.analytics_table_name.as_str(),
            &selected_columns,
            &projection_columns,
            table.document_column.is_some(),
        );
        let rows = self.query_unscoped_sql_json(sql.as_str())?;
        let scanned_rows = rows.len() as u64;
        let mut affected_rows = 0_u64;
        let mut projected_values_scrubbed = 0_u64;
        let mut document_values_scrubbed = 0_u64;

        for row in rows {
            let Some(row_id) = row.get("__id").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let mut assignments = BTreeMap::new();
            for column in &projection_columns {
                if row
                    .get(column.as_str())
                    .is_some_and(|value| !value.is_null())
                {
                    assignments.insert(column.clone(), serde_json::Value::Null);
                    projected_values_scrubbed += 1;
                }
            }
            if let Some(document_column) = table.document_column.as_ref()
                && let Some(document) = row.get(document_column)
            {
                let mut dropped = 0_u64;
                let filtered = filter_json_value(document.clone(), "", policy, &mut dropped)?;
                if dropped > 0 {
                    assignments.insert(document_column.clone(), filtered);
                    document_values_scrubbed += dropped;
                }
            }
            if assignments.is_empty() {
                continue;
            }
            affected_rows += 1;
            if !dry_run {
                match mode {
                    PrivacyTableRemediationMode::Scrub => {
                        self.update_privacy_scrubbed_row(
                            table.analytics_table_name.as_str(),
                            row_id,
                            &assignments,
                        )?;
                    }
                    PrivacyTableRemediationMode::DeleteRows => {
                        self.delete_privacy_violating_row(
                            table.analytics_table_name.as_str(),
                            row_id,
                        )?;
                    }
                }
            }
        }

        Ok(PrivacyTableRemediationReport {
            table_name: table.analytics_table_name.clone(),
            dry_run,
            scanned_rows,
            affected_rows,
            projected_values_scrubbed,
            document_values_scrubbed,
            destination_mutated: !dry_run && affected_rows > 0,
            policy_version: policy.version.clone(),
            mode,
        })
    }

    fn insert_row(
        &self,
        table: &TableRegistration,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<()> {
        // Online ingestion fallback. Backfill and replay writers should use chunk-sized
        // batch append/merge paths once the operations layer owns a
        // destination-writer boundary.
        let columns = row.keys().map(String::as_str).collect::<Vec<_>>();
        let placeholders = placeholder_list(columns.len());
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})",
            sql::quote_identifier(table.analytics_table_name.as_str()),
            columns
                .iter()
                .map(|column| sql::quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let values = columns
            .iter()
            .map(|column| {
                json_value_to_duckdb_value_for_column(
                    row.get(*column).unwrap_or(&serde_json::Value::Null),
                    column_type_for_name(table, column),
                )
            })
            .collect::<Vec<_>>();
        self.conn
            .prepare(sql.as_str())?
            .execute(duckdb::params_from_iter(values))?;
        Ok(())
    }

    fn apply_upsert(
        &self,
        table: &TableRegistration,
        mut row: BTreeMap<String, serde_json::Value>,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        row.insert(
            sql::SOURCE_POSITION_COLUMN.to_string(),
            source_position.map_or(Ok(serde_json::Value::Null), serde_json::to_value)?,
        );
        let Some(source_position) = source_position else {
            return if self.update_row(table, &row)? {
                Ok(IngestOutcome::Updated)
            } else {
                self.insert_row(table, &row)?;
                Ok(IngestOutcome::Inserted)
            };
        };
        let existing_position =
            self.load_row_source_position(table.analytics_table_name.as_str(), &row)?;
        match merge_decision(
            existing_position.as_ref(),
            source_position,
            SourceMutation::Update,
            table.skip_delete,
        )? {
            MergeDecision::Insert | MergeDecision::Append => {
                self.insert_row(table, &row)?;
                self.save_row_source_position(table, &row, source_position, true)?;
                Ok(IngestOutcome::Inserted)
            }
            MergeDecision::Replace => {
                if self.update_row(table, &row)? {
                    self.save_row_source_position(table, &row, source_position, true)?;
                    Ok(IngestOutcome::Updated)
                } else {
                    self.insert_row(table, &row)?;
                    self.save_row_source_position(table, &row, source_position, true)?;
                    Ok(IngestOutcome::Inserted)
                }
            }
            MergeDecision::IgnoreDuplicate
            | MergeDecision::IgnoreStale
            | MergeDecision::RequiresValidation
            | MergeDecision::Delete => Ok(IngestOutcome::Skipped),
        }
    }

    fn apply_delete(
        &self,
        table: &TableRegistration,
        row: &BTreeMap<String, serde_json::Value>,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let Some(source_position) = source_position else {
            self.delete_row(table.analytics_table_name.as_str(), row)?;
            return Ok(IngestOutcome::Deleted);
        };
        let existing_position =
            self.load_row_source_position(table.analytics_table_name.as_str(), row)?;
        match merge_decision(
            existing_position.as_ref(),
            source_position,
            SourceMutation::Delete,
            table.skip_delete,
        )? {
            MergeDecision::Delete => {
                self.delete_row(table.analytics_table_name.as_str(), row)?;
                self.save_row_source_position(table, row, source_position, false)?;
                Ok(IngestOutcome::Deleted)
            }
            MergeDecision::IgnoreDuplicate
            | MergeDecision::IgnoreStale
            | MergeDecision::RequiresValidation
            | MergeDecision::Insert
            | MergeDecision::Replace
            | MergeDecision::Append => Ok(IngestOutcome::Skipped),
        }
    }

    fn load_row_source_position(
        &self,
        table_name: &str,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<Option<SourcePosition>> {
        let tenant_id = row
            .get("tenant_id")
            .ok_or(AnalyticsEngineError::MissingIdentifier("tenant_id"))?;
        let row_id = row
            .get("__id")
            .ok_or(AnalyticsEngineError::MissingIdentifier("__id"))?;
        let table_value = row
            .get("table_name")
            .ok_or(AnalyticsEngineError::MissingIdentifier("table_name"))?;
        let metadata = self.load_metadata_source_position(table_name, row)?;
        if metadata.is_some() {
            return Ok(metadata);
        }
        let sql = format!(
            "SELECT {} FROM {} WHERE table_name = ? AND tenant_id = ? AND __id = ? LIMIT 1",
            sql::quote_identifier(sql::SOURCE_POSITION_COLUMN),
            sql::quote_identifier(table_name)
        );
        let value = self
            .conn
            .prepare(sql.as_str())?
            .query_row(
                duckdb::params_from_iter(vec![
                    json_value_to_duckdb_value(table_value),
                    json_value_to_duckdb_value(tenant_id),
                    json_value_to_duckdb_value(row_id),
                ]),
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        value
            .map(|value| serde_json::from_str(value.as_str()))
            .transpose()
            .map_err(Into::into)
    }

    fn load_metadata_source_position(
        &self,
        analytics_table_name: &str,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<Option<SourcePosition>> {
        let tenant_id = row
            .get("tenant_id")
            .and_then(serde_json::Value::as_str)
            .ok_or(AnalyticsEngineError::MissingIdentifier("tenant_id"))?;
        let row_id = row
            .get("__id")
            .and_then(serde_json::Value::as_str)
            .ok_or(AnalyticsEngineError::MissingIdentifier("__id"))?;
        let source_table_name = row
            .get("table_name")
            .and_then(serde_json::Value::as_str)
            .ok_or(AnalyticsEngineError::MissingIdentifier("table_name"))?;
        let value = self
            .conn
            .prepare(
                "SELECT source_position_json FROM __analytics_row_source_positions
                 WHERE analytics_table_name = ?
                   AND source_table_name = ?
                   AND tenant_id = ?
                   AND source_row_id = ?
                 LIMIT 1",
            )?
            .query_row(
                duckdb::params![analytics_table_name, source_table_name, tenant_id, row_id,],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value
            .map(|value| serde_json::from_str(value.as_str()))
            .transpose()
            .map_err(Into::into)
    }

    fn save_row_source_position(
        &self,
        table: &TableRegistration,
        row: &BTreeMap<String, serde_json::Value>,
        source_position: &SourcePosition,
        row_visible: bool,
    ) -> AnalyticsEngineResult<()> {
        let tenant_id = row
            .get("tenant_id")
            .and_then(serde_json::Value::as_str)
            .ok_or(AnalyticsEngineError::MissingIdentifier("tenant_id"))?;
        let row_id = row
            .get("__id")
            .and_then(serde_json::Value::as_str)
            .ok_or(AnalyticsEngineError::MissingIdentifier("__id"))?;
        let source_table_name = row
            .get("table_name")
            .and_then(serde_json::Value::as_str)
            .ok_or(AnalyticsEngineError::MissingIdentifier("table_name"))?;
        self.conn.execute(
            "DELETE FROM __analytics_row_source_positions
             WHERE analytics_table_name = ?
               AND source_table_name = ?
               AND tenant_id = ?
               AND source_row_id = ?",
            duckdb::params![
                table.analytics_table_name.as_str(),
                source_table_name,
                tenant_id,
                row_id,
            ],
        )?;
        self.conn.execute(
            "INSERT INTO __analytics_row_source_positions
                (analytics_table_name, source_table_name, tenant_id, source_row_id,
                 source_position_json, row_visible, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                table.analytics_table_name.as_str(),
                source_table_name,
                tenant_id,
                row_id,
                serde_json::to_string(source_position)?,
                row_visible,
                current_time_millis(),
            ],
        )?;
        Ok(())
    }

    fn update_row(
        &self,
        table: &TableRegistration,
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
            .map(String::as_str)
            .collect::<Vec<_>>();
        if update_columns.is_empty() {
            return Ok(false);
        }

        let set_clause = assignment_set_clause(update_columns.iter().copied());
        let sql = format!(
            "UPDATE {} SET {set_clause} WHERE table_name = ? AND tenant_id = ? AND __id = ?",
            sql::quote_identifier(table.analytics_table_name.as_str())
        );
        let mut values = update_columns
            .iter()
            .map(|column| {
                json_value_to_duckdb_value_for_column(
                    row.get(*column).unwrap_or(&serde_json::Value::Null),
                    column_type_for_name(table, column),
                )
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

    fn update_privacy_scrubbed_row(
        &self,
        table_name: &str,
        row_id: &str,
        assignments: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<()> {
        let set_clause = assignment_set_clause(assignments.keys().map(String::as_str));
        let sql = format!(
            "UPDATE {} SET {set_clause} WHERE {} = ?",
            sql::quote_identifier(table_name),
            sql::quote_identifier("__id")
        );
        let mut values = assignments
            .values()
            .map(json_value_to_duckdb_value)
            .collect::<Vec<_>>();
        values.push(json_value_to_duckdb_value(&serde_json::Value::String(
            row_id.to_string(),
        )));
        self.conn
            .prepare(sql.as_str())?
            .execute(duckdb::params_from_iter(values))?;
        Ok(())
    }

    fn delete_privacy_violating_row(
        &self,
        table_name: &str,
        row_id: &str,
    ) -> AnalyticsEngineResult<()> {
        let sql = format!(
            "DELETE FROM {} WHERE {} = ?",
            sql::quote_identifier(table_name),
            sql::quote_identifier("__id")
        );
        self.conn
            .prepare(sql.as_str())?
            .execute(duckdb::params![row_id])?;
        Ok(())
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

fn motherduck_config(settings: &DuckLakeCatalogSettings) -> AnalyticsEngineResult<DuckDbConfig> {
    let Some(token) = settings.motherduck_token.as_deref() else {
        return Ok(DuckDbConfig::default());
    };
    Ok(DuckDbConfig::default().with("motherduck_token", token)?)
}

fn execute_convergent_schema_statement(
    conn: &Connection,
    statement: &str,
) -> AnalyticsEngineResult<()> {
    match conn.execute(statement, []) {
        Ok(_) => Ok(()),
        Err(error) if ducklake_schema_converged_concurrently(&error) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn ducklake_schema_converged_concurrently(error: &duckdb::Error) -> bool {
    let message = error.to_string();
    message.contains("DuckLake transaction")
        && message.contains("has been created by another transaction already")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestOutcome {
    Inserted,
    Updated,
    Deleted,
    Skipped,
}

#[derive(Debug)]
pub struct StreamRecordBatchItem {
    pub analytics_table_name: String,
    pub record_key: Vec<u8>,
    pub record: StorageStreamRecord,
}

struct BatchSqlExecutor<'conn> {
    conn: &'conn Connection,
    inserts: BTreeMap<BatchStatementKey, duckdb::Statement<'conn>>,
    updates: BTreeMap<BatchStatementKey, duckdb::Statement<'conn>>,
    deletes: BTreeMap<String, duckdb::Statement<'conn>>,
}

impl<'conn> BatchSqlExecutor<'conn> {
    fn new(conn: &'conn Connection) -> Self {
        Self {
            conn,
            inserts: BTreeMap::new(),
            updates: BTreeMap::new(),
            deletes: BTreeMap::new(),
        }
    }

    fn apply_upsert(
        &mut self,
        table: &TableRegistration,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        if self.update_row(table, row)? {
            Ok(IngestOutcome::Updated)
        } else {
            self.insert_row(table, row)?;
            Ok(IngestOutcome::Inserted)
        }
    }

    fn insert_row(
        &mut self,
        table: &TableRegistration,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<()> {
        let columns = row.keys().cloned().collect::<Vec<_>>();
        let key = BatchStatementKey {
            table_name: table.analytics_table_name.clone(),
            columns,
        };
        if !self.inserts.contains_key(&key) {
            let placeholders = placeholder_list(key.columns.len());
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({placeholders})",
                sql::quote_identifier(table.analytics_table_name.as_str()),
                key.columns
                    .iter()
                    .map(|column| sql::quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            self.inserts
                .insert(key.clone(), self.conn.prepare(sql.as_str())?);
        }
        let values = key
            .columns
            .iter()
            .map(|column| {
                json_value_to_duckdb_value_for_column(
                    row.get(column).unwrap_or(&serde_json::Value::Null),
                    column_type_for_name(table, column),
                )
            })
            .collect::<Vec<_>>();
        self.inserts
            .get_mut(&key)
            .ok_or_else(|| {
                AnalyticsEngineError::TableNotRegistered(table.analytics_table_name.clone())
            })?
            .execute(duckdb::params_from_iter(values))?;
        Ok(())
    }

    fn update_row(
        &mut self,
        table: &TableRegistration,
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

        let columns = row
            .keys()
            .filter(|column| !matches!(column.as_str(), "tenant_id" | "__id" | "table_name"))
            .cloned()
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Ok(false);
        }

        let key = BatchStatementKey {
            table_name: table.analytics_table_name.clone(),
            columns,
        };
        if !self.updates.contains_key(&key) {
            let set_clause = assignment_set_clause(key.columns.iter().map(String::as_str));
            let sql = format!(
                "UPDATE {} SET {set_clause} WHERE table_name = ? AND tenant_id = ? AND __id = ?",
                sql::quote_identifier(table.analytics_table_name.as_str())
            );
            self.updates
                .insert(key.clone(), self.conn.prepare(sql.as_str())?);
        }
        let mut values = key
            .columns
            .iter()
            .map(|column| {
                json_value_to_duckdb_value_for_column(
                    row.get(column).unwrap_or(&serde_json::Value::Null),
                    column_type_for_name(table, column),
                )
            })
            .collect::<Vec<_>>();
        values.push(json_value_to_duckdb_value(table_value));
        values.push(json_value_to_duckdb_value(tenant_id));
        values.push(json_value_to_duckdb_value(row_id));
        let changed = self
            .updates
            .get_mut(&key)
            .ok_or_else(|| {
                AnalyticsEngineError::TableNotRegistered(table.analytics_table_name.clone())
            })?
            .execute(duckdb::params_from_iter(values))?;
        Ok(changed > 0)
    }

    fn apply_delete(
        &mut self,
        table_name: &str,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let tenant_id = row
            .get("tenant_id")
            .ok_or(AnalyticsEngineError::MissingIdentifier("tenant_id"))?;
        let row_id = row
            .get("__id")
            .ok_or(AnalyticsEngineError::MissingIdentifier("__id"))?;
        let table_value = row
            .get("table_name")
            .ok_or(AnalyticsEngineError::MissingIdentifier("table_name"))?;
        if !self.deletes.contains_key(table_name) {
            let sql = format!(
                "DELETE FROM {} WHERE table_name = ? AND tenant_id = ? AND __id = ?",
                sql::quote_identifier(table_name)
            );
            self.deletes
                .insert(table_name.to_string(), self.conn.prepare(sql.as_str())?);
        }
        self.deletes
            .get_mut(table_name)
            .ok_or_else(|| AnalyticsEngineError::TableNotRegistered(table_name.to_string()))?
            .execute(duckdb::params_from_iter(vec![
                json_value_to_duckdb_value(table_value),
                json_value_to_duckdb_value(tenant_id),
                json_value_to_duckdb_value(row_id),
            ]))?;
        Ok(IngestOutcome::Deleted)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BatchStatementKey {
    table_name: String,
    columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivacyIngestOutcome {
    pub outcome: IngestOutcome,
    pub dropped_fields: u64,
    pub policy_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivacyTableRemediationReport {
    pub table_name: String,
    pub dry_run: bool,
    pub mode: PrivacyTableRemediationMode,
    pub scanned_rows: u64,
    pub affected_rows: u64,
    pub projected_values_scrubbed: u64,
    pub document_values_scrubbed: u64,
    pub destination_mutated: bool,
    pub policy_version: String,
}

pub type PrivacyTableScrubReport = PrivacyTableRemediationReport;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivacyTableRemediationMode {
    Scrub,
    DeleteRows,
}

struct FilteredStreamRecord {
    record: StorageStreamRecord,
    dropped_fields: u64,
}

fn filter_stream_record(
    mut record: StorageStreamRecord,
    policy: &PrivacyPolicy,
) -> AnalyticsEngineResult<FilteredStreamRecord> {
    let mut dropped_fields = 0_u64;
    let keys = policy.filter_item(&record.keys)?;
    dropped_fields += keys.dropped_fields;
    record.keys = keys.item;
    if let Some(old_image) = &record.old_image {
        let filtered = policy.filter_item(old_image)?;
        dropped_fields += filtered.dropped_fields;
        record.old_image = Some(filtered.item);
    }
    if let Some(new_image) = &record.new_image {
        let filtered = policy.filter_item(new_image)?;
        dropped_fields += filtered.dropped_fields;
        record.new_image = Some(filtered.item);
    }
    Ok(FilteredStreamRecord {
        record,
        dropped_fields,
    })
}

fn privacy_scrubbable_projection_columns(
    table: &TableRegistration,
    policy: &PrivacyPolicy,
) -> AnalyticsEngineResult<Vec<String>> {
    let mut columns = Vec::new();
    if let Some(projection_columns) = table.projection_columns.as_ref() {
        for column in projection_columns {
            if policy.denies_key(column.column_name.as_str())?
                || policy.denies_path(column.attribute_path.as_str())?
            {
                columns.push(column.column_name.clone());
            }
        }
    }
    if let Some(attribute_names) = table.projection_attribute_names.as_ref() {
        for column in attribute_names {
            if policy.denies_key(column.as_str())? {
                columns.push(column.clone());
            }
        }
    }
    Ok(columns)
}

fn privacy_remediation_scan_sql(
    table_name: &str,
    selected_columns: &[String],
    projection_columns: &[String],
    has_document_column: bool,
) -> String {
    let select_columns = selected_columns
        .iter()
        .map(|column| sql::quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut statement = format!(
        "SELECT {select_columns} FROM {}",
        sql::quote_identifier(table_name)
    );
    if !has_document_column && !projection_columns.is_empty() {
        let predicate = projection_columns
            .iter()
            .map(|column| format!("{} IS NOT NULL", sql::quote_identifier(column)))
            .collect::<Vec<_>>()
            .join(" OR ");
        statement.push_str(" WHERE ");
        statement.push_str(predicate.as_str());
    }
    statement
}

fn filter_json_value(
    value: serde_json::Value,
    path: &str,
    policy: &PrivacyPolicy,
    dropped_fields: &mut u64,
) -> AnalyticsEngineResult<serde_json::Value> {
    match value {
        serde_json::Value::Object(object) => {
            let mut filtered = serde_json::Map::new();
            for (key, value) in object {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                if policy.denies_key(key.as_str())? || policy.denies_path(child_path.as_str())? {
                    *dropped_fields += 1;
                    continue;
                }
                let before = *dropped_fields;
                let value = filter_json_value(value, child_path.as_str(), policy, dropped_fields)?;
                if value.is_null() && *dropped_fields > before {
                    continue;
                }
                filtered.insert(key, value);
            }
            Ok(serde_json::Value::Object(filtered))
        }
        serde_json::Value::Array(values) => {
            let mut filtered = Vec::with_capacity(values.len());
            for value in values {
                let before = *dropped_fields;
                let value = filter_json_value(value, path, policy, dropped_fields)?;
                if !(value.is_null() && *dropped_fields > before) {
                    filtered.push(value);
                }
            }
            Ok(serde_json::Value::Array(filtered))
        }
        value if value_removed_by_policy(&value, policy)? => {
            *dropped_fields += 1;
            Ok(serde_json::Value::Null)
        }
        value => Ok(value),
    }
}

fn value_removed_by_policy(
    value: &serde_json::Value,
    policy: &PrivacyPolicy,
) -> AnalyticsEngineResult<bool> {
    Ok(value
        .as_str()
        .map(|value| policy.denies_value(value))
        .transpose()?
        .unwrap_or(false))
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

fn rollback_batch<T>(
    conn: &Connection,
    error: AnalyticsEngineError,
) -> AnalyticsEngineResult<Vec<T>> {
    let _ = conn.execute_batch("ROLLBACK");
    Err(error)
}

pub(crate) fn columns_for_registration(table: &TableRegistration) -> Vec<AnalyticsColumn> {
    let mut columns = Vec::with_capacity(registration_column_capacity(table));
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

fn registration_column_capacity(table: &TableRegistration) -> usize {
    usize::from(table.document_column.is_some())
        + if table.columns.is_empty() {
            table.projection_columns.as_ref().map_or_else(
                || {
                    table
                        .projection_attribute_names
                        .as_ref()
                        .map_or(0, Vec::len)
                },
                Vec::len,
            )
        } else {
            table.columns.len()
        }
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

fn json_value_to_duckdb_value_for_column(
    value: &serde_json::Value,
    column_type: Option<AnalyticsColumnType>,
) -> duckdb::types::Value {
    if is_timestamp_column(column_type.as_ref()) {
        return match value {
            serde_json::Value::Number(number) => number
                .as_i64()
                .map_or_else(|| json_value_to_duckdb_value(value), timestamp_millis_value),
            serde_json::Value::String(text) => text.parse::<i64>().map_or_else(
                |_| json_value_to_duckdb_value(value),
                timestamp_millis_value,
            ),
            serde_json::Value::Null => duckdb::types::Value::Null,
            serde_json::Value::Bool(_)
            | serde_json::Value::Array(_)
            | serde_json::Value::Object(_) => json_value_to_duckdb_value(value),
        };
    }
    json_value_to_duckdb_value(value)
}

fn timestamp_millis_value(value: i64) -> duckdb::types::Value {
    duckdb::types::Value::Timestamp(TimeUnit::Millisecond, value)
}

fn is_timestamp_column(column_type: Option<&AnalyticsColumnType>) -> bool {
    matches!(
        column_type,
        Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::Timestamp
        })
    )
}

fn column_type_for_name(
    table: &TableRegistration,
    column_name: &str,
) -> Option<AnalyticsColumnType> {
    columns_for_registration(table)
        .into_iter()
        .find(|column| column.column_name == column_name)
        .map(|column| column.column_type)
}

fn assignment_set_clause<'a>(columns: impl IntoIterator<Item = &'a str>) -> String {
    let mut clause = String::new();
    for (index, column) in columns.into_iter().enumerate() {
        if index > 0 {
            clause.push_str(", ");
        }
        clause.push_str(sql::quote_identifier(column).as_str());
        clause.push_str(" = ?");
    }
    clause
}

fn placeholder_list(count: usize) -> String {
    let mut placeholders = String::with_capacity(count.saturating_mul(3).saturating_sub(2));
    for index in 0..count {
        if index > 0 {
            placeholders.push_str(", ");
        }
        placeholders.push('?');
    }
    placeholders
}
