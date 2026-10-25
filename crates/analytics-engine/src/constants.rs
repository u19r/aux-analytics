use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock, Weak},
    time::Duration,
};

use analytics_contract::MAX_STRUCTURED_QUERY_BOUND;
use duckdb::Connection;

// Query bounds and serialization framing.
pub(crate) const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const CACHE_CAPACITY: usize = 10_000;
/// Maximum serialized JSON bytes retained for one query result.
pub const MAX_QUERY_RESULT_BYTES: usize = 8 * 1024 * 1024;
/// Maximum rows retained while materializing one query result.
pub(crate) const MAX_QUERY_RESULT_ROWS: usize = MAX_STRUCTURED_QUERY_BOUND as usize;
pub(crate) const JSON_ARRAY_FRAMING_BYTES: usize = 2;
pub(crate) const JSON_ARRAY_SEPARATOR_BYTES: usize = 1;

// Batch staging and persisted source metadata.
pub(crate) const BATCH_STAGE_METADATA_COLUMNS: usize = 2;
pub(crate) const ORDINAL_COLUMN: &str = "__analytics_batch_ordinal";
pub(crate) const UPSERT_COLUMN: &str = "__analytics_batch_upsert";
pub(crate) const SLOW_DUCKLAKE_PHASE_THRESHOLD: Duration = Duration::from_secs(1);

// Durable authority identity, digests, and scope limits.
pub const EXECUTION_ID_BYTES: usize = 16;
pub const CONTENT_DIGEST_BYTES: usize = 32;
pub const MANIFEST_DIGEST_BYTES: usize = 32;
pub(crate) const MAX_SCOPE_COMPONENT_BYTES: usize = 512;

pub(crate) const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

pub(crate) const MAX_SQL_DOLLAR_TAG_LEN: usize = 32;
pub(crate) const MAX_MAINTENANCE_EXECUTION_ID_BYTES: usize = 512;
pub(crate) const SOURCE_POSITION_COLUMN: &str = "__source_position";
pub(crate) const QUOTED_SOURCE_POSITION_COLUMN: &str = "\"__source_position\"";

// Read-only SQL token policy.
pub(crate) const FORBIDDEN_SQL_KEYWORDS: &[&[u8]] = &[
    b"alter",
    b"attach",
    b"begin",
    b"call",
    b"checkpoint",
    b"commit",
    b"copy",
    b"create",
    b"delete",
    b"detach",
    b"drop",
    b"export",
    b"grant",
    b"import",
    b"insert",
    b"install",
    b"load",
    b"merge",
    b"pragma",
    b"reset",
    b"revoke",
    b"rollback",
    b"set",
    b"transaction",
    b"truncate",
    b"update",
    b"use",
    b"vacuum",
];

// Durable destination schemas for ingestion, receipts, and retention.
pub(crate) const SOURCE_RECORD_STATE_INDEX_SQL: &str = "CREATE INDEX IF NOT EXISTS \
                                                        __analytics_source_record_states_digest
         ON __analytics_source_record_states (source_key_digest)";

pub(crate) const RUNTIME_SCHEMA_STATEMENTS: [&str; 10] = [
    "CREATE TABLE IF NOT EXISTS analytics_checkpoints (
                source_table_name VARCHAR NOT NULL,
                shard_id VARCHAR NOT NULL,
                position VARCHAR NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
    "CREATE TABLE IF NOT EXISTS analytics_system_stream_checkpoints (
                scope_version INTEGER NOT NULL,
                scope_id VARCHAR NOT NULL,
                generation BIGINT NOT NULL,
                checkpoint VARCHAR NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
    "CREATE TABLE IF NOT EXISTS __analytics_row_source_positions (
                analytics_table_name VARCHAR NOT NULL,
                source_table_name VARCHAR NOT NULL,
                tenant_id VARCHAR NOT NULL,
                source_row_id VARCHAR NOT NULL,
                source_position_json JSON NOT NULL,
                row_visible BOOLEAN NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
    "CREATE TABLE IF NOT EXISTS __analytics_source_record_states (
                analytics_table_name VARCHAR NOT NULL,
                source_table_name VARCHAR NOT NULL,
                source_key_digest VARCHAR NOT NULL,
                source_position_json JSON NOT NULL,
                child_identities_json JSON NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
    "CREATE TABLE IF NOT EXISTS __analytics_row_repair_receipts (
                repair_key VARCHAR NOT NULL,
                analytics_table_name VARCHAR NOT NULL,
                source_table_name VARCHAR NOT NULL,
                tenant_id VARCHAR NOT NULL,
                source_row_id VARCHAR NOT NULL,
                source_revision VARCHAR NOT NULL,
                expected_row_json JSON NOT NULL,
                row_visible BOOLEAN NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
    "CREATE TABLE IF NOT EXISTS __analytics_row_repair_authority (
                execution_id VARCHAR NOT NULL,
                fence VARCHAR NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
    "CREATE TABLE IF NOT EXISTS __analytics_row_repair_effects (
                execution_id VARCHAR NOT NULL,
                fence VARCHAR NOT NULL,
                repair_key VARCHAR NOT NULL,
                analytics_table_name VARCHAR NOT NULL,
                source_revision VARCHAR NOT NULL,
                expected_row_json JSON NOT NULL,
                row_visible BOOLEAN NOT NULL,
                consumed BOOLEAN NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
    "CREATE TABLE IF NOT EXISTS __analytics_range_purge_authority (
                job_key BLOB NOT NULL,
                claim_json JSON NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
    "CREATE TABLE IF NOT EXISTS __analytics_range_purge_receipts (
                execution_key BLOB NOT NULL,
                request_id VARCHAR NOT NULL,
                request_json JSON NOT NULL,
                response_json JSON NOT NULL,
                committed_at_ms BIGINT NOT NULL
            )",
    "CREATE TABLE IF NOT EXISTS analytics_source_batch_failures (
                source_table_name VARCHAR NOT NULL,
                shard_id VARCHAR NOT NULL,
                record_position VARCHAR NOT NULL,
                analytics_table_name VARCHAR NOT NULL,
                error_class VARCHAR NOT NULL,
                error_message VARCHAR NOT NULL,
                failed_at_ms BIGINT NOT NULL,
                repaired_at_ms BIGINT,
                fence VARCHAR,
                execution_id BLOB
            )",
];

// Shared connections and receipt locks for local destination authority.
pub(crate) static SHARED_CONNECTIONS: OnceLock<Mutex<BTreeMap<String, Weak<Mutex<Connection>>>>> =
    OnceLock::new();

pub(crate) static REPAIR_RECEIPT_LOCKS: OnceLock<Mutex<BTreeMap<String, Weak<Mutex<()>>>>> =
    OnceLock::new();

// Representative fixtures and paired allocation/CPU measurements.
#[cfg(test)]
pub(crate) const REALISTIC_ATTRIBUTE_COUNT: usize = 10;
#[cfg(test)]
pub(crate) const REALISTIC_PAYLOAD_BYTES: usize = 1_200;
#[cfg(test)]
pub(crate) const REALISTIC_RECORD_COUNT: usize = 1_000;
#[cfg(test)]
pub(crate) const REALISTIC_QUERY_LIMITS: [usize; 4] = [1, 25, 100, 500];
#[cfg(test)]
pub(crate) const CANDIDATE_REPETITIONS: usize = 32;
#[cfg(test)]
pub(crate) const PRODUCTION_WARMUP_RUNS: usize = 2;
#[cfg(test)]
pub(crate) const PRODUCTION_TIMING_TRIALS: usize = 3;
#[cfg(test)]
pub(crate) const MIN_ALLOCATION_CPU_DELTA_PERCENT: f64 = -5.0;
#[cfg(test)]
pub(crate) const PRODUCTION_PAIRED_SAMPLES: usize = 3;
