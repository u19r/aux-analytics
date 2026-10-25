// Current-row comparison and bounded page traversal.
pub(crate) const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
pub(crate) const FNV_PRIME: u64 = 0x0100_0000_01b3;
pub(crate) const MAX_CONSECUTIVE_EMPTY_PAGES: usize = 8;
pub(crate) const MAX_PAGE_WORK: usize = 1_024;
pub(crate) const DUCKDB_CHECK_BATCH_ROWS: usize = 2_048;

// Durable operations and bounded maintenance selections.
pub(crate) const EXECUTION_LEASE_MS: i64 = 300_000;
pub(crate) const MAX_TRIM_PREDICATE_DEPTH: usize = 8;
pub(crate) const MAX_TRIM_PREDICATE_NODES: usize = 64;
pub(crate) const SAMPLE_KEY_LIMIT: usize = 10;

// Persisted backup schema and lowercase hexadecimal encoding.
pub const RAW_BACKUP_SCHEMA_VERSION: u16 = 1;
pub(crate) const HEX: &[u8; 16] = b"0123456789abcdef";

// Compare-and-swap of the full persisted operation fence.
pub(crate) const UPDATE_OPERATION_STATE_SQL: &str = "UPDATE __analytics_operations
         SET phase = ?, status = ?, cancellation_state = ?, cursor_json = ?,
             updated_at_ms = now_ms(), revision = revision + 1, progress_json = ?
         WHERE operation_id = ? AND revision = ? AND phase = ? AND status = ?
           AND cancellation_state = ? AND cursor_json = ? AND progress_json = ?
           AND execution_generation = ? AND execution_owner = ?
           AND (status <> 'running' OR execution_generation = 0
                OR execution_lease_until_ms > now_ms())";

// Current durable operation schema.
pub(crate) const OPERATION_SCHEMA_SQL: &str = "CREATE MACRO IF NOT EXISTS now_ms() AS \
                                               CAST(epoch_ms(current_timestamp) AS BIGINT);
             CREATE TABLE IF NOT EXISTS __analytics_operations (
                operation_id VARCHAR PRIMARY KEY,
                kind VARCHAR NOT NULL,
                actor VARCHAR NOT NULL,
                target_tables_json JSON NOT NULL,
                dry_run BOOLEAN NOT NULL,
                rate_limit_json JSON NOT NULL,
                payload_json JSON NOT NULL,
                phase VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                cancellation_state VARCHAR NOT NULL,
             cursor_json JSON NOT NULL,
             created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                revision BIGINT NOT NULL DEFAULT 0,
                progress_json JSON NOT NULL DEFAULT '{}',
                execution_generation BIGINT NOT NULL DEFAULT 0,
                execution_owner VARCHAR NOT NULL DEFAULT '',
                execution_lease_until_ms BIGINT NOT NULL DEFAULT 0
             );
             CREATE SEQUENCE IF NOT EXISTS __analytics_operation_event_seq START 1;
             CREATE TABLE IF NOT EXISTS __analytics_operation_events (
                operation_id VARCHAR NOT NULL,
                event_id BIGINT DEFAULT nextval('__analytics_operation_event_seq'),
                occurred_at_ms BIGINT NOT NULL,
                kind VARCHAR NOT NULL,
                phase VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                cursor_json JSON NOT NULL,
                counts_json JSON NOT NULL,
                message VARCHAR,
                PRIMARY KEY (operation_id, event_id)
             );
             CREATE TABLE IF NOT EXISTS __analytics_operation_effects (
                operation_id VARCHAR NOT NULL,
                effect_key VARCHAR NOT NULL,
                state VARCHAR NOT NULL,
                started_revision BIGINT NOT NULL,
                attempt_generation BIGINT NOT NULL DEFAULT 0,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (operation_id, effect_key)
             );
             CREATE TABLE IF NOT EXISTS __analytics_operation_reports (
                operation_id VARCHAR PRIMARY KEY,
                report_kind VARCHAR NOT NULL CHECK (report_kind IN
                    ('backfill', 'check', 'table_fix', 'privacy_fix', 'trim', 'raw_backup')),
                report_json JSON NOT NULL,
                created_at_ms BIGINT NOT NULL
             );";
