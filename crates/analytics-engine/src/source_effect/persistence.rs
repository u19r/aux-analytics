use std::num::NonZeroU64;

use duckdb::{Connection, types::Value};
use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};

use super::{
    contracts::{SourceEffectAuthority, SourceEffectScope, SourceEffectSummary},
    validation::{
        current_time_millis, decode_content_digest, decode_count, decode_execution_id, decode_fence,
    },
};
use crate::{
    constants::{CONTENT_DIGEST_BYTES, EXECUTION_ID_BYTES},
    engine::{AnalyticsEngineError, AnalyticsEngineResult},
};

#[derive(Clone, Debug)]
pub(crate) struct StoredAuthority {
    pub(crate) execution_id: [u8; EXECUTION_ID_BYTES],
    pub(crate) fence: NonZeroU64,
}

#[derive(Clone, Debug)]
pub(crate) struct StoredReceipt {
    pub(crate) scope: StoredReceiptScope,
    pub(crate) fence: NonZeroU64,
    pub(crate) content_digest: Option<[u8; CONTENT_DIGEST_BYTES]>,
    pub(crate) summary: SourceEffectSummary,
}

#[derive(Clone, Debug)]
pub(crate) enum StoredReceiptScope {
    Table {
        source_table_name: String,
        shard_id: String,
    },
    System {
        source_scope: SystemStreamScope,
        checkpoint: SystemStreamCheckpointToken,
    },
}

pub(crate) fn schema_statements() -> [&'static str; 4] {
    [
        "CREATE TABLE IF NOT EXISTS __analytics_source_effect_authority (
            authority_version INTEGER NOT NULL,
            source_table_name VARCHAR NOT NULL,
            shard_id VARCHAR NOT NULL,
            fence VARCHAR NOT NULL,
            execution_id BLOB NOT NULL,
            updated_at_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS __analytics_source_effect_receipts (
            receipt_version INTEGER NOT NULL,
            execution_id BLOB NOT NULL,
            source_table_name VARCHAR NOT NULL,
            shard_id VARCHAR NOT NULL,
            fence VARCHAR NOT NULL,
            inserted_count BIGINT NOT NULL,
            updated_count BIGINT NOT NULL,
            deleted_count BIGINT NOT NULL,
            skipped_count BIGINT NOT NULL,
            dropped_fields BIGINT NOT NULL,
            checkpoint_count BIGINT NOT NULL,
            content_digest BLOB,
            committed_at_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS __analytics_system_source_effect_authority (
            authority_version INTEGER NOT NULL,
            scope_version INTEGER NOT NULL,
            scope_id VARCHAR NOT NULL,
            generation BIGINT NOT NULL,
            fence VARCHAR NOT NULL,
            execution_id BLOB NOT NULL,
            updated_at_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS __analytics_system_source_effect_receipts (
            receipt_version INTEGER NOT NULL,
            execution_id BLOB NOT NULL,
            scope_version INTEGER NOT NULL,
            scope_id VARCHAR NOT NULL,
            generation BIGINT NOT NULL,
            checkpoint VARCHAR NOT NULL,
            fence VARCHAR NOT NULL,
            inserted_count BIGINT NOT NULL,
            updated_count BIGINT NOT NULL,
            deleted_count BIGINT NOT NULL,
            skipped_count BIGINT NOT NULL,
            dropped_fields BIGINT NOT NULL,
            checkpoint_count BIGINT NOT NULL,
            content_digest BLOB,
            committed_at_ms BIGINT NOT NULL
        )",
    ]
}

pub(crate) fn load_system_checkpoint(
    conn: &Connection,
) -> AnalyticsEngineResult<Option<(SystemStreamScope, SystemStreamCheckpointToken)>> {
    let mut statement = conn.prepare(
        "SELECT scope_version, scope_id, generation, checkpoint
         FROM analytics_system_stream_checkpoints
         LIMIT 2",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut values = Vec::with_capacity(2);
    for row in rows {
        values.push(row?);
    }
    if values.len() > 1 {
        return Err(AnalyticsEngineError::IncompleteSourceEffectState);
    }
    let Some((version, scope_id, generation, checkpoint)) = values.into_iter().next() else {
        return Ok(None);
    };
    let version =
        u8::try_from(version).map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
    let generation =
        u64::try_from(generation).map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
    let scope = SystemStreamScope::try_new_versioned(version, scope_id, generation)
        .map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
    let checkpoint = SystemStreamCheckpointToken::try_from(checkpoint)
        .map_err(|_| AnalyticsEngineError::InvalidSourceEffectReceipt)?;
    Ok(Some((scope, checkpoint)))
}

pub(crate) fn system_checkpoint_matches(
    conn: &Connection,
    scope: &SourceEffectScope,
    previous_checkpoint: Option<&SystemStreamCheckpointToken>,
) -> AnalyticsEngineResult<bool> {
    let Some((source_scope, checkpoint)) = load_system_checkpoint(conn)? else {
        return Ok(previous_checkpoint.is_none());
    };
    let Some((requested_scope, _)) = scope.system_parts() else {
        return Err(AnalyticsEngineError::InvalidSourceEffectScope);
    };
    Ok(source_scope == *requested_scope && previous_checkpoint == Some(&checkpoint))
}

pub(crate) fn load_authority(
    conn: &Connection,
    scope: &SourceEffectScope,
) -> AnalyticsEngineResult<Option<StoredAuthority>> {
    if let Some((source_scope, _)) = scope.system_parts() {
        let generation = i64::try_from(source_scope.generation())
            .map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
        let mut statement = conn.prepare(
            "SELECT fence, execution_id
             FROM __analytics_system_source_effect_authority
             WHERE scope_version = ? AND scope_id = ? AND generation = ?
             LIMIT 2",
        )?;
        let rows = statement.query_map(
            duckdb::params![
                i32::from(source_scope.version()),
                source_scope.scope_id().as_str(),
                generation,
            ],
            |row| {
                let fence: String = row.get(0)?;
                let execution_id: Vec<u8> = row.get(1)?;
                Ok((fence, execution_id))
            },
        )?;
        let mut values = Vec::with_capacity(2);
        for row in rows {
            values.push(row?);
        }
        return match values.as_slice() {
            [] => Ok(None),
            [(_, _), (_, _)] => Err(AnalyticsEngineError::AmbiguousSourceEffectAuthority),
            [(fence, bytes)] => Ok(Some(StoredAuthority {
                execution_id: decode_execution_id(bytes)?,
                fence: decode_fence(fence.as_str())?,
            })),
            _ => unreachable!("authority query is bounded to two rows"),
        };
    }
    let Some((source_table_name, shard_id)) = scope.table_parts() else {
        return Err(AnalyticsEngineError::InvalidSourceEffectScope);
    };
    let mut statement = conn.prepare(
        "SELECT fence, execution_id
         FROM __analytics_source_effect_authority
         WHERE source_table_name = ? AND shard_id = ?
         LIMIT 2",
    )?;
    let rows = statement.query_map(duckdb::params![source_table_name, shard_id], |row| {
        let fence: String = row.get(0)?;
        let execution_id: Vec<u8> = row.get(1)?;
        Ok((fence, execution_id))
    })?;
    let mut values = Vec::with_capacity(2);
    for row in rows {
        values.push(row?);
    }
    match values.as_slice() {
        [] => Ok(None),
        [(_, _), (_, _)] => Err(AnalyticsEngineError::AmbiguousSourceEffectAuthority),
        [(fence, bytes)] => Ok(Some(StoredAuthority {
            execution_id: decode_execution_id(bytes)?,
            fence: decode_fence(fence.as_str())?,
        })),
        _ => unreachable!("authority query is bounded to two rows"),
    }
}

pub(crate) fn load_receipt(
    conn: &Connection,
    execution_id: &[u8; EXECUTION_ID_BYTES],
) -> AnalyticsEngineResult<Option<StoredReceipt>> {
    let mut statement = conn.prepare(
        "SELECT source_table_name, shard_id, fence, inserted_count,
                updated_count, deleted_count, skipped_count, dropped_fields,
                checkpoint_count, content_digest
         FROM __analytics_source_effect_receipts
         WHERE execution_id = ?
         LIMIT 2",
    )?;
    let rows = statement.query_map([Value::Blob(execution_id.to_vec())], |row| {
        let fence: String = row.get(2)?;
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            fence,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i64>(8)?,
            row.get::<_, Option<Vec<u8>>>(9)?,
        ))
    })?;
    let mut values = Vec::with_capacity(2);
    for row in rows {
        values.push(row?);
    }
    match values.as_slice() {
        [] => Ok(None),
        [_, _] => Err(AnalyticsEngineError::AmbiguousSourceEffectReceipt),
        [
            (
                source_table_name,
                shard_id,
                fence,
                inserted,
                updated,
                deleted,
                skipped,
                dropped,
                checkpoints,
                content_digest,
            ),
        ] => {
            // Validate the persisted epoch even though replay callers only
            // need the outcome summary. A malformed receipt is never an
            // idempotency proof.
            let fence = decode_fence(fence.as_str())?;
            Ok(Some(StoredReceipt {
                scope: StoredReceiptScope::Table {
                    source_table_name: source_table_name.clone(),
                    shard_id: shard_id.clone(),
                },
                fence,
                content_digest: decode_content_digest(content_digest.as_deref())?,
                summary: SourceEffectSummary {
                    inserted: decode_count(*inserted)?,
                    updated: decode_count(*updated)?,
                    deleted: decode_count(*deleted)?,
                    skipped: decode_count(*skipped)?,
                    dropped_fields: decode_count(*dropped)?,
                    checkpoint_count: decode_count(*checkpoints)?,
                },
            }))
        }
        _ => unreachable!("receipt query is bounded to two rows"),
    }
}

pub(crate) fn load_system_receipt(
    conn: &Connection,
    execution_id: &[u8; EXECUTION_ID_BYTES],
) -> AnalyticsEngineResult<Option<StoredReceipt>> {
    let mut statement = conn.prepare(
        "SELECT scope_version, scope_id, generation, checkpoint, fence, inserted_count,
                updated_count, deleted_count, skipped_count, dropped_fields,
                checkpoint_count, content_digest
         FROM __analytics_system_source_effect_receipts
         WHERE execution_id = ?
         LIMIT 2",
    )?;
    let rows = statement.query_map([Value::Blob(execution_id.to_vec())], |row| {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i64>(8)?,
            row.get::<_, i64>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, Option<Vec<u8>>>(11)?,
        ))
    })?;
    let mut values = Vec::with_capacity(2);
    for row in rows {
        values.push(row?);
    }
    match values.as_slice() {
        [] => Ok(None),
        [_, _] => Err(AnalyticsEngineError::AmbiguousSourceEffectReceipt),
        [
            (
                version,
                scope_id,
                generation,
                checkpoint,
                fence,
                inserted,
                updated,
                deleted,
                skipped,
                dropped,
                checkpoints,
                content_digest,
            ),
        ] => {
            let version = u8::try_from(*version)
                .map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
            let generation = u64::try_from(*generation)
                .map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
            let source_scope =
                SystemStreamScope::try_new_versioned(version, scope_id.clone(), generation)
                    .map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
            let checkpoint = SystemStreamCheckpointToken::try_from(checkpoint.clone())
                .map_err(|_| AnalyticsEngineError::InvalidSourceEffectReceipt)?;
            Ok(Some(StoredReceipt {
                scope: StoredReceiptScope::System {
                    source_scope,
                    checkpoint,
                },
                fence: decode_fence(fence.as_str())?,
                content_digest: decode_content_digest(content_digest.as_deref())?,
                summary: SourceEffectSummary {
                    inserted: decode_count(*inserted)?,
                    updated: decode_count(*updated)?,
                    deleted: decode_count(*deleted)?,
                    skipped: decode_count(*skipped)?,
                    dropped_fields: decode_count(*dropped)?,
                    checkpoint_count: decode_count(*checkpoints)?,
                },
            }))
        }
        _ => unreachable!("system receipt query is bounded to two rows"),
    }
}

pub(crate) fn replace_authority(
    conn: &Connection,
    scope: &SourceEffectScope,
    authority: &SourceEffectAuthority,
) -> AnalyticsEngineResult<()> {
    if let Some((source_scope, _)) = scope.system_parts() {
        let generation = i64::try_from(source_scope.generation())
            .map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
        let removed = conn.execute(
            "DELETE FROM __analytics_system_source_effect_authority
             WHERE scope_version = ? AND scope_id = ? AND generation = ?",
            duckdb::params![
                i32::from(source_scope.version()),
                source_scope.scope_id().as_str(),
                generation,
            ],
        )?;
        if removed > 1 {
            return Err(AnalyticsEngineError::AmbiguousSourceEffectAuthority);
        }
        conn.execute(
            "INSERT INTO __analytics_system_source_effect_authority
                (authority_version, scope_version, scope_id, generation,
                 fence, execution_id, updated_at_ms)
             VALUES (1, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                i32::from(source_scope.version()),
                source_scope.scope_id().as_str(),
                generation,
                authority.fence().get().to_string(),
                Value::Blob(authority.execution_id().to_vec()),
                current_time_millis(),
            ],
        )?;
        return Ok(());
    }
    let Some((source_table_name, shard_id)) = scope.table_parts() else {
        return Err(AnalyticsEngineError::InvalidSourceEffectScope);
    };
    let removed = conn.execute(
        "DELETE FROM __analytics_source_effect_authority
         WHERE source_table_name = ? AND shard_id = ?",
        duckdb::params![source_table_name, shard_id],
    )?;
    if removed > 1 {
        return Err(AnalyticsEngineError::AmbiguousSourceEffectAuthority);
    }
    conn.execute(
        "INSERT INTO __analytics_source_effect_authority
            (authority_version, source_table_name, shard_id, fence, execution_id, updated_at_ms)
         VALUES (1, ?, ?, ?, ?, ?)",
        duckdb::params![
            source_table_name,
            shard_id,
            authority.fence().get().to_string(),
            Value::Blob(authority.execution_id().to_vec()),
            current_time_millis(),
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_receipt(
    conn: &Connection,
    scope: &SourceEffectScope,
    authority: &SourceEffectAuthority,
    summary: &SourceEffectSummary,
    content_digest: &[u8; CONTENT_DIGEST_BYTES],
) -> AnalyticsEngineResult<()> {
    if let Some((source_scope, checkpoint)) = scope.system_parts() {
        let generation = i64::try_from(source_scope.generation())
            .map_err(|_| AnalyticsEngineError::InvalidSourceEffectScope)?;
        conn.execute(
            "INSERT INTO __analytics_system_source_effect_receipts
                (receipt_version, execution_id, scope_version, scope_id, generation,
                 checkpoint, fence, inserted_count, updated_count, deleted_count, skipped_count,
                 dropped_fields, checkpoint_count, content_digest, committed_at_ms)
             VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                Value::Blob(authority.execution_id().to_vec()),
                i32::from(source_scope.version()),
                source_scope.scope_id().as_str(),
                generation,
                checkpoint.as_str(),
                authority.fence().get().to_string(),
                i64::try_from(summary.inserted).unwrap_or(i64::MAX),
                i64::try_from(summary.updated).unwrap_or(i64::MAX),
                i64::try_from(summary.deleted).unwrap_or(i64::MAX),
                i64::try_from(summary.skipped).unwrap_or(i64::MAX),
                i64::try_from(summary.dropped_fields).unwrap_or(i64::MAX),
                i64::try_from(summary.checkpoint_count).unwrap_or(i64::MAX),
                Value::Blob(content_digest.to_vec()),
                current_time_millis(),
            ],
        )?;
        return Ok(());
    }
    let Some((source_table_name, shard_id)) = scope.table_parts() else {
        return Err(AnalyticsEngineError::InvalidSourceEffectScope);
    };
    conn.execute(
        "INSERT INTO __analytics_source_effect_receipts
            (receipt_version, execution_id, source_table_name, shard_id, fence,
             inserted_count, updated_count, deleted_count, skipped_count,
             dropped_fields, checkpoint_count, content_digest, committed_at_ms)
         VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            Value::Blob(authority.execution_id().to_vec()),
            source_table_name,
            shard_id,
            authority.fence().get().to_string(),
            i64::try_from(summary.inserted).unwrap_or(i64::MAX),
            i64::try_from(summary.updated).unwrap_or(i64::MAX),
            i64::try_from(summary.deleted).unwrap_or(i64::MAX),
            i64::try_from(summary.skipped).unwrap_or(i64::MAX),
            i64::try_from(summary.dropped_fields).unwrap_or(i64::MAX),
            i64::try_from(summary.checkpoint_count).unwrap_or(i64::MAX),
            Value::Blob(content_digest.to_vec()),
            current_time_millis(),
        ],
    )?;
    Ok(())
}
