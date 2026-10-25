use duckdb::{Connection, OptionalExt, ToSql, types::Value};

use super::{contracts::*, validation::*};
use crate::{
    AnalyticsEngineResult, constants::EXECUTION_ID_BYTES, engine::AnalyticsEngineError, sql,
};

pub(crate) fn schema_statements() -> [&'static str; 3] {
    [
        "CREATE TABLE IF NOT EXISTS __analytics_retention_effect_authority (
            authority_version INTEGER NOT NULL,
            table_name VARCHAR NOT NULL,
            fence VARCHAR NOT NULL,
            execution_id BLOB NOT NULL,
            cursor VARCHAR,
            updated_at_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS __analytics_retention_effect_receipts (
            receipt_version INTEGER NOT NULL,
            execution_id BLOB NOT NULL,
            table_name VARCHAR NOT NULL,
            fence VARCHAR NOT NULL,
            cutoff_ms BIGINT NOT NULL,
            batch_size BIGINT NOT NULL,
            cursor_before VARCHAR,
            cursor_after VARCHAR,
            rows_deleted BIGINT NOT NULL,
            complete BOOLEAN NOT NULL,
            committed_at_ms BIGINT NOT NULL,
            schedule_at_ms BIGINT NOT NULL,
            server_time_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS __analytics_retention_schedule_occurrences (
            occurrence_version INTEGER NOT NULL,
            table_name VARCHAR NOT NULL,
            cutoff_ms BIGINT NOT NULL,
            schedule_at_ms BIGINT NOT NULL,
            server_time_ms BIGINT NOT NULL,
            cursor VARCHAR,
            complete BOOLEAN NOT NULL,
            updated_at_ms BIGINT NOT NULL
        )",
    ]
}

pub(crate) fn load_authority(
    conn: &Connection,
    scope: &RetentionEffectScope,
) -> AnalyticsEngineResult<Option<StoredAuthority>> {
    let mut statement = conn.prepare(
        "SELECT authority_version, fence, execution_id, cursor
         FROM __analytics_retention_effect_authority
         WHERE table_name = ?
         LIMIT 2",
    )?;
    let rows = statement.query_map([scope.table_name()], |row| {
        let version: i64 = row.get(0)?;
        let fence: String = row.get(1)?;
        let execution_id: Vec<u8> = row.get(2)?;
        let cursor: Option<String> = row.get(3)?;
        Ok((version, fence, execution_id, cursor))
    })?;
    let values = rows.collect::<Result<Vec<_>, _>>()?;
    match values.as_slice() {
        [] => Ok(None),
        [_, _] => Err(AnalyticsEngineError::AmbiguousRetentionEffectAuthority),
        [(version, fence, execution_id, cursor)] => {
            decode_version(*version)?;
            Ok(Some(StoredAuthority {
                execution_id: decode_execution_id(execution_id)?,
                fence: decode_fence(fence)?,
                cursor: decode_cursor(cursor.as_deref())?,
            }))
        }
        _ => unreachable!("authority query is bounded to two rows"),
    }
}

pub(crate) fn load_receipt(
    conn: &Connection,
    execution_id: &[u8; EXECUTION_ID_BYTES],
) -> AnalyticsEngineResult<Option<StoredReceipt>> {
    let mut statement = conn.prepare(
        "SELECT receipt_version, table_name, fence, cutoff_ms, batch_size, cursor_before,
                cursor_after, rows_deleted, complete, schedule_at_ms, server_time_ms
         FROM __analytics_retention_effect_receipts
         WHERE execution_id = ?
         LIMIT 2",
    )?;
    let rows = statement.query_map([Value::Blob(execution_id.to_vec())], |row| {
        let version: i64 = row.get(0)?;
        let fence: String = row.get(2)?;
        let cursor_before: Option<String> = row.get(5)?;
        let cursor_after: Option<String> = row.get(6)?;
        Ok((
            version,
            row.get::<_, String>(1)?,
            fence,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            cursor_before,
            cursor_after,
            row.get::<_, i64>(7)?,
            row.get::<_, bool>(8)?,
            row.get::<_, i64>(9)?,
            row.get::<_, i64>(10)?,
        ))
    })?;
    let values = rows.collect::<Result<Vec<_>, _>>()?;
    match values.as_slice() {
        [] => Ok(None),
        [_, _] => Err(AnalyticsEngineError::AmbiguousRetentionEffectReceipt),
        [
            (
                version,
                table_name,
                fence,
                cutoff_ms,
                batch_size,
                cursor_before,
                cursor_after,
                rows,
                complete,
                schedule_at_ms,
                server_time_ms,
            ),
        ] => {
            decode_version(*version)?;
            let cutoff_ms = *cutoff_ms;
            let batch_size = decode_batch_size(*batch_size)?;
            if cutoff_ms < 0 {
                return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
            }
            let schedule_at_ms = *schedule_at_ms;
            if schedule_at_ms != cutoff_ms || schedule_at_ms < 0 {
                return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
            }
            let server_time_ms = decode_server_time(*server_time_ms)?;
            if server_time_ms < u64::try_from(schedule_at_ms).unwrap_or(u64::MAX) {
                return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
            }
            let cursor = decode_cursor(cursor_after.as_deref())?;
            let rows_deleted = decode_count(*rows)?;
            if rows_deleted > batch_size {
                return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
            }
            let summary = RetentionEffectSummary::from_parts(rows_deleted, cursor, *complete);
            if summary.complete() && summary.cursor().is_some() {
                return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
            }
            if !summary.complete() && summary.cursor().is_none() {
                return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
            }
            let table_name = RetentionEffectScope::try_new(table_name.clone())?
                .table_name()
                .to_owned();
            Ok(Some(StoredReceipt {
                table_name,
                fence: decode_fence(fence)?,
                cutoff_ms,
                schedule_at_ms,
                server_time_ms,
                batch_size,
                cursor_before: decode_cursor(cursor_before.as_deref())?,
                summary,
            }))
        }
        _ => unreachable!("receipt query is bounded to two rows"),
    }
}

pub(crate) fn load_schedule_occurrence(
    conn: &Connection,
    scope: &RetentionEffectScope,
    schedule_at_ms: i64,
) -> AnalyticsEngineResult<Option<RetentionScheduleOccurrence>> {
    let mut statement = conn.prepare(
        "SELECT occurrence_version, cutoff_ms, schedule_at_ms, server_time_ms, cursor, complete
         FROM __analytics_retention_schedule_occurrences
         WHERE table_name = ? AND schedule_at_ms = ?
         LIMIT 2",
    )?;
    let rows = statement.query_map(duckdb::params![scope.table_name(), schedule_at_ms], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, bool>(5)?,
        ))
    })?;
    let values = rows.collect::<Result<Vec<_>, _>>()?;
    match values.as_slice() {
        [] => Ok(None),
        [_, _] => Err(AnalyticsEngineError::AmbiguousRetentionScheduleOccurrence),
        [(version, cutoff_ms, schedule_at_ms, server_time_ms, cursor, complete)] => {
            decode_occurrence(
                *version,
                *cutoff_ms,
                *schedule_at_ms,
                *server_time_ms,
                cursor.as_deref(),
                *complete,
            )
            .map(Some)
        }
        _ => unreachable!("schedule occurrence query is bounded to two rows"),
    }
}

pub(crate) fn authorize_schedule_occurrence(
    conn: &Connection,
    scope: &RetentionEffectScope,
    cutoff_ms: i64,
    schedule_at_ms: i64,
    server_time_ms: u64,
) -> AnalyticsEngineResult<()> {
    let server_time = encode_server_time(server_time_ms)?;
    if server_time > current_time_millis() {
        return Err(AnalyticsEngineError::InvalidRetentionEffectRequest(
            "server time cannot be in the future",
        ));
    }
    if let Some(occurrence) = load_schedule_occurrence(conn, scope, schedule_at_ms)? {
        if occurrence.cutoff_ms != cutoff_ms || occurrence.server_time_ms != server_time_ms {
            return Err(AnalyticsEngineError::ConflictingRetentionScheduleOccurrence);
        }
        return Ok(());
    }
    insert_schedule_occurrence(conn, scope, cutoff_ms, schedule_at_ms, server_time)
}

pub(crate) fn advance_schedule_occurrence(
    conn: &Connection,
    request: &RetentionEffectRequest,
    summary: &RetentionEffectSummary,
) -> AnalyticsEngineResult<()> {
    let occurrence = load_schedule_occurrence(conn, &request.scope, request.schedule_at_ms)?
        .ok_or(AnalyticsEngineError::RetentionEffectScheduleUnavailable)?;
    if occurrence.cutoff_ms != request.cutoff_ms
        || occurrence.cursor.as_deref() != request.cursor.as_deref()
    {
        return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
    }
    let changed = conn.execute(
        "UPDATE __analytics_retention_schedule_occurrences
         SET cursor = ?, complete = ?, updated_at_ms = ?
         WHERE table_name = ? AND schedule_at_ms = ? AND server_time_ms = ?",
        duckdb::params![
            summary.cursor(),
            summary.complete(),
            current_time_millis(),
            request.scope.table_name(),
            request.schedule_at_ms,
            encode_server_time(request.server_time_ms)?,
        ],
    )?;
    if changed != 1 {
        return Err(AnalyticsEngineError::IncompleteRetentionEffectState);
    }
    Ok(())
}

fn insert_schedule_occurrence(
    conn: &Connection,
    scope: &RetentionEffectScope,
    cutoff_ms: i64,
    schedule_at_ms: i64,
    server_time_ms: i64,
) -> AnalyticsEngineResult<()> {
    conn.execute(
        "INSERT INTO __analytics_retention_schedule_occurrences
            (occurrence_version, table_name, cutoff_ms, schedule_at_ms, server_time_ms,
             cursor, complete, updated_at_ms)
         VALUES (1, ?, ?, ?, ?, NULL, false, ?)",
        duckdb::params![
            scope.table_name(),
            cutoff_ms,
            schedule_at_ms,
            server_time_ms,
            current_time_millis(),
        ],
    )?;
    Ok(())
}

pub(crate) fn replace_authority(
    conn: &Connection,
    scope: &RetentionEffectScope,
    authority: &RetentionEffectAuthority,
    cursor: Option<&str>,
) -> AnalyticsEngineResult<()> {
    let removed = conn.execute(
        "DELETE FROM __analytics_retention_effect_authority WHERE table_name = ?",
        [scope.table_name()],
    )?;
    if removed > 1 {
        return Err(AnalyticsEngineError::AmbiguousRetentionEffectAuthority);
    }
    conn.execute(
        "INSERT INTO __analytics_retention_effect_authority
            (authority_version, table_name, fence, execution_id, cursor, updated_at_ms)
         VALUES (1, ?, ?, ?, ?, ?)",
        duckdb::params![
            scope.table_name(),
            authority.fence().get().to_string(),
            Value::Blob(authority.execution_id().to_vec()),
            cursor,
            current_time_millis(),
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_receipt(
    conn: &Connection,
    scope: &RetentionEffectScope,
    authority: &RetentionEffectAuthority,
    request: &RetentionEffectRequest,
    cursor_before: Option<&str>,
    summary: &RetentionEffectSummary,
) -> AnalyticsEngineResult<()> {
    conn.execute(
        "INSERT INTO __analytics_retention_effect_receipts
            (receipt_version, execution_id, table_name, fence, cutoff_ms, batch_size,
             cursor_before, cursor_after, rows_deleted, complete, committed_at_ms,
             schedule_at_ms, server_time_ms)
         VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            Value::Blob(authority.execution_id().to_vec()),
            scope.table_name(),
            authority.fence().get().to_string(),
            request.cutoff_ms,
            i64::try_from(request.batch_size).unwrap_or(i64::MAX),
            cursor_before,
            summary.cursor(),
            i64::try_from(summary.rows_deleted()).unwrap_or(i64::MAX),
            summary.complete(),
            current_time_millis(),
            request.schedule_at_ms,
            i64::try_from(request.server_time_ms).unwrap_or(i64::MAX),
        ],
    )?;
    Ok(())
}

pub(crate) fn select_expired_ids(
    conn: &Connection,
    scope: &RetentionEffectScope,
    cutoff_ms: i64,
    cursor: Option<&str>,
    limit: u64,
) -> AnalyticsEngineResult<Vec<String>> {
    let table = sql::quote_identifier(scope.table_name());
    let expiry = sql::quote_identifier(analytics_contract::INTERNAL_EXPIRY_COLUMN);
    let missing = sql::quote_identifier(analytics_contract::INTERNAL_MISSING_RETENTION_COLUMN);
    let id = sql::quote_identifier("__id");
    let sql = if cursor.is_some() {
        format!(
            "SELECT {id} FROM {table} WHERE {expiry} IS NOT NULL AND {expiry} <= ?
             AND {missing} = false AND {id} > ? ORDER BY {id} ASC LIMIT ?"
        )
    } else {
        format!(
            "SELECT {id} FROM {table} WHERE {expiry} IS NOT NULL AND {expiry} <= ?
             AND {missing} = false ORDER BY {id} ASC LIMIT ?"
        )
    };
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let mut statement = conn.prepare(sql.as_str())?;
    let mut rows = if let Some(cursor) = cursor {
        statement.query(duckdb::params![cutoff_ms, cursor, limit])?
    } else {
        statement.query(duckdb::params![cutoff_ms, limit])?
    };
    let mut ids = Vec::new();
    while let Some(row) = rows.next()? {
        ids.push(row.get(0)?);
    }
    Ok(ids)
}

pub(crate) fn delete_expired_ids(
    conn: &Connection,
    scope: &RetentionEffectScope,
    cutoff_ms: i64,
    ids: &[String],
) -> AnalyticsEngineResult<u64> {
    if ids.is_empty() {
        return Ok(0);
    }
    let table = sql::quote_identifier(scope.table_name());
    let expiry = sql::quote_identifier(analytics_contract::INTERNAL_EXPIRY_COLUMN);
    let missing = sql::quote_identifier(analytics_contract::INTERNAL_MISSING_RETENTION_COLUMN);
    let id = sql::quote_identifier("__id");
    let placeholders = std::iter::repeat_n("?", ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let statement = format!(
        "DELETE FROM {table} WHERE {expiry} IS NOT NULL AND {expiry} <= ?
         AND {missing} = false AND {id} IN ({placeholders})"
    );
    let mut values = Vec::with_capacity(ids.len() + 1);
    values.push(Value::BigInt(cutoff_ms));
    values.extend(ids.iter().cloned().map(Value::Text));
    let params = values
        .iter()
        .map(|value| value as &dyn ToSql)
        .collect::<Vec<_>>();
    Ok(conn.execute(statement.as_str(), params.as_slice())? as u64)
}

pub(crate) fn find_pending_schedule_occurrence(
    conn: &Connection,
    scope: &RetentionEffectScope,
) -> AnalyticsEngineResult<Option<RetentionScheduleOccurrence>> {
    let value = conn
        .query_row(
            "SELECT occurrence_version, cutoff_ms, schedule_at_ms, server_time_ms, cursor, \
             complete
         FROM __analytics_retention_schedule_occurrences
         WHERE table_name = ? AND complete = false ORDER BY schedule_at_ms LIMIT 1",
            [scope.table_name()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, bool>(5)?,
                ))
            },
        )
        .optional()?;
    value
        .map(|(version, cutoff, schedule, server, cursor, complete)| {
            decode_occurrence(
                version,
                cutoff,
                schedule,
                server,
                cursor.as_deref(),
                complete,
            )
        })
        .transpose()
}
