use super::*;
use crate::constants::HEX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbTrimRows {
    database_path: String,
}

impl DuckDbTrimRows {
    pub fn new(database_path: impl Into<String>) -> Self {
        Self {
            database_path: database_path.into(),
        }
    }

    fn connect(&self) -> TrimResult<duckdb::Connection> {
        Ok(duckdb::Connection::open(self.database_path.as_str())?)
    }
}

impl TrimCandidateSource for DuckDbTrimRows {
    fn candidate_snapshot(
        &self,
        table_name: &str,
        target: &TrimTarget,
    ) -> TrimResult<TrimCandidateSnapshot> {
        validate_identifier(table_name)?;
        let connection = self.connect()?;
        let candidate_keys = candidate_keys(&connection, table_name, target)?;
        Ok(TrimCandidateSnapshot {
            revision: candidate_revision(candidate_keys.as_slice()),
            candidate_keys,
        })
    }
}

impl TrimDestination for DuckDbTrimRows {
    fn delete_candidate_snapshot_with_key(
        &mut self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        target: &TrimTarget,
        expected: &TrimCandidateSnapshot,
    ) -> TrimResult<TrimDeletionOutcome> {
        validate_identifier(table_name)?;
        let connection = self.connect()?;
        connection.execute_batch("BEGIN TRANSACTION")?;
        let result = (|| {
            Self::ensure_tables_on(&connection)?;
            advance_trim_authority(&connection, authority)?;
            if let Some(receipt) = expected.find_receipt_on(&connection, key)? {
                return Ok(receipt);
            }
            expected.delete_checked_candidates_on(&connection, table_name, target, key)
        })();
        match result {
            Ok(deleted) => {
                connection.execute_batch("COMMIT")?;
                Ok(deleted)
            }
            Err(error) => {
                let _ = connection.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
}

fn persist_trim_effect(
    connection: &duckdb::Connection,
    key: &OperationChunkKey,
    expected: &TrimCandidateSnapshot,
    deleted: u64,
) -> TrimResult<TrimDeletionOutcome> {
    let expected_rows = expected.candidate_rows();
    connection.execute(
        "INSERT INTO __analytics_trim_effects_v2
            (effect_key, candidate_revision, candidate_keys_json, candidate_rows, rows_deleted)
         VALUES (?, ?, ?, ?, ?)",
        duckdb::params![
            key.as_str(),
            expected.revision.as_str(),
            serde_json::to_string(&expected.candidate_keys)
                .map_err(|_| TrimError::StaleCandidateSnapshot)?,
            i64::try_from(expected_rows).unwrap_or(i64::MAX),
            i64::try_from(deleted).unwrap_or(i64::MAX),
        ],
    )?;
    Ok(TrimDeletionOutcome {
        candidate_rows: expected_rows,
        rows_deleted: deleted,
    })
}

fn advance_trim_authority(
    connection: &duckdb::Connection,
    authority: &MaintenanceExecutionAuthority,
) -> TrimResult<()> {
    let mut statement = connection.prepare(
        "SELECT fence FROM __analytics_trim_authority_v2
         WHERE execution_id = ?
         LIMIT 2",
    )?;
    let fences = statement
        .query_map(duckdb::params![authority.execution_id().as_str()], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if fences.len() > 1 {
        return Err(TrimError::AmbiguousDestinationAuthority);
    }
    let current = fences
        .first()
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| TrimError::AmbiguousDestinationAuthority)?;
    if current.is_some_and(|fence| fence > authority.fence().get()) {
        return Err(TrimError::StaleDestinationAuthority);
    }
    if current.is_none_or(|fence| fence < authority.fence().get()) {
        connection.execute(
            "DELETE FROM __analytics_trim_authority_v2 WHERE execution_id = ?",
            duckdb::params![authority.execution_id().as_str()],
        )?;
        connection.execute(
            "INSERT INTO __analytics_trim_authority_v2
                (execution_id, fence, updated_at_ms)
             VALUES (?, ?, ?)",
            duckdb::params![
                authority.execution_id().as_str(),
                authority.fence().get().to_string(),
                current_time_millis(),
            ],
        )?;
    }
    Ok(())
}

fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn candidate_keys(
    connection: &duckdb::Connection,
    table_name: &str,
    target: &TrimTarget,
) -> TrimResult<Vec<String>> {
    let (predicate, values) = match target {
        TrimTarget::RowKeys { keys } => {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            (
                format!("__id IN ({})", placeholders(keys.len())),
                row_ids_for_keys(keys)
                    .into_iter()
                    .map(duckdb::types::Value::Text)
                    .collect::<Vec<_>>(),
            )
        }
        TrimTarget::FullTableDrop => ("TRUE".to_string(), Vec::new()),
        TrimTarget::HorizonTrim { older_than_ms } => (
            format!(
                "{} IS NOT NULL AND {} <= ?",
                quote_identifier(INTERNAL_EXPIRY_COLUMN),
                quote_identifier(INTERNAL_EXPIRY_COLUMN)
            ),
            vec![duckdb::types::Value::BigInt(
                i64::try_from(*older_than_ms).unwrap_or(i64::MAX),
            )],
        ),
        TrimTarget::RowPredicate { predicate } => {
            let mut values = Vec::new();
            let sql = predicate.compile(&mut values)?;
            (sql, values)
        }
        TrimTarget::DuckLakeSnapshotExpiry { .. } | TrimTarget::RawBackupExpiration { .. } => {
            return Err(TrimError::UnsupportedTarget);
        }
    };
    let sql = format!(
        "SELECT __id FROM {} WHERE {} ORDER BY __id ASC",
        quote_identifier(table_name),
        predicate
    );
    let rows = connection
        .prepare(sql.as_str())?
        .query_map(duckdb::params_from_iter(values), |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn candidate_revision(candidate_keys: &[String]) -> String {
    let mut digest = Sha256::new();
    for key in candidate_keys {
        digest.update(key.as_bytes());
        digest.update([0]);
    }
    let digest = digest.finalize();
    let mut output = String::with_capacity(7 + digest.len() * 2);
    output.push_str("sha256:");
    for byte in digest {
        output.push_str(format!("{byte:02x}").as_str());
    }
    output
}

fn delete_candidate_keys(
    connection: &duckdb::Connection,
    table_name: &str,
    candidate_keys: &[String],
) -> TrimResult<u64> {
    if candidate_keys.is_empty() {
        return Ok(0);
    }
    let sql = format!(
        "DELETE FROM {} WHERE __id IN ({})",
        quote_identifier(table_name),
        placeholders(candidate_keys.len())
    );
    let changed = connection
        .prepare(sql.as_str())?
        .execute(duckdb::params_from_iter(candidate_keys.iter()))?;
    Ok(u64::try_from(changed).unwrap_or(u64::MAX))
}

fn validate_identifier(identifier: &str) -> TrimResult<()> {
    if !identifier.is_empty()
        && identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Ok(());
    }
    Err(TrimError::InvalidTableIdentifier(identifier.to_string()))
}

pub(crate) fn preflight_rate_limit<Clock: RateLimitClock>(
    request: &TrimRequest,
    limiter: &OperationRateLimiter,
    clock: &Clock,
) -> TrimResult<()> {
    enforce_rate_limit_with_limiter(
        limiter,
        RateLimitUsage {
            rows: u64::from(!request.dry_run),
            source_requests: 1,
            destination_writes: u64::from(!request.dry_run),
            ..RateLimitUsage::default()
        },
        clock,
    )?;
    Ok(())
}

pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn row_ids_for_keys(keys: &[Vec<u8>]) -> Vec<String> {
    keys.iter().map(|key| row_id_from_key_bytes(key)).collect()
}

fn row_id_from_key_bytes(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

impl DuckDbTrimRows {
    fn ensure_tables_on(connection: &duckdb::Connection) -> TrimResult<()> {
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS __analytics_trim_effects_v2 (
                effect_key VARCHAR NOT NULL,
                candidate_revision VARCHAR NOT NULL,
                candidate_keys_json JSON NOT NULL,
                candidate_rows BIGINT NOT NULL,
                rows_deleted BIGINT NOT NULL
            )",
        )?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS __analytics_trim_authority_v2 (
                execution_id VARCHAR NOT NULL,
                fence VARCHAR NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
        )?;
        Ok(())
    }
}

impl TrimCandidateSnapshot {
    fn find_receipt_on(
        &self,
        connection: &duckdb::Connection,
        key: &OperationChunkKey,
    ) -> TrimResult<Option<TrimDeletionOutcome>> {
        let existing = connection
            .prepare(
                "SELECT candidate_revision, candidate_keys_json, candidate_rows,
                    rows_deleted
                 FROM __analytics_trim_effects_v2
                 WHERE effect_key = ?
                 LIMIT 2",
            )?
            .query_map(duckdb::params![key.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if existing.len() > 1 {
            return Err(TrimError::ConflictingDestinationReceipt);
        }
        if let Some((revision, keys_json, candidate_rows, rows_deleted)) =
            existing.into_iter().next()
        {
            let keys: Vec<String> = serde_json::from_str(keys_json.as_str())
                .map_err(|_| TrimError::ConflictingDestinationReceipt)?;
            if revision != self.revision || keys != self.candidate_keys {
                return Err(TrimError::ConflictingDestinationReceipt);
            }
            return Ok(Some(TrimDeletionOutcome {
                candidate_rows: u64::try_from(candidate_rows).unwrap_or(0),
                rows_deleted: u64::try_from(rows_deleted).unwrap_or(0),
            }));
        }
        Ok(None)
    }

    fn delete_checked_candidates_on(
        &self,
        connection: &duckdb::Connection,
        table_name: &str,
        target: &TrimTarget,
        key: &OperationChunkKey,
    ) -> TrimResult<TrimDeletionOutcome> {
        let actual_keys = candidate_keys(connection, table_name, target)?;
        if candidate_revision(actual_keys.as_slice()) != self.revision
            || actual_keys != self.candidate_keys
        {
            return Err(TrimError::StaleCandidateSnapshot);
        }
        let expected_rows = self.candidate_rows();
        if expected_rows == 0 {
            // An empty full-table candidate set is a successful no-op.
            // Dropping an empty table would make the report claim
            // `destination_mutated = false` while changing the destination
            // schema.
            return persist_trim_effect(connection, key, self, 0);
        }
        let deleted = self.delete_candidate_rows_on(connection, table_name, target)?;
        if deleted != expected_rows {
            return Err(TrimError::DeleteCountExceededDryRun {
                candidate_rows: expected_rows,
                deleted_rows: 0,
            });
        }
        persist_trim_effect(connection, key, self, deleted)
    }
    fn delete_candidate_rows_on(
        &self,
        connection: &duckdb::Connection,
        table_name: &str,
        target: &TrimTarget,
    ) -> TrimResult<u64> {
        Ok(match target {
            TrimTarget::FullTableDrop => {
                connection.execute(
                    format!("DROP TABLE {}", quote_identifier(table_name)).as_str(),
                    [],
                )?;
                self.candidate_rows()
            }
            TrimTarget::RowKeys { .. }
            | TrimTarget::HorizonTrim { .. }
            | TrimTarget::RowPredicate { .. } => {
                delete_candidate_keys(connection, table_name, self.candidate_keys.as_slice())?
            }
            TrimTarget::DuckLakeSnapshotExpiry { .. } | TrimTarget::RawBackupExpiration { .. } => {
                return Err(TrimError::UnsupportedTarget);
            }
        })
    }
}
