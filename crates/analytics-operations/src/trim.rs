use analytics_contract::INTERNAL_EXPIRY_COLUMN;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{OperationId, OperationTypeError};

#[derive(Debug, Error)]
pub enum TrimError {
    #[error(transparent)]
    OperationType(#[from] OperationTypeError),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error("trim table name cannot be empty")]
    EmptyTableName,
    #[error("trim table name is not a safe DuckDB identifier: {0}")]
    InvalidTableIdentifier(String),
    #[error("trim row key cannot be empty")]
    EmptyRowKey,
    #[error("trim row predicate cannot be empty")]
    EmptyPredicate,
    #[error("trim row predicate is not safe for bounded row deletion")]
    UnsafePredicate,
    #[error("trim horizon must be greater than zero")]
    EmptyHorizon,
    #[error("trim request requires at least one selected row key")]
    EmptyRowSelection,
    #[error("destructive trim execution requires a confirmation token")]
    MissingConfirmationToken,
    #[error("trim execution currently supports dry-run only")]
    ApplyNotSupported,
    #[error("trim target is not supported by this destination")]
    UnsupportedTarget,
    #[error(
        "trim destination deleted {deleted_rows} rows, exceeding dry-run estimate {candidate_rows}"
    )]
    DeleteCountExceededDryRun {
        candidate_rows: u64,
        deleted_rows: u64,
    },
}

pub type TrimResult<T> = Result<T, TrimError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrimRequest {
    pub operation_id: OperationId,
    pub table_name: String,
    pub dry_run: bool,
    pub target: TrimTarget,
    pub confirmation_token: Option<String>,
}

impl TrimRequest {
    pub fn validate(&self) -> TrimResult<()> {
        if self.table_name.trim().is_empty() {
            return Err(TrimError::EmptyTableName);
        }
        self.target.validate()?;
        if !self.dry_run && self.confirmation_token.is_none() {
            return Err(TrimError::MissingConfirmationToken);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TrimTarget {
    RowKeys { keys: Vec<Vec<u8>> },
    RowPredicate { predicate_sql: String },
    FullTableDrop,
    HorizonTrim { older_than_ms: u64 },
    DuckLakeSnapshotExpiry { older_than_ms: u64 },
    RawBackupExpiration { older_than_ms: u64 },
}

impl TrimTarget {
    fn validate(&self) -> TrimResult<()> {
        match self {
            Self::RowKeys { keys } if keys.is_empty() => Err(TrimError::EmptyRowSelection),
            Self::RowKeys { keys } if keys.iter().any(Vec::is_empty) => Err(TrimError::EmptyRowKey),
            Self::RowPredicate { predicate_sql } => validate_row_predicate(predicate_sql),
            Self::HorizonTrim { older_than_ms }
            | Self::DuckLakeSnapshotExpiry { older_than_ms }
            | Self::RawBackupExpiration { older_than_ms }
                if *older_than_ms == 0 =>
            {
                Err(TrimError::EmptyHorizon)
            }
            Self::RowKeys { .. }
            | Self::FullTableDrop
            | Self::HorizonTrim { .. }
            | Self::DuckLakeSnapshotExpiry { .. }
            | Self::RawBackupExpiration { .. } => Ok(()),
        }
    }
}

fn validate_row_predicate(predicate_sql: &str) -> TrimResult<()> {
    let predicate = predicate_sql.trim();
    if predicate.is_empty() {
        return Err(TrimError::EmptyPredicate);
    }
    let lower = predicate.to_ascii_lowercase();
    if lower == "true" || lower == "1=1" || lower == "1 = 1" {
        return Err(TrimError::UnsafePredicate);
    }
    if predicate.contains(';') || predicate.contains("--") || predicate.contains("/*") {
        return Err(TrimError::UnsafePredicate);
    }
    let padded = format!(" {lower} ");
    for keyword in [
        " alter ",
        " create ",
        " delete ",
        " drop ",
        " insert ",
        " select ",
        " truncate ",
        " update ",
    ] {
        if padded.contains(keyword) {
            return Err(TrimError::UnsafePredicate);
        }
    }
    if !predicate.contains('=') && !predicate.contains('<') && !predicate.contains('>') {
        return Err(TrimError::UnsafePredicate);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrimReport {
    pub operation_id: OperationId,
    pub table_name: String,
    pub dry_run: bool,
    pub target: TrimTarget,
    pub candidate_rows: u64,
    pub rows_deleted: u64,
    pub destination_mutated: bool,
}

impl TrimReport {
    #[must_use]
    pub fn operation_counts(&self) -> serde_json::Value {
        serde_json::json!({
            "table_name": self.table_name,
            "dry_run": self.dry_run,
            "candidate_rows": self.candidate_rows,
            "rows_deleted": self.rows_deleted,
            "destination_mutated": self.destination_mutated,
        })
    }
}

pub trait TrimCandidateSource {
    fn count_candidates(&self, table_name: &str, target: &TrimTarget) -> TrimResult<u64>;
}

pub trait TrimDestination {
    fn delete_candidates(
        &mut self,
        table_name: &str,
        target: &TrimTarget,
        max_rows: u64,
    ) -> TrimResult<u64>;
}

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
    fn count_candidates(&self, table_name: &str, target: &TrimTarget) -> TrimResult<u64> {
        validate_identifier(table_name)?;
        let connection = self.connect()?;
        match target {
            TrimTarget::RowKeys { keys } => count_row_key_candidates(&connection, table_name, keys),
            TrimTarget::FullTableDrop => count_all_rows(&connection, table_name),
            TrimTarget::HorizonTrim { older_than_ms } => {
                count_horizon_candidates(&connection, table_name, *older_than_ms)
            }
            TrimTarget::RowPredicate { predicate_sql } => {
                count_predicate_candidates(&connection, table_name, predicate_sql)
            }
            TrimTarget::DuckLakeSnapshotExpiry { .. } | TrimTarget::RawBackupExpiration { .. } => {
                Err(TrimError::UnsupportedTarget)
            }
        }
    }
}

impl TrimDestination for DuckDbTrimRows {
    fn delete_candidates(
        &mut self,
        table_name: &str,
        target: &TrimTarget,
        max_rows: u64,
    ) -> TrimResult<u64> {
        validate_identifier(table_name)?;
        let connection = self.connect()?;
        match target {
            TrimTarget::RowKeys { keys } => delete_row_key_candidates(
                &connection,
                table_name,
                keys,
                i64::try_from(max_rows).unwrap_or(i64::MAX),
            ),
            TrimTarget::FullTableDrop => drop_full_table(&connection, table_name, max_rows),
            TrimTarget::HorizonTrim { older_than_ms } => {
                delete_horizon_candidates(&connection, table_name, *older_than_ms, max_rows)
            }
            TrimTarget::RowPredicate { predicate_sql } => {
                delete_predicate_candidates(&connection, table_name, predicate_sql, max_rows)
            }
            TrimTarget::DuckLakeSnapshotExpiry { .. } | TrimTarget::RawBackupExpiration { .. } => {
                Err(TrimError::UnsupportedTarget)
            }
        }
    }
}

fn count_row_key_candidates(
    connection: &duckdb::Connection,
    table_name: &str,
    keys: &[Vec<u8>],
) -> TrimResult<u64> {
    if keys.is_empty() {
        return Ok(0);
    }
    let row_ids = row_ids_for_keys(keys);
    let sql = format!(
        "SELECT count(*) FROM {} WHERE __id IN ({})",
        quote_identifier(table_name),
        placeholders(row_ids.len())
    );
    let count: i64 = connection
        .prepare(sql.as_str())?
        .query_row(duckdb::params_from_iter(row_ids.iter()), |row| row.get(0))?;
    Ok(u64::try_from(count).unwrap_or(0))
}

fn count_all_rows(connection: &duckdb::Connection, table_name: &str) -> TrimResult<u64> {
    let sql = format!("SELECT count(*) FROM {}", quote_identifier(table_name));
    let count: i64 = connection
        .prepare(sql.as_str())?
        .query_row([], |row| row.get(0))?;
    Ok(u64::try_from(count).unwrap_or(0))
}

fn count_horizon_candidates(
    connection: &duckdb::Connection,
    table_name: &str,
    older_than_ms: u64,
) -> TrimResult<u64> {
    let sql = format!(
        "SELECT count(*) FROM {} WHERE {} IS NOT NULL AND {} <= ?",
        quote_identifier(table_name),
        quote_identifier(INTERNAL_EXPIRY_COLUMN),
        quote_identifier(INTERNAL_EXPIRY_COLUMN)
    );
    let count: i64 = connection.prepare(sql.as_str())?.query_row(
        duckdb::params![i64::try_from(older_than_ms).unwrap_or(i64::MAX)],
        |row| row.get(0),
    )?;
    Ok(u64::try_from(count).unwrap_or(0))
}

fn count_predicate_candidates(
    connection: &duckdb::Connection,
    table_name: &str,
    predicate_sql: &str,
) -> TrimResult<u64> {
    validate_row_predicate(predicate_sql)?;
    let sql = format!(
        "SELECT count(*) FROM {} WHERE {}",
        quote_identifier(table_name),
        predicate_sql
    );
    let count: i64 = connection
        .prepare(sql.as_str())?
        .query_row([], |row| row.get(0))?;
    Ok(u64::try_from(count).unwrap_or(0))
}

fn delete_row_key_candidates(
    connection: &duckdb::Connection,
    table_name: &str,
    keys: &[Vec<u8>],
    max_rows: i64,
) -> TrimResult<u64> {
    if keys.is_empty() || max_rows == 0 {
        return Ok(0);
    }
    let row_ids = row_ids_for_keys(keys);
    let sql = format!(
        "DELETE FROM {} WHERE rowid IN (
            SELECT rowid FROM {}
            WHERE __id IN ({})
            LIMIT {max_rows}
        )",
        quote_identifier(table_name),
        quote_identifier(table_name),
        placeholders(row_ids.len())
    );
    let changed = connection
        .prepare(sql.as_str())?
        .execute(duckdb::params_from_iter(row_ids.iter()))?;
    Ok(u64::try_from(changed).unwrap_or(u64::MAX))
}

fn drop_full_table(
    connection: &duckdb::Connection,
    table_name: &str,
    max_rows: u64,
) -> TrimResult<u64> {
    let current_rows = count_all_rows(connection, table_name)?;
    if current_rows > max_rows {
        return Err(TrimError::DeleteCountExceededDryRun {
            candidate_rows: max_rows,
            deleted_rows: current_rows,
        });
    }
    connection.execute(
        format!("DROP TABLE {}", quote_identifier(table_name)).as_str(),
        [],
    )?;
    Ok(current_rows)
}

fn delete_horizon_candidates(
    connection: &duckdb::Connection,
    table_name: &str,
    older_than_ms: u64,
    max_rows: u64,
) -> TrimResult<u64> {
    let current_rows = count_horizon_candidates(connection, table_name, older_than_ms)?;
    if current_rows > max_rows {
        return Err(TrimError::DeleteCountExceededDryRun {
            candidate_rows: max_rows,
            deleted_rows: current_rows,
        });
    }
    let max_rows_limit = i64::try_from(max_rows).unwrap_or(i64::MAX);
    let sql = format!(
        "DELETE FROM {} WHERE rowid IN (
            SELECT rowid FROM {}
            WHERE {} IS NOT NULL AND {} <= ?
            LIMIT {max_rows_limit}
        )",
        quote_identifier(table_name),
        quote_identifier(table_name),
        quote_identifier(INTERNAL_EXPIRY_COLUMN),
        quote_identifier(INTERNAL_EXPIRY_COLUMN)
    );
    let changed = connection.prepare(sql.as_str())?.execute(duckdb::params![
        i64::try_from(older_than_ms).unwrap_or(i64::MAX)
    ])?;
    Ok(u64::try_from(changed).unwrap_or(u64::MAX))
}

fn delete_predicate_candidates(
    connection: &duckdb::Connection,
    table_name: &str,
    predicate_sql: &str,
    max_rows: u64,
) -> TrimResult<u64> {
    let current_rows = count_predicate_candidates(connection, table_name, predicate_sql)?;
    if current_rows > max_rows {
        return Err(TrimError::DeleteCountExceededDryRun {
            candidate_rows: max_rows,
            deleted_rows: current_rows,
        });
    }
    let max_rows_limit = i64::try_from(max_rows).unwrap_or(i64::MAX);
    let sql = format!(
        "DELETE FROM {} WHERE rowid IN (
            SELECT rowid FROM {}
            WHERE {}
            LIMIT {max_rows_limit}
        )",
        quote_identifier(table_name),
        quote_identifier(table_name),
        predicate_sql
    );
    let changed = connection.prepare(sql.as_str())?.execute([])?;
    Ok(u64::try_from(changed).unwrap_or(u64::MAX))
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TrimExecutor;

impl TrimExecutor {
    pub fn execute_dry_run(
        &self,
        request: &TrimRequest,
        source: &impl TrimCandidateSource,
    ) -> TrimResult<TrimReport> {
        request.validate()?;
        if !request.dry_run {
            return Err(TrimError::ApplyNotSupported);
        }
        let candidate_rows =
            source.count_candidates(request.table_name.as_str(), &request.target)?;
        Ok(TrimReport {
            operation_id: request.operation_id.clone(),
            table_name: request.table_name.clone(),
            dry_run: true,
            target: request.target.clone(),
            candidate_rows,
            rows_deleted: 0,
            destination_mutated: false,
        })
    }

    pub fn execute_apply(
        &self,
        request: &TrimRequest,
        source: &impl TrimCandidateSource,
        destination: &mut impl TrimDestination,
    ) -> TrimResult<TrimReport> {
        request.validate()?;
        if request.dry_run {
            return self.execute_dry_run(request, source);
        }
        let candidate_rows =
            source.count_candidates(request.table_name.as_str(), &request.target)?;
        let rows_deleted = destination.delete_candidates(
            request.table_name.as_str(),
            &request.target,
            candidate_rows,
        )?;
        if rows_deleted > candidate_rows {
            return Err(TrimError::DeleteCountExceededDryRun {
                candidate_rows,
                deleted_rows: rows_deleted,
            });
        }
        Ok(TrimReport {
            operation_id: request.operation_id.clone(),
            table_name: request.table_name.clone(),
            dry_run: false,
            target: request.target.clone(),
            candidate_rows,
            rows_deleted,
            destination_mutated: rows_deleted > 0,
        })
    }
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

fn quote_identifier(identifier: &str) -> String {
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
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}
