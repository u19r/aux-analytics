use std::collections::BTreeMap;

use analytics_contract::{
    INTERNAL_EXPIRY_COLUMN, INTERNAL_INGESTED_AT_COLUMN, INTERNAL_MISSING_RETENTION_COLUMN,
    TableRegistration,
};
use duckdb::{Appender, Connection, appender_params_from_iter, types::Value};

use crate::{
    AnalyticsEngineError, AnalyticsEngineResult, IngestOutcome,
    engine::{
        column_type_for_name, columns_for_registration, json_value_to_duckdb_value_for_column,
    },
    sql,
};

const ORDINAL_COLUMN: &str = "__analytics_batch_ordinal";
const UPSERT_COLUMN: &str = "__analytics_batch_upsert";

pub(crate) struct BatchSqlExecutor<'conn> {
    conn: &'conn Connection,
    tables: BTreeMap<String, PendingTable<'conn>>,
}

#[derive(Clone, Copy)]
pub(crate) enum BatchUpsertOutcome {
    Inserted,
    Updated,
}

impl<'conn> BatchSqlExecutor<'conn> {
    pub(crate) fn new(conn: &'conn Connection) -> Self {
        Self {
            conn,
            tables: BTreeMap::new(),
        }
    }

    pub(crate) fn apply_upsert(
        &mut self,
        table: &TableRegistration,
        row: BTreeMap<String, serde_json::Value>,
        outcome: BatchUpsertOutcome,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.stage(table, row, true)?;
        Ok(match outcome {
            BatchUpsertOutcome::Inserted => IngestOutcome::Inserted,
            BatchUpsertOutcome::Updated => IngestOutcome::Updated,
        })
    }

    pub(crate) fn apply_delete(
        &mut self,
        table: &TableRegistration,
        row: BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.stage(table, row, false)?;
        Ok(IngestOutcome::Deleted)
    }

    pub(crate) fn flush(&mut self) -> AnalyticsEngineResult<()> {
        for pending in std::mem::take(&mut self.tables).into_values() {
            pending.flush(self.conn)?;
        }
        Ok(())
    }

    fn stage(
        &mut self,
        table: &TableRegistration,
        row: BTreeMap<String, serde_json::Value>,
        is_upsert: bool,
    ) -> AnalyticsEngineResult<()> {
        validate_identity(&row)?;
        if !self.tables.contains_key(&table.analytics_table_name) {
            let index = self.tables.len();
            let pending = PendingTable::new(self.conn, index, table.clone())?;
            self.tables
                .insert(table.analytics_table_name.clone(), pending);
        }
        self.tables
            .get_mut(&table.analytics_table_name)
            .expect("pending table was inserted")
            .append(row, is_upsert)
    }
}

struct PendingTable<'conn> {
    registration: TableRegistration,
    stage_name: String,
    columns: Vec<String>,
    appender: Option<Appender<'conn>>,
    next_ordinal: i64,
}

impl<'conn> PendingTable<'conn> {
    fn new(
        conn: &'conn Connection,
        index: usize,
        registration: TableRegistration,
    ) -> AnalyticsEngineResult<Self> {
        let stage_name = format!("__analytics_ingest_stage_{index}");
        let columns = batch_columns(&registration);
        create_stage(conn, &stage_name, &registration, &columns)?;
        let mut appender_columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
        appender_columns.extend([ORDINAL_COLUMN, UPSERT_COLUMN]);
        let appender = conn.appender_with_columns(&stage_name, &appender_columns)?;
        Ok(Self {
            registration,
            stage_name,
            columns,
            appender: Some(appender),
            next_ordinal: 0,
        })
    }

    fn append(
        &mut self,
        row: BTreeMap<String, serde_json::Value>,
        is_upsert: bool,
    ) -> AnalyticsEngineResult<()> {
        let mut values = self
            .columns
            .iter()
            .map(|column| {
                json_value_to_duckdb_value_for_column(
                    row.get(column).unwrap_or(&serde_json::Value::Null),
                    column_type_for_name(&self.registration, column),
                )
            })
            .collect::<Vec<_>>();
        values.push(Value::BigInt(self.next_ordinal));
        values.push(Value::Boolean(is_upsert));
        self.next_ordinal = self.next_ordinal.saturating_add(1);
        self.appender
            .as_mut()
            .expect("pending table retains its appender until flush")
            .append_row(appender_params_from_iter(values))?;
        Ok(())
    }

    fn flush(mut self, conn: &Connection) -> AnalyticsEngineResult<()> {
        if let Some(mut appender) = self.appender.take() {
            appender.flush()?;
        }
        replace_rows(conn, &self.stage_name, &self.registration, &self.columns)?;
        conn.execute_batch(
            format!("DROP TABLE {}", sql::quote_identifier(&self.stage_name)).as_str(),
        )?;
        Ok(())
    }
}

fn validate_identity(row: &BTreeMap<String, serde_json::Value>) -> AnalyticsEngineResult<()> {
    for column in ["table_name", "tenant_id", "__id"] {
        if !row.contains_key(column) {
            return Err(AnalyticsEngineError::MissingIdentifier(column));
        }
    }
    Ok(())
}

fn batch_columns(table: &TableRegistration) -> Vec<String> {
    let mut columns = vec![
        "table_name".to_string(),
        "tenant_id".to_string(),
        "__id".to_string(),
    ];
    for column in columns_for_registration(table) {
        if !columns.contains(&column.column_name) {
            columns.push(column.column_name);
        }
    }
    if table.retention.is_some() {
        columns.extend(
            [
                INTERNAL_INGESTED_AT_COLUMN,
                INTERNAL_EXPIRY_COLUMN,
                INTERNAL_MISSING_RETENTION_COLUMN,
            ]
            .into_iter()
            .map(str::to_string),
        );
    }
    columns
}

fn create_stage(
    conn: &Connection,
    stage_name: &str,
    table: &TableRegistration,
    columns: &[String],
) -> AnalyticsEngineResult<()> {
    let selected = quoted_columns(columns);
    let statement = format!(
        "CREATE TEMP TABLE {stage} AS
         SELECT {selected}, 0::BIGINT AS {ordinal}, false::BOOLEAN AS {upsert}
         FROM {target} LIMIT 0",
        stage = sql::quote_identifier(stage_name),
        ordinal = sql::quote_identifier(ORDINAL_COLUMN),
        upsert = sql::quote_identifier(UPSERT_COLUMN),
        target = sql::quote_identifier(&table.analytics_table_name),
    );
    conn.execute_batch(&statement)?;
    Ok(())
}

fn replace_rows(
    conn: &Connection,
    stage_name: &str,
    table: &TableRegistration,
    columns: &[String],
) -> AnalyticsEngineResult<()> {
    let target = sql::quote_identifier(&table.analytics_table_name);
    let stage = sql::quote_identifier(stage_name);
    conn.execute_batch(
        format!(
            "DELETE FROM {target} AS target USING {stage} AS staged
             WHERE target.table_name = staged.table_name
               AND target.tenant_id = staged.tenant_id
               AND target.__id = staged.__id"
        )
        .as_str(),
    )?;
    let selected = quoted_columns(columns);
    conn.execute_batch(
        format!(
            "INSERT INTO {target} ({selected})
             SELECT {selected} FROM (
               SELECT *, row_number() OVER (
                 PARTITION BY table_name, tenant_id, __id
                 ORDER BY {ordinal} DESC
               ) AS __analytics_batch_rank
               FROM {stage}
             ) AS staged
             WHERE __analytics_batch_rank = 1 AND {upsert}",
            ordinal = sql::quote_identifier(ORDINAL_COLUMN),
            upsert = sql::quote_identifier(UPSERT_COLUMN),
        )
        .as_str(),
    )?;
    Ok(())
}

fn quoted_columns(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| sql::quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ")
}
