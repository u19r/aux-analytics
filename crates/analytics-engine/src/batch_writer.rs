use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use analytics_contract::{
    INTERNAL_EXPIRY_COLUMN, INTERNAL_INGESTED_AT_COLUMN, INTERNAL_MISSING_RETENTION_COLUMN,
    SourcePosition, TableRegistration,
};
use duckdb::{Appender, Connection, appender_params_from_iter, types::Value};

use crate::{
    AnalyticsEngineResult, IngestOutcome,
    constants::{
        BATCH_STAGE_METADATA_COLUMNS, ORDINAL_COLUMN, SLOW_DUCKLAKE_PHASE_THRESHOLD, UPSERT_COLUMN,
    },
    engine::columns_for_registration,
    row::batch::{BatchRow, BatchRowIdentity},
    sql,
};

pub(crate) struct BatchSqlExecutor<'conn> {
    conn: &'conn Connection,
    tables: BTreeMap<String, PendingTable<'conn>>,
    next_stage_index: usize,
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
            next_stage_index: 0,
        }
    }

    pub(crate) fn apply_upsert(
        &mut self,
        table: &TableRegistration,
        row: BatchRow,
        outcome: BatchUpsertOutcome,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.stage(table, row, true, source_position)?;
        Ok(match outcome {
            BatchUpsertOutcome::Inserted => IngestOutcome::Inserted,
            BatchUpsertOutcome::Updated => IngestOutcome::Updated,
        })
    }

    pub(crate) fn apply_delete(
        &mut self,
        table: &TableRegistration,
        row: BatchRow,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        self.stage(table, row, false, source_position)?;
        Ok(IngestOutcome::Deleted)
    }

    pub(crate) fn apply_delete_identity(
        &mut self,
        table: &TableRegistration,
        identity: BatchRowIdentity<'_>,
    ) -> AnalyticsEngineResult<()> {
        self.stage(
            table,
            BatchRow::deletion_identity(table, identity),
            false,
            None,
        )
    }

    pub(crate) fn flush(&mut self) -> AnalyticsEngineResult<()> {
        for (table_name, pending) in std::mem::take(&mut self.tables) {
            pending.flush(self.conn, &table_name)?;
        }
        Ok(())
    }

    fn stage(
        &mut self,
        table: &TableRegistration,
        row: BatchRow,
        is_upsert: bool,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<()> {
        let has_retention_columns = row.has_retention_columns();
        if let Some(pending) = self.tables.get_mut(table.analytics_table_name.as_str())
            && (!has_retention_columns || pending.has_retention_columns())
        {
            return pending.append(row, is_upsert, source_position);
        }
        if let Some(pending) = self.tables.remove(table.analytics_table_name.as_str()) {
            pending.flush(self.conn, &table.analytics_table_name)?;
        }
        let pending = PendingTable::try_new(
            self.conn,
            self.next_stage_index,
            table,
            has_retention_columns,
        )?;
        self.next_stage_index += 1;
        self.tables
            .entry(table.analytics_table_name.clone())
            .or_insert(pending)
            .append(row, is_upsert, source_position)
    }
}

struct PendingTable<'conn> {
    stage_name: String,
    columns: Vec<BatchColumn>,
    appender: Option<Appender<'conn>>,
    next_ordinal: i64,
    has_deletes: bool,
}

impl<'conn> PendingTable<'conn> {
    fn try_new(
        conn: &'conn Connection,
        index: usize,
        registration: &TableRegistration,
        has_retention_columns: bool,
    ) -> AnalyticsEngineResult<Self> {
        let stage_name = format!("__analytics_ingest_stage_{index}");
        let columns = batch_layout(registration, has_retention_columns);
        let started = Instant::now();
        create_stage(conn, &stage_name, &columns)?;
        record_slow_table_phase(
            &registration.analytics_table_name,
            "create_stage",
            0,
            started.elapsed(),
        );
        let mut appender_columns = Vec::with_capacity(columns.len() + BATCH_STAGE_METADATA_COLUMNS);
        appender_columns.extend(columns.iter().map(|column| column.name.as_str()));
        appender_columns.extend([ORDINAL_COLUMN, UPSERT_COLUMN]);
        let appender = conn.appender_with_columns(&stage_name, &appender_columns)?;
        Ok(Self {
            stage_name,
            columns,
            appender: Some(appender),
            next_ordinal: 0,
            has_deletes: false,
        })
    }

    fn append(
        &mut self,
        row: BatchRow,
        is_upsert: bool,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<()> {
        let needs_retention_padding = self.has_retention_columns() && !row.has_retention_columns();
        let mut values = row.into_values(source_position)?;
        if needs_retention_padding {
            values.extend([Value::Null, Value::Null, Value::Null]);
        }
        self.append_values(values, is_upsert)
    }

    fn has_retention_columns(&self) -> bool {
        self.columns
            .last()
            .is_some_and(|column| column.name == INTERNAL_MISSING_RETENTION_COLUMN)
    }

    fn append_values(
        &mut self,
        mut values: Vec<Value>,
        is_upsert: bool,
    ) -> AnalyticsEngineResult<()> {
        debug_assert_eq!(values.len(), self.columns.len());
        values.push(Value::BigInt(self.next_ordinal));
        values.push(Value::Boolean(is_upsert));
        self.next_ordinal = self.next_ordinal.saturating_add(1);
        self.has_deletes |= !is_upsert;
        self.appender
            .as_mut()
            .expect("pending table retains its appender until flush")
            .append_row(appender_params_from_iter(values))?;
        Ok(())
    }

    fn flush(mut self, conn: &Connection, table_name: &str) -> AnalyticsEngineResult<()> {
        let staged_rows = usize::try_from(self.next_ordinal).unwrap_or(usize::MAX);
        if let Some(mut appender) = self.appender.take() {
            let started = Instant::now();
            appender.flush()?;
            record_slow_table_phase(table_name, "flush_stage", staged_rows, started.elapsed());
        }
        let started = Instant::now();
        replace_rows(
            conn,
            &self.stage_name,
            table_name,
            &self.columns,
            self.has_deletes,
        )?;
        record_slow_table_phase(table_name, "merge_rows", staged_rows, started.elapsed());
        let started = Instant::now();
        conn.execute_batch(
            format!("DROP TABLE {}", sql::quote_identifier(&self.stage_name)).as_str(),
        )?;
        record_slow_table_phase(table_name, "drop_stage", staged_rows, started.elapsed());
        Ok(())
    }
}

fn record_slow_table_phase(
    analytics_table_name: &str,
    phase: &'static str,
    staged_rows: usize,
    elapsed: Duration,
) {
    if !ducklake_table_phase_is_slow(elapsed) {
        return;
    }
    tracing::warn!(
        operation = "analytics_ingest_batch",
        phase,
        analytics_table_name,
        staged_rows,
        elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        "slow DuckLake table write phase"
    );
}

pub(crate) fn ducklake_table_phase_is_slow(elapsed: Duration) -> bool {
    elapsed >= SLOW_DUCKLAKE_PHASE_THRESHOLD
}

#[derive(Clone)]
struct BatchColumn {
    name: String,
    duckdb_type: &'static str,
}

fn batch_layout(table: &TableRegistration, has_retention_columns: bool) -> Vec<BatchColumn> {
    let mut columns = vec![
        BatchColumn {
            name: "table_name".to_string(),
            duckdb_type: "VARCHAR",
        },
        BatchColumn {
            name: "tenant_id".to_string(),
            duckdb_type: "VARCHAR",
        },
        BatchColumn {
            name: "__id".to_string(),
            duckdb_type: "VARCHAR",
        },
        BatchColumn {
            name: crate::constants::SOURCE_POSITION_COLUMN.to_string(),
            duckdb_type: "JSON",
        },
    ];
    for column in columns_for_registration(table) {
        append_batch_column(
            table,
            &mut columns,
            column.column_name,
            column.column_type.duckdb_type(),
        );
    }
    if has_retention_columns {
        columns.extend([
            BatchColumn {
                name: INTERNAL_INGESTED_AT_COLUMN.to_string(),
                duckdb_type: "BIGINT",
            },
            BatchColumn {
                name: INTERNAL_EXPIRY_COLUMN.to_string(),
                duckdb_type: "BIGINT",
            },
            BatchColumn {
                name: INTERNAL_MISSING_RETENTION_COLUMN.to_string(),
                duckdb_type: "BOOLEAN",
            },
        ]);
    }
    columns
}

pub(crate) fn batch_columns(table: &TableRegistration, has_retention_columns: bool) -> Vec<String> {
    batch_layout(table, has_retention_columns)
        .into_iter()
        .map(|column| column.name)
        .collect()
}

fn append_batch_column(
    table: &TableRegistration,
    columns: &mut Vec<BatchColumn>,
    name: String,
    duckdb_type: &'static str,
) {
    if !is_batch_owned_column(table, &name) && !columns.iter().any(|existing| existing.name == name)
    {
        columns.push(BatchColumn { name, duckdb_type });
    }
}

pub(crate) fn is_batch_owned_column(table: &TableRegistration, column: &str) -> bool {
    matches!(
        column,
        "table_name" | "tenant_id" | "__id" | crate::constants::SOURCE_POSITION_COLUMN
    ) || (table.retention.is_some()
        && matches!(
            column,
            INTERNAL_INGESTED_AT_COLUMN
                | INTERNAL_EXPIRY_COLUMN
                | INTERNAL_MISSING_RETENTION_COLUMN
        ))
}

fn create_stage(
    conn: &Connection,
    stage_name: &str,
    columns: &[BatchColumn],
) -> AnalyticsEngineResult<()> {
    let statement = create_stage_statement_for_layout(stage_name, columns);
    conn.execute_batch(&statement)?;
    Ok(())
}

fn create_stage_statement_for_layout(stage_name: &str, columns: &[BatchColumn]) -> String {
    let mut definitions = columns
        .iter()
        .map(|column| {
            format!(
                "{} {}",
                sql::quote_identifier(&column.name),
                column.duckdb_type
            )
        })
        .collect::<Vec<_>>();
    definitions.push(format!("{} BIGINT", sql::quote_identifier(ORDINAL_COLUMN)));
    definitions.push(format!("{} BOOLEAN", sql::quote_identifier(UPSERT_COLUMN)));
    format!(
        "CREATE TEMP TABLE {} ({})",
        sql::quote_identifier(stage_name),
        definitions.join(", ")
    )
}

#[cfg(test)]
pub(crate) fn create_stage_statement(
    stage_name: &str,
    table: &TableRegistration,
    columns: &[String],
) -> String {
    let layout = batch_layout(table, table.retention.is_some());
    let selected = columns
        .iter()
        .map(|name| {
            layout
                .iter()
                .find(|column| column.name == *name)
                .cloned()
                .expect("batch columns are declared by their table registration")
        })
        .collect::<Vec<_>>();
    create_stage_statement_for_layout(stage_name, selected.as_slice())
}

fn replace_rows(
    conn: &Connection,
    stage_name: &str,
    analytics_table_name: &str,
    columns: &[BatchColumn],
    has_deletes: bool,
) -> AnalyticsEngineResult<()> {
    let column_names = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    conn.execute_batch(&BatchSqlExecutor::build_replacement_statement(
        stage_name,
        analytics_table_name,
        &column_names,
        has_deletes,
    ))?;
    Ok(())
}

fn quoted_columns(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| sql::quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ")
}

impl BatchSqlExecutor<'_> {
    fn build_update_assignments(columns: &[String]) -> String {
        columns
            .iter()
            .map(|column| {
                let column = sql::quote_identifier(column);
                if column == sql::quote_identifier(crate::constants::SOURCE_POSITION_COLUMN) {
                    format!("{column} = COALESCE(staged.{column}, target.{column})")
                } else {
                    format!("{column} = staged.{column}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub(crate) fn build_replacement_statement(
        stage_name: &str,
        analytics_table_name: &str,
        columns: &[String],
        has_deletes: bool,
    ) -> String {
        let target = sql::quote_identifier(analytics_table_name);
        let latest_rows = Self::build_latest_rows_query(stage_name);
        let materialized = sql::quote_identifier(&format!("{stage_name}_latest"));
        let mut statement = String::new();
        let source = if has_deletes {
            statement.push_str(&format!(
                "CREATE TEMP TABLE {materialized} AS {latest_rows}; "
            ));
            format!("SELECT * FROM {materialized}")
        } else {
            latest_rows
        };
        statement.push_str(&Self::build_upsert_statement(&target, &source, columns));
        if has_deletes {
            statement.push_str(&Self::build_delete_statement(&target, &source));
            statement.push_str(&format!("; DROP TABLE {materialized}"));
        }
        statement
    }

    fn build_latest_rows_query(stage_name: &str) -> String {
        format!(
            "SELECT * EXCLUDE (__analytics_batch_rank) FROM (
               SELECT *, row_number() OVER (
                 PARTITION BY table_name, tenant_id, __id ORDER BY {ordinal} DESC
               ) AS __analytics_batch_rank FROM {stage}
             ) WHERE __analytics_batch_rank = 1",
            ordinal = sql::quote_identifier(ORDINAL_COLUMN),
            stage = sql::quote_identifier(stage_name),
        )
    }

    fn build_upsert_statement(target: &str, source: &str, columns: &[String]) -> String {
        let values = columns
            .iter()
            .map(|column| format!("staged.{}", sql::quote_identifier(column)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "MERGE INTO {target} AS target
             USING (SELECT * FROM ({source}) WHERE {upsert}) AS staged
             ON target.table_name = staged.table_name
                AND target.tenant_id = staged.tenant_id AND target.__id = staged.__id
             WHEN MATCHED THEN UPDATE SET {assignments}
             WHEN NOT MATCHED THEN INSERT ({selected}) VALUES ({values})",
            upsert = sql::quote_identifier(UPSERT_COLUMN),
            assignments = Self::build_update_assignments(columns),
            selected = quoted_columns(columns),
        )
    }

    fn build_delete_statement(target: &str, source: &str) -> String {
        format!(
            "; DELETE FROM {target} AS target
             USING (SELECT * FROM ({source}) WHERE NOT {upsert}) AS staged
             WHERE target.table_name = staged.table_name
               AND target.tenant_id = staged.tenant_id AND target.__id = staged.__id",
            upsert = sql::quote_identifier(UPSERT_COLUMN),
        )
    }
}
