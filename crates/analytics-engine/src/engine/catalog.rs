use analytics_contract::{
    AnalyticsColumn, AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn,
    RetentionPolicy, TableRegistration,
};
use duckdb::Connection;

use super::transaction::EngineTransaction;
use crate::{
    engine::{AnalyticsEngine, AnalyticsEngineResult, IngestRetention},
    retention_effect, source_effect, sql,
};

impl AnalyticsEngine {
    pub(super) fn registered_table<'a>(
        &self,
        manifest: &'a AnalyticsManifest,
        name: &str,
    ) -> AnalyticsEngineResult<&'a TableRegistration> {
        manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == name)
            .ok_or_else(|| super::AnalyticsEngineError::TableNotRegistered(name.to_string()))
    }

    /// Validates the manifest and converges its analytical table schemas.
    pub fn ensure_manifest(&self, manifest: &AnalyticsManifest) -> AnalyticsEngineResult<()> {
        manifest.validate()?;
        for table in &manifest.tables {
            crate::condition::validate_condition(table, &mut self.caches.borrow_mut())?;
        }

        // Credential refresh issues CREATE OR REPLACE SECRET. Keep it behind
        // manifest validation so rejected registrations have no connection
        // catalog side effect.
        self.refresh_object_storage_credentials_if_needed()?;
        self.initialize_runtime_schema()?;
        // Schema convergence is restartable. Commit each registered table on
        // its own: the Aux catalog permits ALTER COLUMN changes for one table
        // per commit.

        for table in &manifest.tables {
            let transaction = EngineTransaction::begin(self)?;
            transaction.finish(self.ensure_table(table))?;
        }
        Ok(())
    }

    pub(super) fn initialize_runtime_schema(&self) -> AnalyticsEngineResult<()> {
        for statement in crate::constants::RUNTIME_SCHEMA_STATEMENTS {
            execute_convergent_schema_statement(&self.conn, statement)?;
        }
        // DuckLake provides table layouts rather than native DuckDB indexes.
        if !self.supports_table_layout {
            execute_convergent_schema_statement(
                &self.conn,
                crate::constants::SOURCE_RECORD_STATE_INDEX_SQL,
            )?;
        }
        if self.destination_fence.supports_sources() {
            for statement in source_effect::schema_statements() {
                execute_convergent_schema_statement(&self.conn, statement)?;
            }
            for statement in retention_effect::schema_statements() {
                execute_convergent_schema_statement(&self.conn, statement)?;
            }
        }
        Ok(())
    }

    fn ensure_table(&self, table: &TableRegistration) -> AnalyticsEngineResult<()> {
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
    ducklake_schema_convergence_message(error.to_string().as_str())
}

pub(crate) fn ducklake_schema_convergence_message(message: &str) -> bool {
    message.contains("DuckLake transaction")
        && (message.contains("has been created by another transaction already")
            || (message.contains("Transaction conflict - invalid mutation:")
                && message.contains("name already exists")))
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
