use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbCheckRows {
    database_path: String,
    check_rows_table: String,
}

#[derive(Debug, Clone)]
pub struct DuckDbAnalyticalCheckRows {
    connection: super::duckdb_connection::DuckDbReadConnection,
    projection: CheckRowProjection,
}

impl DuckDbAnalyticalCheckRows {
    pub fn try_new(
        database_path: impl Into<String>,
        table_name: impl Into<String>,
        key_column: impl Into<String>,
        hash_columns: Vec<String>,
    ) -> CheckResult<Self> {
        Self::try_new_with_projection(
            database_path,
            CheckRowProjection::try_new(table_name, key_column, hash_columns)?,
        )
    }

    pub fn try_new_with_projection(
        database_path: impl Into<String>,
        projection: CheckRowProjection,
    ) -> CheckResult<Self> {
        projection.validate()?;
        Ok(Self {
            connection: super::duckdb_connection::DuckDbReadConnection::File(database_path.into()),
            projection,
        })
    }

    /// Scan a live database using its native owner. This preserves committed
    /// writer changes and attached catalogs without opening a separate
    /// database.
    pub fn try_from_connection(
        connection: &Connection,
        projection: CheckRowProjection,
    ) -> CheckResult<Self> {
        projection.validate()?;
        Ok(Self {
            connection: super::duckdb_connection::DuckDbReadConnection::try_from_connection(
                connection,
            )?,
            projection,
        })
    }
}

impl CheckRowSource for DuckDbAnalyticalCheckRows {
    fn backend_kind(&self) -> CheckBackendKind {
        CheckBackendKind::DuckDb
    }

    fn support(&self) -> CheckBackendSupport {
        CheckBackendSupport::Supported
    }

    fn rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        let mut stream = self.row_stream(target_tables)?;
        let mut check_rows = Vec::new();
        while let Some(row) = stream.next_row()? {
            check_rows.push(row);
        }
        Ok(check_rows)
    }

    fn row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        if !target_tables.contains(&self.projection.table_name) {
            return Ok(CheckRowStream::from_sorted(Vec::new()));
        }
        let scan = super::duckdb_scan::DuckDbCheckScan::try_new(
            self.connection.open_connection()?,
            self.projection.clone(),
        )?;
        Ok(CheckRowStream::from_source(Box::new(scan)))
    }
}

impl DuckDbCheckRows {
    pub fn try_new(
        database_path: impl Into<String>,
        check_rows_table: impl Into<String>,
    ) -> CheckResult<Self> {
        let check_rows_table = check_rows_table.into();
        validate_identifier(check_rows_table.as_str())?;
        Ok(Self {
            database_path: database_path.into(),
            check_rows_table,
        })
    }

    fn connect(&self) -> CheckResult<Connection> {
        Connection::open(Path::new(self.database_path.as_str())).map_err(CheckError::from)
    }
}

impl CheckRowSource for DuckDbCheckRows {
    fn backend_kind(&self) -> CheckBackendKind {
        CheckBackendKind::DuckDb
    }

    fn support(&self) -> CheckBackendSupport {
        CheckBackendSupport::Supported
    }

    fn rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        if target_tables.is_empty() {
            return Ok(Vec::new());
        }
        let connection = self.connect()?;
        let mut sql = format!(
            "SELECT table_name, row_key, row_hash, source_position, contains_private_data FROM {} \
             WHERE table_name IN (",
            quote_identifier(&self.check_rows_table)
        );
        for index in 0..target_tables.len() {
            if index > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
        }
        sql.push_str(") ORDER BY table_name, row_key");
        let mut statement = connection.prepare(&sql)?;
        let mut rows = statement.query(::duckdb::params_from_iter(target_tables))?;
        let mut check_rows = Vec::new();
        while let Some(row) = rows.next()? {
            check_rows.push(CheckRow {
                table: row.get(0)?,
                key: row.get(1)?,
                row_hash: row.get::<_, i64>(2)?.cast_unsigned(),
                source_position: row
                    .get::<_, Option<String>>(3)?
                    .as_deref()
                    .map(source_position_json_to_check_position)
                    .transpose()?
                    .unwrap_or_default(),
                contains_private_data: row.get(4)?,
            });
        }
        Ok(check_rows)
    }

    fn row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        Ok(CheckRowStream::from_sorted(self.rows(target_tables)?))
    }
}
