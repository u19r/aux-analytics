use storage_types::VecMap;

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionCurrentRowReader<Reader> {
    reader: Reader,
    page_size: usize,
}

#[derive(Debug, Clone)]
pub struct DuckDbTableScanCurrentRowReader {
    connection: super::duckdb_connection::DuckDbReadConnection,
}

impl<Reader> ProductionCurrentRowReader<Reader> {
    #[must_use]
    pub fn new(reader: Reader, page_size: usize) -> Self {
        Self { reader, page_size }
    }
}

impl DuckDbTableScanCurrentRowReader {
    #[must_use]
    pub fn new(database_path: impl Into<String>) -> Self {
        Self {
            connection: super::duckdb_connection::DuckDbReadConnection::File(database_path.into()),
        }
    }

    /// Retain the native database owner for live writers and attached catalogs.
    pub fn try_from_connection(connection: &Connection) -> CheckResult<Self> {
        Ok(Self {
            connection: super::duckdb_connection::DuckDbReadConnection::try_from_connection(
                connection,
            )?,
        })
    }
}

impl ProductionCurrentRowPageReader for DuckDbTableScanCurrentRowReader {
    fn open_row_stream(
        &self,
        projection: CheckRowProjection,
        page_size: usize,
    ) -> CheckResult<ProjectedCurrentRowStream> {
        projection.validate()?;
        let scan = super::duckdb_scan::DuckDbScan::try_new(
            self.connection.open_connection()?,
            projection,
            page_size,
            CheckRowProjection::read_projected_row,
        )?;
        Ok(ProjectedCurrentRowStream::from_duckdb_scan(scan))
    }

    fn current_row_page(
        &self,
        request: &CurrentRowPageRequest,
    ) -> CheckResult<ProjectedCurrentRowPage> {
        if request.page_size == 0 {
            return Err(CheckError::InvalidBackendConfiguration(
                "production current-row page size must be greater than zero".to_string(),
            ));
        }
        let offset = current_row_cursor_offset(request.cursor.as_deref())?;
        request.projection.validate()?;
        let mut sql = request.projection.ordered_select_sql();
        sql.push_str(" LIMIT ? OFFSET ?");
        let page_size_sql = current_row_sql_bound(request.page_size, "page size")?;
        let offset_sql = current_row_sql_bound(offset, "cursor offset")?;
        let connection = self.connection.open_connection()?;
        let mut statement = connection.prepare(sql.as_str())?;
        let mut rows = statement.query(params![page_size_sql, offset_sql])?;
        let mut projected_rows = Vec::new();
        while let Some(row) = rows.next()? {
            projected_rows.push(request.projection.read_projected_row(row)?);
        }
        let next_offset = offset + projected_rows.len();
        let next_cursor = if projected_rows.len() == request.page_size {
            Some(next_offset.to_string())
        } else {
            None
        };
        Ok(ProjectedCurrentRowPage {
            rows: projected_rows,
            next_cursor,
            partition: None,
        })
    }

    fn stream_order(&self) -> CurrentRowStreamOrder {
        CurrentRowStreamOrder::GloballySortedByKey
    }
}

impl<Reader> CurrentRowStreamReader for ProductionCurrentRowReader<Reader>
where Reader: ProductionCurrentRowPageReader + Clone + 'static
{
    fn current_row_stream(
        &self,
        projection: &CheckRowProjection,
    ) -> CheckResult<ProjectedCurrentRowStream> {
        self.reader
            .open_row_stream(projection.clone(), self.page_size)
    }
}

impl ProjectedCurrentRow {
    #[must_use]
    pub fn new(
        key: impl Into<String>,
        values: VecMap<String, Option<String>>,
        source_position: impl Into<crate::CheckSourcePosition>,
    ) -> Self {
        Self {
            key: key.into(),
            values,
            source_position: source_position.into(),
            contains_private_data: false,
        }
    }

    #[must_use]
    pub fn with_private_data(mut self, contains_private_data: bool) -> Self {
        self.contains_private_data = contains_private_data;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureCurrentRowReader {
    rows: Vec<ProjectedCurrentRow>,
}

impl FixtureCurrentRowReader {
    #[must_use]
    pub fn new(rows: Vec<ProjectedCurrentRow>) -> Self {
        Self { rows }
    }
}

impl CurrentRowStreamReader for FixtureCurrentRowReader {
    fn current_row_stream(
        &self,
        _projection: &CheckRowProjection,
    ) -> CheckResult<ProjectedCurrentRowStream> {
        Ok(ProjectedCurrentRowStream::from_unsorted_rows(
            self.rows.clone(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixturePagedCurrentRowReader {
    pages: Vec<ProjectedCurrentRowPage>,
}

impl FixturePagedCurrentRowReader {
    #[must_use]
    pub fn new(pages: Vec<ProjectedCurrentRowPage>) -> Self {
        Self { pages }
    }
}

impl CurrentRowPageReader for FixturePagedCurrentRowReader {
    fn current_row_pages(
        &self,
        _projection: &CheckRowProjection,
    ) -> CheckResult<Vec<ProjectedCurrentRowPage>> {
        Ok(self.pages.clone())
    }
}

impl<T> CurrentRowStreamReader for T
where T: CurrentRowPageReader
{
    fn current_row_stream(
        &self,
        projection: &CheckRowProjection,
    ) -> CheckResult<ProjectedCurrentRowStream> {
        Ok(ProjectedCurrentRowStream::from_pages(sort_projected_pages(
            self.current_row_pages(projection)?,
        )))
    }
}

impl<T> CurrentRowReader for T
where T: CurrentRowStreamReader
{
    fn current_rows(
        &self,
        projection: &CheckRowProjection,
    ) -> CheckResult<Vec<ProjectedCurrentRow>> {
        self.current_row_stream(projection)?.into_rows()
    }
}

impl<T> CheckRowSource for Box<T>
where T: CheckRowSource + ?Sized
{
    fn backend_kind(&self) -> CheckBackendKind {
        self.as_ref().backend_kind()
    }

    fn support(&self) -> CheckBackendSupport {
        self.as_ref().support()
    }

    fn rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        self.as_ref().rows(target_tables)
    }

    fn row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        self.as_ref().row_stream(target_tables)
    }
}
