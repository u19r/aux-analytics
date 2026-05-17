use std::{collections::BTreeMap, fmt, path::Path};

use analytics_contract::TableRegistration;
use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::{
    CheckError, CheckResult, CheckRow, CheckRowStream, CheckRowStreamItem, CheckRowStreamSource,
    LocalCheckDataset,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckBackendKind {
    LocalFixture,
    AuxStorage,
    DynamoDb,
    DuckDb,
    DuckLake,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CheckBackendSupport {
    Supported,
    Partial { reason: String },
    Unsupported { reason: String },
}

impl CheckBackendSupport {
    #[must_use]
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported)
    }

    fn ensure_supported(&self, kind: CheckBackendKind) -> CheckResult<()> {
        match self {
            Self::Supported => Ok(()),
            Self::Partial { reason } => Err(CheckError::PartiallySupportedBackend {
                backend: kind,
                reason: reason.clone(),
            }),
            Self::Unsupported { reason } => Err(CheckError::UnsupportedBackend {
                backend: kind,
                reason: reason.clone(),
            }),
        }
    }
}

pub trait CheckRowSource {
    fn backend_kind(&self) -> CheckBackendKind;
    fn support(&self) -> CheckBackendSupport;
    fn rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>>;

    fn row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        Ok(CheckRowStream::from_unsorted(self.rows(target_tables)?))
    }
}

pub trait CurrentRowReader {
    fn current_rows(
        &self,
        projection: &CheckRowProjection,
    ) -> CheckResult<Vec<ProjectedCurrentRow>>;
}

pub trait CurrentRowStreamReader {
    fn current_row_stream(
        &self,
        projection: &CheckRowProjection,
    ) -> CheckResult<ProjectedCurrentRowStream>;
}

pub trait CurrentRowPageReader {
    fn current_row_pages(
        &self,
        projection: &CheckRowProjection,
    ) -> CheckResult<Vec<ProjectedCurrentRowPage>>;
}

pub trait ProductionCurrentRowPageReader {
    fn current_row_page(
        &self,
        request: &CurrentRowPageRequest,
    ) -> CheckResult<ProjectedCurrentRowPage>;

    fn stream_order(&self) -> CurrentRowStreamOrder {
        CurrentRowStreamOrder::Unordered
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedCurrentRowItem {
    pub partition: Option<String>,
    pub row: ProjectedCurrentRow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedCurrentRow {
    pub key: String,
    pub values: BTreeMap<String, Option<String>>,
    pub source_position: u64,
    pub contains_private_data: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectedCurrentRowPage {
    pub rows: Vec<ProjectedCurrentRow>,
    pub next_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentRowPageRequest {
    pub projection: CheckRowProjection,
    pub cursor: Option<String>,
    pub page_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentRowStreamOrder {
    GloballySortedByKey,
    PartitionSortedByKey,
    Unordered,
}

pub struct ProjectedCurrentRowStream {
    state: ProjectedCurrentRowStreamState,
}

enum ProjectedCurrentRowStreamState {
    Buffered {
        pages: Vec<ProjectedCurrentRowPage>,
        page_index: usize,
        row_index: usize,
    },
    Paged {
        source: Box<dyn ProjectedCurrentRowPageSource>,
        current_rows: Vec<ProjectedCurrentRow>,
        current_partition: Option<String>,
        row_index: usize,
        exhausted: bool,
    },
}

trait ProjectedCurrentRowPageSource {
    fn next_page(&mut self) -> CheckResult<Option<ProjectedCurrentRowPage>>;
}

struct AdapterPageSource<Reader> {
    reader: Reader,
    projection: CheckRowProjection,
    cursor: Option<String>,
    page_size: usize,
    stream_order: CurrentRowStreamOrder,
    previous_partition: Option<String>,
}

impl ProjectedCurrentRowPage {
    #[must_use]
    pub fn terminal(rows: Vec<ProjectedCurrentRow>) -> Self {
        Self {
            rows,
            next_cursor: None,
            partition: None,
        }
    }

    #[must_use]
    pub fn with_next_cursor(
        rows: Vec<ProjectedCurrentRow>,
        next_cursor: impl Into<String>,
    ) -> Self {
        Self {
            rows,
            next_cursor: Some(next_cursor.into()),
            partition: None,
        }
    }

    #[must_use]
    pub fn terminal_partition(
        partition: impl Into<String>,
        rows: Vec<ProjectedCurrentRow>,
    ) -> Self {
        Self {
            rows,
            next_cursor: None,
            partition: Some(partition.into()),
        }
    }

    #[must_use]
    pub fn partition_with_next_cursor(
        partition: impl Into<String>,
        rows: Vec<ProjectedCurrentRow>,
        next_cursor: impl Into<String>,
    ) -> Self {
        Self {
            rows,
            next_cursor: Some(next_cursor.into()),
            partition: Some(partition.into()),
        }
    }
}

impl ProjectedCurrentRowStream {
    #[must_use]
    pub fn from_unsorted_rows(rows: Vec<ProjectedCurrentRow>) -> Self {
        Self::from_sorted_rows(sort_projected_current_rows(rows))
    }

    #[must_use]
    pub fn from_sorted_rows(rows: Vec<ProjectedCurrentRow>) -> Self {
        Self::from_pages(vec![ProjectedCurrentRowPage::terminal(rows)])
    }

    #[must_use]
    pub fn from_pages(pages: Vec<ProjectedCurrentRowPage>) -> Self {
        Self {
            state: ProjectedCurrentRowStreamState::Buffered {
                pages,
                page_index: 0,
                row_index: 0,
            },
        }
    }

    pub fn from_page_reader<Reader>(
        reader: Reader,
        projection: CheckRowProjection,
        page_size: usize,
    ) -> CheckResult<Self>
    where
        Reader: ProductionCurrentRowPageReader + 'static,
    {
        let stream_order = reader.stream_order();
        if stream_order == CurrentRowStreamOrder::Unordered {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "production current-row reader for {} must yield globally sorted or \
                 partition-sorted rows for bounded check comparison; declared {:?}",
                projection.table_name, stream_order
            )));
        }
        Ok(Self {
            state: ProjectedCurrentRowStreamState::Paged {
                source: Box::new(AdapterPageSource {
                    reader,
                    projection,
                    cursor: None,
                    page_size,
                    stream_order,
                    previous_partition: None,
                }),
                current_rows: Vec::new(),
                current_partition: None,
                row_index: 0,
                exhausted: false,
            },
        })
    }

    pub fn next_row(&mut self) -> CheckResult<Option<ProjectedCurrentRow>> {
        Ok(self.next_item()?.map(|item| item.row))
    }

    pub fn next_item(&mut self) -> CheckResult<Option<ProjectedCurrentRowItem>> {
        match &mut self.state {
            ProjectedCurrentRowStreamState::Buffered {
                pages,
                page_index,
                row_index,
            } => loop {
                let Some(page) = pages.get(*page_index) else {
                    return Ok(None);
                };
                if let Some(row) = page.rows.get(*row_index) {
                    *row_index += 1;
                    return Ok(Some(ProjectedCurrentRowItem {
                        partition: page.partition.clone(),
                        row: row.clone(),
                    }));
                }
                *page_index += 1;
                *row_index = 0;
            },
            ProjectedCurrentRowStreamState::Paged {
                source,
                current_rows,
                current_partition,
                row_index,
                exhausted,
            } => loop {
                if let Some(row) = current_rows.get(*row_index) {
                    *row_index += 1;
                    return Ok(Some(ProjectedCurrentRowItem {
                        partition: current_partition.clone(),
                        row: row.clone(),
                    }));
                }
                if *exhausted {
                    return Ok(None);
                }
                match source.next_page()? {
                    Some(page) => {
                        *exhausted = page.next_cursor.is_none();
                        current_partition.clone_from(&page.partition);
                        *current_rows = page.rows;
                        *row_index = 0;
                    }
                    None => *exhausted = true,
                }
            },
        }
    }

    pub fn into_rows(mut self) -> CheckResult<Vec<ProjectedCurrentRow>> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row()? {
            rows.push(row);
        }
        Ok(rows)
    }
}

impl fmt::Debug for ProjectedCurrentRowStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            ProjectedCurrentRowStreamState::Buffered { pages, .. } => formatter
                .debug_struct("ProjectedCurrentRowStream")
                .field("mode", &"buffered")
                .field("pages", &pages.len())
                .finish(),
            ProjectedCurrentRowStreamState::Paged { exhausted, .. } => formatter
                .debug_struct("ProjectedCurrentRowStream")
                .field("mode", &"paged")
                .field("exhausted", exhausted)
                .finish(),
        }
    }
}

impl<Reader> ProjectedCurrentRowPageSource for AdapterPageSource<Reader>
where Reader: ProductionCurrentRowPageReader
{
    fn next_page(&mut self) -> CheckResult<Option<ProjectedCurrentRowPage>> {
        let request = CurrentRowPageRequest {
            projection: self.projection.clone(),
            cursor: self.cursor.clone(),
            page_size: self.page_size,
        };
        let mut page = self.reader.current_row_page(&request)?;
        page.rows = sort_projected_current_rows(page.rows);
        self.validate_partition(&page)?;
        self.cursor.clone_from(&page.next_cursor);
        if page.rows.is_empty() && page.next_cursor.is_none() {
            return Ok(None);
        }
        if page.rows.is_empty() {
            return self.next_page();
        }
        Ok(Some(page))
    }
}

impl<Reader> AdapterPageSource<Reader> {
    fn validate_partition(&mut self, page: &ProjectedCurrentRowPage) -> CheckResult<()> {
        if self.stream_order != CurrentRowStreamOrder::PartitionSortedByKey {
            return Ok(());
        }
        let partition = page.partition.as_ref().ok_or_else(|| {
            CheckError::InvalidBackendConfiguration(format!(
                "partition-sorted production current-row reader for {} returned a page without a \
                 partition key",
                self.projection.table_name
            ))
        })?;
        if let Some(previous) = self.previous_partition.as_ref()
            && partition < previous
        {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "partition-sorted production current-row reader for {} returned partition \
                 {partition} after {previous}",
                self.projection.table_name
            )));
        }
        self.previous_partition = Some(partition.clone());
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionCurrentRowReader<Reader> {
    reader: Reader,
    page_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbTableScanCurrentRowReader {
    database_path: String,
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
            database_path: database_path.into(),
        }
    }

    fn connect(&self) -> CheckResult<Connection> {
        Connection::open(Path::new(self.database_path.as_str())).map_err(CheckError::from)
    }
}

impl ProductionCurrentRowPageReader for DuckDbTableScanCurrentRowReader {
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
        let connection = self.connect()?;
        let select_columns = request
            .projection
            .hash_columns
            .iter()
            .map(|column| format!("cast({} as varchar)", quote_identifier(column)))
            .collect::<Vec<_>>();
        let source_position_expression = request
            .projection
            .source_position_column
            .as_ref()
            .map_or_else(|| "NULL".to_string(), |column| quote_identifier(column));
        let private_expression = request.projection.private_data_column.as_ref().map_or_else(
            || "false".to_string(),
            |column| format!("coalesce({}, false)", quote_identifier(column)),
        );
        let sql = format!(
            "select cast({key} as varchar), {hash_columns}, {source_position_expression}, \
             {private_expression} from {table} order by {key} limit ? offset ?",
            key = quote_identifier(&request.projection.key_column),
            hash_columns = select_columns.join(", "),
            table = quote_identifier(&request.projection.table_name)
        );
        let page_size_sql = current_row_sql_bound(request.page_size, "page size")?;
        let offset_sql = current_row_sql_bound(offset, "cursor offset")?;
        let mut statement = connection.prepare(sql.as_str())?;
        let mut rows = statement.query(params![page_size_sql, offset_sql])?;
        let mut projected_rows = Vec::new();
        while let Some(row) = rows.next()? {
            let mut values = BTreeMap::new();
            for (column_index, column) in request.projection.hash_columns.iter().enumerate() {
                values.insert(
                    column.clone(),
                    row.get::<_, Option<String>>(column_index + 1)?,
                );
            }
            let source_position_json =
                row.get::<_, Option<String>>(request.projection.hash_columns.len() + 1)?;
            projected_rows.push(
                ProjectedCurrentRow::new(
                    row.get::<_, String>(0)?,
                    values,
                    source_position_json
                        .as_deref()
                        .map(source_position_json_to_u64)
                        .transpose()?
                        .unwrap_or_default(),
                )
                .with_private_data(row.get(request.projection.hash_columns.len() + 2)?),
            );
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
        ProjectedCurrentRowStream::from_page_reader(
            self.reader.clone(),
            projection.clone(),
            self.page_size,
        )
    }
}

impl ProjectedCurrentRow {
    #[must_use]
    pub fn new(
        key: impl Into<String>,
        values: BTreeMap<String, Option<String>>,
        source_position: u64,
    ) -> Self {
        Self {
            key: key.into(),
            values,
            source_position,
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

#[deprecated(
    since = "0.1.0",
    note = "use FixtureCurrentRowReader; this compatibility alias is for tests and small fixtures \
            only"
)]
pub type StaticCurrentRowReader = FixtureCurrentRowReader;

#[deprecated(
    since = "0.1.0",
    note = "use FixturePagedCurrentRowReader; this compatibility alias is for tests and small \
            fixtures only"
)]
pub type StaticPagedCurrentRowReader = FixturePagedCurrentRowReader;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalFixtureCheckRows {
    dataset: LocalCheckDataset,
}

impl LocalFixtureCheckRows {
    #[must_use]
    pub fn new(dataset: LocalCheckDataset) -> Self {
        Self { dataset }
    }
}

impl CheckRowSource for LocalFixtureCheckRows {
    fn backend_kind(&self) -> CheckBackendKind {
        CheckBackendKind::LocalFixture
    }

    fn support(&self) -> CheckBackendSupport {
        CheckBackendSupport::Supported
    }

    fn rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        self.support().ensure_supported(self.backend_kind())?;
        Ok(crate::check::filter_target_tables(
            &self.dataset.rows,
            target_tables,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticCheckBackendRows {
    kind: CheckBackendKind,
    support: CheckBackendSupport,
    rows: Vec<CheckRow>,
}

pub type AuxStorageCheckRows = StaticCheckBackendRows;
pub type DynamoDbCheckRows = StaticCheckBackendRows;
pub type DuckLakeCheckRows = StaticCheckBackendRows;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionBackedCheckRows<Reader> {
    kind: CheckBackendKind,
    projection: CheckRowProjection,
    reader: Reader,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckRowProjection {
    pub table_name: String,
    pub key_column: String,
    pub hash_columns: Vec<String>,
    pub source_position_column: Option<String>,
    pub private_data_column: Option<String>,
}

impl CheckRowProjection {
    pub fn new(
        table_name: impl Into<String>,
        key_column: impl Into<String>,
        hash_columns: Vec<String>,
    ) -> CheckResult<Self> {
        Self::new_with_optional_columns(table_name, key_column, hash_columns, None, None)
    }

    pub fn new_with_optional_columns(
        table_name: impl Into<String>,
        key_column: impl Into<String>,
        hash_columns: Vec<String>,
        source_position_column: Option<String>,
        private_data_column: Option<String>,
    ) -> CheckResult<Self> {
        let projection = Self {
            table_name: table_name.into(),
            key_column: key_column.into(),
            hash_columns,
            source_position_column,
            private_data_column,
        };
        projection.validate()?;
        Ok(projection)
    }

    pub fn from_table_registration(table: &TableRegistration) -> CheckResult<Self> {
        let hash_columns = table
            .projection_columns
            .as_deref()
            .map(|columns| {
                columns
                    .iter()
                    .map(|column| column.column_name.clone())
                    .collect::<Vec<_>>()
            })
            .filter(|columns| !columns.is_empty())
            .unwrap_or_else(|| {
                table.projection_attribute_names.clone().unwrap_or_else(|| {
                    table
                        .columns
                        .iter()
                        .map(|column| column.column_name.clone())
                        .collect()
                })
            });
        Self::new_with_optional_columns(
            table.analytics_table_name.clone(),
            "__id".to_string(),
            hash_columns,
            Some("__source_position".to_string()),
            None,
        )
    }

    fn validate(&self) -> CheckResult<()> {
        validate_identifier(self.table_name.as_str())?;
        validate_identifier(self.key_column.as_str())?;
        if self.hash_columns.is_empty() {
            return Err(CheckError::InvalidBackendConfiguration(
                "check row projection requires at least one hash column".to_string(),
            ));
        }
        for column in &self.hash_columns {
            validate_identifier(column.as_str())?;
        }
        if let Some(column) = self.source_position_column.as_deref() {
            validate_identifier(column)?;
        }
        if let Some(column) = self.private_data_column.as_deref() {
            validate_identifier(column)?;
        }
        Ok(())
    }
}

impl StaticCheckBackendRows {
    #[must_use]
    pub fn supported(kind: CheckBackendKind, rows: Vec<CheckRow>) -> Self {
        Self {
            kind,
            support: CheckBackendSupport::Supported,
            rows,
        }
    }

    #[must_use]
    pub fn partial(kind: CheckBackendKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            support: CheckBackendSupport::Partial {
                reason: reason.into(),
            },
            rows: Vec::new(),
        }
    }

    #[must_use]
    pub fn unsupported(kind: CheckBackendKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            support: CheckBackendSupport::Unsupported {
                reason: reason.into(),
            },
            rows: Vec::new(),
        }
    }
}

impl<Reader> ProjectionBackedCheckRows<Reader>
where Reader: CurrentRowStreamReader
{
    pub fn fixture_aux_storage(
        projection: CheckRowProjection,
        reader: Reader,
    ) -> CheckResult<Self> {
        Self::new(CheckBackendKind::AuxStorage, projection, reader)
    }

    pub fn fixture_dynamodb(projection: CheckRowProjection, reader: Reader) -> CheckResult<Self> {
        Self::new(CheckBackendKind::DynamoDb, projection, reader)
    }

    pub fn fixture_ducklake(projection: CheckRowProjection, reader: Reader) -> CheckResult<Self> {
        Self::new(CheckBackendKind::DuckLake, projection, reader)
    }

    #[deprecated(
        since = "0.1.0",
        note = "use fixture_aux_storage for tests/small fixtures or full_table_aux_storage for \
                production checks"
    )]
    pub fn aux_storage(projection: CheckRowProjection, reader: Reader) -> CheckResult<Self> {
        Self::fixture_aux_storage(projection, reader)
    }

    #[deprecated(
        since = "0.1.0",
        note = "use fixture_dynamodb for tests/small fixtures or full_table_dynamodb for \
                production checks"
    )]
    pub fn dynamodb(projection: CheckRowProjection, reader: Reader) -> CheckResult<Self> {
        Self::fixture_dynamodb(projection, reader)
    }

    #[deprecated(
        since = "0.1.0",
        note = "use fixture_ducklake for tests/small fixtures or full_table_ducklake for \
                production checks"
    )]
    pub fn ducklake(projection: CheckRowProjection, reader: Reader) -> CheckResult<Self> {
        Self::fixture_ducklake(projection, reader)
    }

    pub fn new(
        kind: CheckBackendKind,
        projection: CheckRowProjection,
        reader: Reader,
    ) -> CheckResult<Self> {
        if matches!(
            kind,
            CheckBackendKind::LocalFixture | CheckBackendKind::DuckDb
        ) {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "{kind:?} is not a current-row backend"
            )));
        }
        Ok(Self {
            kind,
            projection,
            reader,
        })
    }
}

impl<Reader> ProjectionBackedCheckRows<ProductionCurrentRowReader<Reader>>
where Reader: ProductionCurrentRowPageReader + Clone + 'static
{
    pub fn full_table_aux_storage(
        projection: CheckRowProjection,
        reader: Reader,
        page_size: usize,
    ) -> CheckResult<Self> {
        Self::new(
            CheckBackendKind::AuxStorage,
            projection,
            ProductionCurrentRowReader::new(reader, page_size),
        )
    }

    pub fn full_table_dynamodb(
        projection: CheckRowProjection,
        reader: Reader,
        page_size: usize,
    ) -> CheckResult<Self> {
        Self::new(
            CheckBackendKind::DynamoDb,
            projection,
            ProductionCurrentRowReader::new(reader, page_size),
        )
    }

    pub fn full_table_ducklake(
        projection: CheckRowProjection,
        reader: Reader,
        page_size: usize,
    ) -> CheckResult<Self> {
        Self::new(
            CheckBackendKind::DuckLake,
            projection,
            ProductionCurrentRowReader::new(reader, page_size),
        )
    }

    #[deprecated(
        since = "0.1.0",
        note = "use full_table_aux_storage; production_* was ambiguous with fixture compatibility \
                readers"
    )]
    pub fn production_aux_storage(
        projection: CheckRowProjection,
        reader: Reader,
        page_size: usize,
    ) -> CheckResult<Self> {
        Self::full_table_aux_storage(projection, reader, page_size)
    }

    #[deprecated(
        since = "0.1.0",
        note = "use full_table_dynamodb; production_* was ambiguous with fixture compatibility \
                readers"
    )]
    pub fn production_dynamodb(
        projection: CheckRowProjection,
        reader: Reader,
        page_size: usize,
    ) -> CheckResult<Self> {
        Self::full_table_dynamodb(projection, reader, page_size)
    }

    #[deprecated(
        since = "0.1.0",
        note = "use full_table_ducklake; production_* was ambiguous with fixture compatibility \
                readers"
    )]
    pub fn production_ducklake(
        projection: CheckRowProjection,
        reader: Reader,
        page_size: usize,
    ) -> CheckResult<Self> {
        Self::full_table_ducklake(projection, reader, page_size)
    }
}

impl<Reader> CheckRowSource for ProjectionBackedCheckRows<Reader>
where Reader: CurrentRowStreamReader
{
    fn backend_kind(&self) -> CheckBackendKind {
        self.kind
    }

    fn support(&self) -> CheckBackendSupport {
        CheckBackendSupport::Supported
    }

    fn rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        self.support().ensure_supported(self.backend_kind())?;
        let rows = projected_stream_to_check_rows(
            &self.projection,
            self.reader.current_row_stream(&self.projection)?,
        )?;
        Ok(crate::check::filter_target_tables(&rows, target_tables))
    }

    fn row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        self.support().ensure_supported(self.backend_kind())?;
        if !target_tables.contains(&self.projection.table_name) {
            return Ok(CheckRowStream::from_sorted(Vec::new()));
        }
        Ok(CheckRowStream::from_source(Box::new(
            ProjectedCheckRowStreamSource {
                projection: self.projection.clone(),
                stream: self.reader.current_row_stream(&self.projection)?,
            },
        )))
    }
}

impl CheckRowSource for StaticCheckBackendRows {
    fn backend_kind(&self) -> CheckBackendKind {
        self.kind
    }

    fn support(&self) -> CheckBackendSupport {
        self.support.clone()
    }

    fn rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        self.support.ensure_supported(self.kind)?;
        Ok(crate::check::filter_target_tables(
            &self.rows,
            target_tables,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbCheckRows {
    database_path: String,
    check_rows_table: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbAnalyticalCheckRows {
    database_path: String,
    projection: CheckRowProjection,
}

impl DuckDbAnalyticalCheckRows {
    pub fn new(
        database_path: impl Into<String>,
        table_name: impl Into<String>,
        key_column: impl Into<String>,
        hash_columns: Vec<String>,
    ) -> CheckResult<Self> {
        Self::new_with_projection(
            database_path,
            CheckRowProjection::new(table_name, key_column, hash_columns)?,
        )
    }

    pub fn new_with_private_data_column(
        database_path: impl Into<String>,
        table_name: impl Into<String>,
        key_column: impl Into<String>,
        hash_columns: Vec<String>,
        private_data_column: Option<String>,
    ) -> CheckResult<Self> {
        Self::new_with_projection(
            database_path,
            CheckRowProjection::new_with_optional_columns(
                table_name,
                key_column,
                hash_columns,
                None,
                private_data_column,
            )?,
        )
    }

    pub fn new_with_projection(
        database_path: impl Into<String>,
        projection: CheckRowProjection,
    ) -> CheckResult<Self> {
        Ok(Self {
            database_path: database_path.into(),
            projection,
        })
    }

    fn connect(&self) -> CheckResult<Connection> {
        Connection::open(Path::new(self.database_path.as_str())).map_err(CheckError::from)
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
        self.support().ensure_supported(self.backend_kind())?;
        Ok(crate::check::filter_target_tables(
            &self.read_ordered_rows()?,
            target_tables,
        ))
    }

    fn row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        self.support().ensure_supported(self.backend_kind())?;
        Ok(CheckRowStream::from_sorted(
            crate::check::filter_target_tables(&self.read_ordered_rows()?, target_tables),
        ))
    }
}

impl DuckDbAnalyticalCheckRows {
    fn read_ordered_rows(&self) -> CheckResult<Vec<CheckRow>> {
        let connection = self.connect()?;
        let select_columns = self
            .projection
            .hash_columns
            .iter()
            .map(|column| format!("cast({} as varchar)", quote_identifier(column)))
            .collect::<Vec<_>>();
        let source_position_expression = self
            .projection
            .source_position_column
            .as_ref()
            .map_or_else(|| "NULL".to_string(), |column| quote_identifier(column));
        let private_expression = self.projection.private_data_column.as_ref().map_or_else(
            || "false".to_string(),
            |column| format!("coalesce({}, false)", quote_identifier(column)),
        );
        let sql = format!(
            "select cast({key} as varchar), {hash_columns}, {source_position_expression}, \
             {private_expression} from {table} order by {key}",
            key = quote_identifier(&self.projection.key_column),
            hash_columns = select_columns.join(", "),
            table = quote_identifier(&self.projection.table_name)
        );
        let mut statement = connection.prepare(sql.as_str())?;
        let mut rows = statement.query(params![])?;
        let mut check_rows = Vec::new();
        while let Some(row) = rows.next()? {
            let key = row.get::<_, String>(0)?;
            let mut hash = FNV_OFFSET_BASIS;
            for column_index in 0..self.projection.hash_columns.len() {
                let value = row.get::<_, Option<String>>(column_index + 1)?;
                hash = fnv1a(hash, value.unwrap_or_default().as_bytes());
            }
            let source_position_json =
                row.get::<_, Option<String>>(self.projection.hash_columns.len() + 1)?;
            check_rows.push(CheckRow {
                table: self.projection.table_name.clone(),
                key,
                row_hash: hash,
                source_position: source_position_json
                    .as_deref()
                    .map(source_position_json_to_u64)
                    .transpose()?
                    .unwrap_or_default(),
                contains_private_data: row.get(self.projection.hash_columns.len() + 2)?,
            });
        }
        Ok(check_rows)
    }
}

impl DuckDbCheckRows {
    pub fn new(
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
        self.support().ensure_supported(self.backend_kind())?;
        Ok(crate::check::filter_target_tables(
            &self.read_ordered_rows()?,
            target_tables,
        ))
    }

    fn row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        self.support().ensure_supported(self.backend_kind())?;
        Ok(CheckRowStream::from_sorted(
            crate::check::filter_target_tables(&self.read_ordered_rows()?, target_tables),
        ))
    }
}

impl DuckDbCheckRows {
    fn read_ordered_rows(&self) -> CheckResult<Vec<CheckRow>> {
        let connection = self.connect()?;
        let mut statement = connection.prepare(&format!(
            "select table_name, row_key, row_hash, source_position, contains_private_data from {} \
             order by table_name, row_key",
            self.check_rows_table
        ))?;
        let mut rows = statement.query(params![])?;
        let mut check_rows = Vec::new();
        while let Some(row) = rows.next()? {
            check_rows.push(CheckRow {
                table: row.get(0)?,
                key: row.get(1)?,
                row_hash: row.get::<_, i64>(2)?.cast_unsigned(),
                source_position: row.get::<_, i64>(3)?.cast_unsigned(),
                contains_private_data: row.get(4)?,
            });
        }
        Ok(check_rows)
    }
}

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0100_0000_01b3;

fn validate_identifier(identifier: &str) -> CheckResult<()> {
    if !identifier.is_empty()
        && identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Ok(());
    }
    Err(CheckError::InvalidBackendConfiguration(format!(
        "invalid DuckDB check rows table identifier: {identifier}"
    )))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn projected_stream_to_check_rows(
    projection: &CheckRowProjection,
    mut stream: ProjectedCurrentRowStream,
) -> CheckResult<Vec<CheckRow>> {
    let mut rows = Vec::new();
    while let Some(row) = stream.next_row()? {
        rows.push(projected_current_row_to_check_row(projection, row)?);
    }
    Ok(rows)
}

struct ProjectedCheckRowStreamSource {
    projection: CheckRowProjection,
    stream: ProjectedCurrentRowStream,
}

impl CheckRowStreamSource for ProjectedCheckRowStreamSource {
    fn next_item(&mut self) -> CheckResult<Option<CheckRowStreamItem>> {
        self.stream
            .next_item()?
            .map(|item| {
                Ok(CheckRowStreamItem {
                    partition: item.partition,
                    row: projected_current_row_to_check_row(&self.projection, item.row)?,
                })
            })
            .transpose()
    }
}

fn projected_current_row_to_check_row(
    projection: &CheckRowProjection,
    row: ProjectedCurrentRow,
) -> CheckResult<CheckRow> {
    let mut hash = FNV_OFFSET_BASIS;
    for column in &projection.hash_columns {
        let value = row.values.get(column).ok_or_else(|| {
            CheckError::InvalidBackendConfiguration(format!(
                "current-row reader for {} did not return projected column {column}",
                projection.table_name
            ))
        })?;
        hash = fnv1a(hash, value.as_deref().unwrap_or_default().as_bytes());
    }
    Ok(CheckRow {
        table: projection.table_name.clone(),
        key: row.key,
        row_hash: hash,
        source_position: row.source_position,
        contains_private_data: row.contains_private_data,
    })
}

fn sort_projected_pages(mut pages: Vec<ProjectedCurrentRowPage>) -> Vec<ProjectedCurrentRowPage> {
    let rows = sort_projected_current_rows(
        pages
            .drain(..)
            .flat_map(|page| page.rows)
            .collect::<Vec<_>>(),
    );
    vec![ProjectedCurrentRowPage::terminal(rows)]
}

fn sort_projected_current_rows(mut rows: Vec<ProjectedCurrentRow>) -> Vec<ProjectedCurrentRow> {
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    rows
}

fn source_position_json_to_u64(value: &str) -> CheckResult<u64> {
    let source_position = serde_json::from_str::<AnalyticalSourcePosition>(value)?;
    Ok(match source_position {
        AnalyticalSourcePosition::DynamoDbStreamSequence {
            sequence_number, ..
        } => sequence_number
            .parse::<u64>()
            .unwrap_or_else(|_| fnv1a(FNV_OFFSET_BASIS, sequence_number.as_bytes())),
        AnalyticalSourcePosition::AuxStorageStreamCursor { cursor, .. } => cursor
            .parse::<u64>()
            .unwrap_or_else(|_| fnv1a(FNV_OFFSET_BASIS, cursor.as_bytes())),
        AnalyticalSourcePosition::DynamoDbExportSnapshot { exported_at_ms, .. } => exported_at_ms,
        AnalyticalSourcePosition::DynamoDbScanChunk { item_index, .. } => item_index,
        AnalyticalSourcePosition::RawBackupObjectOffset { offset, .. } => offset,
    })
}

fn current_row_cursor_offset(cursor: Option<&str>) -> CheckResult<usize> {
    cursor.map_or(Ok(0), |value| {
        value.parse::<usize>().map_err(|_| {
            CheckError::InvalidBackendConfiguration(format!(
                "production current-row cursor must be a row offset, got {value}"
            ))
        })
    })
}

fn current_row_sql_bound(value: usize, name: &str) -> CheckResult<i64> {
    i64::try_from(value).map_err(|_| {
        CheckError::InvalidBackendConfiguration(format!(
            "production current-row {name} exceeds DuckDB integer range"
        ))
    })
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AnalyticalSourcePosition {
    DynamoDbStreamSequence { sequence_number: String },
    AuxStorageStreamCursor { cursor: String },
    DynamoDbExportSnapshot { exported_at_ms: u64 },
    DynamoDbScanChunk { item_index: u64 },
    RawBackupObjectOffset { offset: u64 },
}
