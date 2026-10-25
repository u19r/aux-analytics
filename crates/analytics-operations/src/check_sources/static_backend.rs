use super::*;

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
    pub fn try_new(
        table_name: impl Into<String>,
        key_column: impl Into<String>,
        hash_columns: Vec<String>,
    ) -> CheckResult<Self> {
        Self::try_new_with_optional_columns(table_name, key_column, hash_columns, None, None)
    }

    pub fn try_new_with_optional_columns(
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
        Self::try_new_with_optional_columns(
            table.analytics_table_name.clone(),
            "__id".to_string(),
            hash_columns,
            Some("__source_position".to_string()),
            None,
        )
    }

    pub(super) fn validate(&self) -> CheckResult<()> {
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
        Self::try_new(CheckBackendKind::AuxStorage, projection, reader)
    }

    pub fn fixture_dynamodb(projection: CheckRowProjection, reader: Reader) -> CheckResult<Self> {
        Self::try_new(CheckBackendKind::DynamoDb, projection, reader)
    }

    pub fn fixture_ducklake(projection: CheckRowProjection, reader: Reader) -> CheckResult<Self> {
        Self::try_new(CheckBackendKind::DuckLake, projection, reader)
    }

    pub fn try_new(
        kind: CheckBackendKind,
        projection: CheckRowProjection,
        reader: Reader,
    ) -> CheckResult<Self> {
        projection.validate()?;
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
        Self::try_new(
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
        Self::try_new(
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
        Self::try_new(
            CheckBackendKind::DuckLake,
            projection,
            ProductionCurrentRowReader::new(reader, page_size),
        )
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
        if !target_tables.contains(&self.projection.table_name) {
            return Ok(Vec::new());
        }
        projected_stream_to_check_rows(
            &self.projection,
            self.reader.current_row_stream(&self.projection)?,
        )
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
