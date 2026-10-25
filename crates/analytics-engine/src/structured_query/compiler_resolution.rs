use super::*;

impl<'a> QueryCompiler<'a> {
    pub(super) fn resolve_table_for_column(
        &self,
        table_alias: Option<&str>,
        column_name: &str,
    ) -> AnalyticsEngineResult<&QueryTable<'a>> {
        if let Some(table_alias) = table_alias {
            let table = self.table_by_alias(table_alias)?;
            validate_registered_column(&table.registered_columns, column_name)?;
            return Ok(table);
        }
        let mut matches = self
            .tables
            .iter()
            .filter(|table| table.registered_columns.contains_key(column_name));
        let Some(table) = matches.next() else {
            return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "column {column_name} is not registered"
            )));
        };
        if matches.next().is_some() {
            return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "column {column_name} is ambiguous; table_alias is required"
            )));
        }
        Ok(table)
    }

    pub(super) fn resolve_table_for_document_path(
        &self,
        table_alias: Option<&str>,
        document_column: &str,
    ) -> AnalyticsEngineResult<&QueryTable<'a>> {
        if let Some(table_alias) = table_alias {
            let table = self.table_by_alias(table_alias)?;
            validate_document_column(table.table, &table.registered_columns, document_column)?;
            return Ok(table);
        }
        let mut matches = self.tables.iter().filter(|table| {
            validate_document_column(table.table, &table.registered_columns, document_column)
                .is_ok()
        });
        let Some(table) = matches.next() else {
            return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "{document_column} is not a registered document column"
            )));
        };
        if matches.next().is_some() {
            return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "document column {document_column} is ambiguous; table_alias is required"
            )));
        }
        Ok(table)
    }

    pub(super) fn table_by_alias(&self, alias: &str) -> AnalyticsEngineResult<&QueryTable<'a>> {
        self.tables
            .iter()
            .find(|table| table.alias == alias)
            .ok_or_else(|| {
                AnalyticsEngineError::InvalidStructuredQuery(format!(
                    "table alias {alias} is not registered"
                ))
            })
    }

    pub(super) fn primary_table(&self) -> AnalyticsEngineResult<&QueryTable<'a>> {
        self.tables.first().ok_or_else(|| {
            AnalyticsEngineError::InvalidStructuredQuery(
                "structured query has no primary table".to_string(),
            )
        })
    }
}
