use storage_types::VecMap;

use super::{
    CheckResult, CheckRowHash, CheckRowProjection, CheckSourcePosition, ProjectedCurrentRow,
    quote_identifier, source_position_json_to_check_position,
};
use crate::CheckRow;

impl CheckRowProjection {
    pub(super) fn ordered_select_sql(&self) -> String {
        let mut statement = String::from("SELECT CAST(");
        let key = quote_identifier(&self.key_column);
        statement.push_str(&key);
        statement.push_str(" AS VARCHAR)");
        for column in &self.hash_columns {
            statement.push_str(", CAST(");
            statement.push_str(&quote_identifier(column));
            statement.push_str(" AS VARCHAR)");
        }
        statement.push_str(", ");
        if let Some(column) = &self.source_position_column {
            statement.push_str(&quote_identifier(column));
        } else {
            statement.push_str("NULL");
        }
        statement.push_str(", ");
        if let Some(column) = &self.private_data_column {
            statement.push_str("COALESCE(");
            statement.push_str(&quote_identifier(column));
            statement.push_str(", false)");
        } else {
            statement.push_str("false");
        }
        statement.push_str(" FROM ");
        statement.push_str(&quote_identifier(&self.table_name));
        statement.push_str(" ORDER BY ");
        statement.push_str(&key);
        statement
    }

    pub(super) fn read_check_row(&self, row: &duckdb::Row<'_>) -> CheckResult<CheckRow> {
        let mut hash = CheckRowHash::new();
        for index in 1..=self.hash_columns.len() {
            let value = row.get::<_, Option<String>>(index)?;
            hash.push_field(value.as_deref());
        }
        let (source_position, contains_private_data) = self.read_position_and_privacy_flag(row)?;
        Ok(CheckRow {
            table: self.table_name.clone(),
            key: row.get(0)?,
            row_hash: hash.finish(),
            source_position,
            contains_private_data,
        })
    }

    pub(super) fn read_projected_row(
        &self,
        row: &duckdb::Row<'_>,
    ) -> CheckResult<ProjectedCurrentRow> {
        let mut values = VecMap::with_capacity(self.hash_columns.len());
        for (index, column) in self.hash_columns.iter().enumerate() {
            values.insert(column.clone(), row.get::<_, Option<String>>(index + 1)?);
        }
        let (source_position, contains_private_data) = self.read_position_and_privacy_flag(row)?;
        Ok(
            ProjectedCurrentRow::new(row.get::<_, String>(0)?, values, source_position)
                .with_private_data(contains_private_data),
        )
    }

    fn read_position_and_privacy_flag(
        &self,
        row: &duckdb::Row<'_>,
    ) -> CheckResult<(CheckSourcePosition, bool)> {
        let position_json = row.get::<_, Option<String>>(self.hash_columns.len() + 1)?;
        let position = position_json
            .as_deref()
            .map(source_position_json_to_check_position)
            .transpose()?
            .unwrap_or_default();
        Ok((position, row.get(self.hash_columns.len() + 2)?))
    }
}
