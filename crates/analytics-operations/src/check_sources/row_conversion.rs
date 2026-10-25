use super::*;

pub(super) fn validate_identifier(identifier: &str) -> CheckResult<()> {
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

pub(super) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(super) fn projected_stream_to_check_rows(
    projection: &CheckRowProjection,
    mut stream: ProjectedCurrentRowStream,
) -> CheckResult<Vec<CheckRow>> {
    let mut rows = Vec::new();
    while let Some(row) = stream.next_row()? {
        rows.push(projected_current_row_to_check_row(projection, row)?);
    }
    Ok(rows)
}

pub(super) struct ProjectedCheckRowStreamSource {
    pub(super) projection: CheckRowProjection,
    pub(super) stream: ProjectedCurrentRowStream,
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

pub(super) fn projected_current_row_to_check_row(
    projection: &CheckRowProjection,
    row: ProjectedCurrentRow,
) -> CheckResult<CheckRow> {
    let mut hash = CheckRowHash::new();
    for column in &projection.hash_columns {
        let value = row.values.get(column).ok_or_else(|| {
            CheckError::InvalidBackendConfiguration(format!(
                "current-row reader for {} did not return projected column {column}",
                projection.table_name
            ))
        })?;
        hash.push_field(value.as_deref());
    }
    Ok(CheckRow {
        table: projection.table_name.clone(),
        key: row.key,
        row_hash: hash.finish(),
        source_position: row.source_position,
        contains_private_data: row.contains_private_data,
    })
}

pub(super) fn sort_projected_pages(
    mut pages: Vec<ProjectedCurrentRowPage>,
) -> Vec<ProjectedCurrentRowPage> {
    let rows = sort_projected_current_rows(
        pages
            .drain(..)
            .flat_map(|page| page.rows)
            .collect::<Vec<_>>(),
    );
    vec![ProjectedCurrentRowPage::terminal(rows)]
}

pub(super) fn sort_projected_current_rows(
    mut rows: Vec<ProjectedCurrentRow>,
) -> Vec<ProjectedCurrentRow> {
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    rows
}

pub(super) fn source_position_json_to_check_position(
    value: &str,
) -> CheckResult<CheckSourcePosition> {
    CheckSourcePosition::parse_source_position(value)
        .map_err(CheckError::InvalidBackendConfiguration)
}

pub(super) fn current_row_cursor_offset(cursor: Option<&str>) -> CheckResult<usize> {
    cursor.map_or(Ok(0), |value| {
        value.parse::<usize>().map_err(|_| {
            CheckError::InvalidBackendConfiguration(format!(
                "production current-row cursor must be a row offset, got {value}"
            ))
        })
    })
}

pub(super) fn current_row_sql_bound(value: usize, name: &str) -> CheckResult<i64> {
    i64::try_from(value).map_err(|_| {
        CheckError::InvalidBackendConfiguration(format!(
            "production current-row {name} exceeds DuckDB integer range"
        ))
    })
}
