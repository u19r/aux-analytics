use super::*;

#[cfg(test)]
pub(crate) fn structured_query_sql(
    table: &TableRegistration,
    query: &StructuredQuery,
) -> AnalyticsEngineResult<String> {
    let manifest = AnalyticsManifest::new(vec![table.clone()]);
    structured_query_sql_for_manifest(&manifest, query)
}

#[cfg(test)]
pub(crate) fn tenant_scoped_structured_query_sql(
    table: &TableRegistration,
    query: &StructuredQuery,
    target_tenant_id: &str,
) -> AnalyticsEngineResult<String> {
    let manifest = AnalyticsManifest::new(vec![table.clone()]);
    tenant_scoped_structured_query_sql_for_manifest(&manifest, query, target_tenant_id)
}

#[cfg(test)]
pub(crate) fn structured_query_sql_for_manifest(
    manifest: &AnalyticsManifest,
    query: &StructuredQuery,
) -> AnalyticsEngineResult<String> {
    QueryCompiler::try_new(manifest, query, false)?.compile(query, None)
}

#[cfg(test)]
pub(crate) fn tenant_scoped_structured_query_sql_for_manifest(
    manifest: &AnalyticsManifest,
    query: &StructuredQuery,
    target_tenant_id: &str,
) -> AnalyticsEngineResult<String> {
    if target_tenant_id.is_empty() {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(
            "target tenant id is required".to_string(),
        ));
    }
    QueryCompiler::try_new(manifest, query, false)?.compile(query, Some(target_tenant_id))
}

#[cfg(test)]
pub(crate) fn performance_resolve_table_for_column(
    manifest: &AnalyticsManifest,
    query: &StructuredQuery,
    column_name: &str,
    optimized: bool,
) -> AnalyticsEngineResult<String> {
    let compiler = QueryCompiler::try_new(manifest, query, false)?;
    let alias = if optimized {
        compiler
            .resolve_table_for_column(None, column_name)?
            .alias
            .clone()
    } else {
        let matches = compiler
            .tables
            .iter()
            .filter(|table| table.registered_columns.contains_key(column_name))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [table] => table.alias.clone(),
            [] => {
                return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                    "column {column_name} is not registered"
                )));
            }
            _ => {
                return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                    "column {column_name} is ambiguous; table_alias is required"
                )));
            }
        }
    };
    Ok(alias)
}

#[cfg(test)]
pub(crate) fn performance_resolve_table_for_document_path(
    manifest: &AnalyticsManifest,
    query: &StructuredQuery,
    document_column: &str,
    optimized: bool,
) -> AnalyticsEngineResult<String> {
    let compiler = QueryCompiler::try_new(manifest, query, false)?;
    let alias = if optimized {
        compiler
            .resolve_table_for_document_path(None, document_column)?
            .alias
            .clone()
    } else {
        let matches = compiler
            .tables
            .iter()
            .filter(|table| {
                validate_document_column(table.table, &table.registered_columns, document_column)
                    .is_ok()
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [table] => table.alias.clone(),
            [] => {
                return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                    "{document_column} is not a registered document column"
                )));
            }
            _ => {
                return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                    "document column {document_column} is ambiguous; table_alias is required"
                )));
            }
        }
    };
    Ok(alias)
}
