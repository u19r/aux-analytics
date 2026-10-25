use super::*;

#[derive(Debug)]
pub struct PreparedStructuredQuery {
    pub(crate) sql: String,
    pub(crate) parameters: Vec<duckdb::types::Value>,
    metadata: PreparedQueryMetadata,
}

impl PreparedStructuredQuery {
    #[must_use]
    pub fn metadata(&self) -> &PreparedQueryMetadata {
        &self.metadata
    }
}

pub fn prepare_unscoped_structured_query(
    manifest: &AnalyticsManifest,
    query: &StructuredQuery,
) -> AnalyticsEngineResult<PreparedStructuredQuery> {
    validate_structured_query(query)?;
    prepare_validated_structured_query(manifest, query, None)
}

pub fn prepare_tenant_structured_query(
    manifest: &AnalyticsManifest,
    query: &StructuredQuery,
    target_tenant_id: &str,
) -> AnalyticsEngineResult<PreparedStructuredQuery> {
    validate_structured_query(query)?;
    // Surface a conflicting tenant predicate even when the target itself is
    // missing, but only after structural validation.  The structured boundary
    // retains evidence that a cross-tenant predicate was attempted; callers
    // still reject the request as missing its required target when they
    // classify the result.
    reject_conflicting_tenant_predicates(query, target_tenant_id)?;
    if target_tenant_id.is_empty() {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(
            "target tenant id is required".to_string(),
        ));
    }
    prepare_validated_structured_query(manifest, query, Some(target_tenant_id))
}

fn prepare_validated_structured_query(
    manifest: &AnalyticsManifest,
    query: &StructuredQuery,
    target_tenant_id: Option<&str>,
) -> AnalyticsEngineResult<PreparedStructuredQuery> {
    let compiler = QueryCompiler::try_new(manifest, query, true)?;
    let sql = compiler.compile(query, target_tenant_id)?;
    let result_columns = compiler.result_columns(query)?;
    let parameters = compiler.parameters.into_inner();
    Ok(PreparedStructuredQuery {
        sql,
        parameters,
        metadata: prepared_metadata(query, result_columns)?,
    })
}
