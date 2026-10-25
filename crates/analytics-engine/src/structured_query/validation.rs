use super::*;

pub(crate) fn validate_join_policy(table: &TableRegistration) -> AnalyticsEngineResult<()> {
    if table.join_policy.allowed_as_join {
        return Ok(());
    }
    let message = match table.table_scope {
        TableScope::GlobalReference { .. } => format!(
            "joined table {} is not allowed as reference data",
            table.analytics_table_name
        ),
        TableScope::TenantScoped => format!(
            "joined table {} is not allowed by join policy",
            table.analytics_table_name
        ),
    };
    Err(AnalyticsEngineError::InvalidStructuredQuery(message))
}

pub(crate) fn validate_grouped_selects(
    compiler: &QueryCompiler<'_>,
    query: &StructuredQuery,
) -> AnalyticsEngineResult<()> {
    let has_aggregate = query.select.iter().any(is_aggregate_select);
    let has_non_aggregate = query
        .select
        .iter()
        .any(|select| non_aggregate_select_expression(select).is_some());
    if !has_aggregate {
        return Ok(());
    }
    if has_non_aggregate && query.group_by.is_empty() {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(
            "non-aggregate selects must appear in group_by".to_string(),
        ));
    }
    let grouped = query
        .group_by
        .iter()
        .map(|expression| {
            compiler
                .resolve_expression(expression)
                .map(|resolved| resolved.signature)
        })
        .collect::<AnalyticsEngineResult<Vec<_>>>()?;
    for select in &query.select {
        let Some(expression) = non_aggregate_select_expression(select) else {
            continue;
        };
        let signature = compiler.resolve_expression(&expression)?.signature;
        if !grouped.contains(&signature) {
            return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "selected expression {} must appear in group_by",
                select.output_alias()
            )));
        }
    }
    Ok(())
}

pub(crate) fn is_aggregate_select(select: &QuerySelect) -> bool {
    matches!(
        select,
        QuerySelect::Count { .. }
            | QuerySelect::Sum { .. }
            | QuerySelect::Min { .. }
            | QuerySelect::Max { .. }
            | QuerySelect::Avg { .. }
            | QuerySelect::CountDistinct { .. }
    )
}

pub(crate) fn non_aggregate_select_expression(select: &QuerySelect) -> Option<QueryExpression> {
    match select {
        QuerySelect::Column {
            table_alias,
            column_name,
            ..
        } => Some(QueryExpression::Column {
            table_alias: table_alias.clone(),
            column_name: column_name.clone(),
        }),
        QuerySelect::DocumentPath {
            table_alias,
            document_column,
            path,
            ..
        } => Some(QueryExpression::DocumentPath {
            table_alias: table_alias.clone(),
            document_column: document_column.clone(),
            path: path.clone(),
        }),
        QuerySelect::Expression { expression, .. } => Some(expression.clone()),
        QuerySelect::Count { .. }
        | QuerySelect::Sum { .. }
        | QuerySelect::Min { .. }
        | QuerySelect::Max { .. }
        | QuerySelect::Avg { .. }
        | QuerySelect::CountDistinct { .. } => None,
    }
}

pub(crate) fn validate_document_column(
    table: &TableRegistration,
    registered_columns: &std::collections::HashMap<String, AnalyticsColumnType>,
    document_column: &str,
) -> AnalyticsEngineResult<()> {
    validate_registered_column(registered_columns, document_column)?;
    if table.document_column.as_deref() != Some(document_column) {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
            "{document_column} is not the document column for {}",
            table.analytics_table_name
        )));
    }
    Ok(())
}

pub(crate) fn validate_registered_column(
    registered_columns: &std::collections::HashMap<String, AnalyticsColumnType>,
    column_name: &str,
) -> AnalyticsEngineResult<()> {
    if registered_columns.contains_key(column_name) {
        return Ok(());
    }
    Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
        "column {column_name} is not registered"
    )))
}

pub(super) fn reject_conflicting_tenant_predicates(
    query: &StructuredQuery,
    target_tenant_id: &str,
) -> AnalyticsEngineResult<()> {
    for predicate in &query.filters {
        let conflicting = match predicate {
            QueryPredicate::Eq { expression, value } => {
                is_tenant_column_expression(expression) && value.as_str() != Some(target_tenant_id)
            }
            QueryPredicate::NotEq { expression, .. }
            | QueryPredicate::Gt { expression, .. }
            | QueryPredicate::Gte { expression, .. }
            | QueryPredicate::Lt { expression, .. }
            | QueryPredicate::Lte { expression, .. }
            | QueryPredicate::IsNull { expression }
            | QueryPredicate::IsNotNull { expression } => is_tenant_column_expression(expression),
            QueryPredicate::DocumentMatches { .. } => false,
        };
        if conflicting {
            return Err(AnalyticsEngineError::TenantPredicateConflict);
        }
    }
    Ok(())
}

fn is_tenant_column_expression(expression: &QueryExpression) -> bool {
    matches!(
        expression,
        QueryExpression::Column { column_name, .. } if column_name == "tenant_id"
    )
}

pub(super) fn validate_structured_query(query: &StructuredQuery) -> AnalyticsEngineResult<()> {
    query
        .validate_shape()
        .map_err(|err| AnalyticsEngineError::InvalidStructuredQuery(err.to_string()))?;
    if query.limit == Some(0) {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(
            "limit must be greater than zero".to_string(),
        ));
    }
    Ok(())
}
