use analytics_contract::{
    QueryExpression, QueryOrder, QueryPredicate, QuerySelect, SortOrder, StructuredQuery,
    TableRegistration,
};

use crate::{
    engine::{AnalyticsEngineError, AnalyticsEngineResult, columns_for_registration},
    sql,
};

fn registered_column_names(table: &TableRegistration) -> std::collections::HashSet<String> {
    let mut names = std::collections::HashSet::from([
        "tenant_id".to_string(),
        "__id".to_string(),
        "table_name".to_string(),
    ]);
    for column in columns_for_registration(table) {
        names.insert(column.column_name);
    }
    names
}

pub(crate) fn structured_query_sql(
    table: &TableRegistration,
    query: &StructuredQuery,
) -> AnalyticsEngineResult<String> {
    structured_query_sql_with_tenant(table, query, None)
}

pub(crate) fn tenant_scoped_structured_query_sql(
    table: &TableRegistration,
    query: &StructuredQuery,
    target_tenant_id: &str,
) -> AnalyticsEngineResult<String> {
    if target_tenant_id.is_empty() {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(
            "target tenant id is required".to_string(),
        ));
    }
    structured_query_sql_with_tenant(table, query, Some(target_tenant_id))
}

fn structured_query_sql_with_tenant(
    table: &TableRegistration,
    query: &StructuredQuery,
    target_tenant_id: Option<&str>,
) -> AnalyticsEngineResult<String> {
    let registered_columns = registered_column_names(table);
    let select_clause = query
        .select
        .iter()
        .map(|select| select_sql(table, &registered_columns, select))
        .collect::<AnalyticsEngineResult<Vec<_>>>()?
        .join(", ");
    let mut sql = format!(
        "SELECT {select_clause} FROM {}",
        sql::quote_identifier(table.analytics_table_name.as_str())
    );
    let mut filters = query
        .filters
        .iter()
        .map(|predicate| predicate_sql(table, &registered_columns, predicate))
        .collect::<AnalyticsEngineResult<Vec<_>>>()?;
    if let Some(target_tenant_id) = target_tenant_id {
        filters.push(format!(
            "{} = {}",
            sql::quote_identifier("tenant_id"),
            literal_sql(&serde_json::Value::String(target_tenant_id.to_string()))?
        ));
    }
    if !filters.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(filters.join(" AND ").as_str());
    }
    if !query.group_by.is_empty() {
        let group_by = query
            .group_by
            .iter()
            .map(|expression| expression_sql(table, &registered_columns, expression))
            .collect::<AnalyticsEngineResult<Vec<_>>>()?
            .join(", ");
        sql.push_str(" GROUP BY ");
        sql.push_str(group_by.as_str());
    }
    if !query.order_by.is_empty() {
        let order_by = query
            .order_by
            .iter()
            .map(|order| order_sql(table, &registered_columns, order))
            .collect::<AnalyticsEngineResult<Vec<_>>>()?
            .join(", ");
        sql.push_str(" ORDER BY ");
        sql.push_str(order_by.as_str());
    }
    if let Some(limit) = query.limit {
        sql.push_str(" LIMIT ");
        sql.push_str(limit.to_string().as_str());
    }
    Ok(sql)
}

fn select_sql(
    table: &TableRegistration,
    registered_columns: &std::collections::HashSet<String>,
    select: &QuerySelect,
) -> AnalyticsEngineResult<String> {
    match select {
        QuerySelect::Column { column_name, alias } => {
            validate_registered_column(registered_columns, column_name)?;
            let expression = sql::quote_identifier(column_name);
            Ok(alias_sql(
                expression.as_str(),
                alias.as_deref().unwrap_or(column_name),
            ))
        }
        QuerySelect::DocumentPath {
            document_column,
            path,
            alias,
        } => {
            let expression = document_path_sql(table, registered_columns, document_column, path)?;
            Ok(alias_sql(expression.as_str(), alias))
        }
        QuerySelect::Count { alias } => Ok(alias_sql("count(*)", alias)),
    }
}

fn predicate_sql(
    table: &TableRegistration,
    registered_columns: &std::collections::HashSet<String>,
    predicate: &QueryPredicate,
) -> AnalyticsEngineResult<String> {
    match predicate {
        QueryPredicate::Eq { expression, value } => {
            binary_predicate_sql(table, registered_columns, expression, "=", value)
        }
        QueryPredicate::NotEq { expression, value } => {
            binary_predicate_sql(table, registered_columns, expression, "<>", value)
        }
        QueryPredicate::Gt { expression, value } => {
            binary_predicate_sql(table, registered_columns, expression, ">", value)
        }
        QueryPredicate::Gte { expression, value } => {
            binary_predicate_sql(table, registered_columns, expression, ">=", value)
        }
        QueryPredicate::Lt { expression, value } => {
            binary_predicate_sql(table, registered_columns, expression, "<", value)
        }
        QueryPredicate::Lte { expression, value } => {
            binary_predicate_sql(table, registered_columns, expression, "<=", value)
        }
        QueryPredicate::IsNull { expression } => Ok(format!(
            "{} IS NULL",
            expression_sql(table, registered_columns, expression)?
        )),
        QueryPredicate::IsNotNull { expression } => Ok(format!(
            "{} IS NOT NULL",
            expression_sql(table, registered_columns, expression)?
        )),
    }
}

fn binary_predicate_sql(
    table: &TableRegistration,
    registered_columns: &std::collections::HashSet<String>,
    expression: &QueryExpression,
    operator: &str,
    value: &serde_json::Value,
) -> AnalyticsEngineResult<String> {
    Ok(format!(
        "{} {operator} {}",
        expression_sql(table, registered_columns, expression)?,
        literal_sql(value)?
    ))
}

fn order_sql(
    table: &TableRegistration,
    registered_columns: &std::collections::HashSet<String>,
    order: &QueryOrder,
) -> AnalyticsEngineResult<String> {
    let direction = match order.direction {
        Some(SortOrder::Desc) => "DESC",
        Some(SortOrder::Asc) | None => "ASC",
    };
    Ok(format!(
        "{} {direction}",
        expression_sql(table, registered_columns, &order.expression)?
    ))
}

fn expression_sql(
    table: &TableRegistration,
    registered_columns: &std::collections::HashSet<String>,
    expression: &QueryExpression,
) -> AnalyticsEngineResult<String> {
    match expression {
        QueryExpression::Column { column_name } => {
            validate_registered_column(registered_columns, column_name)?;
            Ok(sql::quote_identifier(column_name))
        }
        QueryExpression::DocumentPath {
            document_column,
            path,
        } => document_path_sql(table, registered_columns, document_column, path),
    }
}

fn document_path_sql(
    table: &TableRegistration,
    registered_columns: &std::collections::HashSet<String>,
    document_column: &str,
    path: &str,
) -> AnalyticsEngineResult<String> {
    validate_registered_column(registered_columns, document_column)?;
    if table.document_column.as_deref() != Some(document_column) {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
            "{document_column} is not the document column for {}",
            table.analytics_table_name
        )));
    }
    let json_path = format!("$.{path}");
    Ok(format!(
        "json_extract_string({}, '{}')",
        sql::quote_identifier(document_column),
        escape_sql_string(json_path.as_str())
    ))
}

fn validate_registered_column(
    registered_columns: &std::collections::HashSet<String>,
    column_name: &str,
) -> AnalyticsEngineResult<()> {
    if registered_columns.contains(column_name) {
        return Ok(());
    }
    Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
        "column {column_name} is not registered"
    )))
}

fn alias_sql(expression: &str, alias: &str) -> String {
    format!("{expression} AS {}", sql::quote_identifier(alias))
}

fn literal_sql(value: &serde_json::Value) -> AnalyticsEngineResult<String> {
    match value {
        serde_json::Value::Null => Ok("NULL".to_string()),
        serde_json::Value::Bool(value) => Ok(if *value { "TRUE" } else { "FALSE" }.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::String(value) => Ok(format!("'{}'", escape_sql_string(value))),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let encoded = serde_json::to_string(value)?;
            Ok(format!("'{}'::JSON", escape_sql_string(encoded.as_str())))
        }
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
