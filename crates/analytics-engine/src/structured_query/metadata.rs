use super::*;

pub(crate) fn parameter_fingerprint(value: &duckdb::types::Value) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::mem::discriminant(value).hash(&mut hasher);
    match value {
        duckdb::types::Value::Null => {}
        duckdb::types::Value::Boolean(value) => value.hash(&mut hasher),
        duckdb::types::Value::BigInt(value) => value.hash(&mut hasher),
        duckdb::types::Value::UBigInt(value) => value.hash(&mut hasher),
        duckdb::types::Value::Double(value) => {
            let bits = if *value == 0.0 {
                0.0_f64.to_bits()
            } else {
                value.to_bits()
            };
            bits.hash(&mut hasher);
        }
        duckdb::types::Value::Text(value) => value.hash(&mut hasher),
        other => format!("{other:?}").hash(&mut hasher),
    }
    hasher.finish()
}

pub(crate) fn prepared_metadata(
    query: &StructuredQuery,
    result_columns: Vec<PreparedQueryResultColumn>,
) -> AnalyticsEngineResult<PreparedQueryMetadata> {
    Ok(PreparedQueryMetadata {
        query_hash: query.query_hash()?,
        tables: std::iter::once(query.analytics_table_name.clone())
            .chain(
                query
                    .joins
                    .iter()
                    .map(|join| join.analytics_table_name.clone()),
            )
            .collect(),
        limit: Some(query.effective_limit()),
        logical_source_count: bounded_count(query.joins.len().saturating_add(1)),
        join_count: bounded_count(query.joins.len()),
        filter_count: bounded_count(query.filters.len()),
        group_expression_count: bounded_count(query.group_by.len()),
        order_expression_count: bounded_count(query.order_by.len()),
        aggregate_count: bounded_count(
            query
                .select
                .iter()
                .filter(|select| is_aggregate_select(select))
                .count(),
        ),
        conditional_expression_count: bounded_count(
            query
                .select
                .iter()
                .filter_map(select_query_expression)
                .chain(query.filters.iter().filter_map(predicate_query_expression))
                .chain(query.group_by.iter())
                .chain(query.order_by.iter().map(|order| &order.expression))
                .filter(|expression| matches!(expression, QueryExpression::Conditional { .. }))
                .count(),
        ),
        result_columns,
    })
}

pub(crate) fn select_query_expression(select: &QuerySelect) -> Option<&QueryExpression> {
    match select {
        QuerySelect::Expression { expression, .. }
        | QuerySelect::Sum { expression, .. }
        | QuerySelect::Min { expression, .. }
        | QuerySelect::Max { expression, .. }
        | QuerySelect::Avg { expression, .. }
        | QuerySelect::CountDistinct { expression, .. } => Some(expression),
        QuerySelect::Column { .. }
        | QuerySelect::DocumentPath { .. }
        | QuerySelect::Count { .. } => None,
    }
}

pub(crate) fn select_result_source(select: &QuerySelect) -> Option<(Option<&str>, &str)> {
    match select {
        QuerySelect::Column {
            table_alias,
            column_name,
            ..
        } => Some((table_alias.as_deref(), column_name.as_str())),
        QuerySelect::Expression { expression, .. }
        | QuerySelect::Sum { expression, .. }
        | QuerySelect::Min { expression, .. }
        | QuerySelect::Max { expression, .. } => expression_column(expression),
        QuerySelect::DocumentPath { .. }
        | QuerySelect::Count { .. }
        | QuerySelect::Avg { .. }
        | QuerySelect::CountDistinct { .. } => None,
    }
}

pub(crate) fn expression_column(expression: &QueryExpression) -> Option<(Option<&str>, &str)> {
    let QueryExpression::Column {
        table_alias,
        column_name,
    } = expression
    else {
        return None;
    };
    Some((table_alias.as_deref(), column_name.as_str()))
}

pub(crate) fn predicate_query_expression(predicate: &QueryPredicate) -> Option<&QueryExpression> {
    match predicate {
        QueryPredicate::Eq { expression, .. }
        | QueryPredicate::NotEq { expression, .. }
        | QueryPredicate::Gt { expression, .. }
        | QueryPredicate::Gte { expression, .. }
        | QueryPredicate::Lt { expression, .. }
        | QueryPredicate::Lte { expression, .. }
        | QueryPredicate::IsNull { expression }
        | QueryPredicate::IsNotNull { expression } => Some(expression),
        QueryPredicate::DocumentMatches { .. } => None,
    }
}

pub(crate) fn bounded_count(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

impl<'a> QueryTable<'a> {
    pub(super) fn new(alias: String, table: &'a TableRegistration, render_alias: bool) -> Self {
        Self {
            alias,
            table,
            registered_columns: registered_column_names(table),
            render_alias,
        }
    }

    pub(super) fn table_sql(&self) -> String {
        let table_name = sql::quote_identifier(self.table.analytics_table_name.as_str());
        if self.render_alias {
            return format!(
                "{table_name} AS {}",
                sql::quote_identifier(self.alias.as_str())
            );
        }
        table_name
    }

    pub(super) fn column_sql(&self, column_name: &str) -> String {
        let column = sql::quote_identifier(column_name);
        if self.render_alias {
            return format!("{}.{}", sql::quote_identifier(self.alias.as_str()), column);
        }
        column
    }
}

pub(crate) fn registered_column_names(
    table: &TableRegistration,
) -> std::collections::HashMap<String, AnalyticsColumnType> {
    let string_type = AnalyticsColumnType::Primitive {
        primitive: PrimitiveColumnType::VarChar,
    };
    let mut columns = std::collections::HashMap::from([
        ("tenant_id".to_string(), string_type.clone()),
        ("__id".to_string(), string_type.clone()),
        ("table_name".to_string(), string_type),
    ]);
    for column in columns_for_registration(table) {
        columns.insert(column.column_name, column.column_type);
    }
    columns
}

pub(crate) fn prepared_result_value_type(
    column_type: &AnalyticsColumnType,
) -> Option<PreparedQueryResultValueType> {
    let AnalyticsColumnType::Primitive { primitive } = column_type else {
        return None;
    };
    match primitive {
        PrimitiveColumnType::BigInt => Some(PreparedQueryResultValueType::I64),
        PrimitiveColumnType::UnsignedBigInt => Some(PreparedQueryResultValueType::U64),
        PrimitiveColumnType::Decimal => Some(PreparedQueryResultValueType::Decimal),
        PrimitiveColumnType::Double => Some(PreparedQueryResultValueType::F64),
        PrimitiveColumnType::Boolean
        | PrimitiveColumnType::VarChar
        | PrimitiveColumnType::Json
        | PrimitiveColumnType::StorageJson
        | PrimitiveColumnType::Timestamp => None,
    }
}

pub(crate) fn find_registered_table<'a>(
    manifest: &'a AnalyticsManifest,
    analytics_table_name: &str,
) -> AnalyticsEngineResult<&'a TableRegistration> {
    manifest
        .tables
        .iter()
        .find(|table| table.analytics_table_name == analytics_table_name)
        .ok_or_else(|| AnalyticsEngineError::TableNotRegistered(analytics_table_name.to_string()))
}

#[derive(Debug, Clone)]
pub struct PreparedQueryMetadata {
    pub query_hash: String,
    pub tables: Vec<String>,
    pub limit: Option<u32>,
    pub logical_source_count: u32,
    pub join_count: u32,
    pub filter_count: u32,
    pub group_expression_count: u32,
    pub order_expression_count: u32,
    pub aggregate_count: u32,
    pub conditional_expression_count: u32,
    pub result_columns: Vec<PreparedQueryResultColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedQueryResultColumn {
    pub name: String,
    pub value_type: PreparedQueryResultValueType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedQueryResultValueType {
    I64,
    U64,
    Decimal,
    F64,
}

impl PreparedQueryMetadata {
    pub fn from_query(query: &StructuredQuery) -> AnalyticsEngineResult<Self> {
        prepared_metadata(query, Vec::new())
    }
}
