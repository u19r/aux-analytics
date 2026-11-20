use super::*;

pub(crate) fn parameter_fingerprint(value: &duckdb::types::Value) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hash_parameter_value(value, &mut hasher);
    hasher.finish()
}

fn hash_parameter_value(value: &duckdb::types::Value, hasher: &mut impl Hasher) {
    use duckdb::types::Value;

    match value {
        Value::Null => hasher.write_u8(0),
        Value::Boolean(value) => {
            hasher.write_u8(1);
            hasher.write_u8(u8::from(*value));
        }
        Value::TinyInt(value) => {
            hasher.write_u8(2);
            hasher.write(&value.to_le_bytes());
        }
        Value::SmallInt(value) => {
            hasher.write_u8(3);
            hasher.write(&value.to_le_bytes());
        }
        Value::Int(value) => {
            hasher.write_u8(4);
            hasher.write(&value.to_le_bytes());
        }
        Value::BigInt(value) => {
            hasher.write_u8(5);
            hasher.write(&value.to_le_bytes());
        }
        Value::HugeInt(value) => {
            hasher.write_u8(6);
            hasher.write(&value.to_le_bytes());
        }
        Value::UHugeInt(value) => {
            hasher.write_u8(7);
            hasher.write(&value.to_le_bytes());
        }
        Value::UTinyInt(value) => {
            hasher.write_u8(8);
            hasher.write(&value.to_le_bytes());
        }
        Value::USmallInt(value) => {
            hasher.write_u8(9);
            hasher.write(&value.to_le_bytes());
        }
        Value::UInt(value) => {
            hasher.write_u8(10);
            hasher.write(&value.to_le_bytes());
        }
        Value::UBigInt(value) => {
            hasher.write_u8(11);
            hasher.write(&value.to_le_bytes());
        }
        Value::Float(value) => {
            hasher.write_u8(12);
            let bits = if *value == 0.0 {
                0.0_f32.to_bits()
            } else {
                value.to_bits()
            };
            hasher.write(&bits.to_le_bytes());
        }
        Value::Double(value) => {
            hasher.write_u8(13);
            let bits = if *value == 0.0 {
                0.0_f64.to_bits()
            } else {
                value.to_bits()
            };
            hasher.write(&bits.to_le_bytes());
        }
        Value::Decimal(value) => {
            hasher.write_u8(14);
            value.hash(hasher);
        }
        Value::Timestamp(unit, value) => {
            hasher.write_u8(15);
            unit.hash(hasher);
            hasher.write(&value.to_le_bytes());
        }
        Value::Text(value) => {
            hasher.write_u8(16);
            hash_length_prefixed(value.as_bytes(), hasher);
        }
        Value::Blob(value) => {
            hasher.write_u8(17);
            hash_length_prefixed(value, hasher);
        }
        Value::Geometry(value) => {
            hasher.write_u8(18);
            hash_length_prefixed(value, hasher);
        }
        Value::Date32(value) => {
            hasher.write_u8(19);
            hasher.write(&value.to_le_bytes());
        }
        Value::Time64(unit, value) => {
            hasher.write_u8(20);
            unit.hash(hasher);
            hasher.write(&value.to_le_bytes());
        }
        Value::Interval {
            months,
            days,
            nanos,
        } => {
            hasher.write_u8(21);
            hasher.write(&months.to_le_bytes());
            hasher.write(&days.to_le_bytes());
            hasher.write(&nanos.to_le_bytes());
        }
        Value::List(values) => {
            hasher.write_u8(22);
            hash_value_sequence(values, hasher);
        }
        Value::Enum(value) => {
            hasher.write_u8(23);
            hash_length_prefixed(value.as_bytes(), hasher);
        }
        Value::Struct(fields) => {
            hasher.write_u8(24);
            hasher.write(&fields.iter().count().to_le_bytes());
            for (name, field) in fields.iter() {
                hash_length_prefixed(name.as_bytes(), hasher);
                hash_parameter_value(field, hasher);
            }
        }
        Value::Array(values) => {
            hasher.write_u8(25);
            hash_value_sequence(values, hasher);
        }
        Value::Map(entries) => {
            hasher.write_u8(26);
            hasher.write(&entries.iter().count().to_le_bytes());
            for (key, entry) in entries.iter() {
                hash_parameter_value(key, hasher);
                hash_parameter_value(entry, hasher);
            }
        }
        Value::Union(inner) => {
            hasher.write_u8(27);
            hash_parameter_value(inner, hasher);
        }
        // `Value` is non-exhaustive; a collision is safe because equality
        // controls reuse.
        _ => hasher.write_u8(u8::MAX),
    }
}

fn hash_length_prefixed(bytes: &[u8], hasher: &mut impl Hasher) {
    hasher.write(&bytes.len().to_le_bytes());
    hasher.write(bytes);
}

fn hash_value_sequence(values: &[duckdb::types::Value], hasher: &mut impl Hasher) {
    hasher.write(&values.len().to_le_bytes());
    for value in values {
        hash_parameter_value(value, hasher);
    }
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
