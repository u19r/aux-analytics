#[cfg(test)]
use analytics_contract::{AnalyticsColumnType, PrimitiveColumnType, TableRegistration};
#[cfg(test)]
use duckdb::types::TimeUnit;

#[cfg(test)]
use super::catalog::columns_for_registration;
use crate::sql;

pub(crate) fn json_value_to_duckdb_value(value: &serde_json::Value) -> duckdb::types::Value {
    match value {
        serde_json::Value::String(text) => duckdb::types::Value::Text(text.clone()),
        serde_json::Value::Number(number) => duckdb::types::Value::Text(number.to_string()),
        serde_json::Value::Bool(value) => duckdb::types::Value::Boolean(*value),
        serde_json::Value::Null => duckdb::types::Value::Null,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            duckdb::types::Value::Text(value.to_string())
        }
    }
}

#[cfg(test)]
pub(crate) fn json_value_to_duckdb_value_for_column(
    value: &serde_json::Value,
    column_type: Option<&AnalyticsColumnType>,
) -> duckdb::types::Value {
    if matches!(
        column_type,
        Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::Timestamp
        })
    ) {
        return match value {
            serde_json::Value::Number(number) => number.as_i64().map_or_else(
                || json_value_to_duckdb_value(value),
                |millis| duckdb::types::Value::Timestamp(TimeUnit::Millisecond, millis),
            ),
            serde_json::Value::String(text) => text.parse::<i64>().map_or_else(
                |_| json_value_to_duckdb_value(value),
                |millis| duckdb::types::Value::Timestamp(TimeUnit::Millisecond, millis),
            ),
            serde_json::Value::Null => duckdb::types::Value::Null,
            serde_json::Value::Bool(_)
            | serde_json::Value::Array(_)
            | serde_json::Value::Object(_) => json_value_to_duckdb_value(value),
        };
    }
    json_value_to_duckdb_value(value)
}

#[cfg(test)]
pub(crate) fn column_type_for_name(
    table: &TableRegistration,
    column_name: &str,
) -> Option<AnalyticsColumnType> {
    columns_for_registration(table)
        .into_iter()
        .find(|column| column.column_name == column_name)
        .map(|column| column.column_type)
}

pub(crate) fn assignment_set_clause<'a>(columns: impl IntoIterator<Item = &'a str>) -> String {
    let columns = columns.into_iter();
    let mut clause = String::with_capacity(columns.size_hint().0.saturating_mul(8));
    for (index, column) in columns.enumerate() {
        if index > 0 {
            clause.push_str(", ");
        }
        clause.push_str(sql::quote_identifier(column).as_str());
        clause.push_str(" = ?");
    }
    clause
}

pub(crate) fn placeholder_list(count: usize) -> String {
    let mut placeholders = String::with_capacity(count.saturating_mul(3).saturating_sub(2));
    for index in 0..count {
        if index > 0 {
            placeholders.push_str(", ");
        }
        placeholders.push('?');
    }
    placeholders
}
