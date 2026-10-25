#[cfg(test)]
use std::collections::BTreeMap;
use std::fmt::Write;

use analytics_contract::{PrivacyPolicyMatcher, StorageStreamRecord, TableRegistration};

#[cfg(test)]
use super::{column_type_for_name, json_value_to_duckdb_value_for_column};
use crate::sql;

pub(crate) struct PrivacyRowIdentity<'a> {
    pub(crate) table_name: &'a str,
    pub(crate) tenant_id: &'a str,
    pub(crate) row_id: &'a str,
}

#[cfg(test)]
pub(crate) fn update_row_values_for_test(
    table: &TableRegistration,
    row: &BTreeMap<String, serde_json::Value>,
    update_columns: &[&str],
) -> Vec<duckdb::types::Value> {
    let mut values = Vec::with_capacity(update_columns.len() + 3);
    values.extend(update_columns.iter().map(|column| {
        json_value_to_duckdb_value_for_column(
            row.get(*column).unwrap_or(&serde_json::Value::Null),
            column_type_for_name(table, column).as_ref(),
        )
    }));
    values
}

pub(crate) struct FilteredStreamRecord {
    pub(crate) record: StorageStreamRecord,
    pub(crate) dropped_fields: u64,
}

pub(crate) fn filter_stream_record(
    mut record: StorageStreamRecord,
    matcher: &PrivacyPolicyMatcher<'_>,
) -> FilteredStreamRecord {
    let mut dropped_fields = 0_u64;
    let keys = matcher.filter_item(std::mem::take(&mut record.keys));
    dropped_fields += keys.dropped_fields;
    record.keys = keys.item;
    if let Some(old_image) = record.old_image.take() {
        let filtered = matcher.filter_item(old_image);
        dropped_fields += filtered.dropped_fields;
        record.old_image = Some(filtered.item);
    }
    if let Some(new_image) = record.new_image.take() {
        let filtered = matcher.filter_item(new_image);
        dropped_fields += filtered.dropped_fields;
        record.new_image = Some(filtered.item);
    }
    FilteredStreamRecord {
        record,
        dropped_fields,
    }
}

pub(crate) fn privacy_scrubbable_projection_columns<'a>(
    table: &'a TableRegistration,
    matcher: &PrivacyPolicyMatcher<'_>,
) -> Vec<&'a str> {
    let mut columns = Vec::new();
    if let Some(projection_columns) = table.projection_columns.as_ref() {
        for column in projection_columns {
            if matcher.denies_key(column.column_name.as_str())
                || matcher.denies_path(column.attribute_path.as_str())
            {
                columns.push(column.column_name.as_str());
            }
        }
    }
    if let Some(attribute_names) = table.projection_attribute_names.as_ref() {
        for column in attribute_names {
            if matcher.denies_key(column.as_str()) {
                columns.push(column.as_str());
            }
        }
    }
    columns
}

pub(crate) fn privacy_remediation_scan_sql(
    table_name: &str,
    projection_columns: &[&str],
    document_column: Option<&str>,
) -> String {
    let mut statement = String::from("SELECT table_name, tenant_id, __id");
    let quoted_columns = projection_columns
        .iter()
        .map(|column| sql::quote_identifier(column))
        .collect::<Vec<_>>();
    for column in &quoted_columns {
        statement.push_str(", (");
        statement.push_str(column);
        statement.push_str(" IS NOT NULL)");
    }
    if let Some(column) = document_column {
        statement.push_str(", CAST(");
        statement.push_str(&sql::quote_identifier(column));
        statement.push_str(" AS VARCHAR)");
    }
    statement.push_str(" FROM ");
    statement.push_str(&sql::quote_identifier(table_name));
    if document_column.is_none() && !quoted_columns.is_empty() {
        statement.push_str(" WHERE ");
        for (index, column) in quoted_columns.iter().enumerate() {
            if index > 0 {
                statement.push_str(" OR ");
            }
            statement.push_str(column);
            statement.push_str(" IS NOT NULL");
        }
    }
    statement
}

pub(super) fn filter_json_value(
    value: serde_json::Value,
    path: &str,
    matcher: &PrivacyPolicyMatcher<'_>,
    dropped_fields: &mut u64,
) -> serde_json::Value {
    if (!path.is_empty() && matcher.denies_path(path))
        || value
            .as_str()
            .is_some_and(|value| matcher.denies_value(value))
    {
        *dropped_fields += 1;
        return serde_json::Value::Null;
    }
    match value {
        serde_json::Value::Object(object) => {
            let mut filtered = serde_json::Map::new();
            for (key, value) in object {
                if matcher.denies_key(key.as_str()) {
                    *dropped_fields += 1;
                    continue;
                }
                let child_path = build_privacy_child_path(path, key.as_str());
                let before = *dropped_fields;
                let value = filter_json_value(value, child_path.as_str(), matcher, dropped_fields);
                if value.is_null() && *dropped_fields > before {
                    continue;
                }
                filtered.insert(key, value);
            }
            serde_json::Value::Object(filtered)
        }
        serde_json::Value::Array(values) => {
            filter_json_array(values, path, matcher, dropped_fields)
        }
        value => value,
    }
}

fn filter_json_array(
    values: Vec<serde_json::Value>,
    path: &str,
    matcher: &PrivacyPolicyMatcher<'_>,
    dropped_fields: &mut u64,
) -> serde_json::Value {
    let mut filtered = Vec::with_capacity(values.len());
    let mut child_path = path.to_string();
    for (index, value) in values.into_iter().enumerate() {
        write!(child_path, "[{index}]").expect("writing a path to a String cannot fail");
        let before = *dropped_fields;
        let value = filter_json_value(value, child_path.as_str(), matcher, dropped_fields);
        child_path.truncate(path.len());
        if !(value.is_null() && *dropped_fields > before) {
            filtered.push(value);
        }
    }
    serde_json::Value::Array(filtered)
}

fn build_privacy_child_path(path: &str, key: &str) -> String {
    if path.is_empty() {
        return key.to_string();
    }
    let mut child_path = String::with_capacity(path.len() + 1 + key.len());
    child_path.push_str(path);
    child_path.push('.');
    child_path.push_str(key);
    child_path
}
