#[cfg(test)]
use std::collections::BTreeMap;

use analytics_contract::{
    AnalyticsColumnType, PrimitiveColumnType, SourcePosition, StorageItem, StorageValue,
    TableRegistration,
};
#[cfg(test)]
use analytics_contract::{
    INTERNAL_EXPIRY_COLUMN, INTERNAL_INGESTED_AT_COLUMN, INTERNAL_MISSING_RETENTION_COLUMN,
};
use duckdb::types::{TimeUnit, Value};

use super::{
    identity::row_id_from_record,
    materialization::{RowSource, retention_basis_ms, storage_item_to_json_text},
};
#[cfg(test)]
use crate::engine::{column_type_for_name, json_value_to_duckdb_value_for_column};
use crate::{
    batch_writer::{batch_columns, is_batch_owned_column},
    constants::BATCH_STAGE_METADATA_COLUMNS,
    engine::{AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, IngestRetention},
    projection,
};

pub(crate) struct BatchRow {
    values: Vec<Value>,
    has_retention_columns: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchRowIdentity<'row> {
    pub table_name: &'row str,
    pub tenant_id: &'row str,
    pub row_id: &'row str,
}

impl BatchRow {
    fn new(values: Vec<Value>, has_retention_columns: bool) -> Self {
        Self {
            values,
            has_retention_columns,
        }
    }

    pub(crate) fn deletion_identity(
        table: &TableRegistration,
        identity: BatchRowIdentity<'_>,
    ) -> Self {
        let mut values = vec![Value::Null; batch_columns(table, false).len()];
        values[0] = Value::Text(identity.table_name.to_string());
        values[1] = Value::Text(identity.tenant_id.to_string());
        values[2] = Value::Text(identity.row_id.to_string());
        Self::new(values, false)
    }

    pub(crate) fn has_retention_columns(&self) -> bool {
        self.has_retention_columns
    }

    pub(crate) fn identity(&self) -> AnalyticsEngineResult<BatchRowIdentity<'_>> {
        Ok(BatchRowIdentity {
            table_name: identity_value(&self.values, 0, "table_name")?,
            tenant_id: identity_value(&self.values, 1, "tenant_id")?,
            row_id: identity_value(&self.values, 2, "__id")?,
        })
    }

    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        self.values.get(..3) == other.values.get(..3)
    }

    pub(crate) fn into_values(
        mut self,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<Vec<Value>> {
        self.write_source_position(source_position)?;
        Ok(self.values)
    }

    pub(crate) fn values(&self) -> &[Value] {
        self.values.as_slice()
    }

    pub(crate) fn set_source_position(
        &mut self,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<()> {
        self.write_source_position(source_position)?;
        Ok(())
    }

    fn write_source_position(
        &mut self,
        source_position: Option<&SourcePosition>,
    ) -> AnalyticsEngineResult<()> {
        self.values[3] = source_position
            .map(serde_json::to_string)
            .transpose()?
            .map(Value::Text)
            .unwrap_or(Value::Null);
        Ok(())
    }
}

fn identity_value<'a>(
    values: &'a [Value],
    index: usize,
    column: &'static str,
) -> AnalyticsEngineResult<&'a str> {
    match values.get(index) {
        Some(Value::Text(value)) => Ok(value),
        _ => Err(AnalyticsEngineError::MissingIdentifier(column)),
    }
}

impl AnalyticsEngine {
    #[cfg(test)]
    pub(crate) fn batch_row_from_json(
        &self,
        table: &TableRegistration,
        row: &BTreeMap<String, serde_json::Value>,
    ) -> AnalyticsEngineResult<BatchRow> {
        let has_retention_columns =
            table.retention.is_some() || row.contains_key(INTERNAL_INGESTED_AT_COLUMN);
        let columns = batch_columns(table, has_retention_columns);
        let mut values = Vec::with_capacity(columns.len());
        values.extend([
            Value::Text(json_identity_value(row, "table_name")?),
            Value::Text(json_identity_value(row, "tenant_id")?),
            Value::Text(json_identity_value(row, "__id")?),
            Value::Null,
        ]);
        values.extend(columns.iter().skip(4).map(|column| {
            json_batch_value(
                table,
                column,
                row.get(column).unwrap_or(&serde_json::Value::Null),
            )
        }));
        Ok(BatchRow::new(values, has_retention_columns))
    }

    pub(crate) fn batch_row_from_item(
        &self,
        table: &TableRegistration,
        source: RowSource<'_>,
        item: &StorageItem,
        ingested_at_ms: i64,
        retention: Option<&IngestRetention>,
    ) -> AnalyticsEngineResult<BatchRow> {
        #[cfg(test)]
        if crate::engine::ingestion_test_support::is_json_bridge_active() {
            let row = self.row_from_item(table, source, item, ingested_at_ms, retention)?;
            return self.batch_row_from_json(table, &row);
        }
        let tenant_id = self
            .tenant_resolver
            .resolve(table, source.source_table_name, item)?;
        let row_id = row_id_from_record(table, source, item, &mut self.caches.borrow_mut())?;
        let has_retention_columns = table.retention.is_some() || retention.is_some();
        let mut values = Vec::with_capacity(batch_row_capacity(table, has_retention_columns));
        values.extend([
            Value::Text(source.source_table_name.to_string()),
            Value::Text(tenant_id.unwrap_or_default()),
            Value::Text(row_id),
            Value::Null,
        ]);
        self.append_batch_columns(table, item, &mut values)?;
        if has_retention_columns {
            if let Some(retention) = retention {
                append_batch_retention_columns(&mut values, item, ingested_at_ms, retention)?;
            } else {
                values.extend([Value::Null, Value::Null, Value::Null]);
            }
        }
        Ok(BatchRow::new(values, has_retention_columns))
    }
}

#[cfg(test)]
fn json_identity_value(
    row: &BTreeMap<String, serde_json::Value>,
    column: &'static str,
) -> AnalyticsEngineResult<String> {
    row.get(column)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or(AnalyticsEngineError::MissingIdentifier(column))
}

#[cfg(test)]
fn json_batch_value(table: &TableRegistration, column: &str, value: &serde_json::Value) -> Value {
    match column {
        INTERNAL_INGESTED_AT_COLUMN | INTERNAL_EXPIRY_COLUMN => value
            .as_i64()
            .map(Value::BigInt)
            .unwrap_or_else(|| crate::engine::json_value_to_duckdb_value(value)),
        INTERNAL_MISSING_RETENTION_COLUMN => value
            .as_bool()
            .map(Value::Boolean)
            .unwrap_or_else(|| crate::engine::json_value_to_duckdb_value(value)),
        _ => json_value_to_duckdb_value_for_column(
            value,
            column_type_for_name(table, column).as_ref(),
        ),
    }
}

fn batch_row_capacity(table: &TableRegistration, has_retention: bool) -> usize {
    BATCH_STAGE_METADATA_COLUMNS
        + 4
        + usize::from(table.document_column.is_some())
        + if !table.columns.is_empty() {
            table.columns.len()
        } else if let Some(columns) = table.projection_columns.as_deref() {
            columns.len()
        } else {
            table
                .projection_attribute_names
                .as_ref()
                .map_or(0, Vec::len)
        }
        + 3 * usize::from(has_retention)
}

impl AnalyticsEngine {
    fn append_batch_columns(
        &self,
        table: &TableRegistration,
        item: &StorageItem,
        values: &mut Vec<Value>,
    ) -> AnalyticsEngineResult<()> {
        if let Some(document_column) = table.document_column.as_deref()
            && !is_batch_owned_column(table, document_column)
        {
            values.push(Value::Text(storage_item_to_json_text(item)?));
        }
        if !table.columns.is_empty() {
            return self.append_declared_columns(table, item, values);
        }
        if table.projection_columns.is_some() {
            return self.append_projected_columns(table, item, values);
        }
        if let Some(attribute_names) = table.projection_attribute_names.as_deref() {
            validate_selected_attributes(item, attribute_names)?;
            let column_type = default_varchar_column_type();
            for name in attribute_names {
                if BatchRow::should_append_projection_column(table, name) {
                    append_batch_value(name, &column_type, item.get(name), values)?;
                }
            }
        }
        Ok(())
    }

    fn append_declared_columns(
        &self,
        table: &TableRegistration,
        item: &StorageItem,
        values: &mut Vec<Value>,
    ) -> AnalyticsEngineResult<()> {
        let first_column = values.len();
        values.extend(
            table
                .columns
                .iter()
                .filter(|column| {
                    BatchRow::should_append_projection_column(table, &column.column_name)
                })
                .map(|_| Value::Null),
        );
        self.for_each_projected_attribute(table, item, |name, value| {
            if contains_unsupported_null(value) {
                return Err(AnalyticsEngineError::AttributeConversion(name.to_string()));
            }
            for (index, column) in table
                .columns
                .iter()
                .filter(|column| {
                    BatchRow::should_append_projection_column(table, &column.column_name)
                })
                .enumerate()
            {
                if column.column_name == name {
                    values[first_column + index] =
                        storage_value_to_duckdb_value(name, value, &column.column_type)?;
                }
            }
            Ok(())
        })
    }

    fn append_projected_columns(
        &self,
        table: &TableRegistration,
        item: &StorageItem,
        values: &mut Vec<Value>,
    ) -> AnalyticsEngineResult<()> {
        let projection =
            projection::CachedProjection::compile_for_table(table, &mut self.caches.borrow_mut())?;
        for (column, segments) in &projection.columns {
            let value = projection::extract_from_item(item, segments);
            if value.as_deref().is_some_and(contains_unsupported_null) {
                return Err(AnalyticsEngineError::AttributeConversion(
                    column.column_name.clone(),
                ));
            }
            if BatchRow::should_append_projection_column(table, &column.column_name) {
                append_batch_value(
                    &column.column_name,
                    column
                        .column_type
                        .as_ref()
                        .unwrap_or(&default_varchar_column_type()),
                    value.as_deref(),
                    values,
                )?;
            }
        }
        Ok(())
    }
}

impl BatchRow {
    fn should_append_projection_column(table: &TableRegistration, name: &str) -> bool {
        !is_batch_owned_column(table, name) && table.document_column.as_deref() != Some(name)
    }
}

fn validate_selected_attributes(
    item: &StorageItem,
    attribute_names: &[String],
) -> AnalyticsEngineResult<()> {
    for attribute_name in attribute_names {
        if let Some(value) = item.get(attribute_name)
            && contains_unsupported_null(value)
        {
            return Err(AnalyticsEngineError::AttributeConversion(
                attribute_name.clone(),
            ));
        }
    }
    Ok(())
}

fn append_batch_value(
    column_name: &str,
    column_type: &AnalyticsColumnType,
    value: Option<&StorageValue>,
    values: &mut Vec<Value>,
) -> AnalyticsEngineResult<()> {
    values.push(value.map_or(Ok(Value::Null), |value| {
        storage_value_to_duckdb_value(column_name, value, column_type)
    })?);
    Ok(())
}

fn append_batch_retention_columns(
    values: &mut Vec<Value>,
    item: &StorageItem,
    ingested_at_ms: i64,
    retention: &IngestRetention,
) -> AnalyticsEngineResult<()> {
    values.push(Value::BigInt(ingested_at_ms));
    let expiry = if let Some(period_ms) = retention.period_ms {
        let basis = retention_basis_ms(item, ingested_at_ms, &retention.timestamp)?;
        Some(
            basis
                .checked_add(i64::try_from(period_ms).unwrap_or(i64::MAX))
                .ok_or(AnalyticsEngineError::RetentionOverflow)?,
        )
    } else {
        None
    };
    values.push(expiry.map_or(Value::Null, Value::BigInt));
    values.push(Value::Boolean(retention.missing_retention));
    Ok(())
}

fn contains_unsupported_null(value: &StorageValue) -> bool {
    match value {
        StorageValue::NULL(false) => true,
        StorageValue::L(values) => values.iter().any(contains_unsupported_null),
        StorageValue::M(values) => values.values().any(contains_unsupported_null),
        _ => false,
    }
}

fn default_varchar_column_type() -> AnalyticsColumnType {
    AnalyticsColumnType::Primitive {
        primitive: PrimitiveColumnType::VarChar,
    }
}

fn storage_value_to_duckdb_value(
    column_name: &str,
    value: &StorageValue,
    column_type: &AnalyticsColumnType,
) -> AnalyticsEngineResult<Value> {
    if matches!(
        column_type,
        AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::StorageJson
        }
    ) {
        return serde_json::to_string(value)
            .map(Value::Text)
            .map_err(AnalyticsEngineError::from);
    }
    if matches!(
        column_type,
        AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::Timestamp
        }
    ) {
        return convert_storage_timestamp(column_name, value);
    }
    if matches!(
        column_type,
        AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::Decimal
        }
    ) && let StorageValue::N(value) = value
    {
        return Ok(Value::Text(value.clone()));
    }
    storage_value_to_generic_duckdb_value(column_name, value)
}

fn convert_storage_timestamp(
    column_name: &str,
    value: &StorageValue,
) -> AnalyticsEngineResult<Value> {
    Ok(match value {
        StorageValue::N(value) => value
            .parse::<i64>()
            .map_or_else(|_| Value::Text(normalized_number(value)), timestamp_value),
        StorageValue::S(value) => value
            .parse::<i64>()
            .map_or_else(|_| Value::Text(value.clone()), timestamp_value),
        StorageValue::NULL(true) => Value::Null,
        _ => storage_value_to_generic_duckdb_value(column_name, value)?,
    })
}

fn storage_value_to_generic_duckdb_value(
    column_name: &str,
    value: &StorageValue,
) -> AnalyticsEngineResult<Value> {
    Ok(match value {
        StorageValue::S(value) | StorageValue::B(value) => Value::Text(value.clone()),
        StorageValue::N(value) => Value::Text(normalized_number(value)),
        StorageValue::SS(_) | StorageValue::BS(_) | StorageValue::NS(_) => {
            Value::Text(storage_value_to_json_text(column_name, value)?)
        }
        StorageValue::BOOL(value) => Value::Boolean(*value),
        StorageValue::NULL(true) => Value::Null,
        StorageValue::NULL(false) => {
            return Err(AnalyticsEngineError::AttributeConversion(
                column_name.to_string(),
            ));
        }
        StorageValue::L(_) | StorageValue::M(_) => {
            Value::Text(storage_value_to_json_text(column_name, value)?)
        }
    })
}

fn timestamp_value(value: i64) -> Value {
    Value::Timestamp(TimeUnit::Millisecond, value)
}

fn normalized_number(value: &str) -> String {
    value
        .parse::<serde_json::Number>()
        .map_or_else(|_| value.to_string(), |number| number.to_string())
}

fn storage_value_to_json_text(
    column_name: &str,
    value: &StorageValue,
) -> AnalyticsEngineResult<String> {
    value
        .to_json_text()
        .map_err(|_| AnalyticsEngineError::AttributeConversion(column_name.to_string()))
}
