use std::collections::BTreeMap;

use analytics_contract::{
    AnalyticsColumnType, INTERNAL_EXPIRY_COLUMN, INTERNAL_INGESTED_AT_COLUMN,
    INTERNAL_MISSING_RETENTION_COLUMN, PrimitiveColumnType, RetentionTimestamp, StorageItem,
    StorageValue, TableRegistration,
};

use super::identity;
use crate::{
    engine::{AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, IngestRetention},
    projection,
};

#[derive(Clone, Copy)]
pub(crate) struct RowSource<'source> {
    pub source_table_name: &'source str,
    pub record_key: &'source [u8],
    pub record_keys: &'source StorageItem,
    /// Prevalidated System physical-owner identity, without source key
    /// plaintext.
    pub stream_key_digest: Option<&'source str>,
}

impl AnalyticsEngine {
    pub(super) fn for_each_projected_attribute(
        &self,
        table: &TableRegistration,
        item: &StorageItem,
        mut convert: impl FnMut(&str, &StorageValue) -> AnalyticsEngineResult<()>,
    ) -> AnalyticsEngineResult<()> {
        if table.projection_columns.is_some() {
            let projection = projection::CachedProjection::compile_for_table(
                table,
                &mut self.caches.borrow_mut(),
            )?;
            for (column, segments) in &projection.columns {
                if let Some(value) = projection::extract_from_item(item, segments) {
                    convert(&column.column_name, value.as_ref())?;
                }
            }
            return Ok(());
        }
        if let Some(names) = table.projection_attribute_names.as_deref() {
            for (name, value) in item {
                if names.iter().any(|candidate| candidate == name) {
                    convert(name, value)?;
                }
            }
            return Ok(());
        }
        if !table.columns.is_empty() {
            for (name, value) in item {
                convert(name, value)?;
            }
        }
        Ok(())
    }

    pub(crate) fn row_from_item(
        &self,
        table: &TableRegistration,
        source: RowSource<'_>,
        item: &StorageItem,
        ingested_at_ms: i64,
        retention: Option<&IngestRetention>,
    ) -> AnalyticsEngineResult<BTreeMap<String, serde_json::Value>> {
        let tenant_id = self
            .tenant_resolver
            .resolve(table, source.source_table_name, item)?;
        let mut row = BTreeMap::new();
        self.for_each_projected_attribute(table, item, |name, value| {
            row.insert(
                name.to_string(),
                projected_value_to_json(table, name, value)?,
            );
            Ok(())
        })?;
        if let Some(document_column) = table.document_column.as_deref() {
            row.insert(document_column.to_string(), storage_item_to_json(item)?);
        }
        row.insert(
            "table_name".to_string(),
            serde_json::Value::String(source.source_table_name.to_string()),
        );
        row.insert(
            "tenant_id".to_string(),
            serde_json::Value::String(tenant_id.unwrap_or_default()),
        );
        row.insert(
            "__id".to_string(),
            serde_json::Value::String(identity::row_id_from_record(
                table,
                source,
                item,
                &mut self.caches.borrow_mut(),
            )?),
        );
        if let Some(retention) = retention {
            apply_retention_columns(&mut row, item, ingested_at_ms, retention)?;
        }
        Ok(row)
    }
}

fn apply_retention_columns(
    row: &mut BTreeMap<String, serde_json::Value>,
    item: &StorageItem,
    ingested_at_ms: i64,
    retention: &IngestRetention,
) -> AnalyticsEngineResult<()> {
    row.insert(
        INTERNAL_INGESTED_AT_COLUMN.to_string(),
        serde_json::Value::Number(ingested_at_ms.into()),
    );
    row.insert(
        INTERNAL_MISSING_RETENTION_COLUMN.to_string(),
        serde_json::Value::Bool(retention.missing_retention),
    );
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
    row.insert(
        INTERNAL_EXPIRY_COLUMN.to_string(),
        expiry.map_or(serde_json::Value::Null, |value| {
            serde_json::Value::Number(value.into())
        }),
    );
    Ok(())
}

pub(super) fn retention_basis_ms(
    item: &StorageItem,
    ingested_at_ms: i64,
    timestamp: &RetentionTimestamp,
) -> AnalyticsEngineResult<i64> {
    match timestamp {
        RetentionTimestamp::IngestedAt => Ok(ingested_at_ms),
        RetentionTimestamp::Attribute { attribute_path } => {
            let segments = projection::parse_attribute_path(attribute_path, None)?;
            let value = projection::extract_from_item(item, &segments).ok_or_else(|| {
                AnalyticsEngineError::InvalidRetentionTimestamp(attribute_path.clone())
            })?;
            storage_value_to_i64(&value, attribute_path)
        }
    }
}

fn storage_value_to_i64(value: &StorageValue, attribute_path: &str) -> AnalyticsEngineResult<i64> {
    match value {
        StorageValue::N(value) => value.parse::<i64>().map_err(|_| {
            AnalyticsEngineError::InvalidRetentionTimestamp(attribute_path.to_string())
        }),
        _ => Err(AnalyticsEngineError::InvalidRetentionTimestamp(
            attribute_path.to_string(),
        )),
    }
}

pub(super) fn string_attribute<'a>(
    item: &'a StorageItem,
    attribute_name: &str,
) -> AnalyticsEngineResult<&'a str> {
    let Some(StorageValue::S(value)) = item.get(attribute_name) else {
        return Err(AnalyticsEngineError::MissingAttribute(
            attribute_name.to_string(),
        ));
    };
    Ok(value.as_str())
}

fn projected_value_to_json(
    table: &TableRegistration,
    name: &str,
    value: &StorageValue,
) -> AnalyticsEngineResult<serde_json::Value> {
    match (find_projection_primitive(table, name), value) {
        (Some(PrimitiveColumnType::StorageJson), _) => {
            serde_json::to_value(value).map_err(AnalyticsEngineError::from)
        }
        (Some(PrimitiveColumnType::Decimal), StorageValue::N(number)) => {
            Ok(serde_json::Value::String(number.clone()))
        }
        _ => value
            .to_json_value()
            .map_err(|_| AnalyticsEngineError::AttributeConversion(name.to_string())),
    }
}

fn find_projection_primitive(table: &TableRegistration, name: &str) -> Option<PrimitiveColumnType> {
    let column_type = if table.columns.is_empty() {
        table
            .projection_columns
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|column| column.column_name == name)
            .and_then(|column| column.column_type.as_ref())
    } else {
        table
            .columns
            .iter()
            .find(|column| column.column_name == name)
            .map(|column| &column.column_type)
    };
    match column_type {
        Some(AnalyticsColumnType::Primitive { primitive }) => Some(*primitive),
        _ => None,
    }
}

pub(crate) fn storage_item_to_json(item: &StorageItem) -> AnalyticsEngineResult<serde_json::Value> {
    let mut object = serde_json::Map::with_capacity(item.len());
    let mut ordered = item.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|(left, _)| *left);
    for (name, value) in ordered {
        let json = value
            .to_json_value()
            .map_err(|_| AnalyticsEngineError::AttributeConversion(name.clone()))?;
        object.insert(name.clone(), json);
    }
    Ok(serde_json::Value::Object(object))
}

pub(super) fn storage_item_to_json_text(item: &StorageItem) -> AnalyticsEngineResult<String> {
    let mut output = Vec::new();
    let mut ordered = item.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|(name, _)| *name);
    output.push(b'{');
    for (index, (name, value)) in ordered.into_iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        serde_json::to_writer(&mut output, name)?;
        output.push(b':');
        value
            .append_json(&mut output)
            .map_err(|_| AnalyticsEngineError::AttributeConversion(name.clone()))?;
    }
    output.push(b'}');
    // The JSON serializer emits UTF-8, including escaped source strings.
    Ok(String::from_utf8(output).expect("serialized storage JSON is UTF-8"))
}

#[cfg(test)]
pub(crate) fn performance_storage_item_to_json(
    item: &StorageItem,
) -> AnalyticsEngineResult<serde_json::Value> {
    storage_item_to_json(item)
}
