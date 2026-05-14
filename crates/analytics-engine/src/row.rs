use std::collections::BTreeMap;

use analytics_contract::{
    INTERNAL_EXPIRY_COLUMN, INTERNAL_INGESTED_AT_COLUMN, INTERNAL_MISSING_RETENTION_COLUMN,
    RetentionTimestamp, RowIdentity, StorageItem, StorageValue, TableRegistration, TenantSelector,
};

use crate::{
    cache::EngineCaches,
    engine::{AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, IngestRetention},
    projection,
};

impl AnalyticsEngine {
    pub(crate) fn row_from_item(
        &self,
        table: &TableRegistration,
        record_key: &[u8],
        record_keys: &StorageItem,
        item: StorageItem,
        ingested_at_ms: i64,
        retention: Option<&IngestRetention>,
    ) -> AnalyticsEngineResult<BTreeMap<String, serde_json::Value>> {
        let full_item = item.clone();
        let full_item_json = storage_item_to_json(&full_item)?;
        let item = if let Some(columns) = table.projection_columns.as_deref() {
            projection::project_item(&item, columns, table.expression_attribute_names.as_ref())?
        } else if let Some(attribute_names) = table.projection_attribute_names.as_deref() {
            item.into_iter()
                .filter(|(name, _)| attribute_names.iter().any(|candidate| candidate == name))
                .collect()
        } else if table.columns.is_empty() {
            StorageItem::new()
        } else {
            item
        };

        let tenant_id = resolve_tenant_id(table, &full_item, &mut self.caches.borrow_mut())?;
        let mut row = attribute_map_to_json(item)?;
        if let Some(document_column) = table.document_column.as_deref() {
            row.insert(document_column.to_string(), full_item_json);
        }
        row.insert(
            "table_name".to_string(),
            serde_json::Value::String(table.source_table_name.clone()),
        );
        row.insert(
            "tenant_id".to_string(),
            serde_json::Value::String(tenant_id.unwrap_or_default()),
        );
        row.insert(
            "__id".to_string(),
            serde_json::Value::String(row_id_from_record(
                table,
                record_key,
                record_keys,
                &full_item,
                &mut self.caches.borrow_mut(),
            )?),
        );
        if let Some(retention) = retention {
            apply_retention_columns(&mut row, &full_item, ingested_at_ms, retention)?;
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

fn retention_basis_ms(
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

fn resolve_tenant_id(
    table: &TableRegistration,
    item: &StorageItem,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<Option<String>> {
    if let Some(tenant_id) = table.tenant_id.as_ref() {
        return Ok(Some(tenant_id.clone()));
    }

    match &table.tenant_selector {
        TenantSelector::None => Ok(None),
        TenantSelector::Attribute { attribute_name } => {
            Ok(Some(string_attribute(item, attribute_name)?.to_string()))
        }
        TenantSelector::AttributeRegex {
            attribute_name,
            regex,
            capture,
        } => Ok(Some(regex_capture(
            attribute_name,
            string_attribute(item, attribute_name)?,
            regex,
            capture,
            caches,
        )?)),
        TenantSelector::PartitionKeyPrefix { attribute_name }
        | TenantSelector::TableNameOrPartitionKeyPrefix { attribute_name } => {
            let Some(StorageValue::S(value)) = item.get(attribute_name) else {
                return Err(AnalyticsEngineError::MissingTenant);
            };
            value
                .split('#')
                .nth(1)
                .map(ToString::to_string)
                .map(Some)
                .ok_or(AnalyticsEngineError::MissingTenant)
        }
        TenantSelector::TableName => Err(AnalyticsEngineError::MissingTenant),
    }
}

fn string_attribute<'a>(
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

fn regex_capture(
    attribute_name: &str,
    value: &str,
    regex: &str,
    capture: &str,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<String> {
    let regex = cached_regex(attribute_name, regex, caches)?;
    let captures = regex
        .captures(value)
        .ok_or_else(|| AnalyticsEngineError::RegexNoMatch {
            attribute_name: attribute_name.to_string(),
        })?;
    captures
        .name(capture)
        .map(|value| value.as_str().to_string())
        .ok_or_else(|| AnalyticsEngineError::RegexMissingCapture {
            attribute_name: attribute_name.to_string(),
            capture: capture.to_string(),
        })
}

fn cached_regex(
    attribute_name: &str,
    regex: &str,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<regex::Regex> {
    let cache_key = regex.to_string();
    if let Some(regex) = caches.regexes.get(&cache_key) {
        return Ok(regex);
    }
    let compiled = regex::Regex::new(regex).map_err(|err| AnalyticsEngineError::InvalidRegex {
        attribute_name: attribute_name.to_string(),
        message: err.to_string(),
    })?;
    caches.regexes.insert(cache_key, compiled.clone());
    Ok(compiled)
}

fn attribute_map_to_json(
    item: StorageItem,
) -> AnalyticsEngineResult<BTreeMap<String, serde_json::Value>> {
    let mut row = BTreeMap::new();
    for (name, value) in item {
        let json = value
            .to_json_value()
            .map_err(|_| AnalyticsEngineError::AttributeConversion(name.clone()))?;
        row.insert(name, json);
    }
    Ok(row)
}

fn storage_item_to_json(item: &StorageItem) -> AnalyticsEngineResult<serde_json::Value> {
    let mut object = serde_json::Map::new();
    let ordered = item.iter().collect::<BTreeMap<_, _>>();
    for (name, value) in ordered {
        let json = value
            .to_json_value()
            .map_err(|_| AnalyticsEngineError::AttributeConversion(name.clone()))?;
        object.insert(name.clone(), json);
    }
    Ok(serde_json::Value::Object(object))
}

fn row_id_from_key_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn row_id_from_record(
    table: &TableRegistration,
    record_key: &[u8],
    record_keys: &StorageItem,
    item: &StorageItem,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<String> {
    match &table.row_identity {
        RowIdentity::RecordKey => Ok(row_id_from_key_bytes(record_key)),
        RowIdentity::StreamKeys => {
            let key_json = storage_item_to_json(record_keys)?;
            Ok(row_id_from_key_bytes(key_json.to_string().as_bytes()))
        }
        RowIdentity::Attribute { attribute_name } => Ok(row_id_from_key_bytes(
            string_attribute(item, attribute_name)?.as_bytes(),
        )),
        RowIdentity::AttributeRegex {
            attribute_name,
            regex,
            capture,
        } => {
            let value = regex_capture(
                attribute_name,
                string_attribute(item, attribute_name)?,
                regex,
                capture,
                caches,
            )?;
            Ok(row_id_from_key_bytes(value.as_bytes()))
        }
    }
}
