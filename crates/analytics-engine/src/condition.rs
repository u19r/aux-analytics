use std::collections::HashMap;

use analytics_contract::{StorageItem, StorageValue, TableRegistration};
use storage_condition::{Condition, evaluate_condition, parse_condition_expression};
use storage_types::AttributeValue;

use crate::{
    cache::EngineCaches,
    engine::{AnalyticsEngineError, AnalyticsEngineResult},
};

#[derive(Debug, Clone)]
pub(crate) struct CachedCondition {
    signature: String,
    condition: Condition,
}

pub(crate) fn validate_condition(
    table: &TableRegistration,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<()> {
    if table.condition_expression.is_some() {
        cached_registration_condition(table, caches)?;
    }
    Ok(())
}

pub(crate) fn item_matches_registration(
    table: &TableRegistration,
    item: &StorageItem,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<bool> {
    if table.condition_expression.is_none() {
        return Ok(true);
    }
    let condition = cached_registration_condition(table, caches)?;
    let item = storage_item_to_attribute_values(item);
    Ok(evaluate_condition(&item, &condition))
}

fn cached_registration_condition(
    table: &TableRegistration,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<Condition> {
    let cache_key = table.analytics_table_name.clone();
    let signature = condition_signature(table)?;
    if let Some(cached) = caches.conditions.get(&cache_key)
        && cached.signature == signature
    {
        return Ok(cached.condition);
    }

    let condition = parse_registration_condition(table)?;
    caches.conditions.insert(
        cache_key,
        CachedCondition {
            signature,
            condition: condition.clone(),
        },
    );
    Ok(condition)
}

fn parse_registration_condition(table: &TableRegistration) -> AnalyticsEngineResult<Condition> {
    let expression = table.condition_expression.as_deref().ok_or_else(|| {
        AnalyticsEngineError::InvalidConditionExpression(
            "condition expression is required".to_string(),
        )
    })?;
    let values = table.expression_attribute_values.as_ref().map(|values| {
        values
            .iter()
            .map(|(name, value)| (name.clone(), storage_value_to_attribute_value(value)))
            .collect::<HashMap<_, _>>()
    });
    parse_condition_expression(
        expression,
        table.expression_attribute_names.as_ref(),
        values.as_ref(),
    )
    .map_err(AnalyticsEngineError::InvalidConditionExpression)
}

fn condition_signature(table: &TableRegistration) -> AnalyticsEngineResult<String> {
    serde_json::to_string(&serde_json::json!({
        "condition_expression": table.condition_expression,
        "expression_attribute_names": table.expression_attribute_names,
        "expression_attribute_values": table.expression_attribute_values,
    }))
    .map_err(AnalyticsEngineError::Json)
}

fn storage_item_to_attribute_values(item: &StorageItem) -> HashMap<String, AttributeValue> {
    item.iter()
        .map(|(name, value)| (name.clone(), storage_value_to_attribute_value(value)))
        .collect()
}

fn storage_value_to_attribute_value(value: &StorageValue) -> AttributeValue {
    match value {
        StorageValue::S(value) => AttributeValue::S(value.clone()),
        StorageValue::N(value) => AttributeValue::N(value.clone()),
        StorageValue::B(value) => AttributeValue::B(value.clone()),
        StorageValue::SS(values) => AttributeValue::SS(values.clone()),
        StorageValue::NS(values) => AttributeValue::NS(values.clone()),
        StorageValue::BS(values) => AttributeValue::BS(values.clone()),
        StorageValue::BOOL(value) => AttributeValue::BOOL(*value),
        StorageValue::NULL(value) => AttributeValue::NULL(*value),
        StorageValue::L(values) => AttributeValue::L(
            values
                .iter()
                .map(storage_value_to_attribute_value)
                .collect(),
        ),
        StorageValue::M(values) => AttributeValue::M(storage_item_to_attribute_values(values)),
    }
}
