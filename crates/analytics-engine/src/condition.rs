use std::collections::HashMap;

use analytics_contract::{StorageItem, StorageValue, TableRegistration};
use lru_ttl_cache::CacheRead;
use storage_condition::{Condition, evaluate_condition, parse_condition_expression_typed};
use storage_types::{AttributeMap, AttributeValue, VecMap};

use crate::{
    cache::EngineCaches,
    engine::{AnalyticsEngineError, AnalyticsEngineResult},
};

#[derive(Debug, Clone)]
pub(crate) struct CachedCondition {
    signature: ConditionSignature,
    condition: Condition,
    root_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConditionSignature {
    condition_expression: Option<String>,
    expression_attribute_names: Option<VecMap<String, String>>,
    expression_attribute_values: Option<VecMap<String, StorageValue>>,
}

impl ConditionSignature {
    fn from_table(table: &TableRegistration) -> Self {
        Self {
            condition_expression: table.condition_expression.clone(),
            expression_attribute_names: table.expression_attribute_names.clone(),
            expression_attribute_values: table.expression_attribute_values.clone(),
        }
    }

    fn matches(&self, table: &TableRegistration) -> bool {
        self.condition_expression.as_ref() == table.condition_expression.as_ref()
            && self.expression_attribute_names.as_ref() == table.expression_attribute_names.as_ref()
            && self.expression_attribute_values.as_ref()
                == table.expression_attribute_values.as_ref()
    }
}

pub(crate) fn validate_condition(
    table: &TableRegistration,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<()> {
    if table.condition_expression.is_some() {
        let _ = cached_registration_condition(table, caches)?;
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
    let item = storage_item_to_condition_attributes(item, &condition.root_names);
    Ok(evaluate_condition(&item, &condition.condition))
}

fn cached_registration_condition(
    table: &TableRegistration,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<CacheRead<CachedCondition>> {
    let cache_key = &table.analytics_table_name;
    if let Some(cached) = caches.conditions.get_ref(cache_key)
        && cached.signature.matches(table)
    {
        return Ok(cached);
    }

    let condition = parse_registration_condition(table)?;
    let root_names = collect_condition_root_names(&condition);
    Ok(caches
        .conditions
        .insert_ref(
            cache_key.clone(),
            CachedCondition {
                signature: ConditionSignature::from_table(table),
                condition,
                root_names,
            },
        )
        .expect("condition cache insert"))
}

fn parse_registration_condition(table: &TableRegistration) -> AnalyticsEngineResult<Condition> {
    let expression = table.condition_expression.as_deref().ok_or_else(|| {
        AnalyticsEngineError::InvalidConditionExpression(
            "condition expression is required".to_string(),
        )
    })?;
    let values = table
        .expression_attribute_values
        .as_ref()
        .map(storage_values_to_attribute_map);
    parse_condition_expression_typed(
        expression,
        table.expression_attribute_names.as_ref(),
        values.as_ref(),
    )
    .map_err(AnalyticsEngineError::InvalidConditionExpression)
}

#[cfg(test)]
pub(crate) fn performance_cached_condition(
    table: &TableRegistration,
) -> AnalyticsEngineResult<CachedCondition> {
    let condition = parse_registration_condition(table)?;
    let root_names = collect_condition_root_names(&condition);
    Ok(CachedCondition {
        signature: ConditionSignature::from_table(table),
        condition,
        root_names,
    })
}

fn collect_condition_root_names(condition: &Condition) -> Vec<String> {
    let mut names = Vec::new();
    condition.visit_attribute_paths(&mut |path| {
        let root = path.split(['.', '[']).next().unwrap_or(path);
        names.push(root.to_string());
    });
    names.sort_unstable();
    names.dedup();
    names
}

fn storage_item_to_condition_attributes(item: &StorageItem, roots: &[String]) -> AttributeMap {
    let mut values = AttributeMap::with_capacity(roots.len());
    for name in roots {
        let value = match item.get(name) {
            Some(value) => Some(value),
            None if name == "entity_type" => item.get("et"),
            None => None,
        };
        if let Some(value) = value {
            values.insert(name.clone(), storage_value_to_attribute_value(value));
        }
    }
    values
}

#[cfg(test)]
pub(crate) fn performance_cached_condition_matches(
    cached: &CachedCondition,
    table: &TableRegistration,
) -> bool {
    cached.signature.matches(table)
}

#[cfg(test)]
fn storage_item_to_attribute_values(item: &StorageItem) -> AttributeMap {
    let mut values = AttributeMap::with_capacity(item.len() + 1);
    for (name, value) in item {
        values.insert(name.clone(), storage_value_to_attribute_value(value));
    }
    if !values.contains_key("entity_type")
        && let Some(entity_type) = values.get("et").cloned()
    {
        values.insert("entity_type".to_string(), entity_type);
    }
    values
}

#[cfg(test)]
pub(crate) fn performance_evaluate_cached_condition(
    cached: &CachedCondition,
    item: &StorageItem,
) -> bool {
    let item = storage_item_to_attribute_values(item);
    evaluate_condition(&item, &cached.condition)
}

fn storage_values_to_attribute_map(values: &VecMap<String, StorageValue>) -> AttributeMap {
    values
        .iter()
        .map(|(name, value)| (name.clone(), storage_value_to_attribute_value(value)))
        .collect()
}

fn storage_item_to_attribute_hash_map(item: &StorageItem) -> HashMap<String, AttributeValue> {
    let mut values = HashMap::with_capacity(item.len() + 1);
    for (name, value) in item {
        values.insert(name.clone(), storage_value_to_attribute_value(value));
    }
    if !values.contains_key("entity_type")
        && let Some(entity_type) = values.get("et").cloned()
    {
        values.insert("entity_type".to_string(), entity_type);
    }
    values
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
        StorageValue::M(values) => AttributeValue::M(storage_item_to_attribute_hash_map(values)),
    }
}
