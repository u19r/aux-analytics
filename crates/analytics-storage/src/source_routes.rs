use std::collections::HashMap;

#[cfg(test)]
use analytics_contract::AnalyticsManifest;
use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue, TableRegistration};
use storage_condition::{Condition, evaluate_condition, parse_condition_expression};
use storage_types::AttributeValue;

use crate::{
    error::{
        AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
        AnalyticsStorageResult,
    },
    planning::SourceTablePlan,
};

#[derive(Debug, Clone)]
pub(crate) struct SourceRoute {
    source_table_name: String,
    source_table_name_prefix: Option<String>,
    destination: DestinationRoute,
}

#[derive(Debug, Clone)]
struct DestinationRoute {
    analytics_table_name: String,
    condition: Option<Condition>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CompiledSourceRoutes {
    routes: Vec<SourceRoute>,
}

impl CompiledSourceRoutes {
    #[cfg(test)]
    pub(crate) fn from_manifest(manifest: &AnalyticsManifest) -> AnalyticsStorageResult<Self> {
        Self::from_registrations(&manifest.tables)
    }

    pub(crate) fn from_plans(plans: &[SourceTablePlan]) -> AnalyticsStorageResult<Self> {
        let mut routes = Vec::new();
        for plan in plans {
            if plan.registrations.is_empty() {
                routes.extend(
                    plan.analytics_table_names
                        .iter()
                        .map(|analytics_table_name| SourceRoute {
                            source_table_name: plan.source_table_name.clone(),
                            source_table_name_prefix: plan.source_table_name_prefix.clone(),
                            destination: DestinationRoute {
                                analytics_table_name: analytics_table_name.clone(),
                                condition: None,
                            },
                        }),
                );
            } else {
                routes.extend(Self::from_registrations(&plan.registrations)?.routes);
            }
        }
        Ok(Self { routes })
    }

    fn from_registrations(registrations: &[TableRegistration]) -> AnalyticsStorageResult<Self> {
        let routes = registrations
            .iter()
            .map(|registration| {
                Ok(SourceRoute {
                    source_table_name: registration.source_table_name.clone(),
                    source_table_name_prefix: registration.source_table_name_prefix.clone(),
                    destination: DestinationRoute {
                        analytics_table_name: registration.analytics_table_name.clone(),
                        condition: compile_condition(registration)?,
                    },
                })
            })
            .collect::<AnalyticsStorageResult<Vec<_>>>()?;
        Ok(Self { routes })
    }

    pub(crate) fn analytics_tables<'a>(
        &'a self,
        source_table_name: &str,
        record: &StorageStreamRecord,
    ) -> Vec<&'a str> {
        self.routes
            .iter()
            .filter(|route| route.matches(source_table_name, record))
            .map(|route| route.destination.analytics_table_name.as_str())
            .collect()
    }

    pub(crate) fn contains_source(&self, source_table_name: &str) -> bool {
        self.routes
            .iter()
            .any(|route| route.source_matches(source_table_name))
    }
}

impl SourceRoute {
    fn matches(&self, source_table_name: &str, record: &StorageStreamRecord) -> bool {
        self.source_matches(source_table_name) && self.destination.matches(record)
    }

    fn source_matches(&self, source_table_name: &str) -> bool {
        self.source_table_name_prefix.as_deref().map_or_else(
            || self.source_table_name == source_table_name,
            |prefix| source_table_name.starts_with(prefix),
        )
    }
}

impl DestinationRoute {
    fn matches(&self, record: &StorageStreamRecord) -> bool {
        let Some(condition) = &self.condition else {
            return true;
        };
        record
            .old_image
            .iter()
            .chain(record.new_image.iter())
            .any(|item| evaluate_condition(&storage_item_to_attribute_values(item), condition))
    }
}

fn compile_condition(
    registration: &TableRegistration,
) -> AnalyticsStorageResult<Option<Condition>> {
    let Some(expression) = registration.condition_expression.as_deref() else {
        return Ok(None);
    };
    let values = registration
        .expression_attribute_values
        .as_ref()
        .map(|values| {
            values
                .iter()
                .map(|(name, value)| (name.clone(), storage_value_to_attribute_value(value)))
                .collect::<HashMap<_, _>>()
        });
    parse_condition_expression(
        expression,
        registration.expression_attribute_names.as_ref(),
        values.as_ref(),
    )
    .map(Some)
    .map_err(|message| {
        AnalyticsStorageError::with_debug(
            AnalyticsStorageErrorKind::InvalidConditionExpression,
            AnalyticsStorageErrorDebug::Message(message),
        )
    })
}

fn storage_item_to_attribute_values(item: &StorageItem) -> HashMap<String, AttributeValue> {
    let mut values = item
        .iter()
        .map(|(name, value)| (name.clone(), storage_value_to_attribute_value(value)))
        .collect::<HashMap<_, _>>();
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
        StorageValue::M(values) => AttributeValue::M(storage_item_to_attribute_values(values)),
    }
}
