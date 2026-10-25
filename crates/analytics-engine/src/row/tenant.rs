use analytics_contract::{StorageItem, StorageValue, TableRegistration, TenantSelector};

use super::materialization::string_attribute;
use crate::{
    cache::RegexCache,
    engine::{AnalyticsEngineError, AnalyticsEngineResult},
};

/// Resolves registered tenant policy against the actual source identity without
/// a connection.
#[derive(Default)]
pub struct TenantResolver {
    regexes: RegexCache,
}

impl TenantResolver {
    pub fn resolve(
        &self,
        table: &TableRegistration,
        source_table_name: &str,
        item: &StorageItem,
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
            } => Ok(Some(self.regexes.capture(
                attribute_name,
                string_attribute(item, attribute_name)?,
                regex,
                capture,
            )?)),
            TenantSelector::PartitionKeyPrefix { attribute_name }
            | TenantSelector::TableNameOrPartitionKeyPrefix { attribute_name } => {
                let Some(StorageValue::S(value)) = item.get(attribute_name) else {
                    return Err(AnalyticsEngineError::MissingTenant);
                };
                tenant_id_from_partition_key_prefix(value)
                    .map(Some)
                    .ok_or(AnalyticsEngineError::MissingTenant)
            }
            TenantSelector::TableName => Ok(Some(source_table_name.to_string())),
        }
    }
}

fn tenant_id_from_partition_key_prefix(value: &str) -> Option<String> {
    let prefix = value.split('#').next()?.trim();
    if prefix.is_empty() {
        return None;
    }
    Some(prefix.strip_prefix("ns_").map_or_else(
        || prefix.to_string(),
        |storage_key| format!("t_{storage_key}"),
    ))
}

impl std::fmt::Debug for TenantResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TenantResolver")
            .finish_non_exhaustive()
    }
}
