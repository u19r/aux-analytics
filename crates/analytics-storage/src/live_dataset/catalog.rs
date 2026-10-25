use analytics_contract::{AnalyticsDataset, DatasetRegistration};
use serde::{Deserialize, Serialize};
use storage_types::{AttributeMap, AttributeValue, KeyAttributeType, KeyType, TableName};

use super::{LiveDatasetError, LiveDatasetManager};

#[derive(Serialize, Deserialize)]
pub(super) struct StoredDataset {
    pub dataset: AnalyticsDataset,
    pub cursor_secret: String,
    pub source_incarnation: String,
}

impl StoredDataset {
    pub(super) fn try_new(
        tenant: &str,
        request: DatasetRegistration,
    ) -> Result<Self, LiveDatasetError> {
        LiveDatasetManager::validate_identifier(&request.dataset_id)?;
        let source = LiveDatasetManager::source_table(tenant, &request.application_id)?;
        if request.allowed_fields.is_empty() || request.allowed_fields.len() > 32 {
            return Err(LiveDatasetError::InvalidQuery);
        }
        for field in &request.allowed_fields {
            LiveDatasetManager::validate_identifier(field)?;
            if field.starts_with('_') || ["pk", "sk"].contains(&field.as_str()) {
                return Err(LiveDatasetError::InvalidQuery);
            }
        }
        let version = request
            .expected_version
            .checked_add(1)
            .ok_or(LiveDatasetError::InvalidQuery)?;
        Ok(Self {
            dataset: AnalyticsDataset {
                tenant: tenant.to_owned(),
                dataset_id: request.dataset_id,
                application_id: request.application_id,
                allowed_fields: request.allowed_fields,
                source_table: source.to_string(),
                source_index: crate::constants::APPLICATION_DATASET_SOURCE_INDEX.into(),
                version,
            },
            source_incarnation: String::new(),
            cursor_secret: format!("{}{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4()),
        })
    }

    fn key(tenant: &str, dataset_id: &str) -> storage_types::KeyAttributes {
        let key = format!("ANALYTICS_DATASET#{tenant}#{dataset_id}");
        [
            ("pk".into(), AttributeValue::S(key.clone())),
            ("sk".into(), AttributeValue::S(key)),
        ]
        .into_iter()
        .collect()
    }

    fn item(&self) -> Result<AttributeMap, LiveDatasetError> {
        let mut item: AttributeMap = Self::key(&self.dataset.tenant, &self.dataset.dataset_id)
            .into_iter()
            .map(|attribute| (attribute.name, attribute.value))
            .collect();
        item.insert(
            "version",
            AttributeValue::N(self.dataset.version.to_string()),
        );
        item.insert(
            "data",
            AttributeValue::S(serde_json::to_string(self).map_err(|_| LiveDatasetError::Storage)?),
        );
        Ok(item)
    }
}

impl LiveDatasetManager {
    pub(super) async fn validate_source(
        &self,
        dataset: &AnalyticsDataset,
    ) -> Result<super::ApplicationSource, LiveDatasetError> {
        let table = TableName::new(&dataset.source_table);
        let info = self
            .db
            .get_table_info(&table)
            .await
            .map_err(|_| LiveDatasetError::SourceUnavailable)?;
        let valid_keys = info.key_schema.len() == 2
            && info.key_schema.iter().all(|key| {
                matches!(
                    (key.attribute_name.as_str(), key.key_type.clone()),
                    ("pk", KeyType::Hash) | ("sk", KeyType::Range)
                )
            });
        let string_keys = ["pk", "sk"].iter().all(|name| {
            info.attribute_definitions.iter().any(|attribute| {
                attribute.attribute_name == *name && attribute.attribute_type == KeyAttributeType::S
            })
        });
        if !valid_keys || !string_keys {
            return Err(LiveDatasetError::SourceUnavailable);
        }
        self.verify_source_index(dataset, &table).await
    }

    async fn verify_source_index(
        &self,
        dataset: &AnalyticsDataset,
        table: &TableName,
    ) -> Result<super::ApplicationSource, LiveDatasetError> {
        let item = self
            .db
            .get_item_with_consistent_read(table.clone(), Self::source_marker_key(), true)
            .await
            .map_err(|_| LiveDatasetError::SourceUnavailable)?
            .ok_or(LiveDatasetError::SourceUnavailable)?;
        let item = item
            .to_attribute_map()
            .map_err(|_| LiveDatasetError::SourceUnavailable)?;
        super::ApplicationSource::from_marker(&dataset.tenant, &dataset.application_id, &item)
    }

    pub(super) async fn save_dataset(
        &self,
        table: TableName,
        dataset: &StoredDataset,
        companions: Vec<storage_types::TransactEncodeItem>,
    ) -> Result<(), LiveDatasetError> {
        let expected = dataset.dataset.version - 1;
        let item = storage_types::WireItem::try_from(dataset.item()?)
            .map_err(|_| LiveDatasetError::Storage)?;
        let mut put = storage_types::TransactEncodePutRequest::builder()
            .table_name(table)
            .item(storage_types::single_table_entity::WireEntity::unindexed(
                item,
            ))
            .build();
        put.condition_expression = Some(if expected == 0 {
            "attribute_not_exists(pk)".into()
        } else {
            "#version = :version".into()
        });
        if expected != 0 {
            put.expression_attribute_names = Some(
                [("#version".into(), "version".into())]
                    .into_iter()
                    .collect(),
            );
            put.expression_attribute_values = Some(
                [(":version".into(), AttributeValue::N(expected.to_string()))]
                    .into_iter()
                    .collect(),
            );
        }
        let mut items = vec![
            storage_types::TransactEncodeItem::builder()
                .put(put)
                .build(),
        ];
        items.extend(companions);
        self.commit_catalog_write(items).await
    }

    async fn commit_catalog_write(
        &self,
        transact_items: Vec<storage_types::TransactEncodeItem>,
    ) -> Result<(), LiveDatasetError> {
        self.db
            .transact_write_items_encode(storage_types::TransactWriteItemsEncodeRequest {
                transact_items,
                ..Default::default()
            })
            .await
            .map_err(map_catalog_error)?;
        Ok(())
    }

    pub(super) async fn get_dataset(
        &self,
        tenant: &str,
        table: TableName,
        id: &str,
    ) -> Result<StoredDataset, LiveDatasetError> {
        Self::validate_identifier(id)?;
        let item = self
            .db
            .get_item_with_consistent_read(table, StoredDataset::key(tenant, id), true)
            .await
            .map_err(|_| LiveDatasetError::Storage)?
            .ok_or(LiveDatasetError::NotFound)?;
        let item = item
            .to_attribute_map()
            .map_err(|_| LiveDatasetError::Storage)?;
        let Some(AttributeValue::S(data)) = item.get("data") else {
            return Err(LiveDatasetError::Storage);
        };
        let stored: StoredDataset =
            serde_json::from_str(data).map_err(|_| LiveDatasetError::Storage)?;
        if stored.dataset.tenant != tenant {
            return Err(LiveDatasetError::NotFound);
        }
        Ok(stored)
    }

    pub async fn remove_dataset(
        &self,
        tenant: &str,
        table: TableName,
        revision: analytics_contract::DatasetRevision,
        companions: Vec<storage_types::TransactEncodeItem>,
    ) -> Result<(), LiveDatasetError> {
        let stored = self
            .get_dataset(tenant, table.clone(), &revision.dataset_id)
            .await?;
        #[cfg(test)]
        if let Some(pause) = &self.page_pause {
            pause.observe_page().await;
        }
        let delete = storage_types::TransactDeleteRequest {
            table_name: table,
            key: StoredDataset::key(tenant, &revision.dataset_id),
            condition_expression: Some("#version = :version AND #data = :loaded_data".into()),
            expression_attribute_names: Some(
                [
                    ("#version".into(), "version".into()),
                    ("#data".into(), "data".into()),
                ]
                .into_iter()
                .collect(),
            ),
            expression_attribute_values: Some(
                [
                    (
                        ":version".into(),
                        AttributeValue::N(revision.version.to_string()),
                    ),
                    (
                        ":loaded_data".into(),
                        AttributeValue::S(
                            serde_json::to_string(&stored)
                                .map_err(|_| LiveDatasetError::Storage)?,
                        ),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        };
        let mut items = vec![
            storage_types::TransactEncodeItem::builder()
                .delete(delete)
                .build(),
        ];
        items.extend(companions);
        self.commit_catalog_write(items).await
    }
}

fn map_catalog_error(error: storage_types::StorageError) -> LiveDatasetError {
    if error.is_canceled_condition_check_failed(0) {
        LiveDatasetError::VersionConflict
    } else {
        LiveDatasetError::Storage
    }
}
