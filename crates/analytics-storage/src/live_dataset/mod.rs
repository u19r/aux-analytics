use std::sync::Arc;

use analytics_contract::{AnalyticsDataset, DatasetRegistration};
use storage::DatabaseManager;
use storage_types::TableName;

use crate::LiveDatasetError;

mod catalog;
mod cursor;
mod query;
mod source;
pub use source::{ApplicationMutation, ApplicationRecord, ApplicationSource};

/// Owns live dataset catalog and indexed read policy. The caller supplies an
/// authenticated tenant and its canonical metadata table, never source names.
#[derive(Clone)]
pub struct LiveDatasetManager {
    db: Arc<DatabaseManager>,
    #[cfg(test)]
    page_pause: Option<Arc<DatasetPagePause>>,
}

impl LiveDatasetManager {
    pub fn new(db: Arc<DatabaseManager>) -> Self {
        Self {
            db,
            #[cfg(test)]
            page_pause: None,
        }
    }

    pub fn source_table(tenant: &str, application: &str) -> Result<TableName, LiveDatasetError> {
        Self::validate_identifier(tenant)?;
        Self::validate_identifier(application)?;
        Ok(TableName::new(&format!(
            "app_{}_{tenant}_{application}",
            tenant.len()
        )))
    }

    /// The application backend writes this descriptor during its normal source
    /// setup. Registration verifies it rather than guessing from key names.
    pub fn source_index_marker(
        tenant: &str,
        application: &str,
    ) -> Result<storage_types::AttributeMap, LiveDatasetError> {
        Self::source_table(tenant, application)?;
        let mut marker: storage_types::AttributeMap = Self::source_marker_key()
            .into_iter()
            .map(|attribute| (attribute.name, attribute.value))
            .collect();
        marker.insert("tenant", storage_types::AttributeValue::S(tenant.into()));
        marker.insert(
            "application_id",
            storage_types::AttributeValue::S(application.into()),
        );
        marker.insert(
            "source_index",
            storage_types::AttributeValue::S(
                crate::constants::APPLICATION_DATASET_SOURCE_INDEX.into(),
            ),
        );
        Ok(marker)
    }

    fn source_marker_key() -> storage_types::KeyAttributes {
        [
            (
                "pk".into(),
                storage_types::AttributeValue::S("ANALYTICS_SOURCE#v1".into()),
            ),
            (
                "sk".into(),
                storage_types::AttributeValue::S("SCHEMA".into()),
            ),
        ]
        .into_iter()
        .collect()
    }

    pub async fn register_dataset(
        &self,
        tenant: &str,
        metadata_table: TableName,
        request: DatasetRegistration,
        companions: Vec<storage_types::TransactEncodeItem>,
    ) -> Result<AnalyticsDataset, LiveDatasetError> {
        let mut dataset = catalog::StoredDataset::try_new(tenant, request)?;
        dataset.source_incarnation = self.validate_source(&dataset.dataset).await?.incarnation();
        self.save_dataset(metadata_table, &dataset, companions)
            .await?;
        Ok(dataset.dataset)
    }

    fn validate_identifier(value: &str) -> Result<(), LiveDatasetError> {
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(LiveDatasetError::InvalidQuery);
        }
        Ok(())
    }
}

#[cfg(test)]
mod live_dataset_tests;

#[cfg(test)]
struct DatasetPagePause {
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    observed: std::sync::atomic::AtomicBool,
}
#[cfg(test)]
impl DatasetPagePause {
    async fn observe_page(&self) {
        if !self
            .observed
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.reached.notify_one();
            self.resume.notified().await;
        }
    }
}
