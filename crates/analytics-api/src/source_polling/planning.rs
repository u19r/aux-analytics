use analytics_storage::{PollBatch, SourceCheckpoint as StorageSourceCheckpoint, SourceTablePlan};
use config::AnalyticsSourceConfig;

use crate::error::ApiResult;
pub(crate) fn source_table_count(
    source: &AnalyticsSourceConfig,
    manifest: &analytics_contract::AnalyticsManifest,
) -> usize {
    if !source.tables.is_empty() {
        return source.tables.len();
    }
    let mut source_tables = std::collections::BTreeSet::new();
    for table in &manifest.tables {
        source_tables.insert(table.source_table_name.as_str());
    }
    source_tables.len()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourcePollingStartup {
    Disabled,
    Starting,
}

pub(crate) fn source_polling_startup(
    processor_enabled: bool,
    configured_table_count: usize,
    poller_is_empty: bool,
) -> SourcePollingStartup {
    if !processor_enabled || configured_table_count == 0 || poller_is_empty {
        SourcePollingStartup::Disabled
    } else {
        SourcePollingStartup::Starting
    }
}

pub(crate) trait IntoStorageCheckpoint {
    fn into_storage_checkpoint(self) -> StorageSourceCheckpoint;
}

impl IntoStorageCheckpoint for analytics_engine::RecordCheckpoint {
    fn into_storage_checkpoint(self) -> StorageSourceCheckpoint {
        StorageSourceCheckpoint {
            source_table_name: self.source_table_name().to_string(),
            shard_id: self.shard_id().to_string(),
            position: self.position().to_string(),
        }
    }
}

impl IntoStorageCheckpoint for analytics_engine::SourceCheckpoint {
    fn into_storage_checkpoint(self) -> StorageSourceCheckpoint {
        StorageSourceCheckpoint {
            source_table_name: self.source_table_name,
            shard_id: self.shard_id,
            position: self.position,
        }
    }
}

pub(crate) fn storage_checkpoints_from_engine<C: IntoStorageCheckpoint>(
    checkpoints: Vec<C>,
) -> Vec<StorageSourceCheckpoint> {
    checkpoints
        .into_iter()
        .map(IntoStorageCheckpoint::into_storage_checkpoint)
        .collect()
}

/// Derive the destination-fence scope from the storage batch boundary.
///
/// Storage validates that all records and checkpoints agree before this
/// helper asks the engine to validate the scope components themselves. Empty
/// batches intentionally have no source effect scope.
pub(crate) fn source_effect_scope(
    batch: &PollBatch,
) -> ApiResult<Option<analytics_engine::SourceEffectScope>> {
    if let Some(system_stream) = batch.system_stream.as_ref() {
        return Ok(Some(analytics_engine::SourceEffectScope::try_new_system(
            system_stream.source_scope.clone(),
            system_stream.previous_checkpoint.clone(),
            system_stream.checkpoint.clone(),
        )?));
    }
    let Some(scope) = batch.source_scope()? else {
        return Ok(None);
    };
    let predecessor = batch
        .source_predecessor
        .as_ref()
        .map(|checkpoint| {
            if checkpoint.source_table_name != scope.source_table_name()
                || checkpoint.shard_id != scope.shard_id()
                || checkpoint.is_iterator()
            {
                return Err(analytics_engine::AnalyticsEngineError::InvalidSourceEffectScope);
            }
            Ok(checkpoint.position.clone())
        })
        .transpose()?;
    Ok(Some(
        analytics_engine::SourceEffectScope::try_new_with_predecessor(
            scope.source_table_name(),
            scope.shard_id(),
            predecessor,
        )?,
    ))
}

pub(crate) async fn effective_source_table_plans(
    source: &AnalyticsSourceConfig,
    manifest: &analytics_contract::AnalyticsManifest,
) -> ApiResult<Vec<SourceTablePlan>> {
    Ok(SourceTablePlan::from_source(source, manifest)?)
}
