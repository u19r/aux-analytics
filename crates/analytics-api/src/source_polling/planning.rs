use std::sync::Arc;

use analytics_api::AppState;
use analytics_storage::{
    SourceCheckpoint as StorageSourceCheckpoint, SourceTablePlan, table_plans,
};
use config::AnalyticsSourceConfig;
#[cfg(test)]
use config::AnalyticsStreamType;

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

pub(crate) fn storage_checkpoints_from_engine(
    checkpoints: Vec<analytics_engine::SourceCheckpoint>,
) -> Vec<StorageSourceCheckpoint> {
    checkpoints
        .into_iter()
        .map(|checkpoint| StorageSourceCheckpoint {
            source_table_name: checkpoint.source_table_name,
            shard_id: checkpoint.shard_id,
            position: checkpoint.position,
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn source_plans_are_aux_storage_only(plans: &[SourceTablePlan]) -> bool {
    !plans.is_empty()
        && plans
            .iter()
            .all(|plan| plan.stream_type() == AnalyticsStreamType::AuxStorage)
}
pub(crate) async fn effective_source_table_plans(
    source: &AnalyticsSourceConfig,
    manifest: &analytics_contract::AnalyticsManifest,
    _app_state: &Arc<AppState>,
) -> ApiResult<Vec<SourceTablePlan>> {
    Ok(table_plans(source, manifest)?)
}
