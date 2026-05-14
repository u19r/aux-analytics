use analytics_contract::AnalyticsManifest;
use config::{AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType};

use crate::error::{
    AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
    AnalyticsStorageResult,
};

#[derive(Debug, Clone)]
pub(crate) struct SourceTablePlan {
    pub(crate) source_table_name: String,
    pub(crate) analytics_table_names: Vec<String>,
    pub(crate) stream_type: AnalyticsStreamType,
    pub(crate) stream_identifier: Option<String>,
}

pub(crate) fn table_plans(
    source: &AnalyticsSourceConfig,
    manifest: &AnalyticsManifest,
) -> AnalyticsStorageResult<Vec<SourceTablePlan>> {
    source
        .tables
        .iter()
        .map(|table| table_plan(source, manifest, table))
        .collect()
}

fn table_plan(
    source: &AnalyticsSourceConfig,
    manifest: &AnalyticsManifest,
    table: &AnalyticsSourceTableConfig,
) -> AnalyticsStorageResult<SourceTablePlan> {
    let analytics_table_names = manifest
        .tables
        .iter()
        .filter(|manifest_table| manifest_table.source_table_name == table.table_name)
        .map(|manifest_table| manifest_table.analytics_table_name.clone())
        .collect::<Vec<_>>();
    if analytics_table_names.is_empty() {
        return Err(AnalyticsStorageError::with_debug(
            AnalyticsStorageErrorKind::UnregisteredSourceTable,
            AnalyticsStorageErrorDebug::SourceTableName(table.table_name.clone()),
        ));
    }
    Ok(SourceTablePlan {
        source_table_name: table.table_name.clone(),
        analytics_table_names,
        stream_type: table
            .stream_type
            .or(source.stream_type)
            .unwrap_or(AnalyticsStreamType::AuxStorage),
        stream_identifier: table.stream_identifier.clone(),
    })
}
