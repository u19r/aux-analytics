use analytics_contract::AnalyticsManifest;
use config::{AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType};

use crate::error::{
    AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
    AnalyticsStorageResult,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTablePlan {
    pub(crate) source_table_name: String,
    pub(crate) analytics_table_names: Vec<String>,
    pub(crate) stream_type: AnalyticsStreamType,
    pub(crate) stream_identifier: Option<String>,
}

impl SourceTablePlan {
    #[must_use]
    pub fn aux_storage(source_table_name: String, analytics_table_names: Vec<String>) -> Self {
        Self {
            source_table_name,
            analytics_table_names,
            stream_type: AnalyticsStreamType::AuxStorage,
            stream_identifier: None,
        }
    }

    #[must_use]
    pub fn source_table_name(&self) -> &str {
        &self.source_table_name
    }

    #[must_use]
    pub fn analytics_table_names(&self) -> &[String] {
        &self.analytics_table_names
    }

    #[must_use]
    pub fn stream_type(&self) -> AnalyticsStreamType {
        self.stream_type
    }
}

pub fn table_plans(
    source: &AnalyticsSourceConfig,
    manifest: &AnalyticsManifest,
) -> AnalyticsStorageResult<Vec<SourceTablePlan>> {
    if source.tables.is_empty() {
        return Ok(manifest_table_plans(source, manifest));
    }
    source
        .tables
        .iter()
        .map(|table| table_plan(source, manifest, table))
        .collect()
}

fn manifest_table_plans(
    source: &AnalyticsSourceConfig,
    manifest: &AnalyticsManifest,
) -> Vec<SourceTablePlan> {
    let mut plans = Vec::new();
    for table in &manifest.tables {
        if plans
            .iter()
            .any(|plan: &SourceTablePlan| plan.source_table_name == table.source_table_name)
        {
            continue;
        }
        plans.push(SourceTablePlan {
            source_table_name: table.source_table_name.clone(),
            analytics_table_names: manifest
                .tables
                .iter()
                .filter(|candidate| candidate.source_table_name == table.source_table_name)
                .map(|candidate| candidate.analytics_table_name.clone())
                .collect(),
            stream_type: source
                .stream_type
                .unwrap_or(AnalyticsStreamType::AuxStorage),
            stream_identifier: None,
        });
    }
    plans
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
