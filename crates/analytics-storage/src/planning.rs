use analytics_contract::{AnalyticsManifest, TableRegistration};
use config::{AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType};

use crate::error::{
    AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
    AnalyticsStorageResult,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTablePlan {
    pub(crate) source_table_name: String,
    pub(crate) source_table_name_prefix: Option<String>,
    pub(crate) analytics_table_names: Vec<String>,
    pub(crate) registrations: Vec<TableRegistration>,
    pub(crate) stream_type: AnalyticsStreamType,
    pub(crate) stream_identifier: Option<String>,
}

/// Borrowed identity and routing view for a configured source table.
///
/// The view keeps storage-owned plan fields private while giving transport
/// owners the stable information needed to authorize and schedule a source.
#[derive(Debug, Clone, Copy)]
pub struct SourceTablePlanView<'a> {
    pub source_table_name: &'a str,
    pub source_table_name_prefix: Option<&'a str>,
    pub analytics_table_names: &'a [String],
    pub stream_type: AnalyticsStreamType,
}

impl SourceTablePlan {
    #[must_use]
    pub fn view(&self) -> SourceTablePlanView<'_> {
        SourceTablePlanView {
            source_table_name: &self.source_table_name,
            source_table_name_prefix: self.source_table_name_prefix.as_deref(),
            analytics_table_names: &self.analytics_table_names,
            stream_type: self.stream_type,
        }
    }

    #[must_use]
    pub fn aux_storage(source_table_name: String, analytics_table_names: Vec<String>) -> Self {
        Self {
            source_table_name,
            source_table_name_prefix: None,
            analytics_table_names,
            registrations: Vec::new(),
            stream_type: AnalyticsStreamType::AuxStorage,
            stream_identifier: None,
        }
    }

    pub fn from_source(
        source: &AnalyticsSourceConfig,
        manifest: &AnalyticsManifest,
    ) -> AnalyticsStorageResult<Vec<Self>> {
        if source.tables.is_empty() {
            return Ok(Self::from_manifest(source, manifest));
        }
        source
            .tables
            .iter()
            .map(|table| Self::from_configured_table(source, manifest, table))
            .collect()
    }

    fn from_manifest(source: &AnalyticsSourceConfig, manifest: &AnalyticsManifest) -> Vec<Self> {
        let mut plans = Vec::new();
        for table in &manifest.tables {
            if plans
                .iter()
                .any(|plan: &Self| plan.source_table_name == table.source_table_name)
            {
                continue;
            }
            plans.push(Self {
                source_table_name: table.source_table_name.clone(),
                source_table_name_prefix: table.source_table_name_prefix.clone(),
                analytics_table_names: manifest
                    .tables
                    .iter()
                    .filter(|candidate| candidate.source_table_name == table.source_table_name)
                    .map(|candidate| candidate.analytics_table_name.clone())
                    .collect(),
                registrations: manifest
                    .tables
                    .iter()
                    .filter(|candidate| candidate.source_table_name == table.source_table_name)
                    .cloned()
                    .collect(),
                stream_type: source
                    .stream_type
                    .unwrap_or(AnalyticsStreamType::AuxStorage),
                stream_identifier: None,
            });
        }
        plans
    }

    fn from_configured_table(
        source: &AnalyticsSourceConfig,
        manifest: &AnalyticsManifest,
        table: &AnalyticsSourceTableConfig,
    ) -> AnalyticsStorageResult<Self> {
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
        Ok(Self {
            source_table_name: table.table_name.clone(),
            source_table_name_prefix: None,
            analytics_table_names,
            registrations: manifest
                .tables
                .iter()
                .filter(|manifest_table| manifest_table.source_table_name == table.table_name)
                .cloned()
                .collect(),
            stream_type: table
                .stream_type
                .or(source.stream_type)
                .unwrap_or(AnalyticsStreamType::AuxStorage),
            stream_identifier: table.stream_identifier.clone(),
        })
    }
}
