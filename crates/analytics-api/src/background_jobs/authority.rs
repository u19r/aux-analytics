use std::collections::{HashMap, HashSet};

use analytics_storage::SourceTablePlan;

use crate::{
    background_jobs::{
        contracts::{SourceJobKind, validate_source_table_name},
        error::BackgroundJobError,
    },
    constants::MAX_SOURCE_AUTHORIZED_IDENTITIES,
};

/// Configuration-derived source identities accepted by source handlers.
///
/// Shards are deliberately absent: AWS stream shards are discovered at runtime
/// and remain dynamic within an authorized source table.
#[derive(Debug, Default)]
pub(crate) struct SourceJobAuthority {
    source_tables: HashMap<SourceJobKind, HashSet<String>>,
    source_prefixes: HashMap<SourceJobKind, Vec<String>>,
}

impl SourceJobAuthority {
    pub(crate) fn from_plans(plans: &[SourceTablePlan]) -> Result<Self, BackgroundJobError> {
        let mut source_tables = HashMap::with_capacity(SourceJobKind::ALL.len());
        let mut source_prefixes = HashMap::with_capacity(SourceJobKind::ALL.len());
        let mut identity_count = 0;
        for plan in plans {
            let view = plan.view();
            validate_source_table_name(view.source_table_name).map_err(|_| {
                BackgroundJobError::InvalidConfiguration(
                    "configured source table identity is invalid",
                )
            })?;
            let kind = SourceJobKind::from_stream_type(view.stream_type);
            let tables = source_tables.entry(kind).or_insert_with(HashSet::new);
            if tables.insert(view.source_table_name.to_string()) {
                identity_count += 1;
                if identity_count > MAX_SOURCE_AUTHORIZED_IDENTITIES {
                    return Err(BackgroundJobError::InvalidConfiguration(
                        "configured source identity bound exceeded",
                    ));
                }
            }
            if let Some(prefix) = view.source_table_name_prefix {
                source_prefixes
                    .entry(kind)
                    .or_insert_with(Vec::new)
                    .push(prefix.to_string());
            }
        }
        Ok(Self {
            source_tables,
            source_prefixes,
        })
    }

    pub(crate) fn contains(&self, kind: SourceJobKind, source_table_name: &str) -> bool {
        self.source_tables
            .get(&kind)
            .is_some_and(|tables| tables.contains(source_table_name))
            || self.source_prefixes.get(&kind).is_some_and(|prefixes| {
                prefixes
                    .iter()
                    .any(|prefix| source_table_name.starts_with(prefix))
            })
    }

    pub(crate) fn contains_system(&self, kind: SourceJobKind) -> bool {
        self.source_tables.contains_key(&kind) || self.source_prefixes.contains_key(&kind)
    }
}
