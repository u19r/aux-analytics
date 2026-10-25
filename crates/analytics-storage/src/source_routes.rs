use crate::{
    AnalyticsStorageResult,
    error::{AnalyticsStorageError, AnalyticsStorageErrorKind},
    planning::SourceTablePlan,
};

#[derive(Debug, Clone)]
pub(crate) struct SourceRoute {
    source_table_name: String,
    source_table_name_prefix: Option<String>,
    analytics_table_names: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CompiledSourceRoutes {
    routes: Vec<SourceRoute>,
}

impl CompiledSourceRoutes {
    pub(crate) fn from_plans(plans: &[SourceTablePlan]) -> AnalyticsStorageResult<Self> {
        let mut routes = Vec::new();
        for plan in plans {
            if plan.registrations.is_empty() {
                if !plan.analytics_table_names.is_empty() {
                    routes.push(SourceRoute {
                        source_table_name: plan.source_table_name.clone(),
                        source_table_name_prefix: plan.source_table_name_prefix.clone(),
                        analytics_table_names: plan.analytics_table_names.clone(),
                    });
                }
            } else {
                for registration in &plan.registrations {
                    let Some(route) = routes.iter_mut().find(|route| {
                        route.source_table_name == registration.source_table_name
                            && route.source_table_name_prefix
                                == registration.source_table_name_prefix
                    }) else {
                        routes.push(SourceRoute {
                            source_table_name: registration.source_table_name.clone(),
                            source_table_name_prefix: registration.source_table_name_prefix.clone(),
                            analytics_table_names: vec![registration.analytics_table_name.clone()],
                        });
                        continue;
                    };
                    if !route
                        .analytics_table_names
                        .contains(&registration.analytics_table_name)
                    {
                        route
                            .analytics_table_names
                            .push(registration.analytics_table_name.clone());
                    }
                }
            }
        }
        Ok(Self { routes })
    }

    pub(crate) fn contains_source(&self, source_table_name: &str) -> bool {
        self.analytics_tables_for_source(source_table_name)
            .is_ok_and(|tables| !tables.is_empty())
    }

    pub(crate) fn analytics_tables_for_source(
        &self,
        source_table_name: &str,
    ) -> AnalyticsStorageResult<&[String]> {
        let mut matching = self
            .routes
            .iter()
            .filter(|route| route.source_matches(source_table_name));
        let Some(first) = matching.next() else {
            return Ok(&[]);
        };
        if matching.any(|route| {
            route.analytics_table_names.len() != first.analytics_table_names.len()
                || route
                    .analytics_table_names
                    .iter()
                    .any(|table| !first.analytics_table_names.contains(table))
                || first
                    .analytics_table_names
                    .iter()
                    .any(|table| !route.analytics_table_names.contains(table))
        }) {
            return Err(AnalyticsStorageError::new(
                AnalyticsStorageErrorKind::AmbiguousSourceRoute,
            ));
        }
        Ok(&first.analytics_table_names)
    }
}

impl SourceRoute {
    fn source_matches(&self, source_table_name: &str) -> bool {
        self.source_table_name_prefix.as_deref().map_or_else(
            || self.source_table_name == source_table_name,
            |prefix| source_table_name.starts_with(prefix),
        )
    }
}
