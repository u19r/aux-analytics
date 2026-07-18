use std::{sync::Arc, time::Duration};

use analytics_api::AppState;
use analytics_storage::{
    SourceCheckpoint as StorageSourceCheckpoint, SourcePoller, SourceTablePlan, table_plans,
};
use config::{AnalyticsSourceConfig, AnalyticsStreamType};
use tokio::time::error::Elapsed;

use crate::{
    error::ApiResult,
    source_polling::{health::*, metrics::*, time::*},
};
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

pub(crate) fn source_plans_are_aux_storage_only(plans: &[SourceTablePlan]) -> bool {
    !plans.is_empty()
        && plans
            .iter()
            .all(|plan| plan.stream_type() == AnalyticsStreamType::AuxStorage)
}
pub(crate) async fn rebuild_source_poller(
    source: &AnalyticsSourceConfig,
    manifest: &analytics_contract::AnalyticsManifest,
    app_state: &Arc<AppState>,
) -> ApiResult<SourcePoller> {
    let checkpoints = app_state
        .engine
        .with_read(|engine| engine.load_source_checkpoints())
        .await??;
    let storage_checkpoints = storage_checkpoints_from_engine(checkpoints);
    let plans = effective_source_table_plans(source, manifest, app_state).await?;
    Ok(SourcePoller::from_plans(source, plans, &storage_checkpoints).await?)
}

async fn effective_source_poll_plan_signature(
    source: &AnalyticsSourceConfig,
    manifest: &analytics_contract::AnalyticsManifest,
    app_state: &Arc<AppState>,
) -> ApiResult<Vec<String>> {
    let plans = effective_source_table_plans(source, manifest, app_state).await?;
    Ok(source_table_plan_signature(&plans))
}

pub(crate) async fn effective_source_poll_plan_signature_with_timeout(
    source: &AnalyticsSourceConfig,
    manifest: &analytics_contract::AnalyticsManifest,
    app_state: &Arc<AppState>,
) -> Result<Vec<String>, SourcePollPlanRefreshFailure> {
    let timeout = source_poll_plan_refresh_timeout(source);
    match tokio::time::timeout(
        timeout,
        effective_source_poll_plan_signature(source, manifest, app_state),
    )
    .await
    {
        Ok(Ok(signature)) => Ok(signature),
        Ok(Err(error)) => Err(SourcePollPlanRefreshFailure::Error(error.to_string())),
        Err(error) => Err(SourcePollPlanRefreshFailure::Timeout(
            source_poll_plan_refresh_timeout_error(timeout, error),
        )),
    }
}

pub(crate) enum SourcePollPlanRefreshFailure {
    Timeout(String),
    Error(String),
}

pub(crate) fn source_poll_plan_refresh_timeout(source: &AnalyticsSourceConfig) -> Duration {
    Duration::from_millis(source.poll_request_timeout_ms.max(1))
}

pub(crate) fn source_poll_plan_refresh_timeout_error(timeout: Duration, _error: Elapsed) -> String {
    format!(
        "analytics source poll plan refresh timed out after {} ms",
        timeout.as_millis()
    )
}

pub(crate) async fn effective_source_table_plans(
    source: &AnalyticsSourceConfig,
    manifest: &analytics_contract::AnalyticsManifest,
    app_state: &Arc<AppState>,
) -> ApiResult<Vec<SourceTablePlan>> {
    if !source.tables.is_empty() {
        return Ok(table_plans(source, manifest)?);
    }
    let static_plans = static_source_table_plans(source, manifest)?;
    let dynamic_plans = registered_source_table_plans(
        manifest,
        app_state,
        source_poll_plan_refresh_timeout(source),
    )
    .await?;
    Ok(merge_source_table_plans(static_plans, dynamic_plans))
}

pub(crate) fn static_source_table_plans(
    source: &AnalyticsSourceConfig,
    manifest: &analytics_contract::AnalyticsManifest,
) -> ApiResult<Vec<SourceTablePlan>> {
    let mut plans = table_plans(source, manifest)?;
    if manifest_has_registered_tables_source(manifest) {
        plans.retain(|plan| {
            plan.analytics_table_names()
                .iter()
                .any(|name| name == REGISTERED_TABLES_TABLE_NAME)
        });
    }
    Ok(plans)
}

async fn registered_source_table_plans(
    manifest: &analytics_contract::AnalyticsManifest,
    app_state: &Arc<AppState>,
    timeout: Duration,
) -> ApiResult<Vec<SourceTablePlan>> {
    if !manifest_has_registered_tables_source(manifest) {
        return Ok(Vec::new());
    }
    let rows = app_state
        .engine
        .with_read(|engine| {
            engine.query_unscoped_sql_json_with_timeout(
                "SELECT tenant_id, db_table_name, analytics_table_name, status FROM \
                 analytics_registered_tables WHERE status = 'Ready'",
                timeout,
            )
        })
        .await??;
    let plans = registered_rows_to_source_table_plans(manifest, &rows);
    {
        let mut health = app_state.source_health.write().await;
        apply_source_registry_refresh_health(&mut health, rows.len(), plans.len(), now_ms());
    }
    tracing::info!(
        registered_row_count = rows.len(),
        dynamic_plan_count = plans.len(),
        "analytics source poll plan dynamic registry refreshed"
    );
    Ok(plans)
}

fn manifest_has_registered_tables_source(manifest: &analytics_contract::AnalyticsManifest) -> bool {
    manifest
        .tables
        .iter()
        .any(|table| table.analytics_table_name == REGISTERED_TABLES_TABLE_NAME)
}

pub(crate) fn registered_rows_to_source_table_plans(
    manifest: &analytics_contract::AnalyticsManifest,
    rows: &[serde_json::Value],
) -> Vec<SourceTablePlan> {
    let registered_analytics_tables = manifest
        .tables
        .iter()
        .map(|table| (table.analytics_table_name.as_str(), table))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut by_source_table = std::collections::BTreeMap::<String, Vec<String>>::new();
    for row in rows {
        let Some(source_table_name) = row.get("db_table_name").and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let Some(analytics_table_name) = row
            .get("analytics_table_name")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if row.get("status").and_then(serde_json::Value::as_str)
            != Some(REGISTERED_TABLE_READY_STATUS)
        {
            continue;
        }
        if row.get("tenant_id").and_then(serde_json::Value::as_str) == Some("system") {
            continue;
        }
        let Some(registration) = registered_analytics_tables.get(analytics_table_name) else {
            continue;
        };
        if registration.document_column.is_none() {
            continue;
        }
        if source_table_name.trim().is_empty()
            || analytics_table_name == REGISTERED_TABLES_TABLE_NAME
        {
            continue;
        }
        if !source_table_name_matches_registration(registration, source_table_name) {
            continue;
        }
        by_source_table
            .entry(source_table_name.to_string())
            .or_default()
            .push(analytics_table_name.to_string());
    }
    by_source_table
        .into_iter()
        .map(|(source_table_name, mut analytics_table_names)| {
            analytics_table_names.sort();
            analytics_table_names.dedup();
            SourceTablePlan::aux_storage(source_table_name, analytics_table_names)
        })
        .collect()
}

fn source_table_name_matches_registration(
    registration: &analytics_contract::TableRegistration,
    source_table_name: &str,
) -> bool {
    match registration.source_table_name_prefix.as_deref() {
        Some(prefix) => source_table_name.starts_with(prefix),
        None => registration.source_table_name == source_table_name,
    }
}

pub(crate) fn merge_source_table_plans(
    static_plans: Vec<SourceTablePlan>,
    dynamic_plans: Vec<SourceTablePlan>,
) -> Vec<SourceTablePlan> {
    if dynamic_plans.is_empty() {
        return static_plans;
    }
    let dynamic_analytics_tables = dynamic_plans
        .iter()
        .flat_map(|plan| plan.analytics_table_names().iter().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    let mut merged = static_plans
        .into_iter()
        .filter(|plan| {
            !plan
                .analytics_table_names()
                .iter()
                .any(|name| dynamic_analytics_tables.contains(name.as_str()))
        })
        .collect::<Vec<_>>();
    merged.extend(dynamic_plans);
    merged.sort_by(|left, right| left.source_table_name().cmp(right.source_table_name()));
    merged
}

pub(crate) fn source_table_plan_signature(plans: &[SourceTablePlan]) -> Vec<String> {
    let mut entries = plans
        .iter()
        .map(|plan| {
            format!(
                "{}\t{}",
                plan.source_table_name(),
                plan.analytics_table_names().join(",")
            )
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries
}
