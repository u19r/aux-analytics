use std::sync::Arc;

use analytics_api::{AppState, SourceHealth};
use analytics_storage::SourcePoller;
use config::{AnalyticsIngestConfig, AnalyticsSourceConfig};

use crate::{
    error::ApiResult,
    source_polling::{
        hashed_range::run_hashed_range_source_poller, legacy::run_source_poller, planning::*,
    },
};
pub(crate) async fn spawn_source_polling(
    source: &AnalyticsSourceConfig,
    ingest: &AnalyticsIngestConfig,
    app_state: Arc<AppState>,
) -> ApiResult<()> {
    let manifest = app_state.manifest.read().await.clone();
    let source_table_count = source_table_count(source, &manifest);
    if source_polling_startup(ingest.processor_enabled, source_table_count, false)
        == SourcePollingStartup::Disabled
    {
        *app_state.source_health.write().await = SourceHealth::disabled();
        if ingest.processor_enabled {
            tracing::info!("analytics source polling disabled: no source tables configured");
        } else {
            tracing::info!("analytics source polling disabled: ingest processor is disabled");
        }
        return Ok(());
    }
    *app_state.source_health.write().await = SourceHealth::starting(source_table_count);
    tracing::info!("analytics source checkpoint load starting");
    let checkpoints = app_state
        .engine
        .with_read(move |engine| engine.load_source_checkpoints())
        .await??;
    tracing::info!(
        checkpoint_count = checkpoints.len(),
        "analytics source checkpoint load complete"
    );
    let storage_checkpoints = storage_checkpoints_from_engine(checkpoints);
    tracing::info!("analytics source poller construction starting");
    let plans = effective_source_table_plans(source, &manifest, &app_state).await?;
    let source_plan_signature = source_table_plan_signature(&plans);
    let hashed_range_aux_storage = source_plans_are_aux_storage_only(&plans);
    let poller = SourcePoller::from_plans(source, plans, &storage_checkpoints).await?;
    tracing::info!(
        poller_is_empty = poller.is_empty(),
        "analytics source poller constructed"
    );
    if source_polling_startup(
        ingest.processor_enabled,
        source_table_count,
        poller.is_empty(),
    ) == SourcePollingStartup::Disabled
    {
        *app_state.source_health.write().await = SourceHealth::disabled();
        tracing::info!("analytics source polling disabled: no pollable source tables");
        return Ok(());
    }
    if hashed_range_aux_storage {
        tokio::spawn(run_hashed_range_source_poller(
            poller,
            source_plan_signature,
            source.clone(),
            ingest.clone(),
            app_state,
        ));
    } else {
        tokio::spawn(run_source_poller(
            poller,
            source_plan_signature,
            source.clone(),
            app_state,
        ));
    }
    Ok(())
}
