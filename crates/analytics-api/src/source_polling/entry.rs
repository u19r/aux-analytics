use std::sync::Arc;

use analytics_storage::SystemStreamResume;
use config::{AnalyticsIngestConfig, AnalyticsSourceConfig};

use crate::{
    AppState, SourceHealth,
    clock::now_ms,
    error::ApiResult,
    source_polling::{
        health::{apply_source_durable_checkpoints, apply_source_poll_error_health},
        planning::*,
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

    let Some(runtime) = app_state.background_jobs.clone() else {
        *app_state.source_health.write().await = SourceHealth::disabled();
        tracing::warn!(
            "analytics source polling remains disabled because the background job runtime is \
             unavailable"
        );
        return Ok(());
    };

    *app_state.source_health.write().await = SourceHealth::starting(source_table_count);
    tracing::info!("analytics source checkpoint load starting");
    let checkpoints = app_state.load_source_checkpoints_for_polling().await?;
    tracing::info!(
        checkpoint_count = checkpoints.len(),
        "analytics source checkpoint load complete"
    );
    let storage_checkpoints = storage_checkpoints_from_engine(checkpoints);
    {
        let mut health = app_state.source_health.write().await;
        apply_source_durable_checkpoints(&mut health, &storage_checkpoints);
    }

    let system_resume = app_state.load_system_resume_for_polling().await?;
    if system_resume.is_some() {
        metrics::gauge!(crate::constants::SOURCE_CHECKPOINTS_METRIC)
            .set(storage_checkpoints.len().saturating_add(1) as f64);
    }

    tracing::info!("analytics source polling plan construction starting");
    let plans = match effective_source_table_plans(source, &manifest).await {
        Ok(plans) => plans,
        Err(error) => {
            let mut health = app_state.source_health.write().await;
            apply_source_poll_error_health(
                &mut health,
                format!("analytics source polling plan construction failed: {error}"),
                now_ms(),
            );
            return Err(error);
        }
    };
    runtime
        .start_source_polling(source.clone(), plans, &storage_checkpoints, system_resume)
        .await
}

impl AppState {
    async fn checkpoint_load_result<T>(
        &self,
        label: &str,
        result: Result<
            Result<T, analytics_engine::AnalyticsEngineError>,
            crate::AnalyticsEngineAccessError,
        >,
    ) -> ApiResult<T> {
        let (message, error) = match result {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => (
                format!("analytics {label} load failed: {error}"),
                error.into(),
            ),
            Err(error) => (
                format!("analytics {label} worker failed: {error}"),
                error.into(),
            ),
        };
        let mut health = self.source_health.write().await;
        apply_source_poll_error_health(&mut health, message, now_ms());
        Err(error)
    }

    async fn load_source_checkpoints_for_polling(
        &self,
    ) -> ApiResult<Vec<analytics_engine::RecordCheckpoint>> {
        let result = self
            .engine
            .with_read(|engine| engine.load_source_checkpoints())
            .await;
        self.checkpoint_load_result("source checkpoint", result)
            .await
    }

    async fn load_system_resume_for_polling(&self) -> ApiResult<Option<SystemStreamResume>> {
        let result = self
            .engine
            .with_read(|engine| engine.load_system_stream_checkpoint())
            .await;
        self.checkpoint_load_result("system stream checkpoint", result)
            .await
            .map(|checkpoint| {
                checkpoint.map(|(scope, checkpoint)| SystemStreamResume::new(scope, checkpoint))
            })
    }
}
