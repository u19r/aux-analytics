use std::sync::Arc;

use analytics_engine::AnalyticsEngine;
use config::RootConfig;

use super::ProcessDuty;
use crate::{
    AppState, RetentionHealth, background_jobs::BackgroundJobRuntime, error::ApiResult,
    retention::RetentionRuntime,
};

pub(crate) struct AppStateStartupInput<'a> {
    pub root: &'a RootConfig,
    pub manifest: analytics_contract::AnalyticsManifest,
    pub storage_backend: analytics_engine::StorageBackend,
    pub privacy_policy: Option<Arc<analytics_contract::PrivacyPolicy>>,
    pub duty: ProcessDuty,
}

pub(crate) async fn initialize_app_state(
    input: AppStateStartupInput<'_>,
) -> ApiResult<Arc<AppState>> {
    let AppStateStartupInput {
        root,
        manifest,
        storage_backend,
        privacy_policy,
        duty,
    } = input;
    let mut app_state = initialize_query_state(root, manifest, &storage_backend, privacy_policy)?;
    if duty.can_run_background_work() {
        let retention = RetentionRuntime::from_config(&root.analytics.retention)
            .await?
            .map(Arc::new);
        app_state = app_state.with_retention(retention);
        if let Some(path) = root.analytics.operations.operation_store_path.as_deref() {
            app_state = app_state.with_operation_store_path(path);
        }
        app_state = compose_background_jobs(root, &storage_backend, app_state).await?;
    }
    let app_state = Arc::new(app_state);
    Ok(app_state)
}

fn initialize_query_state(
    root: &RootConfig,
    manifest: analytics_contract::AnalyticsManifest,
    storage_backend: &analytics_engine::StorageBackend,
    privacy_policy: Option<Arc<analytics_contract::PrivacyPolicy>>,
) -> ApiResult<AppState> {
    tracing::info!("analytics engine connecting");
    let engine = AnalyticsEngine::connect(storage_backend)?;
    tracing::info!("analytics engine connected");
    tracing::info!(
        manifest_table_count = manifest.tables.len(),
        "analytics manifest initialization starting"
    );
    engine.ensure_manifest(&manifest)?;
    tracing::info!("analytics manifest initialized");
    for table in &root.analytics.retention.tables {
        engine.ensure_retention_columns(table.analytics_table_name.as_str())?;
    }
    Ok(AppState::new(
        crate::AnalyticsEngineAccess::backend_aware_with_max_read_connections(
            engine,
            storage_backend.clone(),
            root.analytics.query.max_read_connections,
        ),
        manifest,
    )
    .with_privacy_policy(privacy_policy))
}

pub(crate) async fn start_retention_jobs(app_state: &Arc<AppState>) {
    tracing::info!("analytics retention background initialization starting");
    let retention_table_count = app_state
        .retention
        .as_ref()
        .map_or(0, |retention| retention.table_names().count());
    if retention_table_count == 0 {
        *app_state.retention_health.write().await = RetentionHealth::disabled();
    } else if let Some(runtime) = app_state.background_jobs.as_ref() {
        *app_state.retention_health.write().await =
            RetentionHealth::starting(retention_table_count);
        runtime.start_retention();
    } else {
        *app_state.retention_health.write().await = RetentionHealth::disabled();
        tracing::warn!(
            "analytics retention remains disabled because the background job runtime is \
             unavailable"
        );
    }
    tracing::info!("analytics retention background initialized");
}

async fn compose_background_jobs(
    root: &RootConfig,
    storage_backend: &analytics_engine::StorageBackend,
    mut app_state: AppState,
) -> ApiResult<AppState> {
    let background_jobs_needed =
        root.analytics.ingest.processor_enabled || app_state.retention.is_some();
    if background_jobs_needed {
        if let Some(runtime) =
            BackgroundJobRuntime::compose(&root.analytics.job_store, storage_backend, &app_state)
                .await?
        {
            app_state = app_state.with_background_jobs(runtime);
        } else {
            tracing::warn!(
                "background job runtime could not be enabled; source polling and retention are \
                 fail-closed"
            );
        }
    }
    Ok(app_state)
}
