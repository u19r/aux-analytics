use std::{sync::Arc, time::Duration};

use analytics_storage::{PollBatch, SourcePoller, SourceTablePlan, SystemStreamResume};
use bg_jobs::{JobId, JobOutcome, Timestamp, runtime::JobSystem, test_support::raw::ClaimedJob};
use config::{AnalyticsJobStoreConfig, AnalyticsSourceConfig};
use storage::{
    RemoteScheduledJobStoreConfig, ScheduledJobStore, connect_remote_scheduled_job_store,
};

use super::{initialize_source_runtime, source_deadline_at_ms, source_job_shared_for_bdd};
use crate::{
    AppState,
    background_jobs::{
        contracts::{SourceJobKind, SourceJobPayload},
        error::{BackgroundJobError, api_config_error},
        job_catalog,
        observation::{BackgroundJobObserver, SourceJobShared, set_source_phase},
        test_support,
        test_support::{monotonic_now, wall_now},
    },
    bdd_test_support::{SourceRuntime, SystemStreamCutoverReceipt},
    error::{ApiError, ApiErrorDebug, ApiErrorKind, ApiResult},
};

struct BddSystemStreamRuntime {
    system: JobSystem<ScheduledJobStore>,
    shared: Arc<SourceJobShared>,
    timeout: Duration,
}

impl BddSystemStreamRuntime {
    async fn build(
        source: &AnalyticsSourceConfig,
        job_store: &AnalyticsJobStoreConfig,
        app_state: &Arc<AppState>,
    ) -> ApiResult<Self> {
        let timeout = crate::background_jobs::runtime::limits::bounded_timeout(
            source.poll_request_timeout_ms,
        )
        .map_err(config_error)?;
        let plans = load_aux_storage_plans(source, app_state).await?;
        let system_resume = load_system_resume(app_state).await?;
        let shared = source_job_shared_for_bdd(app_state);
        install_poller(source, &shared, plans, system_resume).await?;
        let job_store_timeout = crate::background_jobs::runtime::limits::bounded_job_store_timeout(
            job_store.request_timeout_ms,
        )
        .map_err(config_error)?;
        let store = connect_scheduled_store(job_store, job_store_timeout).await?;
        let system = build_job_system(&shared, store, timeout)?;
        Ok(Self {
            system,
            shared,
            timeout,
        })
    }

    async fn next_batch(&self) -> ApiResult<PollBatch> {
        let poller = self
            .shared
            .pollers
            .lock()
            .await
            .get(&SourceJobKind::AuxStorage)
            .cloned()
            .ok_or_else(|| api_config_error(BackgroundJobError::PollerNotRegistered))?;
        set_source_phase(&self.shared, crate::SourcePollingPhase::Polling).await;
        Ok(poller.lock().await.poll_once().await?)
    }

    async fn apply_batch(&self, batch: PollBatch) -> ApiResult<usize> {
        let records = batch.records.len();
        let now = wall_now();
        let claim = self.enqueue_and_claim(batch, now).await?;
        let outcome = self.execute_claim(&claim).await?;
        self.system
            .complete(claim, outcome, now)
            .await
            .map_err(api_config_error)?;
        Ok(records)
    }

    async fn enqueue_and_claim(&self, batch: PollBatch, now: Timestamp) -> ApiResult<ClaimedJob> {
        let deadline_at_ms =
            source_deadline_at_ms(u128::try_from(now.as_millis()).unwrap_or(0), self.timeout)
                .map_err(config_error)?;
        let manifest = self.shared.manifest.read().await;
        let encoded = SourceJobPayload::from_batch(
            SourceJobKind::AuxStorage,
            batch,
            deadline_at_ms,
            analytics_engine::manifest_digest(&manifest),
        )
        .map_err(api_config_error)?
        .encode()
        .map_err(api_config_error)?;
        let job_id = JobId::generate().map_err(api_config_error)?;
        self.system
            .enqueue_raw(
                super::source_lifecycle::source_job_with_id(
                    job_id,
                    SourceJobKind::AuxStorage,
                    encoded,
                    now,
                ),
                now,
            )
            .await
            .map_err(api_config_error)?;
        self.system
            .claim(job_id, now)
            .await
            .map_err(api_config_error)
    }

    async fn execute_claim(&self, claim: &ClaimedJob) -> ApiResult<JobOutcome> {
        self.system
            .execute_claimed(claim, wall_now(), monotonic_now)
            .await
            .map_err(api_config_error)
    }
}

async fn load_aux_storage_plans(
    source: &AnalyticsSourceConfig,
    app_state: &Arc<AppState>,
) -> ApiResult<Vec<analytics_storage::SourceTablePlan>> {
    let manifest = app_state.manifest.read().await.clone();
    let plans = SourceTablePlan::from_source(source, &manifest)?;
    let plans = plans
        .into_iter()
        .filter(|plan| plan.view().stream_type == SourceJobKind::AuxStorage.stream_type())
        .collect::<Vec<_>>();
    if plans.is_empty() {
        return Err(api_config_error(BackgroundJobError::InvalidConfiguration(
            "aux-storage source poller has no configured tables",
        )));
    }
    Ok(plans)
}

async fn load_system_resume(app_state: &Arc<AppState>) -> ApiResult<Option<SystemStreamResume>> {
    let checkpoint = app_state
        .engine
        .with_read(|engine| engine.load_system_stream_checkpoint())
        .await??;
    Ok(checkpoint
        .map(|(source_scope, checkpoint)| SystemStreamResume::new(source_scope, checkpoint)))
}

async fn install_poller(
    source: &AnalyticsSourceConfig,
    shared: &Arc<SourceJobShared>,
    plans: Vec<analytics_storage::SourceTablePlan>,
    system_resume: Option<SystemStreamResume>,
) -> ApiResult<()> {
    initialize_source_runtime(shared, &plans).await?;
    let poller =
        SourcePoller::from_plans_with_system_resume(source, plans, &[], system_resume).await?;
    shared.pollers.lock().await.insert(
        SourceJobKind::AuxStorage,
        Arc::new(tokio::sync::Mutex::new(poller)),
    );
    Ok(())
}

async fn connect_scheduled_store(
    job_store: &AnalyticsJobStoreConfig,
    timeout: Duration,
) -> ApiResult<ScheduledJobStore> {
    let endpoint = job_store.endpoint_url.as_deref().ok_or_else(|| {
        api_config_error(BackgroundJobError::InvalidConfiguration(
            "aux-storage scheduled store endpoint is missing",
        ))
    })?;
    let config = RemoteScheduledJobStoreConfig::new([endpoint], timeout)
        .map_err(|error| ApiError::with_source(ApiErrorKind::Config, error))?;
    connect_remote_scheduled_job_store(config)
        .await
        .map_err(|error| ApiError::with_source(ApiErrorKind::Config, error))
}

fn build_job_system(
    shared: &Arc<SourceJobShared>,
    store: ScheduledJobStore,
    timeout: Duration,
) -> ApiResult<JobSystem<ScheduledJobStore>> {
    let catalog = job_catalog(Arc::clone(shared), None, timeout)?;
    let observer = Arc::new(BackgroundJobObserver::new(
        Arc::clone(&shared.source_health),
        shared.worker_id.clone(),
    ));
    test_support::build_distributed_system(
        store,
        catalog,
        b"analytics-api:bdd-aux-system-stream",
        2,
        Some(observer),
    )
    .map_err(api_config_error)
}

pub(crate) async fn system_stream_cutover(
    source: AnalyticsSourceConfig,
    job_store: AnalyticsJobStoreConfig,
    app_state: Arc<AppState>,
) -> ApiResult<SystemStreamCutoverReceipt> {
    let (first_page_records, first_progress_commits) =
        recover_committed_page(&source, &job_store, &app_state).await?;

    let resumed_runtime = BddSystemStreamRuntime::build(&source, &job_store, &app_state).await?;
    let resumed_page = resumed_runtime.next_batch().await?;
    let resumed_page_records = resumed_runtime.apply_batch(resumed_page).await?;
    let checkpoint_only_page = resumed_runtime.next_batch().await?;
    if !checkpoint_only_page.records.is_empty()
        || !checkpoint_only_page.checkpoints.is_empty()
        || checkpoint_only_page.system_stream.is_some()
    {
        return Err(api_config_error(BackgroundJobError::InvalidConfiguration(
            "assembled aux-storage idle page must not create a durable job effect",
        )));
    }
    let resumed_progress_commits = resumed_runtime
        .shared
        .progress_commits
        .load(std::sync::atomic::Ordering::Relaxed);
    Ok(SystemStreamCutoverReceipt {
        runtime: SourceRuntime::JobSystem,
        first_page_records,
        first_run_progress_commits: first_progress_commits,
        resumed_page_records,
        checkpoint_only_pages: 1,
        progress_commits: first_progress_commits + resumed_progress_commits,
    })
}

async fn recover_committed_page(
    source: &AnalyticsSourceConfig,
    job_store: &AnalyticsJobStoreConfig,
    app_state: &Arc<AppState>,
) -> ApiResult<(usize, usize)> {
    let stale_runtime = BddSystemStreamRuntime::build(source, job_store, app_state).await?;
    let stale_page = stale_runtime.next_batch().await?;
    let committing_runtime = BddSystemStreamRuntime::build(source, job_store, app_state).await?;
    let first_page = committing_runtime.next_batch().await?;
    let records = committing_runtime.apply_batch(first_page).await?;
    stale_runtime.apply_batch(stale_page).await?;
    let recovered_page = stale_runtime.next_batch().await?;
    if recovered_page.records.is_empty() {
        return Err(config_error(
            "stale runtime must resume the next source page",
        ));
    }
    let progress_commits = committing_runtime
        .shared
        .progress_commits
        .load(std::sync::atomic::Ordering::Relaxed);
    if stale_runtime
        .shared
        .progress_commits
        .load(std::sync::atomic::Ordering::Relaxed)
        != 0
    {
        return Err(config_error(
            "stale source effects must not count progress twice",
        ));
    }
    Ok((records, progress_commits))
}

fn config_error(message: &'static str) -> ApiError {
    ApiError::with_debug(
        ApiErrorKind::Config,
        ApiErrorDebug::Message(message.to_string()),
    )
}
