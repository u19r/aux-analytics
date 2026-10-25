use std::{sync::Arc, time::Duration};

use analytics_storage::{PollBatch, SourceCheckpoint, SourcePoller, SourceTablePlan};
use bg_jobs::{
    JobId, JobOutcome,
    runtime::JobSystem,
    test_support::{FakeStore, raw::ClaimedJob},
};
use config::AnalyticsSourceConfig;

use super::source_lifecycle;
use crate::{
    AppState, SourceHealth, SourcePollingPhase,
    background_jobs::{
        bounded_timeout,
        contracts::{SourceJobKind, SourceJobPayload},
        error::{BackgroundJobError, api_config_error},
        job_catalog,
        observation::{
            BackgroundJobObserver, SourceJobShared, record_source_failure, set_source_phase,
        },
        source_deadline_at_ms, source_job_shared_for_bdd, test_support,
        test_support::{drive_request, monotonic_now, wall_now},
    },
    error::{ApiError, ApiErrorDebug, ApiErrorKind, ApiResult},
};

pub(super) struct BddSourceRuntime {
    pub(super) system: JobSystem<FakeStore>,
    pub(super) shared: Arc<SourceJobShared>,
    pub(super) plans: Vec<SourceTablePlan>,
    pub(super) kinds: Vec<SourceJobKind>,
    pub(super) timeout: Duration,
}

impl BddSourceRuntime {
    pub(super) async fn build(
        source: &AnalyticsSourceConfig,
        app_state: &Arc<AppState>,
    ) -> ApiResult<Self> {
        let timeout = bounded_timeout(source.poll_request_timeout_ms).map_err(config_error)?;
        let manifest = app_state.manifest.read().await.clone();
        let plans = SourceTablePlan::from_source(source, &manifest)?;
        let checkpoints = load_checkpoints(app_state).await?;
        let shared = source_job_shared_for_bdd(app_state);
        initialize_source_runtime(&shared, &plans).await?;
        let kinds = register_pollers(source, &shared, &plans, &checkpoints).await?;
        let system = build_job_system(&shared, timeout)?;
        Ok(Self {
            system,
            shared,
            plans,
            kinds,
            timeout,
        })
    }

    pub(super) async fn poll(&self, kind: SourceJobKind) -> ApiResult<PollBatch> {
        let poller = self
            .shared
            .pollers
            .lock()
            .await
            .get(&kind)
            .cloned()
            .ok_or_else(|| api_config_error(BackgroundJobError::PollerNotRegistered))?;
        Ok(poller.lock().await.poll_once().await?)
    }

    pub(super) async fn drive_batch(
        &self,
        kind: SourceJobKind,
        batch: PollBatch,
    ) -> ApiResult<bool> {
        let failure_metadata = source_failure_metadata(&batch);
        let manifest = self.shared.manifest.read().await;
        let payload = SourceJobPayload::from_batch(
            kind,
            batch,
            source_deadline_at_ms(
                u128::try_from(wall_now().as_millis()).unwrap_or(0),
                self.timeout,
            )
            .map_err(config_error)?,
            analytics_engine::manifest_digest(&manifest),
        )
        .map_err(api_config_error)?;
        let encoded = payload.encode().map_err(api_config_error)?;
        let now = wall_now();
        let job_id = JobId::generate().map_err(api_config_error)?;
        self.system
            .enqueue_raw(
                source_lifecycle::source_job_with_id(job_id, kind, encoded, now),
                now,
            )
            .await
            .map_err(api_config_error)?;
        let request = drive_request().map_err(api_config_error)?;
        let report = self
            .system
            .drive(request, now, monotonic_now)
            .await
            .map_err(api_config_error)?;
        let failed = report.counts.retried > 0
            || matches!(
                self.shared.source_health.read().await.phase,
                SourcePollingPhase::Degraded
            );
        if failed {
            record_source_failure(&self.shared, &failure_metadata).await;
        }
        Ok(failed)
    }

    pub(super) async fn execute_claim(&self, claim: &ClaimedJob) -> ApiResult<JobOutcome> {
        self.system
            .execute_claimed(claim, wall_now(), monotonic_now)
            .await
            .map_err(api_config_error)
    }

    pub(super) async fn first_batch(&self) -> ApiResult<(SourceJobKind, PollBatch)> {
        let kind = self.kinds.first().copied().ok_or_else(|| {
            api_config_error(BackgroundJobError::InvalidConfiguration(
                "source poller has no job kind",
            ))
        })?;
        set_source_phase(&self.shared, SourcePollingPhase::Polling).await;
        let batch = self.poll(kind).await?;
        if batch.records.is_empty() && batch.checkpoints.is_empty() {
            return Err(api_config_error(BackgroundJobError::InvalidConfiguration(
                "source mock returned an empty first batch",
            )));
        }
        Ok((kind, batch))
    }

    pub(super) fn analytics_table(&self) -> ApiResult<String> {
        self.plans
            .iter()
            .flat_map(|plan| plan.view().analytics_table_names.iter())
            .next()
            .cloned()
            .ok_or_else(|| {
                api_config_error(BackgroundJobError::InvalidConfiguration(
                    "source plan has no analytics table",
                ))
            })
    }
}

async fn load_checkpoints(app_state: &Arc<AppState>) -> ApiResult<Vec<SourceCheckpoint>> {
    Ok(app_state
        .engine
        .with_read(|engine| engine.load_source_checkpoints())
        .await??
        .into_iter()
        .map(engine_checkpoint)
        .collect())
}

pub(super) async fn initialize_source_runtime(
    shared: &Arc<SourceJobShared>,
    plans: &[SourceTablePlan],
) -> ApiResult<()> {
    *shared.source_health.write().await = SourceHealth::starting(plans.len());
    shared
        .initialize_source_authority(plans)
        .map_err(api_config_error)
}

async fn register_pollers(
    source: &AnalyticsSourceConfig,
    shared: &Arc<SourceJobShared>,
    plans: &[SourceTablePlan],
    checkpoints: &[SourceCheckpoint],
) -> ApiResult<Vec<SourceJobKind>> {
    let mut kinds = Vec::new();
    for kind in SourceJobKind::ALL {
        let kind_plans = plans
            .iter()
            .filter(|plan| plan.view().stream_type == kind.stream_type())
            .cloned()
            .collect::<Vec<_>>();
        if kind_plans.is_empty() {
            continue;
        }
        let poller = SourcePoller::from_plans(source, kind_plans, checkpoints).await?;
        if poller.is_empty() {
            continue;
        }
        shared
            .pollers
            .lock()
            .await
            .insert(kind, Arc::new(tokio::sync::Mutex::new(poller)));
        kinds.push(kind);
    }
    if kinds.is_empty() {
        *shared.source_health.write().await = SourceHealth::disabled();
        return Err(api_config_error(BackgroundJobError::InvalidConfiguration(
            "source poller has no configured tables",
        )));
    }
    Ok(kinds)
}

fn build_job_system(
    shared: &Arc<SourceJobShared>,
    timeout: Duration,
) -> ApiResult<JobSystem<FakeStore>> {
    let catalog = job_catalog(Arc::clone(shared), None, timeout)?;
    let observer = Arc::new(BackgroundJobObserver::new(
        Arc::clone(&shared.source_health),
        shared.worker_id.clone(),
    ));
    Ok(test_support::build_system(
        FakeStore::new(),
        catalog,
        b"analytics-api:bdd-source",
        1,
        Some(observer),
    ))
}

pub(super) async fn payload_bytes(
    runtime: &BddSourceRuntime,
    kind: SourceJobKind,
    batch: PollBatch,
) -> ApiResult<Vec<u8>> {
    let deadline = source_deadline_at_ms(
        u128::try_from(wall_now().as_millis()).unwrap_or(0),
        runtime.timeout,
    )
    .map_err(config_error)?;
    let manifest = runtime.shared.manifest.read().await;
    SourceJobPayload::from_batch(
        kind,
        batch,
        deadline,
        analytics_engine::manifest_digest(&manifest),
    )
    .map_err(api_config_error)?
    .encode()
    .map_err(api_config_error)
}

fn source_failure_metadata(batch: &PollBatch) -> PollBatch {
    PollBatch {
        source_response_count: 0,
        source_nonempty_response_count: 0,
        source_record_count: batch
            .source_record_count
            .max(batch.records.len())
            .max(batch.checkpoints.len()),
        source_encoded_bytes: 0,
        records: Vec::new(),
        checkpoints: batch.checkpoints.clone(),
        source_predecessor: None,
        system_stream: None,
    }
}

pub(crate) async fn start_source_polling(
    source: AnalyticsSourceConfig,
    _ingest: config::AnalyticsIngestConfig,
    app_state: Arc<AppState>,
) -> ApiResult<crate::bdd_test_support::SourcePollingReceipt> {
    let runtime = BddSourceRuntime::build(&source, &app_state).await?;
    let mut failed = false;
    for _ in 0..8 {
        let mut progressed = false;
        for kind in runtime.kinds.iter().copied() {
            let batch = runtime.poll(kind).await?;
            if batch.records.is_empty() && batch.checkpoints.is_empty() {
                continue;
            }
            progressed = true;
            set_source_phase(&runtime.shared, SourcePollingPhase::Polling).await;
            if runtime.drive_batch(kind, batch).await? {
                failed = true;
                break;
            }
        }
        if failed || !progressed {
            break;
        }
    }
    Ok(crate::bdd_test_support::SourcePollingReceipt {
        runtime: crate::bdd_test_support::SourceRuntime::JobSystem,
        table_count: runtime.plans.len(),
    })
}

fn engine_checkpoint(checkpoint: analytics_engine::RecordCheckpoint) -> SourceCheckpoint {
    SourceCheckpoint {
        source_table_name: checkpoint.source_table_name().to_string(),
        shard_id: checkpoint.shard_id().to_string(),
        position: checkpoint.position().to_string(),
    }
}

fn config_error(message: &'static str) -> ApiError {
    ApiError::with_debug(
        ApiErrorKind::Config,
        ApiErrorDebug::Message(message.to_string()),
    )
}
