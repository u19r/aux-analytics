use std::{num::NonZeroU16, sync::Arc};

use bg_jobs::{
    FairnessClass, JobId, JobKind, JobOutcome, OutcomeCode, OwnerId, Priority, RetryClass,
    RetryPreset, RetrySelection, Timestamp,
    runtime::MonotonicDeadline,
    test_support::{
        FakeStore,
        codec::{ValueCodec, ValueTag},
        raw::{
            CancellationResult, ClaimedJob, LifecycleObservation, NewJob, OperationId,
            OutcomeResult,
        },
    },
};
use bg_jobs_tokio::test_support::JobClock;
use config::AnalyticsRetentionConfig;
use tokio::sync::RwLock;

use super::super::{
    BackgroundJobRuntime, JobCatalogBundle, error::api_config_error, observation::SourceJobShared,
    retention_contracts::RetentionJobPayload, source_job_shared_for_bdd, test_support::wall_now,
};
use crate::{
    ApiError, ApiResult, AppState, RetentionHealth, RetentionHealthStatus,
    constants::{
        MAX_RETENTION_JOB_TIMEOUT, RETENTION_CUTOFF_MS, RETENTION_JOB_KIND,
        RETENTION_SCHEMA_VERSION, RETENTION_TABLE,
    },
    error::{ApiErrorDebug, ApiErrorKind},
    retention::{RetentionRuntime, retention_schedule_due_for_bdd},
};

#[derive(Debug, Default)]
struct BddRetentionClock;

impl BddRetentionClock {
    fn now_ms(&self) -> u128 {
        u128::try_from(wall_now().as_millis()).unwrap_or(0)
    }
}

impl bg_jobs_tokio::test_support::JobClock for BddRetentionClock {
    fn wall_now(&self) -> Timestamp {
        wall_now()
    }

    fn monotonic_now(&self) -> MonotonicDeadline {
        MonotonicDeadline::ZERO
    }
}

struct RetentionBddRuntime {
    runtime: Arc<BackgroundJobRuntime<FakeStore>>,
    shared: Arc<SourceJobShared>,
    health: Arc<RwLock<RetentionHealth>>,
    store: FakeStore,
}

impl RetentionBddRuntime {
    async fn build(
        config: &AnalyticsRetentionConfig,
        app_state: &Arc<AppState>,
        store: FakeStore,
        owner: u8,
        clock: Arc<BddRetentionClock>,
    ) -> ApiResult<Self> {
        let retention = retention_runtime(config).await?;
        let mut shared = source_job_shared_for_bdd(app_state);
        attach_retention(&mut shared, &retention)?;
        let health = Arc::clone(&app_state.retention_health);
        reset_health(&health, retention.table_names().count()).await;
        let catalog = JobCatalogBundle::try_new(
            Arc::clone(&shared),
            Some((Arc::clone(&retention), Arc::clone(&health))),
            MAX_RETENTION_JOB_TIMEOUT,
        )?;
        let runtime =
            BackgroundJobRuntime::for_test(crate::background_jobs::runtime::TestRuntimeInput {
                store: store.clone(),
                catalog,
                owner: OwnerId::from([owner; 16]),
                shared: Arc::clone(&shared),
                retention: Some(retention),
                health: Some(Arc::clone(&health)),
                source_job_timeout: MAX_RETENTION_JOB_TIMEOUT,
                clock: Arc::clone(&clock) as Arc<dyn JobClock>,
            });
        Ok(Self {
            runtime,
            shared,
            health,
            store,
        })
    }

    async fn materialize(&self) -> ApiResult<()> {
        self.runtime
            .materialize_retention_jobs()
            .await
            .ok_or_else(|| bdd_error("retention schedule did not materialize"))
            .map(|_| ())
    }

    async fn drive(&self) -> ApiResult<()> {
        self.runtime
            .runtime
            .drive_once()
            .await
            .map(|_| ())
            .map_err(api_config_error)
    }

    async fn expired_rows(&self) -> ApiResult<u64> {
        Ok(self
            .shared
            .engine
            .with_read(|engine| {
                engine.expired_retention_count(RETENTION_TABLE, RETENTION_CUTOFF_MS)
            })
            .await??)
    }

    async fn execute_claim(&self, claim: &ClaimedJob) -> ApiResult<JobOutcome> {
        self.runtime
            .system
            .execute_claimed(
                claim,
                crate::background_jobs::test_support::wall_now(),
                crate::background_jobs::test_support::monotonic_now,
            )
            .await
            .map_err(api_config_error)
    }
}

async fn retention_runtime(config: &AnalyticsRetentionConfig) -> ApiResult<Arc<RetentionRuntime>> {
    Ok(Arc::new(
        RetentionRuntime::from_config(config)
            .await?
            .ok_or_else(|| bdd_error("retention runtime is disabled"))?,
    ))
}

fn attach_retention(
    shared: &mut Arc<SourceJobShared>,
    retention: &Arc<RetentionRuntime>,
) -> ApiResult<()> {
    Arc::get_mut(shared)
        .ok_or_else(|| bdd_error("retention BDD shared state was already aliased"))?
        .retention = Some(Arc::clone(retention));
    Ok(())
}

async fn reset_health(health: &RwLock<RetentionHealth>, table_count: usize) {
    *health.write().await = RetentionHealth::starting(table_count);
}

pub(crate) async fn retention_schedule(
    config: AnalyticsRetentionConfig,
    app_state: Arc<AppState>,
) -> ApiResult<crate::bdd_test_support::RetentionScheduleReceipt> {
    let store = FakeStore::new();
    let clock = Arc::new(BddRetentionClock);
    let first =
        RetentionBddRuntime::build(&config, &app_state, store.clone(), 0xc1, Arc::clone(&clock))
            .await?;
    first.materialize().await?;
    let first_payloads = retention_payload_count(&first.store)?;
    let first_due = retention_schedule_due_for_bdd(&first.runtime)
        .await
        .ok_or_else(|| bdd_error("retention schedule was not durable"))?;
    let (restarted, restarted_payloads, restarted_due) =
        restart_retention(&config, &app_state, store, Arc::clone(&clock)).await?;
    if first_payloads != 1 || restarted_payloads != first_payloads || restarted_due != first_due {
        return Err(bdd_error("retention due occurrence was not deduplicated"));
    }
    restarted.drive().await?;
    let eligible_remaining = restarted.expired_rows().await?;
    let (rows_deleted, health, status) = retention_schedule_state(&restarted.health).await?;
    let schedule_advanced = first_due > u64::try_from(clock.now_ms()).unwrap_or(u64::MAX);
    Ok(crate::bdd_test_support::RetentionScheduleReceipt {
        runtime: crate::bdd_test_support::RetentionRuntime::JobSystem,
        due_occurrences: first_payloads,
        schedule_advanced,
        rows_deleted,
        eligible_remaining,
        status,
        health,
    })
}

async fn restart_retention(
    config: &AnalyticsRetentionConfig,
    app_state: &Arc<AppState>,
    store: FakeStore,
    clock: Arc<BddRetentionClock>,
) -> ApiResult<(RetentionBddRuntime, usize, u64)> {
    let restarted = RetentionBddRuntime::build(config, app_state, store, 0xc2, clock).await?;
    restarted.materialize().await?;
    let payloads = retention_payload_count(&restarted.store)?;
    let due = retention_schedule_due_for_bdd(&restarted.runtime)
        .await
        .ok_or_else(|| bdd_error("restarted retention schedule was not durable"))?;
    Ok((restarted, payloads, due))
}

pub(crate) async fn retention_cancel_and_repair(
    config: AnalyticsRetentionConfig,
    app_state: Arc<AppState>,
) -> ApiResult<crate::bdd_test_support::RetentionCancelRepairReceipt> {
    let clock = Arc::new(BddRetentionClock);
    let runtime = RetentionBddRuntime::build(
        &config,
        &app_state,
        FakeStore::new(),
        0xc3,
        Arc::clone(&clock),
    )
    .await?;
    let now = clock.wall_now();
    let payload = repair_payload(&clock)?;
    let decoded_payload = RetentionJobPayload::decode(&payload).map_err(api_config_error)?;
    let table_name = decoded_payload.analytics_table_name().to_owned();
    let cutoff_ms = decoded_payload.cutoff_ms();
    let schedule_at_ms = decoded_payload.schedule_at_ms();
    let server_time_ms = decoded_payload.server_time_ms();
    runtime
        .shared
        .engine
        .with_write(move |engine| {
            engine.authorize_retention_schedule_occurrence(
                table_name.as_str(),
                cutoff_ms,
                schedule_at_ms,
                server_time_ms,
            )
        })
        .await?;
    let cancellation =
        cancel_due_job(&runtime, JobId::from([0xc4; 16]), payload.clone(), now).await?;
    let pre_repair_rows_deleted = runtime.health.read().await.total_rows_deleted;
    let repair_at = seed_repair_job(&runtime, JobId::from([0xc5; 16]), payload, now).await?;
    let (repair, effect) = repair_job(&runtime, JobId::from([0xc5; 16]), repair_at).await?;
    let health = runtime.health.read().await;
    Ok(crate::bdd_test_support::RetentionCancelRepairReceipt {
        cancellation,
        repair,
        effect,
        health: retention_scenario_health(&health.status),
        pre_repair_rows_deleted,
        rows_deleted: health.total_rows_deleted,
    })
}

async fn cancel_due_job(
    runtime: &RetentionBddRuntime,
    id: JobId,
    payload: Vec<u8>,
    now: Timestamp,
) -> ApiResult<crate::bdd_test_support::RetentionLifecycle> {
    let claim = enqueue_and_claim(runtime, id, payload, now).await?;
    let requested = runtime
        .runtime
        .system
        .request_job_cancellation(&claim, now)
        .await
        .map_err(api_config_error)?;
    let cancelled = runtime
        .runtime
        .system
        .cancel_claim(claim, now)
        .await
        .map_err(api_config_error)?;
    if matches!(requested, CancellationResult::Requested)
        && matches!(cancelled, CancellationResult::Cancelled)
    {
        Ok(crate::bdd_test_support::RetentionLifecycle::Cancelled)
    } else {
        Err(bdd_error("retention cancellation did not commit"))
    }
}

async fn seed_repair_job(
    runtime: &RetentionBddRuntime,
    id: JobId,
    payload: Vec<u8>,
    now: Timestamp,
) -> ApiResult<Timestamp> {
    let claim = enqueue_and_claim(runtime, id, payload, now).await?;
    let outcome = runtime
        .runtime
        .system
        .apply_outcome(
            claim,
            JobOutcome::Retry {
                class: RetryClass::Transient,
                not_before: Some(now),
                code: OutcomeCode::try_new("bdd_repair_seed").map_err(api_config_error)?,
            },
            now,
        )
        .await
        .map_err(api_config_error)?;
    match outcome {
        OutcomeResult::Retried { not_before, .. } => Ok(not_before),
        _ => Err(bdd_error("retention repair seed did not schedule")),
    }
}

async fn repair_job(
    runtime: &RetentionBddRuntime,
    id: JobId,
    scheduled_at: Timestamp,
) -> ApiResult<(
    crate::bdd_test_support::RetentionLifecycle,
    crate::bdd_test_support::RetentionLifecycle,
)> {
    let (revision, operation) = repair_authority(runtime, id).await?;
    runtime
        .runtime
        .system
        .repair_job(id, revision, operation)
        .await
        .map_err(api_config_error)?;
    let claim = runtime
        .runtime
        .system
        .claim(id, scheduled_at)
        .await
        .map_err(api_config_error)?;
    let outcome = runtime.execute_claim(&claim).await?;
    if !matches!(&outcome, JobOutcome::Complete(code) if code.as_str() == "retention_applied") {
        return Err(bdd_error("repaired retention effect was not applied"));
    }
    runtime
        .runtime
        .system
        .complete(claim, outcome, scheduled_at)
        .await
        .map(|_| ())
        .map_err(api_config_error)?;
    Ok((
        crate::bdd_test_support::RetentionLifecycle::Repaired,
        crate::bdd_test_support::RetentionLifecycle::Applied,
    ))
}

async fn repair_authority(
    runtime: &RetentionBddRuntime,
    id: JobId,
) -> ApiResult<(u64, OperationId)> {
    match runtime
        .runtime
        .system
        .observe_lifecycle(id)
        .await
        .map_err(api_config_error)?
    {
        LifecycleObservation::Current {
            record,
            operation_id: Some(operation),
            ..
        } => Ok((record.revision, operation)),
        _ => Err(bdd_error("retention repair is not durably observable")),
    }
}

async fn enqueue_and_claim(
    runtime: &RetentionBddRuntime,
    id: JobId,
    payload: Vec<u8>,
    now: Timestamp,
) -> ApiResult<ClaimedJob> {
    runtime
        .runtime
        .system
        .enqueue_raw(retention_job(id, payload, now), now)
        .await
        .map_err(api_config_error)?;
    runtime
        .runtime
        .system
        .claim(id, now)
        .await
        .map_err(api_config_error)
}

fn retention_job(id: JobId, payload: Vec<u8>, now: Timestamp) -> NewJob {
    NewJob {
        job_id: id,
        kind: JobKind::try_new(RETENTION_JOB_KIND).expect("retention kind"),
        schema_version: NonZeroU16::new(RETENTION_SCHEMA_VERSION).expect("retention schema"),
        payload: payload.into(),
        fairness: FairnessClass::low_volume(),
        ordering_key: None,
        priority: Priority::Default,
        scheduled_at: now,
        expires_at: None,
        retry_override: Some(RetrySelection::Preset(RetryPreset::Maintenance)),
        deduplication: None,
    }
}

fn repair_payload(clock: &BddRetentionClock) -> ApiResult<Vec<u8>> {
    let deadline = clock
        .now_ms()
        .saturating_add(MAX_RETENTION_JOB_TIMEOUT.as_millis())
        .try_into()
        .map_err(|_| bdd_error("retention deadline is out of range"))?;
    RetentionJobPayload::from_schedule(
        RETENTION_TABLE,
        RETENTION_CUTOFF_MS,
        1,
        deadline,
        None,
        RETENTION_CUTOFF_MS,
        u64::try_from(clock.now_ms()).map_err(|_| bdd_error("retention server time is invalid"))?,
    )
    .map_err(api_config_error)?
    .encode()
    .map_err(api_config_error)
}

async fn retention_schedule_state(
    health: &RwLock<RetentionHealth>,
) -> ApiResult<(
    u64,
    crate::bdd_test_support::RetentionScenarioHealth,
    crate::bdd_test_support::RetentionScheduleStatus,
)> {
    let health = health.read().await;
    let scenario_health = retention_scenario_health(&health.status);
    let status = match &health.status {
        RetentionHealthStatus::Degraded => {
            crate::bdd_test_support::RetentionScheduleStatus::Incomplete
        }
        RetentionHealthStatus::Healthy => {
            return Err(bdd_error("retention schedule unexpectedly completed"));
        }
        RetentionHealthStatus::Disabled | RetentionHealthStatus::Starting => {
            return Err(bdd_error("retention schedule health is not settled"));
        }
    };
    Ok((health.total_rows_deleted, scenario_health, status))
}

fn retention_scenario_health(
    status: &RetentionHealthStatus,
) -> crate::bdd_test_support::RetentionScenarioHealth {
    if matches!(status, RetentionHealthStatus::Healthy) {
        crate::bdd_test_support::RetentionScenarioHealth::Healthy
    } else {
        crate::bdd_test_support::RetentionScenarioHealth::Degraded
    }
}

fn retention_payload_count(store: &FakeStore) -> ApiResult<usize> {
    Ok(store
        .snapshot()
        .map_err(api_config_error)?
        .into_iter()
        .filter_map(|(_, value)| {
            let value = ValueCodec::decode(value.as_ref()).ok()?;
            (value.tag == ValueTag::PayloadChunk)
                .then(|| RetentionJobPayload::decode(value.body.as_ref()).ok())
                .flatten()
        })
        .count())
}

fn bdd_error(message: &'static str) -> ApiError {
    ApiError::with_debug(
        ApiErrorKind::Config,
        ApiErrorDebug::Message(message.to_string()),
    )
}
