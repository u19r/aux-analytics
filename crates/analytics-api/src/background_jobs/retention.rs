use std::{error::Error, num::NonZeroU16, sync::Arc, time::Instant};

use analytics_engine::{
    RetentionEffectAuthority, RetentionEffectDisposition, RetentionEffectRequest,
    RetentionEffectRequestInput, RetentionEffectScope,
};
use bg_jobs::{
    EffectMode, ExecutionContext, FairnessPolicy, HandlerFuture, JobCatalog, JobHandle, JobHandler,
    JobKind, JobOutcome, JobSpec, OutcomeCode, QueueName, RetryPreset, RetrySelection,
    ValidationError,
};
use bytes::Bytes;
use tokio::sync::RwLock;

use crate::{
    RetentionHealth, RetentionHealthStatus,
    background_jobs::{
        error::{BackgroundJobError, api_config_error},
        observation::SourceJobShared,
        retention_contracts::RetentionJobPayload,
    },
    constants::{
        MAX_RETENTION_JOB_TIMEOUT, RETENTION_BATCH_DURATION_METRIC, RETENTION_BATCHES_METRIC,
        RETENTION_JOB_KIND, RETENTION_QUEUE, RETENTION_ROWS_METRIC, RETENTION_SCHEMA_VERSION,
    },
    retention::RetentionRuntime,
};

pub(super) fn register_catalog(
    catalog: &mut JobCatalog,
    shared: Arc<SourceJobShared>,
    retention: Arc<RetentionRuntime>,
    health: Arc<RwLock<RetentionHealth>>,
    timeout: std::time::Duration,
) -> crate::error::ApiResult<JobHandle<RetentionJobPayload>> {
    catalog
        .register(retention_spec(shared, retention, health, timeout)?)
        .map_err(api_config_error)
}

fn retention_spec(
    shared: Arc<SourceJobShared>,
    retention: Arc<RetentionRuntime>,
    health: Arc<RwLock<RetentionHealth>>,
    timeout: std::time::Duration,
) -> crate::error::ApiResult<JobSpec<RetentionJobPayload, RetentionJobHandler>> {
    let kind = JobKind::try_new(RETENTION_JOB_KIND).map_err(api_config_error)?;
    let schema = NonZeroU16::new(RETENTION_SCHEMA_VERSION).ok_or_else(|| {
        api_config_error(BackgroundJobError::InvalidConfiguration(
            "retention schema version is zero",
        ))
    })?;
    let queue = QueueName::try_new(RETENTION_QUEUE).map_err(api_config_error)?;
    Ok(JobSpec::new(
        kind,
        schema,
        queue,
        RetentionJobHandler {
            shared,
            retention,
            health,
        },
        encode_retention_payload as fn(&RetentionJobPayload) -> Result<Bytes, ValidationError>,
    )
    .policy(
        EffectMode::Fenced,
        FairnessPolicy::low_volume(),
        RetrySelection::Preset(RetryPreset::Maintenance),
        timeout.min(MAX_RETENTION_JOB_TIMEOUT),
    ))
}

fn encode_retention_payload(payload: &RetentionJobPayload) -> Result<Bytes, ValidationError> {
    payload
        .encode()
        .map(Bytes::from)
        .map_err(|_| ValidationError::InvalidValue {
            field: "retention job payload",
        })
}

struct RetentionJobHandler {
    shared: Arc<SourceJobShared>,
    retention: Arc<RetentionRuntime>,
    health: Arc<RwLock<RetentionHealth>>,
}

impl JobHandler for RetentionJobHandler {
    fn execute<'a>(
        &'a self,
        context: ExecutionContext<'a>,
        payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        let shared = Arc::clone(&self.shared);
        let retention = Arc::clone(&self.retention);
        let health = Arc::clone(&self.health);
        Box::pin(
            async move { execute_retention_job(shared, retention, health, context, payload).await },
        )
    }
}

async fn execute_retention_job(
    shared: Arc<SourceJobShared>,
    retention: Arc<RetentionRuntime>,
    health: Arc<RwLock<RetentionHealth>>,
    context: ExecutionContext<'_>,
    encoded: &[u8],
) -> Result<JobOutcome, Box<dyn Error + Send + Sync>> {
    if context.is_cancelled() {
        return Ok(JobOutcome::Cancelled);
    }
    let payload = match RetentionJobPayload::decode(encoded) {
        Ok(payload) => payload,
        Err(error) => {
            mark_failure(&health).await;
            return Err(Box::new(error));
        }
    };
    let table_name = payload.analytics_table_name().to_owned();
    if !retention.is_table_configured(table_name.as_str()) {
        let error =
            BackgroundJobError::RetentionInvalidPayload("retention table is not configured");
        mark_failure(&health).await;
        return Err(Box::new(error));
    }
    if retention_deadline_elapsed(payload.deadline_at_ms()) {
        return Ok(JobOutcome::Cancelled);
    }
    let request = match retention_request(&context, &payload) {
        Ok(request) => request,
        Err(error) => {
            mark_failure(&health).await;
            return Err(Box::new(error));
        }
    };
    if context.is_cancelled() || retention_deadline_elapsed(payload.deadline_at_ms()) {
        return Ok(JobOutcome::Cancelled);
    }
    let started = Instant::now();
    let disposition = match shared
        .engine
        .with_write(move |engine| engine.delete_expired_rows_fenced(request))
        .await
        .map_err(classify_destination_error)
    {
        Ok(disposition) => disposition,
        Err(error) => {
            mark_failure(&health).await;
            return Err(Box::new(error));
        }
    };
    settle_retention_effect(&health, table_name.as_str(), started, disposition).await
}

fn retention_request(
    context: &ExecutionContext<'_>,
    payload: &RetentionJobPayload,
) -> Result<RetentionEffectRequest, BackgroundJobError> {
    let authority =
        RetentionEffectAuthority::try_new(context.execution_id().as_bytes(), context.fence().get())
            .map_err(|_| {
                BackgroundJobError::RetentionInvalidPayload(
                    "retention execution authority is invalid",
                )
            })?;
    let scope = RetentionEffectScope::try_new(payload.analytics_table_name()).map_err(|_| {
        BackgroundJobError::RetentionInvalidPayload("retention table scope is invalid")
    })?;
    RetentionEffectRequest::try_new(RetentionEffectRequestInput {
        authority,
        scope,
        cutoff_ms: payload.cutoff_ms(),
        schedule_at_ms: payload.schedule_at_ms(),
        server_time_ms: payload.server_time_ms(),
        batch_size: payload.batch_size(),
        cursor: payload.cursor().map(ToOwned::to_owned),
    })
    .map_err(|_| BackgroundJobError::RetentionInvalidPayload("retention request is invalid"))
}

async fn settle_retention_effect(
    health: &RwLock<RetentionHealth>,
    table_name: &str,
    started: Instant,
    disposition: RetentionEffectDisposition,
) -> Result<JobOutcome, Box<dyn Error + Send + Sync>> {
    let (outcome, rows_deleted, complete, count_deleted_rows) = match disposition {
        RetentionEffectDisposition::Applied { summary } => {
            ("success", summary.rows_deleted(), summary.complete(), true)
        }
        RetentionEffectDisposition::AlreadyApplied { summary } => (
            "already_applied",
            summary.rows_deleted(),
            summary.complete(),
            false,
        ),
        RetentionEffectDisposition::StaleFence => ("stale_fence", 0, false, false),
    };
    metrics::counter!(RETENTION_BATCHES_METRIC, "table" => table_name.to_string(), "outcome" => outcome).increment(1);
    metrics::histogram!(RETENTION_BATCH_DURATION_METRIC, "table" => table_name.to_string(), "outcome" => outcome).record(duration_ms(started));
    if count_deleted_rows && rows_deleted > 0 {
        metrics::counter!(RETENTION_ROWS_METRIC, "table" => table_name.to_string())
            .increment(rows_deleted);
    }
    if outcome == "success" || outcome == "already_applied" {
        record_retention_settlement(
            health,
            complete,
            if count_deleted_rows { rows_deleted } else { 0 },
            started,
        )
        .await;
    }
    let code = OutcomeCode::try_new(match outcome {
        "success" => "retention_applied",
        "already_applied" => "retention_already_applied",
        _ => "retention_stale_fence",
    })?;
    Ok(JobOutcome::Complete(code))
}

async fn record_retention_settlement(
    health: &RwLock<RetentionHealth>,
    complete: bool,
    rows_deleted: u64,
    started: Instant,
) {
    let mut health = health.write().await;
    health.status = if complete {
        RetentionHealthStatus::Healthy
    } else {
        RetentionHealthStatus::Degraded
    };
    if complete {
        health.last_sweep_completed_at_ms = Some(crate::clock::now_ms());
    }
    health.last_sweep_duration_ms = Some(duration_ms(started));
    health.total_rows_deleted = health.total_rows_deleted.saturating_add(rows_deleted);
}

async fn mark_failure(health: &RwLock<RetentionHealth>) {
    let mut health = health.write().await;
    health.status = RetentionHealthStatus::Degraded;
}

fn classify_destination_error(error: analytics_engine::AnalyticsEngineError) -> BackgroundJobError {
    match error {
        analytics_engine::AnalyticsEngineError::RetentionEffectFenceUnavailable => {
            BackgroundJobError::RetentionDestinationFenceUnavailable
        }
        analytics_engine::AnalyticsEngineError::DuckDb(_)
        | analytics_engine::AnalyticsEngineError::Io(_) => {
            BackgroundJobError::RetentionDestinationBackend
        }
        _ => BackgroundJobError::RetentionDestination,
    }
}

fn duration_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn retention_deadline_elapsed(deadline_at_ms: u64) -> bool {
    crate::clock::now_ms() >= u128::from(deadline_at_ms)
}
