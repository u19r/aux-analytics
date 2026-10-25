use std::time::Duration;

use analytics_storage::PollBatch;
use bg_jobs::{
    FairnessClass, JobId, JobKind, JobOutcome, JobState, OutcomeCode, Priority, RetryClass,
    RetryPreset, RetrySelection, Timestamp,
    test_support::raw::{
        CancellationResult, ClaimedJob, LifecycleObservation, NewJob, OutcomeResult,
    },
};

use super::{BddSourceRuntime, payload_bytes};
use crate::{
    SourceHealthStatus,
    background_jobs::{
        contracts::SourceJobKind,
        error::{BackgroundJobError, api_config_error},
    },
    constants::SOURCE_SCHEMA_VERSION,
    error::ApiResult,
};

pub(crate) async fn source_stale_fence(
    source: config::AnalyticsSourceConfig,
    app_state: std::sync::Arc<crate::AppState>,
) -> ApiResult<crate::bdd_test_support::SourceStaleFenceReceipt> {
    let runtime = BddSourceRuntime::build(&source, &app_state).await?;
    let (kind, batch) = runtime.first_batch().await?;
    let (source_table, shard) = batch_scope(&batch)?;
    let encoded = payload_bytes(&runtime, kind, batch).await?;
    let now = crate::background_jobs::test_support::wall_now();
    let job_id = JobId::from([0xb8; 16]);
    let old_claim = enqueue_and_claim(&runtime, job_id, kind, encoded.clone(), now).await?;
    let reclaim_at = reclaim_source_job(&runtime, job_id, &old_claim).await?;
    let newer_id = JobId::from([0xb9; 16]);
    apply_newer_source_job(&runtime, newer_id, kind, encoded, reclaim_at).await?;
    assert_stale_source_effect(&runtime, &old_claim).await?;
    Ok(crate::bdd_test_support::SourceStaleFenceReceipt {
        lifecycle: crate::bdd_test_support::SourceLifecycle::StaleFence,
        rows_committed: runtime.row_count(&runtime.analytics_table()?).await?,
        checkpoint_position: checkpoint_position(&runtime, &source_table, &shard).await?,
        progress_commits: runtime.progress_commits(),
    })
}

async fn enqueue_and_claim(
    runtime: &BddSourceRuntime,
    job_id: JobId,
    kind: SourceJobKind,
    encoded: Vec<u8>,
    now: Timestamp,
) -> ApiResult<ClaimedJob> {
    runtime
        .system
        .enqueue_raw(source_job_with_id(job_id, kind, encoded, now), now)
        .await
        .map_err(api_config_error)?;
    runtime
        .system
        .claim(job_id, now)
        .await
        .map_err(api_config_error)
}

async fn reclaim_source_job(
    runtime: &BddSourceRuntime,
    job_id: JobId,
    old_claim: &ClaimedJob,
) -> ApiResult<Timestamp> {
    let reclaim_at = lease_expiry(old_claim)?
        .checked_add(Duration::from_millis(1))
        .map_err(api_config_error)?;
    runtime
        .system
        .claim(job_id, reclaim_at)
        .await
        .map(|_| reclaim_at)
        .map_err(api_config_error)
}

async fn apply_newer_source_job(
    runtime: &BddSourceRuntime,
    job_id: JobId,
    kind: SourceJobKind,
    encoded: Vec<u8>,
    now: Timestamp,
) -> ApiResult<()> {
    let claim = enqueue_and_claim(runtime, job_id, kind, encoded, now).await?;
    let outcome = runtime.execute_claim(&claim).await?;
    runtime
        .system
        .complete(claim, outcome, now)
        .await
        .map(|_| ())
        .map_err(api_config_error)
}

async fn assert_stale_source_effect(
    runtime: &BddSourceRuntime,
    old_claim: &ClaimedJob,
) -> ApiResult<()> {
    let outcome = runtime.execute_claim(old_claim).await?;
    if matches!(outcome, JobOutcome::Complete(ref code) if code.as_str() == "source_stale_fence") {
        return Ok(());
    }
    Err(api_config_error(BackgroundJobError::InvalidConfiguration(
        "stale source execution was not fenced",
    )))
}

pub(crate) async fn cancel_and_repair_source(
    source: config::AnalyticsSourceConfig,
    app_state: std::sync::Arc<crate::AppState>,
) -> ApiResult<crate::bdd_test_support::SourceCancelRepairReceipt> {
    let runtime = BddSourceRuntime::build(&source, &app_state).await?;
    let (kind, batch) = runtime.first_batch().await?;
    let encoded = payload_bytes(&runtime, kind, batch).await?;
    let now = crate::background_jobs::test_support::wall_now();
    let cancelled_id = JobId::from([0xba; 16]);
    cancel_source_job(&runtime, cancelled_id, kind, encoded.clone(), now).await?;
    let pre_repair_rows = runtime.row_count(&runtime.analytics_table()?).await?;
    let repair_id = JobId::from([0xbb; 16]);
    let scheduled_at = schedule_repair(&runtime, repair_id, kind, encoded, now).await?;
    repair_source_job(&runtime, repair_id, scheduled_at).await?;
    let health = runtime.shared.source_health.read().await;
    Ok(crate::bdd_test_support::SourceCancelRepairReceipt {
        cancellation: crate::bdd_test_support::SourceLifecycle::Cancelled,
        repair: crate::bdd_test_support::SourceLifecycle::Repaired,
        effect: crate::bdd_test_support::SourceLifecycle::Applied,
        health: if matches!(health.status, SourceHealthStatus::Healthy) {
            crate::bdd_test_support::SourceScenarioHealth::Healthy
        } else {
            crate::bdd_test_support::SourceScenarioHealth::Degraded
        },
        pre_repair_rows,
        rows_committed: runtime.row_count(&runtime.analytics_table()?).await?,
        progress_commits: runtime.progress_commits(),
    })
}

async fn cancel_source_job(
    runtime: &BddSourceRuntime,
    job_id: JobId,
    kind: SourceJobKind,
    encoded: Vec<u8>,
    now: Timestamp,
) -> ApiResult<()> {
    let claim = enqueue_and_claim(runtime, job_id, kind, encoded, now).await?;
    let requested = runtime
        .system
        .request_job_cancellation(&claim, now)
        .await
        .map_err(api_config_error)?;
    let cancelled = runtime
        .system
        .cancel_claim(claim, now)
        .await
        .map_err(api_config_error)?;
    if matches!(requested, CancellationResult::Requested)
        && matches!(cancelled, CancellationResult::Cancelled)
    {
        return Ok(());
    }
    Err(api_config_error(BackgroundJobError::InvalidConfiguration(
        "source cancellation did not commit",
    )))
}

async fn schedule_repair(
    runtime: &BddSourceRuntime,
    job_id: JobId,
    kind: SourceJobKind,
    encoded: Vec<u8>,
    now: Timestamp,
) -> ApiResult<Timestamp> {
    runtime
        .system
        .enqueue_raw(source_job_with_id(job_id, kind, encoded, now), now)
        .await
        .map_err(api_config_error)?;
    let claim = runtime
        .system
        .claim(job_id, now)
        .await
        .map_err(api_config_error)?;
    let retry = runtime
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
    match retry {
        OutcomeResult::Retried { not_before, .. } => Ok(not_before),
        _ => Err(api_config_error(BackgroundJobError::InvalidConfiguration(
            "source repair seed did not schedule",
        ))),
    }
}

async fn repair_source_job(
    runtime: &BddSourceRuntime,
    job_id: JobId,
    scheduled_at: Timestamp,
) -> ApiResult<()> {
    repair_lifecycle(runtime, job_id).await?;
    let repaired_claim = runtime
        .system
        .claim(job_id, scheduled_at)
        .await
        .map_err(api_config_error)?;
    let repaired_outcome = runtime.execute_claim(&repaired_claim).await?;
    if !matches!(repaired_outcome, JobOutcome::Complete(ref code) if code.as_str() == "source_applied")
    {
        return Err(api_config_error(BackgroundJobError::InvalidConfiguration(
            "repaired source effect was not applied",
        )));
    }
    runtime
        .system
        .complete(repaired_claim, repaired_outcome, scheduled_at)
        .await
        .map(|_| ())
        .map_err(api_config_error)
}

async fn repair_lifecycle(runtime: &BddSourceRuntime, job_id: JobId) -> ApiResult<()> {
    let (revision, operation) = match runtime
        .system
        .observe_lifecycle(job_id)
        .await
        .map_err(api_config_error)?
    {
        LifecycleObservation::Current {
            record,
            operation_id: Some(operation),
            ..
        } => (record.revision, operation),
        _ => {
            return Err(api_config_error(BackgroundJobError::InvalidConfiguration(
                "scheduled source repair is not observable",
            )));
        }
    };
    runtime
        .system
        .repair_job(job_id, revision, operation)
        .await
        .map_err(api_config_error)
}

pub(super) fn source_job_with_id(
    id: JobId,
    kind: SourceJobKind,
    encoded: Vec<u8>,
    now: Timestamp,
) -> NewJob {
    NewJob {
        job_id: id,
        kind: JobKind::try_new(kind.job_kind()).expect("source job kind is valid"),
        schema_version: std::num::NonZeroU16::new(SOURCE_SCHEMA_VERSION)
            .expect("source schema is non-zero"),
        payload: encoded.into(),
        fairness: FairnessClass::low_volume(),
        ordering_key: None,
        priority: Priority::Default,
        scheduled_at: now,
        expires_at: None,
        retry_override: Some(RetrySelection::Preset(RetryPreset::Network)),
        deduplication: None,
    }
}

fn lease_expiry(claim: &ClaimedJob) -> ApiResult<Timestamp> {
    match &claim.record().state {
        JobState::Leased(lease) => Ok(lease.expires_at),
        _ => Err(api_config_error(BackgroundJobError::InvalidConfiguration(
            "source claim is not leased",
        ))),
    }
}

fn batch_scope(batch: &PollBatch) -> ApiResult<(String, String)> {
    let scope = batch
        .source_scope()?
        .ok_or_else(|| api_config_error(BackgroundJobError::MissingScope))?;
    Ok((
        scope.source_table_name().to_string(),
        scope.shard_id().to_string(),
    ))
}

async fn checkpoint_position(
    runtime: &BddSourceRuntime,
    source_table: &str,
    shard: &str,
) -> ApiResult<Option<String>> {
    Ok(runtime
        .shared
        .engine
        .with_read(|engine| engine.load_source_checkpoints())
        .await??
        .into_iter()
        .find(|checkpoint| {
            checkpoint.source_table_name() == source_table && checkpoint.shard_id() == shard
        })
        .map(|checkpoint| checkpoint.position().to_string()))
}

async fn row_count(runtime: &BddSourceRuntime, table: &str) -> ApiResult<u64> {
    let quoted = format!("\"{}\"", table.replace('"', "\"\""));
    let rows = runtime
        .shared
        .engine
        .with_read(move |engine| {
            engine
                .query_unscoped_sql_json(format!("select count(*) as count from {quoted}").as_str())
        })
        .await??;
    rows.first()
        .and_then(|row| row["count"].as_i64())
        .and_then(|count| u64::try_from(count).ok())
        .ok_or_else(|| {
            api_config_error(BackgroundJobError::InvalidConfiguration(
                "source row count is not observable",
            ))
        })
}

impl BddSourceRuntime {
    async fn row_count(&self, table: &str) -> ApiResult<u64> {
        row_count(self, table).await
    }

    fn progress_commits(&self) -> usize {
        self.shared
            .progress_commits
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}
