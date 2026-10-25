use std::{sync::Arc, time::Duration};

use analytics_engine::StorageBackend;
use analytics_storage::SourceTablePlan;
use bg_jobs::{
    Deduplication, DeduplicationKey, DriveBudget, QueueSelection, Timestamp, ValidationError,
};
use config::{AnalyticsJobStoreConfig, AnalyticsSourceConfig};

use crate::{
    AppState,
    background_jobs::{error::BackgroundJobError, observation::SourceJobShared},
    constants::{
        MAX_JOB_STORE_REQUEST_TIMEOUT, MAX_SOURCE_DEADLINE_AT_MS, MAX_SOURCE_JOB_TIMEOUT,
        MAX_SOURCE_RECORDS,
    },
    error::{ApiError, ApiErrorDebug, ApiErrorKind, ApiResult},
    source_polling::load_worker_id,
};

pub(super) fn driver_budget(timeout: Duration) -> Result<DriveBudget, ValidationError> {
    let finish_window = timeout.checked_mul(2).ok_or(ValidationError::Overflow {
        field: "source job drive duration",
    })?;
    DriveBudget::try_from(bg_jobs::DriveBudgetInput {
        queues: QueueSelection::all(),
        max_claims: 8,
        max_concurrency: 4,
        max_store_operations: 64,
        stop_claiming_after: timeout,
        finish_after: finish_window,
    })
}

pub(super) fn bound_source_page(
    source: &AnalyticsSourceConfig,
    plans: &[SourceTablePlan],
) -> Result<AnalyticsSourceConfig, BackgroundJobError> {
    let fanout = plans
        .iter()
        .map(|plan| plan.view().analytics_table_names.len())
        .max()
        .unwrap_or(1)
        .max(1);
    let page_limit = MAX_SOURCE_RECORDS / fanout;
    if page_limit == 0 {
        return Err(BackgroundJobError::InvalidConfiguration(
            "source destination fan-out exceeds the job record bound",
        ));
    }
    let mut bounded = source.clone();
    bounded.poll_max_records_per_response =
        bounded.poll_max_records_per_response.min(page_limit as u32);
    Ok(bounded)
}

pub(crate) fn job_store_settings<'a>(
    job_store: &'a AnalyticsJobStoreConfig,
    backend: &StorageBackend,
) -> ApiResult<Option<JobStoreSettings<'a>>> {
    if !analytics_engine::AnalyticsEngine::supports_fenced_backend(backend) {
        tracing::warn!(
            reason = "the analytics destination backend has no source fence",
            "background source jobs are disabled"
        );
        return Ok(None);
    }
    let Some(endpoint) = job_store.endpoint_url.as_deref() else {
        tracing::warn!(
            reason = "the scheduled JobStore endpoint is missing",
            "background source jobs are disabled"
        );
        return Ok(None);
    };
    let timeout = bounded_job_store_timeout(job_store.request_timeout_ms).map_err(|message| {
        ApiError::with_debug(
            ApiErrorKind::Config,
            ApiErrorDebug::Message(message.to_string()),
        )
    })?;
    Ok(Some(JobStoreSettings { endpoint, timeout }))
}

pub(crate) fn source_job_shared(app_state: &AppState) -> Arc<SourceJobShared> {
    Arc::new(SourceJobShared {
        engine: app_state.engine.clone(),
        manifest: Arc::clone(&app_state.manifest),
        source_health: Arc::clone(&app_state.source_health),
        privacy_policy: app_state.privacy_policy.clone(),
        retention: app_state.retention.clone(),
        pollers: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        authorized_source_identities: std::sync::OnceLock::new(),
        worker_id: format!("{}-bg", load_worker_id()),
        #[cfg(any(test, feature = "bdd-test-support"))]
        progress_commits: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    })
}

#[cfg(feature = "bdd-test-support")]
pub(crate) fn source_job_shared_for_bdd(app_state: &AppState) -> Arc<SourceJobShared> {
    source_job_shared(app_state)
}

pub(crate) fn source_job_deduplication(
    batch_id: &str,
    expires_at: Timestamp,
) -> Result<Deduplication, ()> {
    let deduplication_key = DeduplicationKey::try_new(batch_id.as_bytes()).map_err(|_| ())?;
    let deduplication_until = expires_at
        .checked_add(MAX_SOURCE_JOB_TIMEOUT)
        .map_err(|_| ())?;
    Deduplication::try_new(deduplication_key, deduplication_until).map_err(|_| ())
}

#[cfg(any(test, feature = "bdd-test-support"))]
pub(crate) fn bounded_timeout(timeout_ms: u64) -> Result<Duration, &'static str> {
    if timeout_ms == 0 || timeout_ms > MAX_SOURCE_JOB_TIMEOUT.as_millis() as u64 {
        return Err("analytics background job request_timeout_ms is outside its supported bound");
    }
    Ok(Duration::from_millis(timeout_ms))
}

pub(crate) fn bounded_job_store_timeout(timeout_ms: u64) -> Result<Duration, &'static str> {
    if timeout_ms == 0 || timeout_ms > MAX_JOB_STORE_REQUEST_TIMEOUT.as_millis() as u64 {
        return Err(
            "analytics background JobStore request_timeout_ms is outside its supported bound",
        );
    }
    Ok(Duration::from_millis(timeout_ms))
}

pub(crate) fn source_deadline_at_ms(now_ms: u128, timeout: Duration) -> Result<u64, &'static str> {
    let timeout_ms = u64::try_from(timeout.as_millis())
        .map_err(|_| "background source timeout is out of range")?;
    if timeout_ms == 0 || timeout > MAX_SOURCE_JOB_TIMEOUT {
        return Err("background source timeout exceeds its supported bound");
    }
    let deadline_at_ms = now_ms
        .checked_add(u128::from(timeout_ms))
        .ok_or("background source deadline overflowed")?;
    let deadline_at_ms =
        u64::try_from(deadline_at_ms).map_err(|_| "background source deadline is out of range")?;
    if deadline_at_ms == 0 || deadline_at_ms > MAX_SOURCE_DEADLINE_AT_MS {
        return Err("background source deadline exceeds its supported horizon");
    }
    Ok(deadline_at_ms)
}

pub(crate) struct JobStoreSettings<'a> {
    pub(crate) endpoint: &'a str,
    pub(crate) timeout: Duration,
}
