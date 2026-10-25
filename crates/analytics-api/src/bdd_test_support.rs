use std::sync::Arc;

use config::{
    AnalyticsIngestConfig, AnalyticsJobStoreConfig, AnalyticsRetentionConfig, AnalyticsSourceConfig,
};

use crate::{ApiResult, AppState};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceRuntime {
    JobSystem,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourcePollingReceipt {
    pub runtime: SourceRuntime,
    pub table_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemStreamCutoverReceipt {
    pub runtime: SourceRuntime,
    pub first_page_records: usize,
    pub first_run_progress_commits: usize,
    pub resumed_page_records: usize,
    pub checkpoint_only_pages: usize,
    pub progress_commits: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceLifecycle {
    Cancelled,
    Repaired,
    Applied,
    StaleFence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceScenarioHealth {
    Healthy,
    Degraded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStaleFenceReceipt {
    pub lifecycle: SourceLifecycle,
    pub rows_committed: u64,
    pub checkpoint_position: Option<String>,
    pub progress_commits: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceCancelRepairReceipt {
    pub cancellation: SourceLifecycle,
    pub repair: SourceLifecycle,
    pub effect: SourceLifecycle,
    pub health: SourceScenarioHealth,
    pub pre_repair_rows: u64,
    pub rows_committed: u64,
    pub progress_commits: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionRuntime {
    JobSystem,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionScheduleStatus {
    Incomplete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionScenarioHealth {
    Healthy,
    Degraded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionLifecycle {
    Cancelled,
    Repaired,
    Applied,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionScheduleReceipt {
    pub runtime: RetentionRuntime,
    pub due_occurrences: usize,
    pub schedule_advanced: bool,
    pub rows_deleted: u64,
    pub eligible_remaining: u64,
    pub status: RetentionScheduleStatus,
    pub health: RetentionScenarioHealth,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionCancelRepairReceipt {
    pub cancellation: RetentionLifecycle,
    pub repair: RetentionLifecycle,
    pub effect: RetentionLifecycle,
    pub health: RetentionScenarioHealth,
    pub pre_repair_rows_deleted: u64,
    pub rows_deleted: u64,
}

/// Starts the source JobSystem runtime for boundary-level BDD coverage.
pub async fn start_source_polling(
    source: AnalyticsSourceConfig,
    ingest: AnalyticsIngestConfig,
    app_state: Arc<AppState>,
) -> ApiResult<SourcePollingReceipt> {
    crate::background_jobs::start_source_polling(source, ingest, app_state).await
}

/// Runs the assembled AuxStorage SystemStream source through the real
/// DynamoDB-backed scheduled JobStore and fenced analytics handler.
pub async fn system_stream_cutover(
    source: AnalyticsSourceConfig,
    job_store: AnalyticsJobStoreConfig,
    app_state: Arc<AppState>,
) -> ApiResult<SystemStreamCutoverReceipt> {
    crate::background_jobs::system_stream_cutover(source, job_store, app_state).await
}

pub async fn source_stale_fence(
    source: AnalyticsSourceConfig,
    app_state: Arc<AppState>,
) -> ApiResult<SourceStaleFenceReceipt> {
    crate::background_jobs::source_stale_fence(source, app_state).await
}

pub async fn source_cancel_and_repair(
    source: AnalyticsSourceConfig,
    app_state: Arc<AppState>,
) -> ApiResult<SourceCancelRepairReceipt> {
    crate::background_jobs::cancel_and_repair_source(source, app_state).await
}

pub async fn retention_schedule(
    retention: AnalyticsRetentionConfig,
    app_state: Arc<AppState>,
) -> ApiResult<RetentionScheduleReceipt> {
    crate::background_jobs::retention_schedule(retention, app_state).await
}

pub async fn retention_cancel_and_repair(
    retention: AnalyticsRetentionConfig,
    app_state: Arc<AppState>,
) -> ApiResult<RetentionCancelRepairReceipt> {
    crate::background_jobs::retention_cancel_and_repair(retention, app_state).await
}
