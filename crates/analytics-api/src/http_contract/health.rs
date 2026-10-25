use schemars::JsonSchema;
use serde::Serialize;
use utoipa::ToSchema;

/// Health state for the optional source poller.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceHealthStatus {
    /// Source polling is disabled.
    Disabled,
    /// Source polling has been configured and startup is in progress.
    Starting,
    /// Source polling is succeeding.
    Healthy,
    /// Source polling is running but recent polls or checkpoint writes failed.
    Degraded,
}

/// Current phase for the source polling job state machine.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourcePollingPhase {
    Disabled,
    Starting,
    Polling,
    Ingesting,
    Healthy,
    Degraded,
}

/// Current source polling and checkpoint health for diagnostics responses.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "status": "healthy",
    "phase": "healthy",
    "poller_enabled": true,
    "job_id": "analytics_source_polling",
    "worker_id": "analytics-worker-1",
    "phase_started_at_ms": 1_778_670_000_000_u64,
    "table_count": 1,
    "last_poll_started_at_ms": 1_778_670_000_000_u64,
    "last_success_at_ms": 1_778_670_000_200_u64,
    "last_error_at_ms": null,
    "last_error": null,
    "total_polls": 42,
    "total_poll_errors": 0,
    "total_records_ingested": 128,
    "total_ingest_errors": 0,
    "total_checkpoints_saved": 16,
    "total_checkpoint_errors": 0,
    "checkpoints": [{
        "source_table_name": "source_users",
        "shard_id": "shard-0001",
        "position": "49668899999999999999999999999999999999999999999999999999",
        "updated_at_ms": 1_778_670_000_200_u64
    }],
    "table_lag": [{
        "source_table_name": "source_users",
        "versionstamp": "00000000000000000042",
        "lag_ms": 0,
        "cursor_age_ms": 5000,
        "updated_at_ms": 1_778_670_000_200_u64
    }]
}))]
pub struct SourceHealth {
    /// Overall poller state.
    #[schema(example = "healthy")]
    pub status: SourceHealthStatus,
    /// Deterministic phase for the current source polling job step.
    #[serde(default = "default_source_polling_phase")]
    #[schema(default = "disabled", example = "healthy")]
    pub phase: SourcePollingPhase,
    /// Whether source polling is enabled.
    #[schema(default = false, example = true)]
    pub poller_enabled: bool,
    /// Durable job identifier used for source-polling lifecycle observation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = "analytics_source_polling")]
    pub job_id: Option<String>,
    /// Worker identity assigned to source-polling jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = "analytics-worker-1")]
    pub worker_id: Option<String>,
    /// Unix epoch milliseconds when the current phase began.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), example = 1_778_670_000_000_u64)]
    pub phase_started_at_ms: Option<u128>,
    /// Number of source tables configured for polling.
    #[serde(default)]
    #[schema(default = 0, example = 1)]
    pub table_count: usize,
    /// Unix epoch milliseconds when the most recent poll started.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), example = 1_778_670_000_000_u64)]
    pub last_poll_started_at_ms: Option<u128>,
    /// Unix epoch milliseconds for the most recent successful poll.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), example = 1_778_670_000_200_u64)]
    pub last_success_at_ms: Option<u128>,
    /// Unix epoch milliseconds for the most recent poll error.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), example = 1_778_670_000_100_u64)]
    pub last_error_at_ms: Option<u128>,
    /// Last source polling error message, when present.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), example = "analytics source http request failed with status 503: unavailable")]
    pub last_error: Option<String>,
    /// Total poll cycles attempted since process start.
    #[serde(default)]
    #[schema(default = 0, example = 42)]
    pub total_polls: u64,
    /// Total poll cycles that failed since process start.
    #[serde(default)]
    #[schema(default = 0, example = 0)]
    pub total_poll_errors: u64,
    /// Total records successfully ingested by the source poller.
    #[serde(default)]
    #[schema(default = 0, example = 128)]
    pub total_records_ingested: u64,
    /// Total source records that failed ingestion.
    #[serde(default)]
    #[schema(default = 0, example = 0)]
    pub total_ingest_errors: u64,
    /// Total source checkpoints persisted.
    #[serde(default)]
    #[schema(default = 0, example = 16)]
    pub total_checkpoints_saved: u64,
    /// Total source checkpoint persistence failures.
    #[serde(default)]
    #[schema(default = 0, example = 0)]
    pub total_checkpoint_errors: u64,
    /// Last known checkpoint positions by source table and shard.
    #[serde(default)]
    #[schema(default = json!([]), min_items = 0, example = json!([{
        "source_table_name": "source_users",
        "shard_id": "shard-0001",
        "position": "49668899999999999999999999999999999999999999999999999999",
        "updated_at_ms": 1_778_670_000_200_u64
    }]))]
    pub checkpoints: Vec<CheckpointHealth>,
    /// Last known lag summary by source table for hashed-range ingest.
    #[serde(default)]
    #[schema(default = json!([]), min_items = 0, example = json!([{
        "source_table_name": "source_users",
        "versionstamp": "00000000000000000042",
        "lag_ms": 0,
        "cursor_age_ms": 5000,
        "updated_at_ms": 1_778_670_000_200_u64
    }]))]
    pub table_lag: Vec<TableLagHealth>,
}

impl SourceHealth {
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            status: SourceHealthStatus::Disabled,
            phase: SourcePollingPhase::Disabled,
            poller_enabled: false,
            job_id: None,
            worker_id: None,
            phase_started_at_ms: None,
            table_count: 0,
            last_poll_started_at_ms: None,
            last_success_at_ms: None,
            last_error_at_ms: None,
            last_error: None,
            total_polls: 0,
            total_poll_errors: 0,
            total_records_ingested: 0,
            total_ingest_errors: 0,
            total_checkpoints_saved: 0,
            total_checkpoint_errors: 0,
            checkpoints: Vec::new(),
            table_lag: Vec::new(),
        }
    }

    #[must_use]
    pub fn starting(table_count: usize) -> Self {
        Self {
            status: SourceHealthStatus::Starting,
            phase: SourcePollingPhase::Starting,
            poller_enabled: true,
            table_count,
            ..Self::disabled()
        }
    }
}

fn default_source_polling_phase() -> SourcePollingPhase {
    SourcePollingPhase::Disabled
}

/// Source table lag summary for hashed-range ingest.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema, PartialEq, Eq)]
#[schema(example = json!({
    "source_table_name": "source_users",
    "versionstamp": "00000000000000000042",
    "lag_ms": 0,
    "cursor_age_ms": 5000,
    "updated_at_ms": 1_778_670_000_200_u64
}))]
pub struct TableLagHealth {
    pub source_table_name: String,
    pub versionstamp: String,
    pub lag_ms: Option<u64>,
    pub cursor_age_ms: Option<u64>,
    pub updated_at_ms: Option<u128>,
}

/// Source checkpoint position for a single source table shard.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "source_table_name": "source_users",
    "shard_id": "shard-0001",
    "position": "49668899999999999999999999999999999999999999999999999999",
    "updated_at_ms": 1_778_670_000_200_u64
}))]
pub struct CheckpointHealth {
    /// Source table name from the analytics source configuration.
    #[schema(min_length = 1, max_length = 255, example = "source_users")]
    pub source_table_name: String,
    /// Source shard identifier.
    #[schema(min_length = 1, max_length = 255, example = "shard-0001")]
    pub shard_id: String,
    /// Opaque source stream position to resume from.
    #[schema(
        min_length = 1,
        max_length = 1024,
        example = "49668899999999999999999999999999999999999999999999999999"
    )]
    pub position: String,
    /// Unix epoch milliseconds when this checkpoint was updated.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), example = 1_778_670_000_200_u64)]
    pub updated_at_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RetentionHealthStatus {
    Disabled,
    Starting,
    Healthy,
    Degraded,
}

#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "status": "healthy",
    "sweeper_enabled": true,
    "table_count": 1,
    "last_sweep_completed_at_ms": 1_778_670_000_200_u64,
    "last_sweep_duration_ms": 25.0,
    "total_rows_deleted": 128,
    "total_sweep_errors": 0
}))]
pub struct RetentionHealth {
    /// Current retention sweeper state.
    #[schema(example = "healthy")]
    pub status: RetentionHealthStatus,
    /// Whether the retention sweeper is enabled.
    #[schema(example = true)]
    pub sweeper_enabled: bool,
    /// Number of registered tables included in retention sweeps.
    #[serde(default)]
    #[schema(default = 0, example = 1)]
    pub table_count: usize,
    /// Unix epoch milliseconds when the latest sweep completed.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), example = 1_778_670_000_200_u64)]
    pub last_sweep_completed_at_ms: Option<u128>,
    /// Duration of the latest sweep in milliseconds.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), example = 25.0)]
    pub last_sweep_duration_ms: Option<f64>,
    /// Total rows deleted by completed retention sweeps.
    #[serde(default)]
    #[schema(default = 0, example = 128)]
    pub total_rows_deleted: u64,
    /// Total retention sweep failures since process start.
    #[serde(default)]
    #[schema(default = 0, example = 0)]
    pub total_sweep_errors: u64,
}

impl RetentionHealth {
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            status: RetentionHealthStatus::Disabled,
            sweeper_enabled: false,
            table_count: 0,
            last_sweep_completed_at_ms: None,
            last_sweep_duration_ms: None,
            total_rows_deleted: 0,
            total_sweep_errors: 0,
        }
    }

    #[must_use]
    pub fn starting(table_count: usize) -> Self {
        Self {
            status: RetentionHealthStatus::Starting,
            sweeper_enabled: true,
            table_count,
            ..Self::disabled()
        }
    }
}

/// Process diagnostics including source poller state.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "status": "ok",
    "source": SourceHealth::disabled()
}))]
pub(crate) struct DiagnosticsResponse {
    /// Human-readable diagnostic status.
    #[schema(example = "ok")]
    pub status: String,
    /// Source poller health details.
    pub source: SourceHealth,
    /// Retention sweeper health details.
    pub retention: RetentionHealth,
    /// Durable operation store summary without operation payloads.
    pub operations: OperationDiagnostics,
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "configured": true,
    "reachable": true,
    "operation_count": 3,
    "running_operations": 1,
    "degraded_operations": 0,
    "oldest_running_updated_at_ms": 1_778_670_000_200_u64
}))]
pub(crate) struct OperationDiagnostics {
    /// Whether durable operation storage is configured.
    #[schema(example = true)]
    pub configured: bool,
    /// Whether durable operation storage is reachable.
    #[schema(example = true)]
    pub reachable: bool,
    /// Number of durable operations stored.
    #[schema(default = 0, example = 3)]
    pub operation_count: usize,
    /// Number of running or cancelling operations.
    #[schema(default = 0, example = 1)]
    pub running_operations: usize,
    /// Number of failed operations.
    #[schema(default = 0, example = 0)]
    pub degraded_operations: usize,
    /// Unix epoch milliseconds when the oldest running or cancelling operation
    /// was updated.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), example = 1_778_670_000_200_u64)]
    pub oldest_running_updated_at_ms: Option<i64>,
}

/// Basic service health response.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "status": "healthy",
    "source": SourceHealth::disabled()
}))]
pub(crate) struct HealthResponse {
    /// Human-readable health status.
    #[schema(example = "healthy")]
    pub status: String,
    /// Source poller health details.
    pub source: SourceHealth,
    /// Retention sweeper health details.
    pub retention: RetentionHealth,
}

/// Readiness response.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({"status": "ready"}))]
pub(crate) struct ReadyResponse {
    /// Human-readable readiness status.
    #[schema(example = "ready")]
    pub status: String,
}

/// Error response for invalid requests and query failures.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "error": "query must be a single read-only SELECT statement",
    "code": "record_contract"
}))]
pub(crate) struct ErrorResponse {
    /// Human-readable error message.
    #[schema(example = "query must be a single read-only SELECT statement")]
    pub error: String,
    /// Stable machine-readable classification when the endpoint can distinguish
    /// the failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, example = "record_contract")]
    pub code: Option<String>,
}
