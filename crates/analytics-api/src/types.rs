use std::{path::PathBuf, sync::Arc};

use analytics_contract::{
    AnalyticsManifest, PrivacyPolicy, StorageItem, StorageStreamRecord, StructuredQuery,
};
use analytics_engine::{AnalyticsEngine, StorageBackend};
use analytics_operations::{OperationEvent, StoredOperation};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use utoipa::ToSchema;

use crate::AnalyticsEngineAccess;

#[derive(Clone)]
pub struct AppState {
    pub engine: AnalyticsEngineAccess,
    pub manifest: Arc<RwLock<Arc<AnalyticsManifest>>>,
    pub source_health: Arc<RwLock<SourceHealth>>,
    pub retention: Option<Arc<crate::retention::RetentionRuntime>>,
    pub privacy_policy: Option<Arc<PrivacyPolicy>>,
    pub retention_health: Arc<RwLock<RetentionHealth>>,
    pub operation_store_path: Option<Arc<PathBuf>>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationListResponse {
    pub(crate) operations: Vec<OperationStatusResponse>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationStatusResponse {
    pub(crate) operation_id: String,
    pub(crate) kind: String,
    pub(crate) actor: String,
    pub(crate) target_tables: Vec<String>,
    pub(crate) dry_run: bool,
    pub(crate) phase: String,
    pub(crate) status: String,
    pub(crate) cancellation_state: String,
    pub(crate) cursor: Option<OperationCursorResponse>,
    pub(crate) created_at_ms: i64,
    pub(crate) updated_at_ms: i64,
}

impl From<StoredOperation> for OperationStatusResponse {
    fn from(operation: StoredOperation) -> Self {
        Self {
            operation_id: operation.operation_id.to_string(),
            kind: operation.kind.to_string(),
            actor: operation.actor,
            target_tables: operation.target_tables,
            dry_run: operation.dry_run,
            phase: operation.phase.to_string(),
            status: operation.status.to_string(),
            cancellation_state: operation.cancellation_state.to_string(),
            cursor: operation.cursor.map(Into::into),
            created_at_ms: operation.created_at_ms,
            updated_at_ms: operation.updated_at_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationAuditResponse {
    pub(crate) events: Vec<OperationAuditEventResponse>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationAuditEventResponse {
    pub(crate) operation_id: String,
    pub(crate) event_id: u64,
    pub(crate) occurred_at_ms: i64,
    pub(crate) kind: String,
    pub(crate) phase: String,
    pub(crate) status: String,
    pub(crate) cursor: Option<OperationCursorResponse>,
    pub(crate) message: Option<String>,
}

impl From<OperationEvent> for OperationAuditEventResponse {
    fn from(event: OperationEvent) -> Self {
        Self {
            operation_id: event.operation_id.to_string(),
            event_id: event.event_id,
            occurred_at_ms: event.occurred_at_ms,
            kind: event.kind.to_string(),
            phase: event.phase.to_string(),
            status: event.status.to_string(),
            cursor: event.cursor.map(Into::into),
            message: event.message,
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationCancelResponse {
    pub(crate) operation_id: String,
    pub(crate) cancellation_requested: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationCursorResponse {
    pub(crate) label: String,
    pub(crate) position: u64,
}

impl From<analytics_operations::OperationCursor> for OperationCursorResponse {
    fn from(cursor: analytics_operations::OperationCursor) -> Self {
        Self {
            label: cursor.label,
            position: cursor.position,
        }
    }
}

impl AppState {
    #[must_use]
    pub fn new(engine: AnalyticsEngine, manifest: AnalyticsManifest) -> Self {
        Self {
            engine: AnalyticsEngineAccess::shared(engine),
            manifest: Arc::new(RwLock::new(Arc::new(manifest))),
            source_health: Arc::new(RwLock::new(SourceHealth::disabled())),
            retention: None,
            privacy_policy: None,
            retention_health: Arc::new(RwLock::new(RetentionHealth::disabled())),
            operation_store_path: None,
        }
    }

    #[must_use]
    pub fn new_with_backend(
        engine: AnalyticsEngine,
        manifest: AnalyticsManifest,
        backend: StorageBackend,
    ) -> Self {
        Self::new_with_backend_and_max_read_connections(
            engine,
            manifest,
            backend,
            config::DEFAULT_QUERY_MAX_READ_CONNECTIONS,
        )
    }

    #[must_use]
    pub fn new_with_backend_and_max_read_connections(
        engine: AnalyticsEngine,
        manifest: AnalyticsManifest,
        backend: StorageBackend,
        max_read_connections: usize,
    ) -> Self {
        Self {
            engine: AnalyticsEngineAccess::backend_aware_with_max_read_connections(
                engine,
                backend,
                max_read_connections,
            ),
            manifest: Arc::new(RwLock::new(Arc::new(manifest))),
            source_health: Arc::new(RwLock::new(SourceHealth::disabled())),
            retention: None,
            privacy_policy: None,
            retention_health: Arc::new(RwLock::new(RetentionHealth::disabled())),
            operation_store_path: None,
        }
    }

    #[must_use]
    pub fn with_retention(
        engine: AnalyticsEngine,
        manifest: AnalyticsManifest,
        retention: Option<Arc<crate::retention::RetentionRuntime>>,
    ) -> Self {
        Self {
            engine: AnalyticsEngineAccess::shared(engine),
            manifest: Arc::new(RwLock::new(Arc::new(manifest))),
            source_health: Arc::new(RwLock::new(SourceHealth::disabled())),
            retention,
            privacy_policy: None,
            retention_health: Arc::new(RwLock::new(RetentionHealth::disabled())),
            operation_store_path: None,
        }
    }

    #[must_use]
    pub fn with_retention_and_backend(
        engine: AnalyticsEngine,
        manifest: AnalyticsManifest,
        retention: Option<Arc<crate::retention::RetentionRuntime>>,
        backend: StorageBackend,
    ) -> Self {
        Self::with_retention_backend_and_max_read_connections(
            engine,
            manifest,
            retention,
            backend,
            config::DEFAULT_QUERY_MAX_READ_CONNECTIONS,
        )
    }

    #[must_use]
    pub fn with_retention_backend_and_max_read_connections(
        engine: AnalyticsEngine,
        manifest: AnalyticsManifest,
        retention: Option<Arc<crate::retention::RetentionRuntime>>,
        backend: StorageBackend,
        max_read_connections: usize,
    ) -> Self {
        Self {
            engine: AnalyticsEngineAccess::backend_aware_with_max_read_connections(
                engine,
                backend,
                max_read_connections,
            ),
            manifest: Arc::new(RwLock::new(Arc::new(manifest))),
            source_health: Arc::new(RwLock::new(SourceHealth::disabled())),
            retention,
            privacy_policy: None,
            retention_health: Arc::new(RwLock::new(RetentionHealth::disabled())),
            operation_store_path: None,
        }
    }

    #[must_use]
    pub fn with_privacy_policy(mut self, policy: Option<Arc<PrivacyPolicy>>) -> Self {
        self.privacy_policy = policy;
        self
    }

    #[must_use]
    pub fn with_operation_store_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.operation_store_path = Some(Arc::new(path.into()));
        self
    }
}

/// Health state for the optional source poller.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceHealthStatus {
    /// Source polling is not configured or has been disabled.
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
    WaitingForLease,
    Standby,
    LeaseHeld,
    Renewing,
    RefreshingPlan,
    RebuildingPoller,
    Polling,
    Ingesting,
    Checkpointing,
    LeaseLost,
    Timeout,
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
    "lease_token": "analytics-worker-1-1778670000000-1",
    "lease_until_ms": 1_778_670_300_000_u64,
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
    #[schema(example = "healthy")]
    pub phase: SourcePollingPhase,
    /// Whether source polling is enabled for this process.
    #[schema(default = false, example = true)]
    pub poller_enabled: bool,
    /// Durable job identifier used for source-polling lease ownership.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = "analytics_source_polling")]
    pub job_id: Option<String>,
    /// Worker identity currently used by this process for source-polling
    /// leases.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = "analytics-worker-1")]
    pub worker_id: Option<String>,
    /// Fencing token for the current source-polling ownership epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = "analytics-worker-1-1778670000000-1")]
    pub lease_token: Option<String>,
    /// Unix epoch milliseconds when the current lease expires, if this worker
    /// owns it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = 1_778_670_300_000_u64)]
    pub lease_until_ms: Option<i64>,
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
            lease_token: None,
            lease_until_ms: None,
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
    pub status: RetentionHealthStatus,
    pub sweeper_enabled: bool,
    #[serde(default)]
    pub table_count: usize,
    #[serde(default)]
    pub last_sweep_completed_at_ms: Option<u128>,
    #[serde(default)]
    pub last_sweep_duration_ms: Option<f64>,
    #[serde(default)]
    pub total_rows_deleted: u64,
    #[serde(default)]
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

#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "configured": true,
    "reachable": true,
    "operation_count": 3,
    "running_operations": 1,
    "degraded_operations": 0,
    "oldest_running_updated_at_ms": 1_778_670_000_200_u64
}))]
pub(crate) struct OperationDiagnostics {
    pub configured: bool,
    pub reachable: bool,
    pub operation_count: usize,
    pub running_operations: usize,
    pub degraded_operations: usize,
    #[serde(default)]
    pub oldest_running_updated_at_ms: Option<i64>,
}

/// Explicitly unscoped raw SQL query request. The HTTP API validates this shape
/// but rejects execution until an admin authorization boundary exists.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "sql": "select email, org_id from users where org_id = 'org-a' order by email"
}))]
pub(crate) struct UnscopedSqlQueryRequest {
    /// Raw SQL text. The HTTP API rejects raw SQL execution because arbitrary
    /// SQL cannot prove tenant isolation without a separate admin boundary.
    #[schema(
        min_length = 1,
        max_length = 20000,
        example = "select email, org_id from users where org_id = 'org-a' order by email"
    )]
    pub sql: String,
}

/// Explicitly unscoped structured query request.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "query": {
        "analytics_table_name": "legacy_items",
        "select": [{"kind": "document_path", "document_column": "item", "path": "profile.email", "alias": "email"}],
        "filters": [],
        "group_by": [],
        "order_by": [],
        "limit": 100
    }
}))]
pub(crate) struct UnscopedStructuredQueryRequest {
    /// Structured query tree compiled by the analytics engine without a tenant
    /// predicate. Use only for explicitly unscoped data.
    pub query: StructuredQuery,
}

/// Tenant-scoped structured query request for callers that should not
/// construct SQL.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "target_tenant_id": "tenant_01",
    "query": {
        "analytics_table_name": "users",
        "select": [{"kind": "column", "column_name": "email", "alias": null}],
        "filters": [{
            "kind": "eq",
            "expression": {"kind": "column", "column_name": "org_id"},
            "value": "org-a"
        }],
        "group_by": [],
        "order_by": [{"expression": {"kind": "column", "column_name": "email"}, "direction": "asc"}],
        "limit": 100
    }
}))]
pub(crate) struct TenantQueryRequest {
    /// Tenant id that the query is authorized to read. The API injects this as
    /// an engine-owned tenant predicate; callers must not rely on their own
    /// tenant filters for isolation.
    #[schema(min_length = 1, max_length = 255, example = "tenant_01")]
    pub target_tenant_id: String,
    /// Structured query tree compiled by the analytics engine.
    pub query: StructuredQuery,
}

/// Named tenant-scoped structured query for batch execution.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "name": "total_users",
    "query": {
        "analytics_table_name": "users",
        "select": [{"kind": "count", "alias": "count"}],
        "filters": [],
        "group_by": [],
        "order_by": [],
        "limit": 1
    }
}))]
pub(crate) struct TenantQueryBatchItem {
    /// Caller-owned stable name used to correlate results.
    #[schema(min_length = 1, max_length = 255, example = "total_users")]
    pub name: String,
    /// Structured query tree compiled by the analytics engine.
    pub query: StructuredQuery,
}

/// Tenant-scoped structured query batch request.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "target_tenant_id": "tenant_01",
    "queries": [{
        "name": "total_users",
        "query": {
            "analytics_table_name": "users",
            "select": [{"kind": "count", "alias": "count"}],
            "filters": [],
            "group_by": [],
            "order_by": [],
            "limit": 1
        }
    }]
}))]
pub(crate) struct TenantQueryBatchRequest {
    /// Tenant id that each query is authorized to read. The API injects this as
    /// an engine-owned tenant predicate.
    #[schema(min_length = 1, max_length = 255, example = "tenant_01")]
    pub target_tenant_id: String,
    /// Named structured queries to execute against the same tenant scope.
    #[schema(min_items = 1)]
    pub queries: Vec<TenantQueryBatchItem>,
}

/// Query result rows as JSON objects.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "rows": [{"email": "ada@example.com", "org_id": "org-a"}],
    "columns": [{"name": "email", "value_type": "string", "nullable": false}],
    "execution": {
        "query_hash": "sha256:example",
        "row_count": 1,
        "truncated": false,
        "tables": ["users"],
        "elapsed_ms": 7,
        "plan_shape": {
            "logical_source_count": 1,
            "join_count": 0,
            "filter_count": 1,
            "group_expression_count": 0,
            "order_expression_count": 0,
            "aggregate_count": 0,
            "conditional_expression_count": 0
        }
    },
    "source_watermark": {"max_occurred_at_ms": null, "max_ingested_at_ms": null}
}))]
pub struct QueryResponse {
    /// Result rows returned by `DuckDB`.
    #[schema(min_items = 0, example = json!([{"email": "ada@example.com", "org_id": "org-a"}]))]
    pub rows: Vec<serde_json::Value>,
    pub columns: Vec<QueryResultColumn>,
    pub execution: QueryExecutionMetadata,
    pub source_watermark: QuerySourceWatermark,
}

/// Named query result rows.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "name": "total_users",
    "rows": [{"count": 42}],
    "columns": [{"name": "count", "value_type": "i64", "nullable": false}],
    "execution": {
        "query_hash": "sha256:example",
        "row_count": 1,
        "truncated": false,
        "tables": ["users"],
        "elapsed_ms": 7,
        "plan_shape": {
            "logical_source_count": 1,
            "join_count": 0,
            "filter_count": 0,
            "group_expression_count": 0,
            "order_expression_count": 0,
            "aggregate_count": 1,
            "conditional_expression_count": 0
        }
    },
    "source_watermark": {"max_occurred_at_ms": null, "max_ingested_at_ms": null}
}))]
pub struct QueryBatchResult {
    /// Caller-owned stable name from the batch request.
    #[schema(example = "total_users")]
    pub name: String,
    /// Result rows returned by `DuckDB`.
    #[schema(min_items = 0, example = json!([{"count": 42}]))]
    pub rows: Vec<serde_json::Value>,
    pub columns: Vec<QueryResultColumn>,
    pub execution: QueryExecutionMetadata,
    pub source_watermark: QuerySourceWatermark,
}

#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryResultColumn {
    pub name: String,
    pub value_type: QueryResultValueType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryResultValueType {
    Boolean,
    String,
    I64,
    Decimal,
    F64,
    Json,
    TimestampMs,
}

#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryExecutionMetadata {
    pub query_hash: String,
    pub row_count: u64,
    pub truncated: bool,
    pub tables: Vec<String>,
    pub elapsed_ms: u64,
    /// Sanitized logical shape derived from the validated structured query.
    /// This does not claim physical row-scan counts, which are backend and
    /// optimizer dependent.
    pub plan_shape: QueryPlanShape,
}

/// Bounded, literal-free logical query shape for performance evidence.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QueryPlanShape {
    pub logical_source_count: u32,
    pub join_count: u32,
    pub filter_count: u32,
    pub group_expression_count: u32,
    pub order_expression_count: u32,
    pub aggregate_count: u32,
    pub conditional_expression_count: u32,
}

#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct QuerySourceWatermark {
    pub max_occurred_at_ms: Option<i64>,
    pub max_ingested_at_ms: Option<i64>,
}

/// Batch query result rows as JSON objects.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "results": [{"name": "total_users", "rows": [{"count": 42}]}]
}))]
pub(crate) struct QueryBatchResponse {
    /// Results in request order.
    #[schema(min_items = 0)]
    pub results: Vec<QueryBatchResult>,
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

/// Stream ingestion request.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "record_key": "user-1",
    "record": {
        "Keys": {"pk": {"S": "USER#user-1"}},
        "SequenceNumber": "49668899999999999999999999999999999999999999999999999999",
        "NewImage": {
            "pk": {"S": "USER#user-1"},
            "profile": {"M": {"email": {"S": "ada@example.com"}}},
            "org_id": {"S": "org-a"}
        }
    }
}))]
pub struct IngestStreamRecordRequest {
    /// Stable row key for idempotent writes. Defaults to the stream sequence or
    /// event id when omitted.
    #[serde(default)]
    #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 1024, example = "user-1")]
    record_key: Option<String>,
    /// Stream record in the analytics wire format or a standard stream event
    /// envelope.
    record: IngestStreamRecordPayload,
}

impl IngestStreamRecordRequest {
    #[must_use]
    pub fn into_contract_record(self) -> (String, StorageStreamRecord) {
        let fallback_key = self.record.fallback_record_key();
        let record = self.record.into_contract_record();
        let record_key = self.record_key.unwrap_or(fallback_key);
        (record_key, record)
    }
}

/// Batch stream ingestion request.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "records": [{
        "record_key": "user-1",
        "record": {
            "Keys": {"pk": {"S": "USER#user-1"}},
            "SequenceNumber": "49668899999999999999999999999999999999999999999999999999",
            "NewImage": {
                "pk": {"S": "USER#user-1"},
                "profile": {"M": {"email": {"S": "ada@example.com"}}},
                "org_id": {"S": "org-a"}
            }
        }
    }]
}))]
pub(crate) struct IngestStreamRecordBatchRequest {
    /// Stream records from one source poll response. Keep payloads within the
    /// source service response limit, such as `DynamoDB` Streams' 1 MiB page
    /// cap.
    #[schema(min_items = 1)]
    pub(crate) records: Vec<IngestStreamRecordRequest>,
}

/// Batch stream ingestion outcome.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({"record_count": 2, "outcomes": [{"outcome": "inserted"}, {"outcome": "updated"}]}))]
pub(crate) struct IngestBatchResponse {
    /// Number of records accepted by the engine transaction.
    pub(crate) record_count: usize,
    /// Per-record ingest outcomes in request order.
    pub(crate) outcomes: Vec<IngestResponse>,
}

/// Accepted ingestion payload shapes.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[serde(untagged)]
pub(crate) enum IngestStreamRecordPayload {
    /// Domain-agnostic analytics wire format.
    Contract(StorageStreamRecord),
    /// Standard stream event envelope containing a DynamoDB-compatible record.
    Standard(StandardStreamEventRecord),
}

impl IngestStreamRecordPayload {
    fn fallback_record_key(&self) -> String {
        match self {
            Self::Contract(record) => record.sequence_number.clone(),
            Self::Standard(record) => record
                .event_id
                .clone()
                .unwrap_or_else(|| record.storage.sequence_number.clone()),
        }
    }

    fn into_contract_record(self) -> StorageStreamRecord {
        match self {
            Self::Contract(record) => record,
            Self::Standard(record) => StorageStreamRecord {
                keys: record.storage.keys,
                sequence_number: record.storage.sequence_number,
                old_image: record.storage.old_image,
                new_image: record.storage.new_image,
            },
        }
    }
}

/// Standard stream event envelope.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(example = json!({
    "eventID": "event-user-1-insert",
    "eventName": "INSERT",
    "dynamodb": {
        "Keys": {"pk": {"S": "USER#user-1"}},
        "SequenceNumber": "49668899999999999999999999999999999999999999999999999999",
        "NewImage": {
            "pk": {"S": "USER#user-1"},
            "profile": {"M": {"email": {"S": "ada@example.com"}}}
        }
    }
}))]
pub(crate) struct StandardStreamEventRecord {
    /// Source event id used as a fallback ingestion row key.
    #[serde(default, rename = "eventID")]
    #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 1024, example = "event-user-1-insert")]
    event_id: Option<String>,
    /// Source event operation name, such as INSERT, MODIFY, or REMOVE.
    #[allow(dead_code)]
    #[serde(default, rename = "eventName")]
    #[schema(nullable = true, default = json!(null), example = "INSERT")]
    event_name: Option<String>,
    /// DynamoDB-compatible stream record body.
    #[serde(rename = "dynamodb")]
    storage: StandardStreamStorageRecord,
}

/// DynamoDB-compatible stream record body.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "PascalCase")]
#[schema(example = json!({
    "Keys": {"pk": {"S": "USER#user-1"}},
    "SequenceNumber": "49668899999999999999999999999999999999999999999999999999",
    "NewImage": {
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "ada@example.com"}}}
    }
}))]
pub(crate) struct StandardStreamStorageRecord {
    /// Primary key attributes for the source record.
    #[schema(value_type = Object)]
    keys: StorageItem,
    /// Source stream sequence number.
    #[schema(
        min_length = 1,
        max_length = 1024,
        example = "49668899999999999999999999999999999999999999999999999999"
    )]
    sequence_number: String,
    /// Previous record image for update and delete events.
    #[serde(default)]
    #[schema(value_type = Object, nullable = true, default = json!(null), example = json!({
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "old@example.com"}}}
    }))]
    old_image: Option<StorageItem>,
    /// New record image for insert and update events.
    #[serde(default)]
    #[schema(value_type = Object, nullable = true, default = json!(null), example = json!({
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "ada@example.com"}}}
    }))]
    new_image: Option<StorageItem>,
}

/// Stream ingestion outcome.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({"outcome": "inserted"}))]
pub(crate) struct IngestResponse {
    /// Final ingestion action performed by the engine.
    #[schema(example = "inserted")]
    pub outcome: String,
    /// Active privacy policy version, when privacy mode filtered this request.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = "privacy-v1")]
    pub privacy_policy_version: Option<String>,
    /// Count of fields or scalar values removed by the privacy policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = 2)]
    pub privacy_dropped_fields: Option<u64>,
}
