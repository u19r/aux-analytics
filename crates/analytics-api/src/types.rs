use std::sync::Arc;

use analytics_contract::{AnalyticsManifest, StorageItem, StorageStreamRecord, StructuredQuery};
use analytics_engine::AnalyticsEngine;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use utoipa::ToSchema;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Mutex<AnalyticsEngine>>,
    pub manifest: Arc<AnalyticsManifest>,
    pub source_health: Arc<RwLock<SourceHealth>>,
    pub retention: Option<Arc<crate::retention::RetentionRuntime>>,
    pub retention_health: Arc<RwLock<RetentionHealth>>,
}

impl AppState {
    #[must_use]
    pub fn new(engine: AnalyticsEngine, manifest: AnalyticsManifest) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
            manifest: Arc::new(manifest),
            source_health: Arc::new(RwLock::new(SourceHealth::disabled())),
            retention: None,
            retention_health: Arc::new(RwLock::new(RetentionHealth::disabled())),
        }
    }

    #[must_use]
    pub fn with_retention(
        engine: AnalyticsEngine,
        manifest: AnalyticsManifest,
        retention: Option<Arc<crate::retention::RetentionRuntime>>,
    ) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
            manifest: Arc::new(manifest),
            source_health: Arc::new(RwLock::new(SourceHealth::disabled())),
            retention,
            retention_health: Arc::new(RwLock::new(RetentionHealth::disabled())),
        }
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

/// Current source polling and checkpoint health for diagnostics responses.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "status": "healthy",
    "poller_enabled": true,
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
    }]
}))]
pub struct SourceHealth {
    /// Overall poller state.
    #[schema(example = "healthy")]
    pub status: SourceHealthStatus,
    /// Whether source polling is enabled for this process.
    #[schema(default = false, example = true)]
    pub poller_enabled: bool,
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
}

impl SourceHealth {
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            status: SourceHealthStatus::Disabled,
            poller_enabled: false,
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
        }
    }

    #[must_use]
    pub fn starting(table_count: usize) -> Self {
        Self {
            status: SourceHealthStatus::Starting,
            poller_enabled: true,
            table_count,
            ..Self::disabled()
        }
    }
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

/// Query result rows as JSON objects.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "rows": [{"email": "ada@example.com", "org_id": "org-a"}]
}))]
pub(crate) struct QueryResponse {
    /// Result rows returned by `DuckDB`.
    #[schema(min_items = 0, example = json!([{"email": "ada@example.com", "org_id": "org-a"}]))]
    pub rows: Vec<serde_json::Value>,
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
#[schema(example = json!({"error": "query must be a single read-only SELECT statement"}))]
pub(crate) struct ErrorResponse {
    /// Human-readable error message.
    #[schema(example = "query must be a single read-only SELECT statement")]
    pub error: String,
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
}
