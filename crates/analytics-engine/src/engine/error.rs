use analytics_contract::SourcePositionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AnalyticsEngineError {
    #[error("manifest version {actual} is not supported; expected {expected}")]
    UnsupportedManifestVersion { actual: u32, expected: u32 },
    #[error(transparent)]
    InvalidManifest(#[from] analytics_contract::ManifestValidationError),
    #[error(transparent)]
    PrivacyPolicy(#[from] analytics_contract::PrivacyPolicyError),
    #[error("table registration not found for analytics table {0}")]
    TableNotRegistered(String),
    #[error("record does not match the condition for analytics table {0}")]
    RecordDoesNotMatchRegistration(String),
    #[error(transparent)]
    Projection(#[from] crate::projection::ProjectionError),
    #[error("record does not include a tenant id and registration has no tenant id")]
    MissingTenant,
    #[error("record is missing required attribute {0}")]
    MissingAttribute(String),
    #[error("attribute {attribute_name} value does not match regex")]
    RegexNoMatch { attribute_name: String },
    #[error("regex for attribute {attribute_name} does not define capture group {capture}")]
    RegexMissingCapture {
        attribute_name: String,
        capture: String,
    },
    #[error("record is missing required identifier column {0}")]
    MissingIdentifier(&'static str),
    #[error("repair receipt key cannot be empty")]
    EmptyRepairReceiptKey,
    #[error("repair source revision cannot be empty")]
    EmptyRepairSourceRevision,
    #[error("repair receipts do not support row expansion for analytics table {0}")]
    UnsupportedRepairReceiptExpansion(String),
    #[error("stream record was skipped for repair of analytics table {0}")]
    RepairRecordSkipped(String),
    #[error("repair receipt {0} is ambiguous because multiple rows share its key")]
    AmbiguousRepairReceipt(String),
    #[error("repair receipt {0} could not be consumed uniquely")]
    RepairReceiptNotConsumed(String),
    #[error("repair receipt fence is not available")]
    RepairReceiptFenceUnavailable,
    #[error("tenant range purge destination fence is not available")]
    RangePurgeFenceUnavailable,
    #[error("remote job claim is invalid")]
    InvalidRemoteJobClaim,
    #[error("maintenance effect execution id is empty, oversized, or contains control characters")]
    InvalidMaintenanceEffectExecutionId,
    #[error("maintenance effect fence must be non-zero")]
    InvalidMaintenanceEffectFence,
    #[error("maintenance effect fence is stale for execution {0}")]
    StaleMaintenanceEffectFence(String),
    #[error("maintenance effect receipt {0} conflicts with the requested effect")]
    ConflictingMaintenanceEffectReceipt(String),
    #[error("maintenance effect state is incomplete")]
    IncompleteMaintenanceEffectState,
    #[error("maintenance effect authority is ambiguous")]
    AmbiguousMaintenanceEffectAuthority,
    #[error("maintenance effect receipt is ambiguous")]
    AmbiguousMaintenanceEffectReceipt(String),
    #[error("query must be a single read-only SELECT statement")]
    InvalidQuery,
    #[error("query exceeded {timeout_ms}ms execution timeout")]
    QueryTimeout { timeout_ms: u128 },
    #[error("query cancelled")]
    QueryCancelled,
    #[error("structured query tenant predicate conflicts with target tenant")]
    TenantPredicateConflict,
    #[error(
        "query result exceeds the maximum serialized size of {maximum_bytes} bytes \
         ({actual_bytes} bytes)"
    )]
    QueryResultTooLarge {
        actual_bytes: usize,
        maximum_bytes: usize,
    },
    #[error("structured query is invalid: {0}")]
    InvalidStructuredQuery(String),
    #[error("condition expression is invalid: {0}")]
    InvalidConditionExpression(String),
    #[error("regex is invalid for attribute {attribute_name}: {message}")]
    InvalidRegex {
        attribute_name: String,
        message: String,
    },
    #[error("storage item conversion failed for attribute {0}")]
    AttributeConversion(String),
    #[error("retention timestamp is missing or invalid: {0}")]
    InvalidRetentionTimestamp(String),
    #[error("retention period must be greater than zero")]
    InvalidRetentionPeriod,
    #[error("retention period overflowed unix epoch milliseconds")]
    RetentionOverflow,
    #[error("row expansion record is invalid: {0}")]
    InvalidRowExpansionRecord(&'static str),
    #[error("row expansion record is missing a declared path")]
    MissingRowExpansionPath,
    #[error("row expansion produced duplicate row identity")]
    DuplicateExpandedRowIdentity,
    #[error("System source record key is invalid: {0}")]
    InvalidSystemSourceKey(String),
    #[error("System source record owner state is corrupt: {0}")]
    CorruptSystemSourceRecordState(String),
    #[error("System source record position requires source-effect validation")]
    SystemSourceRecordRequiresValidation,
    #[error("iterator checkpoint cannot be persisted as a durable record checkpoint")]
    IteratorCheckpointNotDurable,
    #[error("source effect execution ID must contain exactly 16 bytes (got {actual})")]
    InvalidSourceEffectExecutionId { actual: usize },
    #[error("source effect fence must be non-zero")]
    InvalidSourceEffectFence,
    #[error("source effect manifest digest does not match the supplied manifest")]
    InvalidSourceEffectManifestDigest,
    #[error("source effect scope is empty, oversized, or contains control characters")]
    InvalidSourceEffectScope,
    #[error("source effect batch contains more than one source scope")]
    MixedSourceEffectScope,
    #[error("source effect batch contains duplicate checkpoints for one scope")]
    DuplicateSourceEffectCheckpoint,
    #[error("source effect authority is ambiguous")]
    AmbiguousSourceEffectAuthority,
    #[error("source effect receipt is ambiguous")]
    AmbiguousSourceEffectReceipt,
    #[error("source effect receipt conflicts with the requested source scope")]
    ConflictingSourceEffectReceipt,
    #[error("source effect state is incomplete")]
    IncompleteSourceEffectState,
    #[error("source effect receipt contains invalid data")]
    InvalidSourceEffectReceipt,
    #[error("source effect destination fence is unavailable for this backend")]
    SourceEffectFenceUnavailable,
    #[error("retention effect execution ID must contain exactly 16 bytes (got {actual})")]
    InvalidRetentionEffectExecutionId { actual: usize },
    #[error("retention effect fence must be non-zero")]
    InvalidRetentionEffectFence,
    #[error("retention effect scope is empty, oversized, or contains control characters")]
    InvalidRetentionEffectScope,
    #[error("retention effect request is invalid: {0}")]
    InvalidRetentionEffectRequest(&'static str),
    #[error("retention effect authority is ambiguous")]
    AmbiguousRetentionEffectAuthority,
    #[error("retention effect receipt is ambiguous")]
    AmbiguousRetentionEffectReceipt,
    #[error("retention effect receipt conflicts with the requested scope or batch")]
    ConflictingRetentionEffectReceipt,
    #[error("retention schedule occurrence is not durably authorized")]
    RetentionEffectScheduleUnavailable,
    #[error("retention schedule occurrence is ambiguous")]
    AmbiguousRetentionScheduleOccurrence,
    #[error("retention schedule occurrence conflicts with the requested authority")]
    ConflictingRetentionScheduleOccurrence,
    #[error("retention effect state is incomplete")]
    IncompleteRetentionEffectState,
    #[error("retention effect receipt contains invalid data")]
    InvalidRetentionEffectReceipt,
    #[error("retention effect destination fence is unavailable for this backend")]
    RetentionEffectFenceUnavailable,
    #[error(
        "source batch record is missing shard identity while multiple checkpoints exist for \
         {source_table_name}"
    )]
    AmbiguousSourceShard { source_table_name: String },
    #[error(
        "source batch is blocked by an unresolved record failure at \
         {source_table_name}/{shard_id}/{record_position}"
    )]
    SourceBatchBlocked {
        source_table_name: String,
        shard_id: String,
        record_position: String,
    },
    #[error("source batch failure was rolled back but could not be recorded durably: {message}")]
    SourceBatchFailureNotRecorded { message: String },
    #[error("tenant range purge request is invalid: {0}")]
    InvalidRangePurge(String),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    SourcePosition(#[from] SourcePositionError),
    #[error("AWS credential resolution failed: {0}")]
    AwsCredentials(#[from] aws_credentials::CredentialsError),
}

impl AnalyticsEngineError {
    /// Returns whether the error is a deterministic record-contract failure,
    /// distinct from a retryable runtime, catalog, or storage failure.
    #[must_use]
    pub fn is_record_contract_failure(&self) -> bool {
        matches!(
            self,
            Self::RecordDoesNotMatchRegistration(_)
                | Self::MissingTenant
                | Self::MissingAttribute(_)
                | Self::RegexNoMatch { .. }
                | Self::MissingIdentifier(_)
                | Self::AttributeConversion(_)
                | Self::InvalidRetentionTimestamp(_)
                | Self::InvalidRowExpansionRecord(_)
                | Self::MissingRowExpansionPath
                | Self::DuplicateExpandedRowIdentity
                | Self::InvalidSystemSourceKey(_)
        )
    }
}

pub type AnalyticsEngineResult<T> = Result<T, AnalyticsEngineError>;
