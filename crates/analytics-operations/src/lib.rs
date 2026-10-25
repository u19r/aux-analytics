//! Durable operation state, audit, and control-plane contracts.

mod backfill_execution;
mod backfill_plan;
mod check;
mod check_position;
mod check_sources;
mod constants;
mod error;
mod maintenance;
mod maintenance_execution;
mod operation_contract;
mod privacy_fix;
mod rate_limit;
mod raw_backup;
mod store;
mod table_fix;
mod trim;

pub use backfill_execution::{
    BackfillDestinationWriter, BackfillExecutionError, BackfillExecutionInput,
    BackfillExecutionMetrics, BackfillExecutionProgress, BackfillExecutionReport,
    BackfillExecutionRequest, BackfillExecutionResult, BackfillExecutionState,
    CountingBackfillDestinationWriter, LocalBackfillChunk, LocalBackfillExecutor,
    LocalBackfillFixture, LocalStreamUpdate, SnapshotChunkSource, StreamCatchupSource,
};
pub use backfill_plan::{
    AuxStorageBackfillMetadata, AuxStorageBackfillMetadataAdapter, BackfillCapabilityDiscovery,
    BackfillDiscoveryAdapters, BackfillPlan, BackfillPlanError, BackfillPlanResult,
    BackfillPlanStep, BackfillPlanner, BackfillRequest, BackfillRequestInput,
    BackfillSourceCapabilities, BackfillSourceKind, ConservativeBackfillPlanner,
    DestinationBackend, DynamoDbBackfillMetadata, DynamoDbBackfillMetadataAdapter, SnapshotMethod,
    StaticBackfillCapabilityDiscovery, ValidationMode,
};
#[cfg(test)]
pub(crate) use check::filter_target_tables_baseline;
pub use check::{
    CheckComparisonStrategy, CheckError, CheckMetrics, CheckOutcome, CheckOutputFormat,
    CheckReport, CheckRequest, CheckResult, CheckRow, CheckRowStream, CheckRowStreamItem,
    CheckRowStreamSource, CheckSamples, DestinationRows, LocalCheckDataset, LocalCheckExecutor,
    SourceTruthRows,
};
pub use check_position::CheckSourcePosition;
pub use check_sources::{
    AuxStorageCheckRows, CheckBackendKind, CheckBackendSupport, CheckRowProjection, CheckRowSource,
    CurrentRowPageReader, CurrentRowPageRequest, CurrentRowReader, CurrentRowStreamOrder,
    CurrentRowStreamReader, DuckDbAnalyticalCheckRows, DuckDbCheckRows,
    DuckDbTableScanCurrentRowReader, DuckLakeCheckRows, DynamoDbCheckRows, FixtureCurrentRowReader,
    FixturePagedCurrentRowReader, LocalFixtureCheckRows, ProductionCurrentRowPageReader,
    ProductionCurrentRowReader, ProjectedCurrentRow, ProjectedCurrentRowItem,
    ProjectedCurrentRowPage, ProjectedCurrentRowStream, ProjectionBackedCheckRows,
    StaticCheckBackendRows,
};
pub use error::{MaintenanceError, MaintenanceResult};
pub use maintenance::{
    MaintenanceExecutionReport, MaintenanceOperations, MaintenanceRequest, MaintenanceSubmission,
    MaintenanceSubmissionOutcome,
};
pub use operation_contract::{
    CancellationState, MaintenanceExecutionAuthority, MaintenanceWindow, OperationActor,
    OperationChunkKey, OperationCursor, OperationEvent, OperationEventKind, OperationId,
    OperationKind, OperationPhase, OperationRequest, OperationResult, OperationStatus,
    OperationTypeError, RateLimitPolicy,
};
pub(crate) use privacy_fix::PrivacyFixExecutor;
pub use privacy_fix::{
    PrivacyFixDestructiveAction, PrivacyFixError, PrivacyFixReport, PrivacyFixRequest,
    PrivacyFixResult,
};
pub use rate_limit::{
    OperationRateLimiter, RateLimitClock, RateLimitDecision, RateLimitDenied, RateLimitUsage,
    SystemRateLimitClock, enforce_rate_limit, enforce_rate_limit_with_limiter,
};
pub use raw_backup::{
    FilesystemRawBackupStore, RAW_BACKUP_SCHEMA_VERSION, RawBackupChecksumAlgorithm,
    RawBackupCleanupAuthority, RawBackupCleanupMetadata, RawBackupCleanupProgress,
    RawBackupCompression, RawBackupDestinationKind, RawBackupEncryption, RawBackupError,
    RawBackupMetrics, RawBackupObjectId, RawBackupObjectIndex, RawBackupObjectLayout,
    RawBackupObjectStore, RawBackupRecord, RawBackupReplayExecution, RawBackupReplayRecord,
    RawBackupReplayReport, RawBackupReplayRequest, RawBackupReplaySummary, RawBackupResult,
    RawBackupVerifyReport, RawBackupWriteReport, RawBackupWriteRequest,
};
pub use store::{
    ExecutionAttempt, OperationEffectCompletion, OperationStore, OperationStoreError,
    OperationStoreResult, OperationTransition, StoredOperation,
};
#[cfg(test)]
pub(crate) use table_fix::ProjectionBackedTableFixCurrentSource;
pub(crate) use table_fix::TableFixExecutor;
pub use table_fix::{
    TableFixApplyOutcome, TableFixConfirmation, TableFixError, TableFixReport, TableFixRequest,
    TableFixResult, TableFixSelection, TableFixSource, TableFixSourceSnapshot, TableFixTruthRecord,
    TableFixValidation, VerifiedTableFixDestination, VersionedTableFixCurrentSource,
};
pub use trim::{
    DuckDbTrimRows, TrimCandidateSnapshot, TrimCandidateSource, TrimDeletionOutcome,
    TrimDestination, TrimError, TrimPredicate, TrimPredicateValue, TrimReport, TrimRequest,
    TrimResult, TrimTarget,
};

#[cfg(test)]
mod backfill_execution_tests;
#[cfg(test)]
mod backfill_plan_tests;
#[cfg(test)]
mod check_position_tests;
#[cfg(test)]
mod check_stream_order_tests;
#[cfg(test)]
mod check_tests;
#[cfg(test)]
mod maintenance_executor_performance_tests;
#[cfg(test)]
mod maintenance_tests;
#[cfg(test)]
mod operation_contract_tests;
#[cfg(test)]
mod performance_measurement;
#[cfg(test)]
mod privacy_fix_tests;
#[cfg(test)]
mod quint_backfill_planning_tests;
#[cfg(test)]
mod quint_check_tests;
#[cfg(test)]
mod quint_operations_tests;
#[cfg(test)]
mod quint_privacy_fix_tests;
#[cfg(test)]
mod quint_rate_limit_tests;
#[cfg(test)]
mod quint_raw_backup_tests;
#[cfg(test)]
mod quint_table_fix_tests;
#[cfg(test)]
mod quint_trim_tests;
#[cfg(test)]
mod rate_limit_tests;
#[cfg(test)]
mod raw_backup_tests;
#[cfg(test)]
mod store_test_support;
#[cfg(test)]
mod store_tests;
#[cfg(test)]
mod table_fix_tests;
#[cfg(test)]
mod trim_tests;

#[cfg(test)]
mod check_test_support;
