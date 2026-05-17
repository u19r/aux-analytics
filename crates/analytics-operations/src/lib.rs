//! Durable operation state, audit, and control-plane contracts.

mod backfill_execution;
mod backfill_plan;
mod check;
mod check_sources;
mod privacy_fix;
mod rate_limit;
mod raw_backup;
mod store;
mod table_fix;
mod trim;
mod types;

pub use backfill_execution::{
    BackfillDestinationWriter, BackfillExecutionError, BackfillExecutionMetrics,
    BackfillExecutionReport, BackfillExecutionRequest, BackfillExecutionResult,
    BackfillExecutionState, BackfillWriteOutcome, CountingBackfillDestinationWriter,
    LocalBackfillChunk, LocalBackfillExecutor, LocalBackfillFixture, LocalStreamUpdate,
    SnapshotChunkSource, StreamCatchupSource,
};
pub use backfill_plan::{
    AuxStorageBackfillMetadata, AuxStorageBackfillMetadataAdapter, BackfillCapabilityDiscovery,
    BackfillDiscoveryAdapters, BackfillPlan, BackfillPlanError, BackfillPlanResult,
    BackfillPlanStep, BackfillPlanner, BackfillRequest, BackfillRequestInput,
    BackfillSourceCapabilities, BackfillSourceKind, ConservativeBackfillPlanner,
    DestinationBackend, DynamoDbBackfillMetadata, DynamoDbBackfillMetadataAdapter, SnapshotMethod,
    StaticBackfillCapabilityDiscovery, ValidationMode, build_backfill_request,
};
pub use check::{
    CheckComparisonStrategy, CheckError, CheckMetrics, CheckOutcome, CheckOutputFormat,
    CheckReport, CheckRequest, CheckResult, CheckRow, CheckRowStream, CheckRowStreamItem,
    CheckRowStreamSource, CheckSamples, DestinationRows, LocalCheckDataset, LocalCheckExecutor,
    SourceTruthRows,
};
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
#[allow(deprecated)]
pub use check_sources::{StaticCurrentRowReader, StaticPagedCurrentRowReader};
pub use privacy_fix::{
    PrivacyFixDestructiveAction, PrivacyFixError, PrivacyFixExecutor, PrivacyFixReport,
    PrivacyFixRequest, PrivacyFixResult,
};
pub use rate_limit::{OperationRateLimiter, RateLimitDecision, RateLimitUsage};
pub use raw_backup::{
    FilesystemRawBackupStore, RAW_BACKUP_SCHEMA_VERSION, RawBackupChecksumAlgorithm,
    RawBackupCompression, RawBackupEncryption, RawBackupError, RawBackupMetrics, RawBackupObjectId,
    RawBackupObjectIndex, RawBackupObjectLayout, RawBackupObjectStore, RawBackupRecord,
    RawBackupReplayRecord, RawBackupReplayReport, RawBackupResult, RawBackupVerifyReport,
    RawBackupWriteReport, RawBackupWriteRequest, S3CompatibleObjectClient,
    S3CompatibleRawBackupStore,
};
pub use store::{OperationStore, OperationStoreError, OperationStoreResult, StoredOperation};
pub use table_fix::{
    ProjectionBackedTableFixCurrentSource, TableFixCurrentSource, TableFixDestination,
    TableFixError, TableFixExecutor, TableFixReport, TableFixRequest, TableFixResult,
    TableFixSelection, TableFixSource, TableFixTruthRecord, TableFixValidation,
};
pub use trim::{
    DuckDbTrimRows, TrimCandidateSource, TrimDestination, TrimError, TrimExecutor, TrimReport,
    TrimRequest, TrimResult, TrimTarget,
};
pub use types::{
    CancellationState, MaintenanceWindow, OperationActor, OperationCursor, OperationEvent,
    OperationEventKind, OperationId, OperationKind, OperationPhase, OperationRequest,
    OperationResult, OperationStatus, OperationTypeError, RateLimitPolicy,
};

#[cfg(test)]
mod backfill_execution_tests;
#[cfg(test)]
mod backfill_plan_tests;
#[cfg(test)]
mod check_performance_tests;
#[cfg(test)]
mod check_stream_order_tests;
#[cfg(test)]
mod check_tests;
#[cfg(test)]
mod operation_contract_tests;
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
mod store_tests;
#[cfg(test)]
mod table_fix_tests;
#[cfg(test)]
mod trim_tests;
