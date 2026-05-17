use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::RateLimitPolicy;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BackfillPlanError {
    #[error("backfill request must target at least one table")]
    MissingTargetTables,
    #[error("backfill plan rejected for table {table}: stable row identity is required")]
    MissingRowIdentity { table: String },
    #[error(
        "backfill plan rejected for table {table}: stream-only backfill is unsafe beyond stream \
         retention"
    )]
    UnsafeStreamOnlyBackfill { table: String },
    #[error("backfill plan rejected: source cannot enumerate historical rows")]
    HistoricalEnumerationUnsupported,
    #[error("backfill source kind {0} is not supported")]
    UnsupportedSourceKind(String),
    #[error("backfill capability discovery failed: {0}")]
    CapabilityDiscoveryFailed(String),
    #[error("backfill capability discovery found incomplete support for {source_kind:?}: {reason}")]
    PartialCapabilitySupport {
        source_kind: BackfillSourceKind,
        reason: String,
    },
}

pub type BackfillPlanResult<T> = Result<T, BackfillPlanError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillSourceKind {
    DynamoDb,
    AuxStorage,
    RawBackup,
    LocalFixture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMethod {
    DynamoDbPitrExport,
    DynamoDbParallelScan,
    AuxStorageEnumeration,
    RawBackupReplay,
    LocalFixture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationBackend {
    DuckDb,
    DuckLake,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationMode {
    CountsAndHashes,
    FullRows,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct BackfillSourceCapabilities {
    pub source_kind: BackfillSourceKind,
    pub stream_retention_hours: Option<u16>,
    pub supports_pitr_export: bool,
    pub supports_parallel_scan: bool,
    pub supports_full_enumeration: bool,
    pub supports_raw_replay: bool,
    pub can_prove_deletes: bool,
}

impl BackfillSourceCapabilities {
    #[must_use]
    pub const fn dynamodb_export() -> Self {
        Self {
            source_kind: BackfillSourceKind::DynamoDb,
            stream_retention_hours: Some(24),
            supports_pitr_export: true,
            supports_parallel_scan: true,
            supports_full_enumeration: false,
            supports_raw_replay: false,
            can_prove_deletes: false,
        }
    }

    #[must_use]
    pub const fn aux_storage() -> Self {
        Self {
            source_kind: BackfillSourceKind::AuxStorage,
            stream_retention_hours: None,
            supports_pitr_export: false,
            supports_parallel_scan: false,
            supports_full_enumeration: true,
            supports_raw_replay: false,
            can_prove_deletes: true,
        }
    }

    #[must_use]
    pub const fn raw_backup() -> Self {
        Self {
            source_kind: BackfillSourceKind::RawBackup,
            stream_retention_hours: None,
            supports_pitr_export: false,
            supports_parallel_scan: false,
            supports_full_enumeration: false,
            supports_raw_replay: true,
            can_prove_deletes: false,
        }
    }

    #[must_use]
    pub const fn local_fixture() -> Self {
        Self {
            source_kind: BackfillSourceKind::LocalFixture,
            stream_retention_hours: None,
            supports_pitr_export: false,
            supports_parallel_scan: false,
            supports_full_enumeration: true,
            supports_raw_replay: false,
            can_prove_deletes: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillRequest {
    pub target_tables: Vec<String>,
    pub source: BackfillSourceCapabilities,
    pub requested_snapshot_method: Option<SnapshotMethod>,
    pub filters: Vec<String>,
    pub chunk_target_rows: u64,
    pub rate_limit: RateLimitPolicy,
    pub destination_backend: DestinationBackend,
    pub dry_run: bool,
    pub validation_mode: ValidationMode,
    pub all_tables_have_row_identity: bool,
    pub estimated_table_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillRequestInput {
    pub target_tables: Vec<String>,
    pub source_kind: BackfillSourceKind,
    pub destination_backend: DestinationBackend,
    pub dry_run: bool,
    pub validation_mode: ValidationMode,
    pub all_tables_have_row_identity: bool,
    pub estimated_table_rows: u64,
    pub chunk_target_rows: u64,
    pub rate_limit: RateLimitPolicy,
}

pub trait BackfillCapabilityDiscovery {
    fn discover(
        &self,
        source_kind: BackfillSourceKind,
    ) -> BackfillPlanResult<BackfillSourceCapabilities>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamoDbBackfillMetadata {
    pub metadata_reachable: bool,
    pub pitr_enabled: bool,
    pub scan_enabled: bool,
    pub stream_retention_hours: Option<u16>,
}

pub trait DynamoDbBackfillMetadataAdapter {
    fn discover_dynamodb_metadata(&self) -> BackfillPlanResult<DynamoDbBackfillMetadata>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxStorageBackfillMetadata {
    pub metadata_reachable: bool,
    pub enumeration_enabled: bool,
    pub can_prove_deletes: bool,
}

pub trait AuxStorageBackfillMetadataAdapter {
    fn discover_aux_storage_metadata(&self) -> BackfillPlanResult<AuxStorageBackfillMetadata>;
}

#[derive(Debug, Clone)]
pub struct BackfillDiscoveryAdapters<DynamoDbAdapter, AuxStorageAdapter> {
    dynamodb: DynamoDbAdapter,
    aux_storage: AuxStorageAdapter,
}

impl<DynamoDbAdapter, AuxStorageAdapter>
    BackfillDiscoveryAdapters<DynamoDbAdapter, AuxStorageAdapter>
{
    #[must_use]
    pub const fn new(dynamodb: DynamoDbAdapter, aux_storage: AuxStorageAdapter) -> Self {
        Self {
            dynamodb,
            aux_storage,
        }
    }
}

impl<DynamoDbAdapter, AuxStorageAdapter> BackfillCapabilityDiscovery
    for BackfillDiscoveryAdapters<DynamoDbAdapter, AuxStorageAdapter>
where
    DynamoDbAdapter: DynamoDbBackfillMetadataAdapter,
    AuxStorageAdapter: AuxStorageBackfillMetadataAdapter,
{
    fn discover(
        &self,
        source_kind: BackfillSourceKind,
    ) -> BackfillPlanResult<BackfillSourceCapabilities> {
        match source_kind {
            BackfillSourceKind::DynamoDb => discover_dynamodb(&self.dynamodb),
            BackfillSourceKind::AuxStorage => discover_aux_storage(&self.aux_storage),
            BackfillSourceKind::RawBackup => Ok(BackfillSourceCapabilities::raw_backup()),
            BackfillSourceKind::LocalFixture => Ok(BackfillSourceCapabilities::local_fixture()),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StaticBackfillCapabilityDiscovery;

impl BackfillCapabilityDiscovery for StaticBackfillCapabilityDiscovery {
    fn discover(
        &self,
        source_kind: BackfillSourceKind,
    ) -> BackfillPlanResult<BackfillSourceCapabilities> {
        Ok(match source_kind {
            BackfillSourceKind::DynamoDb => BackfillSourceCapabilities::dynamodb_export(),
            BackfillSourceKind::AuxStorage => BackfillSourceCapabilities::aux_storage(),
            BackfillSourceKind::RawBackup => BackfillSourceCapabilities::raw_backup(),
            BackfillSourceKind::LocalFixture => BackfillSourceCapabilities::local_fixture(),
        })
    }
}

fn discover_dynamodb(
    adapter: &impl DynamoDbBackfillMetadataAdapter,
) -> BackfillPlanResult<BackfillSourceCapabilities> {
    let metadata = adapter.discover_dynamodb_metadata()?;
    if !metadata.metadata_reachable {
        return Err(BackfillPlanError::PartialCapabilitySupport {
            source_kind: BackfillSourceKind::DynamoDb,
            reason: "DynamoDB metadata is not reachable".to_string(),
        });
    }
    if !metadata.pitr_enabled && !metadata.scan_enabled {
        return Err(BackfillPlanError::PartialCapabilitySupport {
            source_kind: BackfillSourceKind::DynamoDb,
            reason: "neither PITR export nor parallel scan is available".to_string(),
        });
    }
    Ok(BackfillSourceCapabilities {
        source_kind: BackfillSourceKind::DynamoDb,
        stream_retention_hours: metadata.stream_retention_hours.or(Some(24)),
        supports_pitr_export: metadata.pitr_enabled,
        supports_parallel_scan: metadata.scan_enabled,
        supports_full_enumeration: false,
        supports_raw_replay: false,
        can_prove_deletes: false,
    })
}

fn discover_aux_storage(
    adapter: &impl AuxStorageBackfillMetadataAdapter,
) -> BackfillPlanResult<BackfillSourceCapabilities> {
    let metadata = adapter.discover_aux_storage_metadata()?;
    if !metadata.metadata_reachable {
        return Err(BackfillPlanError::PartialCapabilitySupport {
            source_kind: BackfillSourceKind::AuxStorage,
            reason: "aux-storage metadata is not reachable".to_string(),
        });
    }
    if !metadata.enumeration_enabled {
        return Err(BackfillPlanError::PartialCapabilitySupport {
            source_kind: BackfillSourceKind::AuxStorage,
            reason: "full enumeration is not available".to_string(),
        });
    }
    Ok(BackfillSourceCapabilities {
        source_kind: BackfillSourceKind::AuxStorage,
        stream_retention_hours: None,
        supports_pitr_export: false,
        supports_parallel_scan: false,
        supports_full_enumeration: true,
        supports_raw_replay: false,
        can_prove_deletes: metadata.can_prove_deletes,
    })
}

pub fn build_backfill_request(
    input: BackfillRequestInput,
    discovery: &impl BackfillCapabilityDiscovery,
) -> BackfillPlanResult<BackfillRequest> {
    Ok(BackfillRequest {
        target_tables: input.target_tables,
        source: discovery.discover(input.source_kind)?,
        requested_snapshot_method: None,
        filters: Vec::new(),
        chunk_target_rows: input.chunk_target_rows,
        rate_limit: input.rate_limit,
        destination_backend: input.destination_backend,
        dry_run: input.dry_run,
        validation_mode: input.validation_mode,
        all_tables_have_row_identity: input.all_tables_have_row_identity,
        estimated_table_rows: input.estimated_table_rows,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillPlan {
    pub target_tables: Vec<String>,
    pub snapshot_method: SnapshotMethod,
    pub destination_backend: DestinationBackend,
    pub dry_run: bool,
    pub chunk_count: u64,
    pub estimated_rows: u64,
    pub source_impact: String,
    pub destination_impact: String,
    pub deletes_can_be_proven: bool,
    pub validation_mode: ValidationMode,
    pub validation_steps: Vec<String>,
    pub required_permissions: Vec<String>,
    pub warnings: Vec<String>,
    pub steps: Vec<BackfillPlanStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillPlanStep {
    pub order: u8,
    pub name: String,
}

pub trait BackfillPlanner {
    fn plan(&self, request: &BackfillRequest) -> BackfillPlanResult<BackfillPlan>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ConservativeBackfillPlanner;

impl BackfillPlanner for ConservativeBackfillPlanner {
    fn plan(&self, request: &BackfillRequest) -> BackfillPlanResult<BackfillPlan> {
        validate_request(request)?;
        let snapshot_method = choose_snapshot_method(request)?;
        let chunk_count = estimate_chunks(request.estimated_table_rows, request.chunk_target_rows);
        Ok(BackfillPlan {
            target_tables: request.target_tables.clone(),
            snapshot_method,
            destination_backend: request.destination_backend,
            dry_run: request.dry_run,
            chunk_count,
            estimated_rows: request.estimated_table_rows,
            source_impact: source_impact(snapshot_method).to_string(),
            destination_impact: "bounded writes through operation rate limits".to_string(),
            deletes_can_be_proven: request.source.can_prove_deletes,
            validation_mode: request.validation_mode,
            validation_steps: validation_steps(request.validation_mode),
            required_permissions: required_permissions(
                snapshot_method,
                request.destination_backend,
            ),
            warnings: warnings(request, snapshot_method),
            steps: vec![
                step(1, "inspect_source"),
                step(2, "inspect_destination"),
                step(3, "choose_snapshot_strategy"),
                step(4, "estimate_chunks"),
                step(5, "reserve_operation"),
                step(6, "run_validation_after_backfill"),
            ],
        })
    }
}

fn validate_request(request: &BackfillRequest) -> BackfillPlanResult<()> {
    if request.target_tables.is_empty() {
        return Err(BackfillPlanError::MissingTargetTables);
    }
    if !request.all_tables_have_row_identity {
        return Err(BackfillPlanError::MissingRowIdentity {
            table: request.target_tables[0].clone(),
        });
    }
    Ok(())
}

fn choose_snapshot_method(request: &BackfillRequest) -> BackfillPlanResult<SnapshotMethod> {
    if let Some(method) = request.requested_snapshot_method {
        validate_snapshot_method(request, method)?;
        return Ok(method);
    }
    if request.source.source_kind == BackfillSourceKind::DynamoDb
        && request.source.supports_pitr_export
    {
        return Ok(SnapshotMethod::DynamoDbPitrExport);
    }
    if request.source.source_kind == BackfillSourceKind::AuxStorage
        && request.source.supports_full_enumeration
    {
        return Ok(SnapshotMethod::AuxStorageEnumeration);
    }
    if request.source.source_kind == BackfillSourceKind::LocalFixture
        && request.source.supports_full_enumeration
    {
        return Ok(SnapshotMethod::LocalFixture);
    }
    if request.source.source_kind == BackfillSourceKind::DynamoDb
        && request.source.supports_parallel_scan
    {
        return Ok(SnapshotMethod::DynamoDbParallelScan);
    }
    if request.source.source_kind == BackfillSourceKind::RawBackup
        && request.source.supports_raw_replay
    {
        return Ok(SnapshotMethod::RawBackupReplay);
    }
    Err(BackfillPlanError::HistoricalEnumerationUnsupported)
}

fn validate_snapshot_method(
    request: &BackfillRequest,
    method: SnapshotMethod,
) -> BackfillPlanResult<()> {
    match method {
        SnapshotMethod::DynamoDbPitrExport
            if request.source.source_kind == BackfillSourceKind::DynamoDb
                && request.source.supports_pitr_export =>
        {
            Ok(())
        }
        SnapshotMethod::DynamoDbParallelScan
            if request.source.source_kind == BackfillSourceKind::DynamoDb
                && request.source.supports_parallel_scan =>
        {
            Ok(())
        }
        SnapshotMethod::AuxStorageEnumeration
            if request.source.source_kind == BackfillSourceKind::AuxStorage
                && request.source.supports_full_enumeration =>
        {
            Ok(())
        }
        SnapshotMethod::RawBackupReplay
            if request.source.source_kind == BackfillSourceKind::RawBackup
                && request.source.supports_raw_replay =>
        {
            Ok(())
        }
        SnapshotMethod::LocalFixture
            if request.source.source_kind == BackfillSourceKind::LocalFixture
                && request.source.supports_full_enumeration =>
        {
            Ok(())
        }
        _ if request.source.stream_retention_hours.is_some() => {
            Err(BackfillPlanError::UnsafeStreamOnlyBackfill {
                table: request.target_tables[0].clone(),
            })
        }
        _ => Err(BackfillPlanError::HistoricalEnumerationUnsupported),
    }
}

fn estimate_chunks(estimated_rows: u64, chunk_target_rows: u64) -> u64 {
    let chunk_target_rows = chunk_target_rows.max(1);
    estimated_rows.div_ceil(chunk_target_rows).max(1)
}

fn source_impact(method: SnapshotMethod) -> &'static str {
    match method {
        SnapshotMethod::DynamoDbPitrExport => "low table read impact; requires PITR export access",
        SnapshotMethod::DynamoDbParallelScan => "bounded table read capacity impact",
        SnapshotMethod::AuxStorageEnumeration => "bounded aux-storage enumeration reads",
        SnapshotMethod::RawBackupReplay => "object-store reads only",
        SnapshotMethod::LocalFixture => "local fixture reads only",
    }
}

fn validation_steps(mode: ValidationMode) -> Vec<String> {
    match mode {
        ValidationMode::CountsAndHashes => vec![
            "compare_table_counts".to_string(),
            "compare_key_set_hashes".to_string(),
            "compare_projected_column_hashes".to_string(),
        ],
        ValidationMode::FullRows => vec![
            "compare_table_counts".to_string(),
            "compare_key_sets".to_string(),
            "compare_full_rows".to_string(),
        ],
    }
}

fn required_permissions(
    method: SnapshotMethod,
    destination_backend: DestinationBackend,
) -> Vec<String> {
    let mut permissions = match method {
        SnapshotMethod::DynamoDbPitrExport => vec![
            "dynamodb:DescribeTable".to_string(),
            "dynamodb:DescribeContinuousBackups".to_string(),
            "dynamodb:ExportTableToPointInTime".to_string(),
            "s3 read access to the export location".to_string(),
        ],
        SnapshotMethod::DynamoDbParallelScan => vec![
            "dynamodb:DescribeTable".to_string(),
            "dynamodb:Scan".to_string(),
        ],
        SnapshotMethod::AuxStorageEnumeration => vec![
            "aux-storage metadata read access".to_string(),
            "aux-storage stream/table enumeration read access".to_string(),
        ],
        SnapshotMethod::RawBackupReplay => vec![
            "object-store list access to the backup prefix".to_string(),
            "object-store read access to backup objects".to_string(),
        ],
        SnapshotMethod::LocalFixture => {
            vec!["local filesystem read access to fixture data".to_string()]
        }
    };
    permissions.push(match destination_backend {
        DestinationBackend::DuckDb => {
            "DuckDB read/write access to destination database".to_string()
        }
        DestinationBackend::DuckLake => {
            "DuckLake catalog and object-store read/write access".to_string()
        }
    });
    permissions
}

fn warnings(request: &BackfillRequest, method: SnapshotMethod) -> Vec<String> {
    let mut warnings = Vec::new();
    if !request.source.can_prove_deletes {
        warnings.push(
            "source cannot prove historical deletes; follow-up check may report extras".to_string(),
        );
    }
    if method == SnapshotMethod::DynamoDbParallelScan {
        warnings.push(
            "parallel scan consumes live table read capacity; keep rate limits conservative"
                .to_string(),
        );
    }
    warnings
}

fn step(order: u8, name: &str) -> BackfillPlanStep {
    BackfillPlanStep {
        order,
        name: name.to_string(),
    }
}
