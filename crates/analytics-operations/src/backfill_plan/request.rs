use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMethod {
    DynamoDbPitrExport,
    DynamoDbParallelScan,
    AuxStorageEnumeration,
    RawBackupReplay,
    LocalFixture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DestinationBackend {
    DuckDb,
    DuckLake,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValidationMode {
    CountsAndHashes,
    FullRows,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
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

impl BackfillRequest {
    pub fn try_new(
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
}
