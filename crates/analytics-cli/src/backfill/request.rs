use std::{fs, path::Path};

use analytics_contract::AnalyticsManifest;
use analytics_operations::{
    AuxStorageBackfillMetadata, AuxStorageBackfillMetadataAdapter, BackfillDiscoveryAdapters,
    BackfillRequest, BackfillRequestInput, BackfillSourceKind, DestinationBackend,
    DynamoDbBackfillMetadata, DynamoDbBackfillMetadataAdapter, RateLimitPolicy, ValidationMode,
};

use crate::{
    backfill::args::BackfillSourceKindArg,
    error::{CliError, CliErrorDebug, CliErrorKind, CliResult},
};

pub(crate) fn read_backfill_request(path: &Path) -> CliResult<BackfillRequest> {
    let contents = fs::read_to_string(path)?;
    serde_json::from_str::<BackfillRequest>(contents.as_str()).map_err(Into::into)
}

pub(crate) struct BackfillPlanInput {
    pub(crate) source_kind: Option<BackfillSourceKindArg>,
    pub(crate) dynamodb_metadata: DynamoDbBackfillMetadata,
    pub(crate) aux_storage_metadata: AuxStorageBackfillMetadata,
    pub(crate) target_tables: Vec<String>,
    pub(crate) estimated_rows: u64,
    pub(crate) chunk_target_rows: u64,
}

pub(crate) fn build_backfill_request_from_manifest(
    request: config::ConfigRequest,
    input: BackfillPlanInput,
) -> CliResult<BackfillRequest> {
    let BackfillPlanInput {
        source_kind: source_kind_arg,
        dynamodb_metadata,
        aux_storage_metadata,
        target_tables,
        estimated_rows,
        chunk_target_rows,
    } = input;
    let resolved = config::resolve_config(request)?;
    let manifest = resolved.manifest.value;
    let target_tables = selected_target_tables(target_tables, &manifest)?;
    let source_kind = source_kind_arg
        .map(BackfillSourceKind::from)
        .or_else(|| source_kind_from_config(&resolved.root.analytics.source))
        .ok_or_else(|| {
            CliError::with_debug(
                CliErrorKind::Config,
                CliErrorDebug::Message(
                    "source kind must be supplied with --source-kind or \
                     analytics.source.stream_type"
                        .to_string(),
                ),
            )
        })?;
    let discovery = BackfillDiscoveryAdapters::new(
        CliDynamoDbBackfillMetadataAdapter {
            metadata: dynamodb_metadata,
        },
        CliAuxStorageBackfillMetadataAdapter {
            metadata: aux_storage_metadata,
        },
    );
    BackfillRequest::try_new(
        BackfillRequestInput {
            target_tables: target_tables.clone(),
            source_kind,
            destination_backend: DestinationBackend::DuckDb,
            dry_run: true,
            validation_mode: ValidationMode::CountsAndHashes,
            all_tables_have_row_identity: all_targets_have_row_identity(&manifest, &target_tables),
            estimated_table_rows: estimated_rows,
            chunk_target_rows,
            rate_limit: RateLimitPolicy::default(),
        },
        &discovery,
    )
    .map_err(Into::into)
}

struct CliDynamoDbBackfillMetadataAdapter {
    metadata: DynamoDbBackfillMetadata,
}

impl DynamoDbBackfillMetadataAdapter for CliDynamoDbBackfillMetadataAdapter {
    fn discover_dynamodb_metadata(
        &self,
    ) -> analytics_operations::BackfillPlanResult<DynamoDbBackfillMetadata> {
        Ok(self.metadata.clone())
    }
}

struct CliAuxStorageBackfillMetadataAdapter {
    metadata: AuxStorageBackfillMetadata,
}

impl AuxStorageBackfillMetadataAdapter for CliAuxStorageBackfillMetadataAdapter {
    fn discover_aux_storage_metadata(
        &self,
    ) -> analytics_operations::BackfillPlanResult<AuxStorageBackfillMetadata> {
        Ok(self.metadata.clone())
    }
}

fn selected_target_tables(
    target_tables: Vec<String>,
    manifest: &AnalyticsManifest,
) -> CliResult<Vec<String>> {
    if !target_tables.is_empty() {
        return Ok(target_tables);
    }
    let tables = manifest
        .tables
        .iter()
        .map(|table| table.analytics_table_name.clone())
        .collect::<Vec<_>>();
    if tables.is_empty() {
        return Err(CliError::with_debug(
            CliErrorKind::ManifestValidation,
            CliErrorDebug::Message("manifest contains no tables".to_string()),
        ));
    }
    Ok(tables)
}

fn all_targets_have_row_identity(manifest: &AnalyticsManifest, target_tables: &[String]) -> bool {
    target_tables.iter().all(|target| {
        manifest
            .tables
            .iter()
            .any(|table| table.analytics_table_name == *target)
    })
}

fn source_kind_from_config(source: &config::AnalyticsSourceConfig) -> Option<BackfillSourceKind> {
    source.stream_type.map(|stream_type| match stream_type {
        config::AnalyticsStreamType::StorageStream => BackfillSourceKind::DynamoDb,
        config::AnalyticsStreamType::AuxStorage => BackfillSourceKind::AuxStorage,
    })
}

impl From<BackfillSourceKindArg> for BackfillSourceKind {
    fn from(value: BackfillSourceKindArg) -> Self {
        match value {
            BackfillSourceKindArg::DynamoDb => Self::DynamoDb,
            BackfillSourceKindArg::AuxStorage => Self::AuxStorage,
            BackfillSourceKindArg::RawBackup => Self::RawBackup,
            BackfillSourceKindArg::LocalFixture => Self::LocalFixture,
        }
    }
}

pub(super) fn read_backfill_execution_request(
    path: &std::path::Path,
) -> CliResult<analytics_operations::BackfillExecutionRequest> {
    Ok(serde_json::from_str::<
        analytics_operations::BackfillExecutionRequest,
    >(std::fs::read_to_string(path)?.as_str())?)
}

pub(super) fn read_local_backfill_fixture(
    path: &std::path::Path,
) -> CliResult<analytics_operations::LocalBackfillFixture> {
    Ok(serde_json::from_str::<
        analytics_operations::LocalBackfillFixture,
    >(std::fs::read_to_string(path)?.as_str())?)
}
