use std::{fs, path::Path};

use analytics_contract::{AnalyticsManifest, read_manifest};
use analytics_operations::{
    AuxStorageBackfillMetadata, AuxStorageBackfillMetadataAdapter, BackfillDiscoveryAdapters,
    BackfillRequest, BackfillRequestInput, BackfillSourceKind, DestinationBackend,
    DynamoDbBackfillMetadata, DynamoDbBackfillMetadataAdapter, RateLimitPolicy, ValidationMode,
    build_backfill_request,
};

use crate::{
    backfill_args::BackfillSourceKindArg,
    error::{CliError, CliErrorDebug, CliErrorKind, CliResult},
    runtime_config::{load_config, resolve_manifest_path},
};

pub(crate) fn read_backfill_request(path: &Path) -> CliResult<BackfillRequest> {
    let contents = fs::read_to_string(path)?;
    serde_json::from_str::<BackfillRequest>(contents.as_str()).map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_backfill_request_from_manifest(
    manifest_arg: Option<&str>,
    config_path: Option<&Path>,
    overrides: &[String],
    source_kind_arg: Option<BackfillSourceKindArg>,
    discovery_args: BackfillDiscoveryArgs,
    target_tables: Vec<String>,
    estimated_rows: u64,
    chunk_target_rows: u64,
) -> CliResult<BackfillRequest> {
    let config = load_config(config_path, overrides)?;
    let manifest_path = resolve_manifest_path(manifest_arg, &config.root)?;
    let manifest = read_manifest(manifest_path.as_str())?;
    let target_tables = selected_target_tables(target_tables, &manifest)?;
    let source_kind = source_kind_arg
        .map(BackfillSourceKind::from)
        .or_else(|| source_kind_from_config(&config.root.analytics.source))
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
            args: discovery_args,
        },
        CliAuxStorageBackfillMetadataAdapter {
            args: discovery_args,
        },
    );
    build_backfill_request(
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

#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct BackfillDiscoveryArgs {
    pub(crate) metadata_reachable: bool,
    pub(crate) dynamodb_pitr_enabled: bool,
    pub(crate) dynamodb_scan_enabled: bool,
    pub(crate) aux_storage_enumeration_enabled: bool,
    pub(crate) aux_storage_deletes_provable: bool,
}

#[derive(Debug, Clone, Copy)]
struct CliDynamoDbBackfillMetadataAdapter {
    args: BackfillDiscoveryArgs,
}

impl DynamoDbBackfillMetadataAdapter for CliDynamoDbBackfillMetadataAdapter {
    fn discover_dynamodb_metadata(
        &self,
    ) -> analytics_operations::BackfillPlanResult<DynamoDbBackfillMetadata> {
        Ok(DynamoDbBackfillMetadata {
            metadata_reachable: self.args.metadata_reachable,
            pitr_enabled: self.args.dynamodb_pitr_enabled,
            scan_enabled: self.args.dynamodb_scan_enabled,
            stream_retention_hours: Some(24),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CliAuxStorageBackfillMetadataAdapter {
    args: BackfillDiscoveryArgs,
}

impl AuxStorageBackfillMetadataAdapter for CliAuxStorageBackfillMetadataAdapter {
    fn discover_aux_storage_metadata(
        &self,
    ) -> analytics_operations::BackfillPlanResult<AuxStorageBackfillMetadata> {
        Ok(AuxStorageBackfillMetadata {
            metadata_reachable: self.args.metadata_reachable,
            enumeration_enabled: self.args.aux_storage_enumeration_enabled,
            can_prove_deletes: self.args.aux_storage_deletes_provable,
        })
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
