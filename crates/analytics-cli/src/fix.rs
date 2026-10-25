use std::{fs, path::Path};

use analytics_contract::{AnalyticsManifest, StorageStreamRecord};
use analytics_engine::{
    AnalyticsEngine, MaintenanceEffectApplyRequest, MaintenanceEffectAuthority,
    MaintenanceEffectDisposition, MaintenanceEffectVerifyRequest,
};
use analytics_operations::{
    FilesystemRawBackupStore, MaintenanceError, MaintenanceExecutionAuthority,
    MaintenanceExecutionReport, MaintenanceOperations, MaintenanceRequest, OperationActor,
    OperationChunkKey, OperationStore, RateLimitPolicy, TableFixApplyOutcome, TableFixError,
    TableFixRequest, TableFixResult, VerifiedTableFixDestination,
};

use crate::{
    error::{CliError, CliErrorDebug, CliErrorKind, CliResult},
    fix_args::FixCommand,
};

pub(crate) fn run_fix_command(command: FixCommand) -> CliResult<String> {
    match command {
        FixCommand::Table {
            request,
            raw_backup_root,
            manifest,
            destination_duckdb,
            operation_store_duckdb,
        } => {
            let request = read_table_fix_request(request.as_path())?;
            let source_store = FilesystemRawBackupStore::new(raw_backup_root);
            let operation_store = OperationStore::connect_duckdb(operation_store_duckdb)?;
            let operations = MaintenanceOperations::new(&operation_store);
            let actor = OperationActor::try_new("cli")?;
            let dry_run = request.dry_run;
            let destination_factory = move || {
                table_fix_destination(dry_run, manifest.as_deref(), destination_duckdb.as_deref())
                    .map_err(|error| MaintenanceError::DestinationInitialization {
                        message: error.to_string(),
                    })
            };
            let report = operations.execute_table_fix_raw_backup_verified_from_store(
                &request,
                &actor,
                RateLimitPolicy::default(),
                &source_store,
                destination_factory,
            )?;
            let MaintenanceExecutionReport::TableFix(report) = report else {
                unreachable!("table-fix facade returned a different report kind")
            };
            Ok(serde_json::to_string_pretty(&report)?)
        }
    }
}

fn table_fix_destination(
    dry_run: bool,
    manifest: Option<&Path>,
    destination_duckdb: Option<&Path>,
) -> CliResult<CliTableFixDestination> {
    if dry_run {
        return Ok(CliTableFixDestination::DryRun);
    }
    let Some(manifest_path) = manifest else {
        return Err(table_fix_apply_error(
            "table fix CLI apply requires --manifest and --destination-duckdb",
        ));
    };
    let Some(destination_duckdb) = destination_duckdb else {
        return Err(table_fix_apply_error(
            "table fix CLI apply requires --manifest and --destination-duckdb",
        ));
    };
    let manifest = config::resolve_config(config::ConfigRequest::cli_manifest_only(
        None,
        Some(manifest_path.to_string_lossy().into_owned()),
        Vec::new(),
    ))?
    .manifest
    .value;
    let engine = AnalyticsEngine::connect_duckdb(destination_duckdb.to_string_lossy().as_ref())?;
    Ok(CliTableFixDestination::Engine(Box::new(
        EngineTableFixDestination {
            manifest,
            engine,
            manifest_ready: false,
        },
    )))
}

fn table_fix_apply_error(message: &str) -> CliError {
    CliError::with_debug(
        CliErrorKind::Operations,
        CliErrorDebug::Message(message.to_string()),
    )
}

struct DryRunTableFixDestination;

impl VerifiedTableFixDestination for DryRunTableFixDestination {
    fn apply_repair_with_key(
        &mut self,
        _authority: &MaintenanceExecutionAuthority,
        _key: &OperationChunkKey,
        _table_name: &str,
        _record_key: &[u8],
        _record: StorageStreamRecord,
        _expected_source_revision: &str,
    ) -> TableFixResult<TableFixApplyOutcome> {
        Err(TableFixError::UnsupportedSource)
    }

    fn verify_repair(
        &self,
        _authority: &MaintenanceExecutionAuthority,
        _key: &OperationChunkKey,
        _table_name: &str,
        _record_key: &[u8],
        _expected: &StorageStreamRecord,
        _expected_source_revision: &str,
    ) -> TableFixResult<bool> {
        Ok(false)
    }
}

enum CliTableFixDestination {
    DryRun,
    Engine(Box<EngineTableFixDestination>),
}

impl VerifiedTableFixDestination for CliTableFixDestination {
    fn apply_repair_with_key(
        &mut self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        expected_source_revision: &str,
    ) -> TableFixResult<TableFixApplyOutcome> {
        match self {
            Self::DryRun => DryRunTableFixDestination.apply_repair_with_key(
                authority,
                key,
                table_name,
                record_key,
                record,
                expected_source_revision,
            ),
            Self::Engine(destination) => destination.apply_repair_with_key(
                authority,
                key,
                table_name,
                record_key,
                record,
                expected_source_revision,
            ),
        }
    }

    fn verify_repair(
        &self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        expected: &StorageStreamRecord,
        expected_source_revision: &str,
    ) -> TableFixResult<bool> {
        match self {
            Self::DryRun => Ok(false),
            Self::Engine(destination) => destination.verify_repair(
                authority,
                key,
                table_name,
                record_key,
                expected,
                expected_source_revision,
            ),
        }
    }
}

struct EngineTableFixDestination {
    manifest: AnalyticsManifest,
    engine: AnalyticsEngine,
    manifest_ready: bool,
}

impl VerifiedTableFixDestination for EngineTableFixDestination {
    fn apply_repair_with_key(
        &mut self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        expected_source_revision: &str,
    ) -> TableFixResult<TableFixApplyOutcome> {
        if !self.manifest_ready {
            self.engine
                .ensure_manifest(&self.manifest)
                .map_err(|error| TableFixError::Destination(error.to_string()))?;
            self.manifest_ready = true;
        }
        let engine_authority = MaintenanceEffectAuthority::try_new(
            authority.execution_id().as_str(),
            authority.fence().get(),
        )
        .map_err(|error| TableFixError::Destination(error.to_string()))?;
        let disposition = self
            .engine
            .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
                manifest: &self.manifest,
                analytics_table_name: table_name,
                record_key,
                record,
                repair_key: key.as_str(),
                source_revision: expected_source_revision,
                authority: &engine_authority,
            })
            .map_err(|error| TableFixError::Destination(error.to_string()))?;
        Ok(match disposition {
            MaintenanceEffectDisposition::Applied => TableFixApplyOutcome::Applied,
            MaintenanceEffectDisposition::AlreadyApplied => TableFixApplyOutcome::AlreadyApplied,
        })
    }

    fn verify_repair(
        &self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        expected: &StorageStreamRecord,
        expected_source_revision: &str,
    ) -> TableFixResult<bool> {
        let engine_authority = MaintenanceEffectAuthority::try_new(
            authority.execution_id().as_str(),
            authority.fence().get(),
        )
        .map_err(|error| TableFixError::Destination(error.to_string()))?;
        self.engine
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &self.manifest,
                analytics_table_name: table_name,
                record_key,
                expected,
                repair_key: key.as_str(),
                source_revision: expected_source_revision,
                authority: &engine_authority,
            })
            .map_err(|error| TableFixError::Destination(error.to_string()))
    }
}

fn read_table_fix_request(path: &Path) -> CliResult<TableFixRequest> {
    let request = serde_json::from_str::<MaintenanceRequest>(fs::read_to_string(path)?.as_str())?;
    let MaintenanceRequest::TableFix(request) = request else {
        return Err(CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(
                "maintenance request kind must be table_fix for fix table".to_string(),
            ),
        ));
    };
    request.validate()?;
    Ok(request)
}
