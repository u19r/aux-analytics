use std::{fs, path::Path};

use analytics_contract::{AnalyticsManifest, StorageStreamRecord, read_manifest};
use analytics_engine::AnalyticsEngine;
use analytics_operations::{
    FilesystemRawBackupStore, OperationActor, OperationEventKind, OperationKind, OperationPhase,
    OperationRequest, OperationStatus, OperationStore, RateLimitPolicy, TableFixDestination,
    TableFixError, TableFixExecutor, TableFixRequest, TableFixResult, TableFixSource,
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
            let store = FilesystemRawBackupStore::new(raw_backup_root);
            let indexes = raw_backup_indexes(&store, &request)?;
            let mut destination = table_fix_destination(
                &request,
                manifest.as_deref(),
                destination_duckdb.as_deref(),
            )?;
            let report = TableFixExecutor::new(&store).execute_raw_backup(
                &request,
                &indexes,
                &mut destination,
            )?;
            let operation_store = OperationStore::connect_duckdb(operation_store_duckdb)?;
            create_or_resume_table_fix_operation(&operation_store, &request)?;
            operation_store.save_table_fix_report(&request.operation_id, &report)?;
            operation_store.transition_with_counts(
                &request.operation_id,
                OperationPhase::Completed,
                OperationStatus::Succeeded,
                None,
                OperationEventKind::Succeeded,
                report.operation_counts(),
                Some("table fix completed"),
            )?;
            Ok(serde_json::to_string_pretty(&report)?)
        }
    }
}

fn table_fix_destination(
    request: &TableFixRequest,
    manifest: Option<&Path>,
    destination_duckdb: Option<&Path>,
) -> CliResult<CliTableFixDestination> {
    if request.dry_run {
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
    let manifest = read_manifest(manifest_path.to_string_lossy().as_ref())?;
    let engine = AnalyticsEngine::connect_duckdb(destination_duckdb.to_string_lossy().as_ref())?;
    engine.ensure_manifest(&manifest)?;
    Ok(CliTableFixDestination::Engine(Box::new(
        EngineTableFixDestination { manifest, engine },
    )))
}

fn table_fix_apply_error(message: &str) -> CliError {
    CliError::with_debug(
        CliErrorKind::Operations,
        CliErrorDebug::Message(message.to_string()),
    )
}

struct DryRunTableFixDestination;

impl TableFixDestination for DryRunTableFixDestination {
    fn apply_repair(
        &mut self,
        _table_name: &str,
        _record_key: &[u8],
        _record: StorageStreamRecord,
    ) -> TableFixResult<()> {
        Err(TableFixError::UnsupportedSource)
    }
}

enum CliTableFixDestination {
    DryRun,
    Engine(Box<EngineTableFixDestination>),
}

impl TableFixDestination for CliTableFixDestination {
    fn apply_repair(
        &mut self,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
    ) -> TableFixResult<()> {
        match self {
            Self::DryRun => DryRunTableFixDestination.apply_repair(table_name, record_key, record),
            Self::Engine(destination) => destination.apply_repair(table_name, record_key, record),
        }
    }
}

struct EngineTableFixDestination {
    manifest: AnalyticsManifest,
    engine: AnalyticsEngine,
}

impl TableFixDestination for EngineTableFixDestination {
    fn apply_repair(
        &mut self,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
    ) -> TableFixResult<()> {
        self.engine
            .ingest_stream_record(&self.manifest, table_name, record_key, record)
            .map_err(|error| TableFixError::Destination(error.to_string()))?;
        Ok(())
    }
}

fn read_table_fix_request(path: &Path) -> CliResult<TableFixRequest> {
    let request = serde_json::from_str::<TableFixRequest>(fs::read_to_string(path)?.as_str())?;
    request.validate()?;
    Ok(request)
}

fn raw_backup_indexes(
    store: &FilesystemRawBackupStore,
    request: &TableFixRequest,
) -> CliResult<Vec<analytics_operations::RawBackupObjectIndex>> {
    let TableFixSource::RawBackup { object_ids } = &request.source else {
        return Err(CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(
                "table fix CLI currently supports raw_backup source only".into(),
            ),
        ));
    };
    object_ids
        .iter()
        .map(|object_id| store.read_index(object_id).map_err(Into::into))
        .collect()
}

fn create_or_resume_table_fix_operation(
    store: &OperationStore,
    request: &TableFixRequest,
) -> CliResult<()> {
    match store.create_operation(&OperationRequest {
        operation_id: request.operation_id.clone(),
        kind: OperationKind::TableFix,
        actor: OperationActor::new("cli")?,
        target_tables: vec![request.table_name.clone()],
        dry_run: request.dry_run,
        rate_limit: RateLimitPolicy::default(),
        payload: serde_json::to_value(request)?,
    }) {
        Ok(()) | Err(analytics_operations::OperationStoreError::DuplicateOperation(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
