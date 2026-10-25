use std::fs;

use analytics_operations::{
    FilesystemRawBackupStore, MaintenanceExecutionReport, MaintenanceOperations,
    MaintenanceRequest, OperationActor, OperationStore, PrivacyFixRequest, RateLimitPolicy,
};

use crate::{
    error::{CliError, CliErrorDebug, CliErrorKind, CliResult},
    privacy_fix_args::PrivacyFixCommand,
};

pub(crate) fn run_privacy_fix_command(command: PrivacyFixCommand) -> CliResult<String> {
    match command {
        PrivacyFixCommand::RawBackup {
            request,
            operation_store_duckdb,
        } => {
            let request = read_privacy_fix_request(request.as_path())?;
            let source_store = FilesystemRawBackupStore::new(&request.source_backup_prefix);
            let clean_store = FilesystemRawBackupStore::new(&request.clean_target_prefix);
            let operation_store = OperationStore::connect_duckdb(operation_store_duckdb)?;
            let operations = MaintenanceOperations::new(&operation_store);
            let actor = OperationActor::try_new("cli")?;
            let report = operations.execute_privacy_fix_raw_backup(
                &request,
                &actor,
                RateLimitPolicy::default(),
                &[],
                &source_store,
                &clean_store,
            )?;
            let MaintenanceExecutionReport::PrivacyFix(report) = report else {
                unreachable!("privacy facade returned a different report kind")
            };
            Ok(serde_json::to_string_pretty(&report)?)
        }
    }
}

fn read_privacy_fix_request(path: &std::path::Path) -> CliResult<PrivacyFixRequest> {
    let request = serde_json::from_str::<MaintenanceRequest>(fs::read_to_string(path)?.as_str())?;
    let MaintenanceRequest::PrivacyFix(request) = request else {
        return Err(cli_message(
            "maintenance request kind must be privacy_fix for privacy-fix raw-backup",
        ));
    };
    request.validate()?;
    Ok(request)
}

fn cli_message(message: impl Into<String>) -> CliError {
    CliError::with_debug(
        CliErrorKind::Operations,
        CliErrorDebug::Message(message.into()),
    )
}
