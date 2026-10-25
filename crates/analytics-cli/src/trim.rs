use std::{fs, path::Path};

use analytics_operations::{
    DuckDbTrimRows, MaintenanceExecutionReport, MaintenanceOperations, MaintenanceRequest,
    OperationActor, OperationStore, RateLimitPolicy, TrimRequest,
};

use crate::{
    error::{CliError, CliErrorDebug, CliErrorKind, CliResult},
    trim_args::TrimCommand,
};

pub(crate) fn run_trim_command(command: TrimCommand) -> CliResult<String> {
    match command {
        TrimCommand::Plan {
            request,
            destination_duckdb,
            operation_store_duckdb,
        } => {
            let request = read_trim_request(request.as_path())?;
            if !destination_duckdb.is_file() {
                return Err(CliError::with_debug(
                    CliErrorKind::Operations,
                    CliErrorDebug::Message(format!(
                        "trim destination DuckDB must be an existing file: {}",
                        destination_duckdb.display()
                    )),
                ));
            }
            let database_path = destination_duckdb.to_string_lossy().into_owned();
            let store = OperationStore::connect_duckdb(operation_store_duckdb)?;
            let operations = MaintenanceOperations::new(&store);
            let actor = OperationActor::try_new("cli")?;
            let source = DuckDbTrimRows::new(database_path.clone());
            let mut destination = DuckDbTrimRows::new(database_path);
            let report = operations.execute_trim(
                &request,
                &actor,
                RateLimitPolicy::default(),
                &source,
                &mut destination,
            )?;
            let MaintenanceExecutionReport::Trim(report) = report else {
                unreachable!("trim facade returned a different report kind")
            };
            Ok(serde_json::to_string_pretty(&report)?)
        }
    }
}

fn read_trim_request(path: &Path) -> CliResult<TrimRequest> {
    let request = serde_json::from_str::<MaintenanceRequest>(fs::read_to_string(path)?.as_str())?;
    let MaintenanceRequest::Trim(request) = request else {
        return Err(CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(
                "maintenance request kind must be trim for trim plan".to_string(),
            ),
        ));
    };
    request.validate()?;
    Ok(request)
}
