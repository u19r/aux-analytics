use std::{fs, path::Path};

use analytics_operations::{
    MaintenanceOperations, MaintenanceRequest, OperationActor, OperationEvent, OperationId,
    OperationStore, StoredOperation,
};
use serde::Serialize;

use crate::{
    error::CliResult,
    operations_args::{OperationsCommand, OperationsOutputArg},
};

pub(crate) fn run_operations_command(command: OperationsCommand) -> CliResult<String> {
    match command {
        OperationsCommand::Submit {
            request,
            duckdb,
            output,
        } => {
            let request = read_maintenance_request(request.as_path())?;
            let store = OperationStore::connect_duckdb(duckdb)?;
            let actor = OperationActor::try_new("cli")?;
            let submission = MaintenanceOperations::new(&store).submit(&request, &actor)?;
            match output {
                OperationsOutputArg::Json => pretty_json(&submission),
                OperationsOutputArg::Text => Ok(format!(
                    "maintenance operation {} {:?}",
                    submission.operation_id, submission.outcome
                )),
            }
        }
        OperationsCommand::List { duckdb, output } => {
            let store = OperationStore::connect_duckdb(duckdb)?;
            let operations = store.list_operations()?;
            render_operations_list(&operations, output)
        }
        OperationsCommand::Status {
            duckdb,
            operation_id,
            output,
        } => {
            let store = OperationStore::connect_duckdb(duckdb)?;
            let operation_id = OperationId::try_new(operation_id)?;
            let operation = store.show_operation(&operation_id)?;
            render_operation_status(&operation, output)
        }
        OperationsCommand::Audit {
            duckdb,
            operation_id,
            output,
        } => {
            let store = OperationStore::connect_duckdb(duckdb)?;
            let operation_id = OperationId::try_new(operation_id)?;
            let events = store.audit_events(&operation_id)?;
            render_audit_events(&events, output)
        }
        OperationsCommand::Cancel {
            duckdb,
            operation_id,
            output,
        } => {
            let store = OperationStore::connect_duckdb(duckdb)?;
            let operation_id = OperationId::try_new(operation_id)?;
            let actor = OperationActor::try_new("cli")?;
            MaintenanceOperations::new(&store).request_cancellation(&operation_id, &actor)?;
            match output {
                OperationsOutputArg::Json => pretty_json(&serde_json::json!({
                    "operation_id": operation_id.as_str(),
                    "cancellation_requested": true
                })),
                OperationsOutputArg::Text => Ok(format!(
                    "cancellation requested for operation {}",
                    operation_id.as_str()
                )),
            }
        }
    }
}

fn read_maintenance_request(path: &Path) -> CliResult<MaintenanceRequest> {
    Ok(serde_json::from_str(fs::read_to_string(path)?.as_str())?)
}

fn render_operations_list(
    operations: &[StoredOperation],
    output: OperationsOutputArg,
) -> CliResult<String> {
    match output {
        OperationsOutputArg::Json => pretty_json(operations),
        OperationsOutputArg::Text => {
            if operations.is_empty() {
                return Ok("no operations".to_string());
            }
            Ok(operations
                .iter()
                .map(|operation| {
                    format!(
                        "{} {} {} phase={} tables={}",
                        operation.operation_id,
                        operation.kind,
                        operation.status,
                        operation.phase,
                        operation.target_tables.join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join("\n"))
        }
    }
}

fn render_operation_status(
    operation: &StoredOperation,
    output: OperationsOutputArg,
) -> CliResult<String> {
    match output {
        OperationsOutputArg::Json => pretty_json(operation),
        OperationsOutputArg::Text => {
            let cursor = operation.cursor.as_ref().map_or_else(
                || "none".to_string(),
                |cursor| format!("{}:{}", cursor.label, cursor.position),
            );
            Ok(format!(
                "operation: {}\nkind: {}\nstatus: {}\nphase: {}\ncancellation: {}\ntables: \
                 {}\ncursor: {}",
                operation.operation_id,
                operation.kind,
                operation.status,
                operation.phase,
                operation.cancellation_state,
                operation.target_tables.join(", "),
                cursor
            ))
        }
    }
}

fn render_audit_events(
    events: &[OperationEvent],
    output: OperationsOutputArg,
) -> CliResult<String> {
    match output {
        OperationsOutputArg::Json => pretty_json(events),
        OperationsOutputArg::Text => {
            if events.is_empty() {
                return Ok("no audit events".to_string());
            }
            Ok(events
                .iter()
                .map(|event| {
                    let message = event.message.as_deref().unwrap_or("");
                    format!(
                        "#{} {} status={} phase={} {}",
                        event.event_id, event.kind, event.status, event.phase, message
                    )
                })
                .collect::<Vec<_>>()
                .join("\n"))
        }
    }
}

fn pretty_json<T: Serialize + ?Sized>(value: &T) -> CliResult<String> {
    Ok(serde_json::to_string_pretty(value)?)
}
