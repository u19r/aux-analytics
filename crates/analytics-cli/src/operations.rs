use analytics_operations::{OperationEvent, OperationId, OperationStore, StoredOperation};

use crate::{
    error::CliResult,
    operations_args::{OperationsCommand, OperationsOutputArg},
};

pub(crate) fn run_operations_command(command: OperationsCommand) -> CliResult<String> {
    match command {
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
            let operation_id = OperationId::new(operation_id)?;
            let operation = store.show_operation(&operation_id)?;
            render_operation_status(&operation, output)
        }
        OperationsCommand::Audit {
            duckdb,
            operation_id,
            output,
        } => {
            let store = OperationStore::connect_duckdb(duckdb)?;
            let operation_id = OperationId::new(operation_id)?;
            let events = store.audit_events(&operation_id)?;
            render_audit_events(&events, output)
        }
        OperationsCommand::Cancel {
            duckdb,
            operation_id,
            output,
        } => {
            let store = OperationStore::connect_duckdb(duckdb)?;
            let operation_id = OperationId::new(operation_id)?;
            store.request_cancellation(&operation_id)?;
            match output {
                OperationsOutputArg::Json => pretty_json_value(&serde_json::json!({
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

fn render_operations_list(
    operations: &[StoredOperation],
    output: OperationsOutputArg,
) -> CliResult<String> {
    match output {
        OperationsOutputArg::Json => pretty_json_value(&serde_json::to_value(operations)?),
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
        OperationsOutputArg::Json => pretty_json_value(&serde_json::to_value(operation)?),
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
        OperationsOutputArg::Json => pretty_json_value(&serde_json::to_value(events)?),
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

fn pretty_json_value(value: &serde_json::Value) -> CliResult<String> {
    Ok(serde_json::to_string_pretty(value)?)
}
