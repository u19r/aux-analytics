use analytics_operations::{
    BackfillExecutionRequest, BackfillPlan, BackfillPlanner, BackfillRequest,
    ConservativeBackfillPlanner, LocalBackfillExecutor, LocalBackfillFixture, OperationStore,
};

use crate::{
    backfill_args::{BackfillCommand, BackfillPlanOutputArg},
    backfill_request::{build_backfill_request_from_manifest, read_backfill_request},
    error::CliResult,
};

pub(crate) fn run_backfill_command(command: BackfillCommand) -> CliResult<String> {
    match command {
        BackfillCommand::Plan {
            request,
            manifest,
            config,
            overrides,
            source_kind,
            metadata_reachable,
            dynamodb_pitr_enabled,
            dynamodb_scan_enabled,
            aux_storage_enumeration_enabled,
            aux_storage_deletes_provable,
            target_table,
            estimated_rows,
            chunk_target_rows,
            output,
        } => {
            if let Some(request) = request {
                let request = read_backfill_request(request.as_path())?;
                return plan_backfill_request(&request, output);
            }
            let request = build_backfill_request_from_manifest(
                manifest.as_deref(),
                config.as_deref(),
                overrides.as_slice(),
                source_kind,
                crate::backfill_request::BackfillDiscoveryArgs {
                    metadata_reachable,
                    dynamodb_pitr_enabled,
                    dynamodb_scan_enabled,
                    aux_storage_enumeration_enabled,
                    aux_storage_deletes_provable,
                },
                target_table,
                estimated_rows,
                chunk_target_rows,
            )?;
            plan_backfill_request(&request, output)
        }
        BackfillCommand::Run {
            request,
            fixture,
            operation_store_duckdb,
        }
        | BackfillCommand::Resume {
            request,
            fixture,
            operation_store_duckdb,
        } => {
            let request = read_backfill_execution_request(request.as_path())?;
            let fixture = read_local_backfill_fixture(fixture.as_path())?;
            let operation_store = OperationStore::connect_duckdb(operation_store_duckdb)?;
            let report =
                LocalBackfillExecutor.execute(&operation_store, &request, &fixture, &fixture)?;
            Ok(serde_json::to_string_pretty(&report)?)
        }
    }
}

fn plan_backfill_request(
    request: &BackfillRequest,
    output: BackfillPlanOutputArg,
) -> CliResult<String> {
    let planner = ConservativeBackfillPlanner;
    let plan = planner.plan(request)?;
    match output {
        BackfillPlanOutputArg::Json => Ok(serde_json::to_string_pretty(&plan)?),
        BackfillPlanOutputArg::Text => Ok(render_plan_text(&plan)),
    }
}

fn render_plan_text(plan: &BackfillPlan) -> String {
    let mut lines = vec![
        "Backfill plan".to_string(),
        format!("tables: {}", plan.target_tables.join(", ")),
        format!("snapshot method: {:?}", plan.snapshot_method),
        format!("destination: {:?}", plan.destination_backend),
        format!("dry run: {}", plan.dry_run),
        format!("estimated rows: {}", plan.estimated_rows),
        format!("chunks: {}", plan.chunk_count),
        format!("source impact: {}", plan.source_impact),
        format!("destination impact: {}", plan.destination_impact),
        format!("deletes can be proven: {}", plan.deletes_can_be_proven),
        format!("validation: {:?}", plan.validation_mode),
    ];
    if !plan.validation_steps.is_empty() {
        lines.push("validation steps:".to_string());
        lines.extend(plan.validation_steps.iter().map(|step| format!("- {step}")));
    }
    if !plan.required_permissions.is_empty() {
        lines.push("required permissions:".to_string());
        lines.extend(
            plan.required_permissions
                .iter()
                .map(|permission| format!("- {permission}")),
        );
    }
    if !plan.warnings.is_empty() {
        lines.push("warnings:".to_string());
        lines.extend(plan.warnings.iter().map(|warning| format!("- {warning}")));
    }
    lines.push("operation steps:".to_string());
    lines.extend(
        plan.steps
            .iter()
            .map(|step| format!("{}. {}", step.order, step.name)),
    );
    lines.join("\n")
}

fn read_backfill_execution_request(path: &std::path::Path) -> CliResult<BackfillExecutionRequest> {
    Ok(serde_json::from_str::<BackfillExecutionRequest>(
        std::fs::read_to_string(path)?.as_str(),
    )?)
}

fn read_local_backfill_fixture(path: &std::path::Path) -> CliResult<LocalBackfillFixture> {
    Ok(serde_json::from_str::<LocalBackfillFixture>(
        std::fs::read_to_string(path)?.as_str(),
    )?)
}
