use std::{fs, path::Path};

use analytics_operations::{
    OperationActor, OperationEventKind, OperationKind, OperationPhase, OperationRequest,
    OperationStatus, OperationStore, RateLimitPolicy, TrimCandidateSource, TrimExecutor,
    TrimRequest, TrimResult, TrimTarget,
};

use crate::{
    error::{CliError, CliErrorDebug, CliErrorKind, CliResult},
    trim_args::TrimCommand,
};

pub(crate) fn run_trim_command(command: TrimCommand) -> CliResult<String> {
    match command {
        TrimCommand::Plan {
            request,
            candidate_rows,
            destination_duckdb,
            operation_store_duckdb,
        } => {
            let request = read_trim_request(request.as_path())?;
            let report = trim_report(&request, candidate_rows, destination_duckdb.as_deref())?;
            let store = OperationStore::connect_duckdb(operation_store_duckdb)?;
            create_or_resume_trim_operation(&store, &request)?;
            store.save_trim_report(&request.operation_id, &report)?;
            store.transition_with_counts(
                &request.operation_id,
                OperationPhase::Completed,
                OperationStatus::Succeeded,
                None,
                OperationEventKind::Succeeded,
                report.operation_counts(),
                Some(if request.dry_run {
                    "trim dry-run completed"
                } else {
                    "trim apply completed"
                }),
            )?;
            Ok(serde_json::to_string_pretty(&report)?)
        }
    }
}

fn trim_report(
    request: &TrimRequest,
    candidate_rows: Option<u64>,
    destination_duckdb: Option<&Path>,
) -> CliResult<analytics_operations::TrimReport> {
    if let Some(destination_duckdb) = destination_duckdb {
        let database_path = destination_duckdb.to_string_lossy().to_string();
        let source = analytics_operations::DuckDbTrimRows::new(database_path.clone());
        if request.dry_run {
            return Ok(TrimExecutor.execute_dry_run(request, &source)?);
        }
        let mut destination = analytics_operations::DuckDbTrimRows::new(database_path);
        return Ok(TrimExecutor.execute_apply(request, &source, &mut destination)?);
    }
    if !request.dry_run {
        return Err(CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(
                "trim CLI apply requires --destination-duckdb for bounded deletion execution"
                    .to_string(),
            ),
        ));
    }
    let candidate_rows = candidate_rows.ok_or_else(|| {
        CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(
                "trim dry-run requires --candidate-rows or --destination-duckdb".to_string(),
            ),
        )
    })?;
    Ok(TrimExecutor.execute_dry_run(request, &FixedTrimCandidateSource { candidate_rows })?)
}

struct FixedTrimCandidateSource {
    candidate_rows: u64,
}

impl TrimCandidateSource for FixedTrimCandidateSource {
    fn count_candidates(&self, _table_name: &str, _target: &TrimTarget) -> TrimResult<u64> {
        Ok(self.candidate_rows)
    }
}

fn read_trim_request(path: &Path) -> CliResult<TrimRequest> {
    let request = serde_json::from_str::<TrimRequest>(fs::read_to_string(path)?.as_str())?;
    request.validate()?;
    Ok(request)
}

fn create_or_resume_trim_operation(store: &OperationStore, request: &TrimRequest) -> CliResult<()> {
    match store.create_operation(&OperationRequest {
        operation_id: request.operation_id.clone(),
        kind: OperationKind::Trim,
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
