use std::{fs, path::Path};

use analytics_contract::{TableRegistration, read_manifest};
use analytics_operations::{
    CheckComparisonStrategy, CheckOutputFormat, CheckReport, CheckRequest, CheckRow,
    CheckRowProjection, CheckRowSource, DuckDbAnalyticalCheckRows, DuckDbCheckRows,
    LocalCheckDataset, LocalCheckExecutor, LocalFixtureCheckRows, OperationActor, OperationId,
    OperationStore, RateLimitPolicy,
};

use crate::{
    check_args::{CheckCommand, CheckOutputArg, CheckStrategyArg},
    error::{CliError, CliErrorDebug, CliErrorKind, CliResult},
};

pub(crate) fn run_check_command(command: CheckCommand) -> CliResult<String> {
    match command {
        CheckCommand::Run {
            operation_store_duckdb,
            operation_id,
            target_table,
            manifest,
            source_rows,
            destination_rows,
            source_duckdb,
            source_duckdb_check_rows_table,
            destination_duckdb,
            destination_duckdb_check_rows_table,
            source_duckdb_analytical,
            destination_duckdb_analytical,
            strategy,
            sample_limit,
            privacy_policy_version,
            output,
        } => {
            let store = OperationStore::connect_duckdb(operation_store_duckdb)?;
            let request = CheckRequest {
                operation_id: OperationId::new(operation_id)?,
                actor: OperationActor::new("cli")?,
                target_tables: target_table.clone(),
                strategy: strategy.into(),
                sample_limit,
                privacy_policy_version,
                output: output.into(),
                rate_limit: RateLimitPolicy::default(),
            };
            let source = check_row_source(
                source_rows.as_deref(),
                source_duckdb.as_deref(),
                source_duckdb_check_rows_table.as_deref(),
                source_duckdb_analytical.as_deref(),
                manifest.as_deref(),
                &target_table,
                "source",
            )?;
            let destination = check_row_source(
                destination_rows.as_deref(),
                destination_duckdb.as_deref(),
                destination_duckdb_check_rows_table.as_deref(),
                destination_duckdb_analytical.as_deref(),
                manifest.as_deref(),
                &target_table,
                "destination",
            )?;
            let report = LocalCheckExecutor.execute(&store, &request, &source, &destination)?;
            render_check_report(&report, output)
        }
    }
}

fn check_row_source(
    rows_path: Option<&Path>,
    duckdb_path: Option<&str>,
    duckdb_check_rows_table: Option<&str>,
    duckdb_analytical_path: Option<&str>,
    manifest_path: Option<&Path>,
    target_tables: &[String],
    label: &str,
) -> CliResult<Box<dyn CheckRowSource>> {
    match (
        rows_path,
        duckdb_path,
        duckdb_check_rows_table,
        duckdb_analytical_path,
    ) {
        (Some(path), None, None, None) => Ok(Box::new(LocalFixtureCheckRows::new(
            read_check_dataset(path)?,
        ))),
        (None, Some(path), Some(table), None) => Ok(Box::new(DuckDbCheckRows::new(path, table)?)),
        (None, None, None, Some(path)) => {
            Ok(Box::new(DuckDbAnalyticalCheckRows::new_with_projection(
                path,
                check_projection_from_manifest(manifest_path, target_tables)?,
            )?))
        }
        (None, None, None, None) => Err(CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(format!(
                "{label} check input requires --{label}-rows or both --{label}-duckdb and \
                 --{label}-duckdb-check-rows-table or --{label}-duckdb-analytical with --manifest"
            )),
        )),
        _ => Err(CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(format!(
                "{label} check input must use exactly one source: local JSON rows, DuckDB check \
                 rows, or manifest-backed DuckDB analytical rows"
            )),
        )),
    }
}

fn check_projection_from_manifest(
    manifest_path: Option<&Path>,
    target_tables: &[String],
) -> CliResult<CheckRowProjection> {
    if target_tables.len() != 1 {
        return Err(CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(
                "manifest-backed DuckDB analytical check input supports exactly one target table"
                    .to_string(),
            ),
        ));
    }
    let manifest_path = manifest_path.ok_or_else(|| {
        CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(
                "--manifest is required for manifest-backed DuckDB analytical check input"
                    .to_string(),
            ),
        )
    })?;
    let Some(target_table) = target_tables.first() else {
        return Err(CliError::with_debug(
            CliErrorKind::Operations,
            CliErrorDebug::Message(
                "manifest-backed DuckDB analytical check input requires one target table"
                    .to_string(),
            ),
        ));
    };
    let manifest = read_manifest(manifest_path.to_string_lossy().as_ref())?;
    let table = manifest
        .tables
        .iter()
        .find(|table| table.analytics_table_name == *target_table)
        .ok_or_else(|| {
            CliError::with_debug(
                CliErrorKind::Operations,
                CliErrorDebug::Message(format!("target table {target_table} is not in manifest")),
            )
        })?;
    check_projection_for_table(table)
}

fn check_projection_for_table(table: &TableRegistration) -> CliResult<CheckRowProjection> {
    CheckRowProjection::from_table_registration(table).map_err(Into::into)
}

fn read_check_dataset(path: &Path) -> CliResult<LocalCheckDataset> {
    let contents = fs::read_to_string(path)?;
    serde_json::from_str::<LocalCheckDataset>(contents.as_str())
        .or_else(|_| {
            serde_json::from_str::<Vec<CheckRow>>(contents.as_str()).map(LocalCheckDataset::new)
        })
        .map_err(Into::into)
}

fn render_check_report(report: &CheckReport, output: CheckOutputArg) -> CliResult<String> {
    match output {
        CheckOutputArg::Json => Ok(serde_json::to_string_pretty(report)?),
        CheckOutputArg::Text => Ok(format!(
            "check report: {}\noutcome: {:?}\nstrategy: {:?}\nchecked rows: {}\nsource rows: \
             {}\ndestination rows: {}\nmissing keys: {}\nextra keys: {}\nmismatched rows: \
             {}\nstale rows: {}\nprivacy violations: {}",
            report.operation_id,
            report.outcome,
            report.strategy,
            report.metrics.checked_rows,
            report.metrics.source_rows,
            report.metrics.destination_rows,
            report.metrics.missing_keys,
            report.metrics.extra_keys,
            report.metrics.mismatched_rows,
            report.metrics.stale_rows,
            report.metrics.privacy_violations
        )),
    }
}

impl From<CheckStrategyArg> for CheckComparisonStrategy {
    fn from(value: CheckStrategyArg) -> Self {
        match value {
            CheckStrategyArg::CountsOnly => Self::CountsOnly,
            CheckStrategyArg::KeySet => Self::KeySet,
            CheckStrategyArg::ChunkHashes => Self::ChunkHashes,
            CheckStrategyArg::FullRows => Self::FullRows,
        }
    }
}

impl From<CheckOutputArg> for CheckOutputFormat {
    fn from(value: CheckOutputArg) -> Self {
        match value {
            CheckOutputArg::Json => Self::Json,
            CheckOutputArg::Text => Self::Text,
        }
    }
}
