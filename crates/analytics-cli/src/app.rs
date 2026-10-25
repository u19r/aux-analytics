use std::time::Instant;

use analytics_contract::AnalyticsManifest;
use analytics_engine::{
    AnalyticsEngine, prepare_tenant_structured_query, prepare_unscoped_structured_query,
};
use analytics_storage::RetentionPolicyLookup;
use clap::CommandFactory as _;
use clap_complete::{Shell, generate};
use config::RootConfig;
use serde::Serialize;

use crate::{
    backfill::run_backfill_command,
    check::run_check_command,
    cli::{Cli, Command, RetentionRepairArgs, SqlQueryArgs},
    error::CliResult,
    fix::run_fix_command,
    operations::run_operations_command,
    privacy_fix::run_privacy_fix_command,
    raw_backup::run_raw_backup_command,
    runtime_config::read_structured_query,
    trim::run_trim_command,
};

pub async fn run(cli: Cli) -> CliResult<Option<String>> {
    match cli.command {
        Command::Schema => pretty_json(&schemars::schema_for!(AnalyticsManifest)).map(Some),
        Command::ConfigSchema => pretty_json(&schemars::schema_for!(RootConfig)).map(Some),
        Command::Openapi => pretty_json(&analytics_api::build_openapi_json()).map(Some),
        Command::Init(args) => initialize_tables(args.into()),
        Command::UnscopedSqlQuery(args) => query_unscoped_sql(args),
        Command::UnscopedStructuredQuery(args) => {
            query_structured(args.configuration.into(), args.query, QueryScope::Unscoped)
        }
        Command::TenantQuery(args) => query_structured(
            args.configuration.into(),
            args.query,
            QueryScope::Tenant(args.target_tenant_id),
        ),
        Command::RepairRetention(args) => repair_retention(args).await,
        Command::Operations(command) => run_operations_command(command).map(Some),
        Command::Backfill(command) => run_backfill_command(command).map(Some),
        Command::Check(command) => run_check_command(command).map(Some),
        Command::RawBackup(command) => run_raw_backup_command(command).map(Some),
        Command::Fix(command) => run_fix_command(command).map(Some),
        Command::PrivacyFix(command) => run_privacy_fix_command(command).map(Some),
        Command::Trim(command) => run_trim_command(command).map(Some),
        Command::Completions { shell } => Ok(Some(generate_completions(shell))),
    }
}

fn pretty_json<T: Serialize + ?Sized>(value: &T) -> CliResult<String> {
    Ok(serde_json::to_string_pretty(value)?)
}

fn generate_completions(shell: Shell) -> String {
    let mut command = Cli::command();
    let mut output = Vec::new();
    generate(shell, &mut command, "aux-analytics", &mut output);
    String::from_utf8_lossy(&output).into_owned()
}

fn initialize_tables(request: config::ConfigRequest) -> CliResult<Option<String>> {
    let resolved = config::resolve_config(request)?;
    let storage_backend = resolved.require_storage_backend()?.clone();
    let manifest = resolved.manifest.value;
    let engine = AnalyticsEngine::connect(&storage_backend)?;
    engine.ensure_manifest(&manifest)?;
    Ok(None)
}

fn query_unscoped_sql(args: SqlQueryArgs) -> CliResult<Option<String>> {
    let request = config::ConfigRequest::cli_without_manifest(
        args.config,
        args.overrides,
        (&args.backend).into(),
    );
    let sql = args.sql;
    let resolved = config::resolve_config(request)?;
    let storage_backend = resolved.require_storage_backend()?.clone();
    let engine = AnalyticsEngine::connect(&storage_backend)?;
    let rows = engine.query_unscoped_sql_json(sql.as_str())?;
    pretty_json(&rows).map(Some)
}

enum QueryScope {
    Unscoped,
    Tenant(String),
}

fn query_structured(
    request: config::ConfigRequest,
    query: std::path::PathBuf,
    scope: QueryScope,
) -> CliResult<Option<String>> {
    let started = Instant::now();
    let resolved = config::resolve_config(request)?;
    let storage_backend = resolved.require_storage_backend()?.clone();
    let manifest = resolved.manifest.value;
    let structured_query = read_structured_query(query.as_path())?;
    let engine = AnalyticsEngine::connect(&storage_backend)?;
    let prepared = match scope {
        QueryScope::Unscoped => prepare_unscoped_structured_query(&manifest, &structured_query)?,
        QueryScope::Tenant(tenant_id) => {
            prepare_tenant_structured_query(&manifest, &structured_query, &tenant_id)?
        }
    };
    let rows = engine.query_prepared_structured_json(&prepared)?;
    pretty_json(&analytics_api::QueryResponse::new(
        rows,
        prepared.metadata(),
        started,
    ))
    .map(Some)
}

async fn repair_retention(repair: RetentionRepairArgs) -> CliResult<Option<String>> {
    let RetentionRepairArgs {
        configuration,
        table,
        tenant_id,
        dry_run,
        batch_size,
    } = repair;
    let request = configuration.into();

    let resolved = config::resolve_config(request)?;
    let storage_backend = resolved.require_storage_backend()?.clone();
    let manifest = resolved.manifest.value;
    let table_config = get_retention_table_config(&resolved.root, &table)?;
    let lookup = RetentionPolicyLookup::from_config(&table_config.tenant_policy).await?;
    let period_ms = lookup.lookup_period_ms(tenant_id.as_str()).await?;
    let engine = AnalyticsEngine::connect(&storage_backend)?;
    engine.ensure_manifest(&manifest)?;
    engine.ensure_retention_columns(table.as_str())?;
    let rows_repaired = if dry_run {
        0
    } else {
        engine.repair_missing_retention(
            table.as_str(),
            tenant_id.as_str(),
            period_ms,
            batch_size.unwrap_or(500),
        )?
    };
    pretty_json(&serde_json::json!({
        "table": table,
        "tenant_id": tenant_id,
        "period_ms": period_ms,
        "dry_run": dry_run,
        "rows_repaired": rows_repaired
    }))
    .map(Some)
}

fn get_retention_table_config<'a>(
    root: &'a RootConfig,
    table: &str,
) -> CliResult<&'a config::AnalyticsRetentionTableConfig> {
    root.analytics
        .retention
        .tables
        .iter()
        .find(|candidate| candidate.analytics_table_name == table)
        .ok_or_else(|| {
            crate::error::CliError::with_debug(
                crate::error::CliErrorKind::Config,
                crate::error::CliErrorDebug::Message(format!(
                    "retention config not found for analytics table {table}"
                )),
            )
        })
}
