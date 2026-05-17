use analytics_contract::{AnalyticsManifest, read_manifest};
use analytics_engine::AnalyticsEngine;
use analytics_storage::RetentionPolicyLookup;
use clap::CommandFactory as _;
use clap_complete::{Shell, generate};
use config::RootConfig;

use crate::{
    backfill::run_backfill_command,
    check::run_check_command,
    cli::{Cli, Command},
    error::CliResult,
    fix::run_fix_command,
    operations::run_operations_command,
    privacy_fix::run_privacy_fix_command,
    raw_backup::run_raw_backup_command,
    runtime_config::{
        load_config, read_structured_query, resolve_manifest_path, validate_source_config,
    },
    trim::run_trim_command,
};

#[allow(clippy::too_many_lines)]
pub async fn run(cli: Cli) -> CliResult<Option<String>> {
    match cli.command {
        Command::Schema => pretty_json_value(serde_json::to_value(schemars::schema_for!(
            AnalyticsManifest
        ))?)
        .map(Some),
        Command::ConfigSchema => {
            pretty_json_value(serde_json::to_value(schemars::schema_for!(RootConfig))?).map(Some)
        }
        Command::Openapi => pretty_json_value(analytics_api::build_openapi_json()).map(Some),
        Command::Init {
            manifest,
            config,
            overrides,
            backend,
        } => {
            let config = load_config(config.as_deref(), overrides.as_slice())?;
            let manifest_path = resolve_manifest_path(manifest.as_deref(), &config.root)?;
            let manifest = read_manifest(manifest_path.as_str())?;
            validate_source_config(&config.root.analytics.source)?;
            let storage_backend =
                config::resolve_storage_backend(&(&backend).into(), &config.root)?;
            let engine = AnalyticsEngine::connect(&storage_backend)?;
            engine.ensure_manifest(&manifest)?;
            Ok(None)
        }
        Command::UnscopedSqlQuery {
            sql,
            config,
            overrides,
            backend,
        } => {
            let config = load_config(config.as_deref(), overrides.as_slice())?;
            let storage_backend =
                config::resolve_storage_backend(&(&backend).into(), &config.root)?;
            let engine = AnalyticsEngine::connect(&storage_backend)?;
            pretty_json_value(serde_json::Value::Array(
                engine.query_unscoped_sql_json(sql.as_str())?,
            ))
            .map(Some)
        }
        Command::UnscopedStructuredQuery {
            query,
            manifest,
            config,
            overrides,
            backend,
        } => {
            let config = load_config(config.as_deref(), overrides.as_slice())?;
            let manifest_path = resolve_manifest_path(manifest.as_deref(), &config.root)?;
            let manifest = read_manifest(manifest_path.as_str())?;
            let structured_query = read_structured_query(query.as_path())?;
            let storage_backend =
                config::resolve_storage_backend(&(&backend).into(), &config.root)?;
            let engine = AnalyticsEngine::connect(&storage_backend)?;
            pretty_json_value(serde_json::Value::Array(
                engine.query_unscoped_structured_json(&manifest, &structured_query)?,
            ))
            .map(Some)
        }
        Command::TenantQuery {
            target_tenant_id,
            query,
            manifest,
            config,
            overrides,
            backend,
        } => {
            let config = load_config(config.as_deref(), overrides.as_slice())?;
            let manifest_path = resolve_manifest_path(manifest.as_deref(), &config.root)?;
            let manifest = read_manifest(manifest_path.as_str())?;
            let structured_query = read_structured_query(query.as_path())?;
            let storage_backend =
                config::resolve_storage_backend(&(&backend).into(), &config.root)?;
            let engine = AnalyticsEngine::connect(&storage_backend)?;
            pretty_json_value(serde_json::Value::Array(
                engine.query_tenant_structured_json(
                    &manifest,
                    &structured_query,
                    target_tenant_id.as_str(),
                )?,
            ))
            .map(Some)
        }
        Command::RepairRetention {
            manifest,
            config,
            overrides,
            backend,
            table,
            tenant_id,
            dry_run,
            batch_size,
        } => {
            let config = load_config(config.as_deref(), overrides.as_slice())?;
            config::validate_retention_config(&config.root.analytics.retention)?;
            let manifest_path = resolve_manifest_path(manifest.as_deref(), &config.root)?;
            let manifest = read_manifest(manifest_path.as_str())?;
            let table_config = config
                .root
                .analytics
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
                })?;
            let lookup = RetentionPolicyLookup::from_config(&table_config.tenant_policy).await?;
            let period_ms = lookup.lookup_period_ms(tenant_id.as_str()).await?;
            let storage_backend =
                config::resolve_storage_backend(&(&backend).into(), &config.root)?;
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
            pretty_json_value(serde_json::json!({
                "table": table,
                "tenant_id": tenant_id,
                "period_ms": period_ms,
                "dry_run": dry_run,
                "rows_repaired": rows_repaired
            }))
            .map(Some)
        }
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

#[allow(clippy::needless_pass_by_value)]
fn pretty_json_value(value: serde_json::Value) -> CliResult<String> {
    Ok(serde_json::to_string_pretty(&value)?)
}

fn generate_completions(shell: Shell) -> String {
    let mut command = Cli::command();
    let mut output = Vec::new();
    generate(shell, &mut command, "aux-analytics", &mut output);
    String::from_utf8_lossy(&output).into_owned()
}
