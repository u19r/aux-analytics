use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use clap_complete::Shell;

use crate::{
    backend_args::BackendArgs, backfill::args::BackfillCommand, check_args::CheckCommand,
    constants::ROOT_LONG_ABOUT, fix_args::FixCommand, operations_args::OperationsCommand,
    privacy_fix_args::PrivacyFixCommand, raw_backup_args::RawBackupCommand, trim_args::TrimCommand,
};

#[derive(Debug, Parser)]
#[command(name = "aux-analytics")]
#[command(
    about = "Run and operate Aux Analytics",
    long_about = ROOT_LONG_ABOUT,
    after_help = "Use `aux-analytics <COMMAND> --help` for command-specific examples and flags."
)]
pub struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    #[command(about = "Print the analytics manifest JSON Schema")]
    Schema,
    #[command(about = "Print the runtime configuration JSON Schema")]
    ConfigSchema,
    #[command(about = "Print the OpenAPI document for the HTTP service")]
    Openapi,
    #[command(
        about = "Initialize analytical tables from a manifest",
        long_about = "Create or migrate the destination tables declared by an analytics \
                      manifest.\n\nThe manifest path can be supplied directly with --manifest or \
                      resolved from --config. Backend flags override the configured destination \
                      for this run."
    )]
    Init(ManifestArgs),
    #[command(name = "unscoped-sql-query")]
    #[command(
        about = "Run an explicit unscoped read-only SQL query",
        long_about = "Run an administrative unscoped SQL query against the analytics \
                      database.\n\nThis command is intended for local/operator use. Public HTTP \
                      raw SQL remains restricted because arbitrary SQL cannot prove tenant \
                      isolation."
    )]
    UnscopedSqlQuery(SqlQueryArgs),
    #[command(name = "unscoped-structured-query")]
    #[command(
        about = "Run an unscoped structured query from JSON",
        long_about = "Run a structured query without adding a tenant predicate. Use this only for \
                      explicitly unscoped/admin data. For tenant-isolated reads, use tenant-query."
    )]
    UnscopedStructuredQuery(StructuredQueryArgs),
    #[command(name = "tenant-query")]
    #[command(
        about = "Run a tenant-scoped structured query from JSON",
        long_about = "Run a structured query with an engine-owned tenant predicate. This is the \
                      preferred CLI query path for callers that should only read one tenant."
    )]
    TenantQuery(TenantQueryArgs),
    #[command(
        name = "repair-retention",
        about = "Repair missing retention metadata for one tenant/table",
        long_about = "Populate missing static retention metadata for rows in one analytical table \
                      and tenant. Use --dry-run to inspect the resolved retention period without \
                      mutating rows."
    )]
    RepairRetention(RetentionRepairArgs),
    #[command(
        subcommand,
        about = "Inspect and control durable operations",
        long_about = "Inspect and control durable Day 2 operations stored in the local operation \
                      database. Use these commands to list known operations, inspect current \
                      state, review audit events, or request cancellation."
    )]
    Operations(OperationsCommand),
    #[command(
        subcommand,
        about = "Plan and run backfill workflows",
        long_about = "Plan backfills for adopting existing source data into analytics. Planning \
                      is dry-run only and reports the selected source strategy, chunking, and \
                      safety constraints before any execution path is used."
    )]
    Backfill(BackfillCommand),
    #[command(
        subcommand,
        about = "Run reconciliation checks",
        long_about = "Run read-only reconciliation checks that compare source truth with \
                      destination analytical rows, persist the report under an operation id, and \
                      print JSON or human-readable output."
    )]
    Check(CheckCommand),
    #[command(
        name = "backup",
        subcommand,
        about = "Inspect and replay raw backup objects",
        long_about = "Validate local raw backup objects used for replay and repair workflows. \
                      Production object stores and raw-backup publication are intentionally not \
                      wired through this CLI; this command exercises local filesystem dry-run \
                      replay validation."
    )]
    RawBackup(RawBackupCommand),
    #[command(
        name = "fix",
        subcommand,
        about = "Run table repair workflows",
        long_about = "Run bounded repair workflows from reviewed request JSON. Table fixes read \
                      local raw backup truth, persist the report in durable operation state, and \
                      support dry-run or confirmed apply mode against a local DuckDB destination."
    )]
    Fix(FixCommand),
    #[command(
        name = "privacy-fix",
        subcommand,
        about = "Remediate private data from backup and analytical storage",
        long_about = "Run privacy remediation workflows with durable operation evidence. Current \
                      CLI support covers local raw-backup dry-run inspection; apply requests fail \
                      closed because local raw-backup destinations do not support atomic \
                      maintenance fencing. Analytical table remediation is available through the \
                      engine API."
    )]
    PrivacyFix(PrivacyFixCommand),
    #[command(
        name = "trim",
        subcommand,
        about = "Run destructive trim workflows",
        long_about = "Plan destructive row/table expiration workflows from reviewed request JSON. \
                      The CLI uses a bounded local DuckDB source and destination, persists the \
                      report in durable operation state, and requires a typed confirmation token \
                      for apply mode."
    )]
    Trim(TrimCommand),
    #[command(
        about = "Generate a shell completion script",
        long_about = "Generate a shell completion script for aux-analytics. Redirect the output \
                      to a file on your shell completion path, then reload your shell's \
                      completion system.\n\nExample for zsh:\n  mkdir -p ~/.zfunc\n  \
                      aux-analytics completions zsh > ~/.zfunc/_aux-analytics\n  echo \
                      'fpath=(~/.zfunc $fpath)' >> ~/.zshrc\n  echo 'autoload -Uz compinit && \
                      compinit' >> ~/.zshrc"
    )]
    Completions {
        #[arg(value_enum, help = "Shell to generate completions for")]
        shell: Shell,
    },
}

#[derive(Debug, Args)]
pub(crate) struct ManifestArgs {
    #[arg(long, value_name = "PATH", help = "Path to analytics manifest JSON")]
    pub(crate) manifest: Option<String>,
    #[arg(long, value_name = "PATH", help = "Path to runtime configuration JSON")]
    pub(crate) config: Option<PathBuf>,
    #[arg(
        long = "overrides",
        value_name = "PATH=VALUE",
        help = "Configuration override, repeatable; example: analytics.manifest_path=manifest.json"
    )]
    pub(crate) overrides: Vec<String>,
    #[command(flatten)]
    pub(crate) backend: BackendArgs,
}

#[derive(Debug, Args)]
pub(crate) struct SqlQueryArgs {
    #[arg(
        long,
        value_name = "SQL",
        help = "Single read-only SELECT/WITH SQL statement"
    )]
    pub(crate) sql: String,
    #[arg(long, value_name = "PATH", help = "Path to runtime configuration JSON")]
    pub(crate) config: Option<PathBuf>,
    #[arg(
        long = "overrides",
        value_name = "PATH=VALUE",
        help = "Configuration override, repeatable; example: analytics.manifest_path=manifest.json"
    )]
    pub(crate) overrides: Vec<String>,
    #[command(flatten)]
    pub(crate) backend: BackendArgs,
}

#[derive(Debug, Args)]
pub(crate) struct StructuredQueryArgs {
    #[arg(long, value_name = "PATH", help = "Path to structured query JSON")]
    pub(crate) query: PathBuf,
    #[command(flatten)]
    pub(crate) configuration: ManifestArgs,
}

#[derive(Debug, Args)]
pub(crate) struct TenantQueryArgs {
    #[arg(
        long,
        value_name = "TENANT_ID",
        help = "Authorized tenant id to inject into the query"
    )]
    pub(crate) target_tenant_id: String,
    #[arg(long, value_name = "PATH", help = "Path to structured query JSON")]
    pub(crate) query: PathBuf,
    #[command(flatten)]
    pub(crate) configuration: ManifestArgs,
}

#[derive(Debug, Args)]
pub(crate) struct RetentionRepairArgs {
    #[command(flatten)]
    pub(crate) configuration: ManifestArgs,
    #[arg(long, value_name = "TABLE", help = "Analytics table name to repair")]
    pub(crate) table: String,
    #[arg(
        long,
        value_name = "TENANT_ID",
        help = "Tenant id whose rows should be repaired"
    )]
    pub(crate) tenant_id: String,
    #[arg(long, help = "Report what would be repaired without updating rows")]
    pub(crate) dry_run: bool,
    #[arg(
        long,
        value_name = "ROWS",
        help = "Maximum rows to repair in this invocation"
    )]
    pub(crate) batch_size: Option<u64>,
}

impl From<ManifestArgs> for config::ConfigRequest {
    fn from(args: ManifestArgs) -> Self {
        Self::cli(
            args.config,
            args.manifest,
            args.overrides,
            (&args.backend).into(),
        )
    }
}
