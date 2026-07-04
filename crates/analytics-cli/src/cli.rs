use std::path::PathBuf;

use clap::{Parser, Subcommand};
use clap_complete::Shell;

use crate::{
    backend_args::BackendArgs, backfill_args::BackfillCommand, check_args::CheckCommand,
    fix_args::FixCommand, operations_args::OperationsCommand, privacy_fix_args::PrivacyFixCommand,
    raw_backup_args::RawBackupCommand, trim_args::TrimCommand,
};

const ROOT_LONG_ABOUT: &str = r"Run and operate Aux Analytics.

Aux Analytics ingests source storage stream records into DuckDB-compatible analytical tables,
queries those tables through an embedded engine, and exposes durable Day 2 operation controls for
planning and monitoring backfills.

Common workflows:
  aux-analytics schema
      Print the analytics manifest JSON Schema.

  aux-analytics config-schema
      Print the runtime configuration JSON Schema.

  aux-analytics init --manifest manifest.json --duckdb analytics.duckdb
      Create or migrate the analytical tables declared by a manifest.

  aux-analytics tenant-query --target-tenant-id tenant-a --query query.json \
    --manifest manifest.json --duckdb analytics.duckdb
      Run a structured query with an engine-owned tenant predicate.

  aux-analytics backfill plan --config config.json --target-table users --output text
      Dry-run a safe existing-table adoption plan.

  aux-analytics backfill run --request backfill-execution.json --fixture fixture.json \
    --operation-store-duckdb operations.duckdb
      Execute a local fixture backfill through durable operation state.

    aux-analytics check run --operation-store-duckdb operations.duckdb \
    --operation-id check_123 --target-table users --source-rows source.json \
    --destination-rows destination.json
      Run a local read-only reconciliation check and persist its report.

  aux-analytics backup write --backup-root backups --operation-id backup_123 \
    --object-id users_0001 --source-identity source_users --records records.json --dry-run
      Validate a local raw backup object write without creating files.

  aux-analytics privacy-fix raw-backup --source-backup-root backups \
    --clean-target-root clean-backups --operation-store-duckdb operations.duckdb \
    --operation-id privacy_fix_123 --object-id users_0001 --privacy-policy privacy.json
      Dry-run privacy remediation for a local raw backup object and persist incident evidence.

  aux-analytics fix table --request table-fix.json --raw-backup-root backups \
    --operation-store-duckdb operations.duckdb
      Dry-run a table repair request from local raw backup truth and persist the report.

  aux-analytics trim plan --request trim.json --candidate-rows 10 \
    --operation-store-duckdb operations.duckdb
      Dry-run a destructive trim request and persist privacy-safe evidence.

  aux-analytics operations status --duckdb operations.duckdb --operation-id op_123 --output text
      Inspect a durable operation.

  aux-analytics completions zsh > ~/.zfunc/_aux-analytics
      Generate shell completion scripts for local installation.

Configuration model:
  Most commands accept --config and --overrides PATH=VALUE. CLI flags override configuration for
  the current invocation only. Backend flags choose the destination store directly when supplied.

Backend selection:
  --duckdb PATH uses a local DuckDB database.
  --ducklake-sqlite-catalog PATH uses DuckLake with a SQLite catalog.
  --ducklake-aux-catalog PATH uses DuckLake with an aux metadata catalog.
  --ducklake-data-path PATH configures the DuckLake data/object location.

Safety model:
  Public raw SQL is intentionally limited. Prefer structured query commands for tenant-scoped
  reads. Backfill planning is dry-run only; execution commands must route through durable operation
  state and audit trails.";

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
    Init {
        #[arg(long, value_name = "PATH", help = "Path to analytics manifest JSON")]
        manifest: Option<String>,
        #[arg(long, value_name = "PATH", help = "Path to runtime configuration JSON")]
        config: Option<PathBuf>,
        #[arg(
            long = "overrides",
            value_name = "PATH=VALUE",
            help = "Configuration override, repeatable; example: \
                    analytics.manifest_path=manifest.json"
        )]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
    },
    #[command(name = "unscoped-sql-query")]
    #[command(
        about = "Run an explicit unscoped read-only SQL query",
        long_about = "Run an administrative unscoped SQL query against the analytics \
                      database.\n\nThis command is intended for local/operator use. Public HTTP \
                      raw SQL remains restricted because arbitrary SQL cannot prove tenant \
                      isolation."
    )]
    UnscopedSqlQuery {
        #[arg(
            long,
            value_name = "SQL",
            help = "Single read-only SELECT/WITH SQL statement"
        )]
        sql: String,
        #[arg(long, value_name = "PATH", help = "Path to runtime configuration JSON")]
        config: Option<PathBuf>,
        #[arg(
            long = "overrides",
            value_name = "PATH=VALUE",
            help = "Configuration override, repeatable; example: \
                    analytics.manifest_path=manifest.json"
        )]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
    },
    #[command(name = "unscoped-structured-query")]
    #[command(
        about = "Run an unscoped structured query from JSON",
        long_about = "Run a structured query without adding a tenant predicate. Use this only for \
                      explicitly unscoped/admin data. For tenant-isolated reads, use tenant-query."
    )]
    UnscopedStructuredQuery {
        #[arg(long, value_name = "PATH", help = "Path to structured query JSON")]
        query: PathBuf,
        #[arg(long, value_name = "PATH", help = "Path to analytics manifest JSON")]
        manifest: Option<String>,
        #[arg(long, value_name = "PATH", help = "Path to runtime configuration JSON")]
        config: Option<PathBuf>,
        #[arg(
            long = "overrides",
            value_name = "PATH=VALUE",
            help = "Configuration override, repeatable; example: \
                    analytics.manifest_path=manifest.json"
        )]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
    },
    #[command(name = "tenant-query")]
    #[command(
        about = "Run a tenant-scoped structured query from JSON",
        long_about = "Run a structured query with an engine-owned tenant predicate. This is the \
                      preferred CLI query path for callers that should only read one tenant."
    )]
    TenantQuery {
        #[arg(
            long,
            value_name = "TENANT_ID",
            help = "Authorized tenant id to inject into the query"
        )]
        target_tenant_id: String,
        #[arg(long, value_name = "PATH", help = "Path to structured query JSON")]
        query: PathBuf,
        #[arg(long, value_name = "PATH", help = "Path to analytics manifest JSON")]
        manifest: Option<String>,
        #[arg(long, value_name = "PATH", help = "Path to runtime configuration JSON")]
        config: Option<PathBuf>,
        #[arg(
            long = "overrides",
            value_name = "PATH=VALUE",
            help = "Configuration override, repeatable; example: \
                    analytics.manifest_path=manifest.json"
        )]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
    },
    #[command(
        name = "repair-retention",
        about = "Repair missing retention metadata for one tenant/table",
        long_about = "Populate missing static retention metadata for rows in one analytical table \
                      and tenant. Use --dry-run to inspect the resolved retention period without \
                      mutating rows."
    )]
    RepairRetention {
        #[arg(long, value_name = "PATH", help = "Path to analytics manifest JSON")]
        manifest: Option<String>,
        #[arg(long, value_name = "PATH", help = "Path to runtime configuration JSON")]
        config: Option<PathBuf>,
        #[arg(
            long = "overrides",
            value_name = "PATH=VALUE",
            help = "Configuration override, repeatable; example: \
                    analytics.manifest_path=manifest.json"
        )]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
        #[arg(long, value_name = "TABLE", help = "Analytics table name to repair")]
        table: String,
        #[arg(
            long,
            value_name = "TENANT_ID",
            help = "Tenant id whose rows should be repaired"
        )]
        tenant_id: String,
        #[arg(long, help = "Report what would be repaired without updating rows")]
        dry_run: bool,
        #[arg(
            long,
            value_name = "ROWS",
            help = "Maximum rows to repair in this invocation"
        )]
        batch_size: Option<u64>,
    },
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
        about = "Write, inspect, and replay raw backup objects",
        long_about = "Operate local raw backup objects used for replay and repair workflows. \
                      Production object stores are intentionally not wired through this CLI yet; \
                      these commands exercise the local filesystem implementation and dry-run \
                      replay validation."
    )]
    RawBackup(RawBackupCommand),
    #[command(
        name = "fix",
        subcommand,
        about = "Run table repair workflows",
        long_about = "Run bounded repair workflows from reviewed request JSON. Current CLI \
                      support covers dry-run table fixes from local raw backup truth; apply mode \
                      requires a production destination adapter and fails closed."
    )]
    Fix(FixCommand),
    #[command(
        name = "privacy-fix",
        subcommand,
        about = "Remediate private data from backup and analytical storage",
        long_about = "Run privacy remediation workflows with durable operation evidence. Current \
                      CLI support covers local raw backup rewrites and verified destructive \
                      cleanup. Analytical table remediation is available through the engine API."
    )]
    PrivacyFix(PrivacyFixCommand),
    #[command(
        name = "trim",
        subcommand,
        about = "Plan destructive trim workflows",
        long_about = "Plan destructive row/table expiration workflows from reviewed request JSON. \
                      Current CLI support is dry-run only; apply mode fails closed until bounded \
                      deletion execution is implemented."
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
