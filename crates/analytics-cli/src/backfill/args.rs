use std::path::PathBuf;

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum BackfillCommand {
    #[command(
        about = "Dry-run a safe backfill plan",
        long_about = "Build a dry-run plan for adopting existing source data into \
                      analytics.\n\nUse --request to load a complete BackfillRequest JSON, or \
                      provide --manifest/--config/--source-kind inputs and let the CLI derive a \
                      conservative request. Planning never writes destination rows."
    )]
    Plan {
        #[arg(
            long,
            value_name = "PATH",
            conflicts_with = "manifest",
            help = "Read a complete BackfillRequest JSON file"
        )]
        request: Option<PathBuf>,
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
        #[arg(
            long,
            value_enum,
            value_name = "KIND",
            help = "Source kind to plan for when it cannot be inferred from config"
        )]
        source_kind: Option<BackfillSourceKindArg>,
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            help = "Whether source metadata is reachable during planning"
        )]
        metadata_reachable: bool,
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            help = "Whether DynamoDB point-in-time export is available"
        )]
        dynamodb_pitr_enabled: bool,
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            help = "Whether DynamoDB parallel scan is available as a fallback"
        )]
        dynamodb_scan_enabled: bool,
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            help = "Whether aux-storage can enumerate historical rows"
        )]
        aux_storage_enumeration_enabled: bool,
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            help = "Whether aux-storage metadata can prove historical deletes"
        )]
        aux_storage_deletes_provable: bool,
        #[arg(
            long,
            value_name = "TABLE",
            help = "Target analytics table to backfill; repeat for multiple tables"
        )]
        target_table: Vec<String>,
        #[arg(
            long,
            value_name = "ROWS",
            default_value_t = 10_000,
            help = "Estimated source rows used for chunk planning"
        )]
        estimated_rows: u64,
        #[arg(
            long,
            value_name = "ROWS",
            default_value_t = 1_000,
            help = "Preferred number of rows per backfill chunk"
        )]
        chunk_target_rows: u64,
        #[arg(
            long,
            value_enum,
            value_name = "OUTPUT",
            default_value_t = BackfillPlanOutputArg::Json,
            help = "Output format"
        )]
        output: BackfillPlanOutputArg,
    },
    #[command(
        about = "Run a local fixture backfill execution",
        long_about = "Execute a BackfillExecutionRequest JSON file against a LocalBackfillFixture \
                      JSON file, persist durable operation state in the operation store, and \
                      print stable JSON. This is the local executable path; production source and \
                      destination adapters must use explicit operation-worker wiring."
    )]
    Run {
        #[arg(long, value_name = "PATH", help = "BackfillExecutionRequest JSON file")]
        request: PathBuf,
        #[arg(long, value_name = "PATH", help = "LocalBackfillFixture JSON file")]
        fixture: PathBuf,
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        operation_store_duckdb: PathBuf,
    },
    #[command(
        about = "Resume a local fixture backfill execution",
        long_about = "Resume a BackfillExecutionRequest using the cursor already stored in the \
                      operation store. The same LocalBackfillFixture JSON must be supplied so the \
                      executor can skip committed chunks and continue from durable state."
    )]
    Resume {
        #[arg(long, value_name = "PATH", help = "BackfillExecutionRequest JSON file")]
        request: PathBuf,
        #[arg(long, value_name = "PATH", help = "LocalBackfillFixture JSON file")]
        fixture: PathBuf,
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        operation_store_duckdb: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum BackfillSourceKindArg {
    DynamoDb,
    AuxStorage,
    RawBackup,
    LocalFixture,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum BackfillPlanOutputArg {
    Json,
    Text,
}
