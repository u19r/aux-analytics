use std::path::PathBuf;

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum CheckCommand {
    #[command(
        about = "Run a local reconciliation check",
        long_about = "Run a read-only reconciliation check from local source and destination row \
                      fixture JSON files. The command persists the check report under the \
                      operation id and prints JSON or text output."
    )]
    Run {
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        operation_store_duckdb: String,
        #[arg(long, value_name = "ID", help = "Operation id for the check run")]
        operation_id: String,
        #[arg(
            long,
            value_name = "TABLE",
            required = true,
            help = "Target analytics table; repeat for multiple tables"
        )]
        target_table: Vec<String>,
        #[arg(
            long,
            value_name = "PATH",
            help = "Analytics manifest JSON used for manifest-backed analytical check inputs"
        )]
        manifest: Option<PathBuf>,
        #[arg(long, value_name = "PATH", help = "Local source truth rows JSON file")]
        source_rows: Option<PathBuf>,
        #[arg(long, value_name = "PATH", help = "Local destination rows JSON file")]
        destination_rows: Option<PathBuf>,
        #[arg(
            long,
            value_name = "PATH",
            help = "DuckDB database containing normalized source check rows"
        )]
        source_duckdb: Option<String>,
        #[arg(
            long,
            value_name = "TABLE",
            help = "DuckDB table with source check rows: \
                    table_name,row_key,row_hash,source_position,contains_private_data"
        )]
        source_duckdb_check_rows_table: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            help = "DuckDB database containing normalized destination check rows"
        )]
        destination_duckdb: Option<String>,
        #[arg(
            long,
            value_name = "TABLE",
            help = "DuckDB table with destination check rows: \
                    table_name,row_key,row_hash,source_position,contains_private_data"
        )]
        destination_duckdb_check_rows_table: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            help = "DuckDB database containing source analytical table rows described by \
                    --manifest"
        )]
        source_duckdb_analytical: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            help = "DuckDB database containing destination analytical table rows described by \
                    --manifest"
        )]
        destination_duckdb_analytical: Option<String>,
        #[arg(
            long,
            value_enum,
            value_name = "STRATEGY",
            default_value_t = CheckStrategyArg::FullRows,
            help = "Comparison strategy"
        )]
        strategy: CheckStrategyArg,
        #[arg(
            long,
            value_name = "COUNT",
            default_value_t = 10,
            help = "Maximum sample keys to include per mismatch class"
        )]
        sample_limit: usize,
        #[arg(
            long,
            value_name = "VERSION",
            help = "Privacy policy version recorded on the report"
        )]
        privacy_policy_version: Option<String>,
        #[arg(
            long,
            value_enum,
            value_name = "OUTPUT",
            default_value_t = CheckOutputArg::Json,
            help = "Output format"
        )]
        output: CheckOutputArg,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum CheckStrategyArg {
    CountsOnly,
    KeySet,
    ChunkHashes,
    FullRows,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum CheckOutputArg {
    Json,
    Text,
}
