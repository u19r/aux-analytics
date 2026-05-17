use std::path::PathBuf;

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum TrimCommand {
    #[command(
        name = "plan",
        about = "Dry-run a destructive trim request",
        long_about = "Execute a TrimRequest JSON file, persist a privacy-safe report in the \
                      operation store, and print stable JSON. Dry-run can use either a reviewed \
                      --candidate-rows fixture count or --destination-duckdb for a live local \
                      count. Apply mode requires --destination-duckdb and a request confirmation \
                      token."
    )]
    Plan {
        #[arg(long, value_name = "PATH", help = "TrimRequest JSON file")]
        request: PathBuf,
        #[arg(
            long,
            value_name = "ROWS",
            help = "Candidate row count from a reviewed local fixture or prior query; dry-run only"
        )]
        candidate_rows: Option<u64>,
        #[arg(
            long,
            value_name = "PATH",
            help = "Local DuckDB analytics database used for row-key trim count/apply"
        )]
        destination_duckdb: Option<PathBuf>,
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        operation_store_duckdb: PathBuf,
    },
}
