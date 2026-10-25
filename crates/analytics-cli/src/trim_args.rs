use std::path::PathBuf;

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum TrimCommand {
    #[command(
        name = "plan",
        about = "Run a destructive trim request",
        long_about = "Run a destructive trim request. Execute a MaintenanceRequest JSON file with \
                      kind=trim, persist a privacy-safe report in the operation store, and print \
                      stable JSON. Dry-run and apply mode both use --destination-duckdb for a \
                      bounded local DuckDB source and destination. Apply mode also requires a \
                      request confirmation token."
    )]
    Plan {
        #[arg(
            long,
            value_name = "PATH",
            help = "Complete MaintenanceRequest JSON file with kind=trim"
        )]
        request: PathBuf,
        #[arg(
            long,
            value_name = "PATH",
            help = "Existing local DuckDB analytics database used for bounded count/apply"
        )]
        destination_duckdb: PathBuf,
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        operation_store_duckdb: PathBuf,
    },
}
