use std::path::PathBuf;

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum FixCommand {
    #[command(
        name = "table",
        about = "Execute a table fix request",
        long_about = "Execute a TableFixRequest JSON file against local raw backup objects, \
                      persist the report in the operation store, and print stable JSON. Dry-run \
                      mode reports selected repairs without mutating analytical tables. Apply \
                      mode requires --manifest and --destination-duckdb and repairs a local \
                      DuckDB analytical destination through the table-fix destination boundary."
    )]
    Table {
        #[arg(long, value_name = "PATH", help = "TableFixRequest JSON file")]
        request: PathBuf,
        #[arg(long, value_name = "DIR", help = "Filesystem raw backup root")]
        raw_backup_root: PathBuf,
        #[arg(
            long,
            value_name = "PATH",
            help = "Analytics manifest JSON file; required for apply mode"
        )]
        manifest: Option<PathBuf>,
        #[arg(
            long,
            value_name = "PATH",
            help = "Local DuckDB analytics database used for table-fix apply mode"
        )]
        destination_duckdb: Option<PathBuf>,
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        operation_store_duckdb: PathBuf,
    },
}
