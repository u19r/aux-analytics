use std::path::PathBuf;

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum PrivacyFixCommand {
    #[command(
        name = "raw-backup",
        about = "Inspect local raw backup objects with a privacy policy",
        long_about = "Scan local raw backup objects, apply a PrivacyPolicy, and persist \
                      incident-safe dry-run evidence in the operation store. The request file is \
                      the complete typed maintenance contract. Apply requests fail closed because \
                      local raw-backup destinations do not support atomic maintenance fencing."
    )]
    RawBackup {
        #[arg(
            long,
            value_name = "PATH",
            help = "Complete MaintenanceRequest JSON file with kind=privacy_fix"
        )]
        request: PathBuf,
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        operation_store_duckdb: PathBuf,
    },
}
