use std::path::PathBuf;

use clap::{Subcommand, ValueEnum};

#[derive(Debug, Subcommand)]
pub(crate) enum RawBackupCommand {
    #[command(
        about = "Write or dry-run a local raw backup object",
        long_about = "Write a local filesystem raw backup object from RawBackupRecord JSON. Use \
                      --dry-run to validate the request and print the object id and record count \
                      without writing files."
    )]
    Write {
        #[arg(long, value_name = "DIR", help = "Filesystem backup root directory")]
        backup_root: PathBuf,
        #[arg(
            long,
            value_name = "ID",
            help = "Operation id recorded in the backup index"
        )]
        operation_id: String,
        #[arg(long, value_name = "ID", help = "Backup object id to create")]
        object_id: String,
        #[arg(long, value_name = "SOURCE", help = "Source table or stream identity")]
        source_identity: String,
        #[arg(long, value_name = "PATH", help = "RawBackupRecord JSON file")]
        records: PathBuf,
        #[arg(
            long,
            value_name = "VERSION",
            help = "Privacy policy version recorded in index"
        )]
        privacy_policy_version: Option<String>,
        #[arg(
            long,
            value_name = "PATH",
            help = "PrivacyPolicy JSON file applied before writing the backup object"
        )]
        privacy_policy: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = RawBackupCompressionArg::None)]
        compression: RawBackupCompressionArg,
        #[arg(
            long,
            help = "Validate and print the planned write without creating files"
        )]
        dry_run: bool,
    },
    #[command(
        name = "replay-dry-run",
        about = "Validate a local raw backup object and report replay count",
        long_about = "Read a local raw backup index and object, verify checksum and schema, and \
                      report how many records would be replayed through ingestion. This command \
                      does not mutate analytical tables."
    )]
    ReplayDryRun {
        #[arg(long, value_name = "DIR", help = "Filesystem backup root directory")]
        backup_root: PathBuf,
        #[arg(long, value_name = "ID", help = "Backup object id to validate")]
        object_id: String,
        #[arg(
            long,
            value_name = "PATH",
            help = "PrivacyPolicy JSON file applied while validating replay output"
        )]
        privacy_policy: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum RawBackupCompressionArg {
    None,
    RunLength,
}
