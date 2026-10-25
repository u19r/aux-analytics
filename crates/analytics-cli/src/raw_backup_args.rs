use std::path::PathBuf;

use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum RawBackupCommand {
    #[command(
        name = "replay-dry-run",
        about = "Validate a local raw backup object and report replay count",
        long_about = "Read a local raw backup index and object, verify checksum and schema, and \
                      report how many records would be replayed through ingestion from one typed \
                      RawBackupReplayRequest JSON document. This command does not mutate \
                      analytical tables."
    )]
    ReplayDryRun {
        #[arg(long, value_name = "DIR", help = "Filesystem backup root directory")]
        backup_root: PathBuf,
        #[arg(
            long,
            value_name = "PATH",
            help = "Complete RawBackupReplayRequest JSON file"
        )]
        request: PathBuf,
    },
}
