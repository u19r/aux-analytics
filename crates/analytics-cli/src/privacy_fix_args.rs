use std::path::PathBuf;

use clap::{Subcommand, ValueEnum};

#[derive(Debug, Subcommand)]
pub(crate) enum PrivacyFixCommand {
    #[command(
        name = "raw-backup",
        about = "Rewrite local raw backup objects with a privacy policy",
        long_about = "Scan local raw backup objects, apply a PrivacyPolicy, optionally write \
                      clean replacement objects to a clean target root, and persist incident-safe \
                      evidence in the operation store. The command is dry-run unless --apply is \
                      supplied. Destructive cleanup requires both --apply and \
                      --confirmation-token."
    )]
    RawBackup {
        #[arg(
            long,
            value_name = "PATH",
            help = "Complete PrivacyFixRequest JSON file; cannot be mixed with request fields"
        )]
        request: Option<PathBuf>,
        #[arg(long, value_name = "DIR", help = "Source filesystem raw backup root")]
        source_backup_root: Option<PathBuf>,
        #[arg(
            long,
            value_name = "DIR",
            help = "Clean target filesystem raw backup root"
        )]
        clean_target_root: Option<PathBuf>,
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        operation_store_duckdb: PathBuf,
        #[arg(long, value_name = "ID", help = "Privacy-fix operation id")]
        operation_id: Option<String>,
        #[arg(
            long = "object-id",
            value_name = "ID",
            help = "Raw backup object id to scan; repeat for multiple objects"
        )]
        object_ids: Vec<String>,
        #[arg(
            long,
            value_name = "PATH",
            help = "PrivacyPolicy JSON file used to detect and rewrite private data"
        )]
        privacy_policy: Option<PathBuf>,
        #[arg(
            long = "table",
            value_name = "TABLE",
            help = "Affected analytics table name to record in the request; repeatable"
        )]
        tables: Vec<String>,
        #[arg(long, help = "Apply clean rewrites; omitted means dry-run")]
        apply: bool,
        #[arg(
            long,
            value_enum,
            default_value_t = PrivacyFixDestructiveActionArg::None,
            help = "Cleanup action for tainted source objects after clean verification"
        )]
        destructive_action: PrivacyFixDestructiveActionArg,
        #[arg(
            long,
            value_name = "TOKEN",
            help = "Required when destructive cleanup is requested"
        )]
        confirmation_token: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum PrivacyFixDestructiveActionArg {
    None,
    QuarantineTainted,
    DeleteTainted,
}
