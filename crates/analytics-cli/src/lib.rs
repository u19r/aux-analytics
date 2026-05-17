mod app;
mod backend_args;
mod backfill;
mod backfill_args;
mod backfill_request;
mod check;
mod check_args;
mod cli;
mod error;
mod fix;
mod fix_args;
mod operations;
mod operations_args;
mod privacy_fix;
mod privacy_fix_args;
mod raw_backup;
mod raw_backup_args;
mod runtime_config;
mod trim;
mod trim_args;

pub use app::run;
pub use cli::Cli;
pub use error::{CliError, CliResult};

#[cfg(test)]
mod app_tests;
#[cfg(test)]
mod error_tests;
#[cfg(test)]
#[path = "main_tests.rs"]
mod main_tests;
#[cfg(test)]
mod quint_cli_operations_tests;
#[cfg(test)]
mod quint_query_security_tests;
