use std::path::PathBuf;

use clap::{Args, Parser};

#[derive(Debug, Parser)]
#[command(name = "aux-analytics-api")]
#[command(about = "Run the Aux Analytics HTTP API")]
pub(crate) struct ApiCli {
    #[arg(long)]
    pub(crate) manifest: Option<String>,
    #[command(flatten)]
    pub(crate) backend: BackendArgs,
    #[arg(long)]
    pub(crate) config: Option<PathBuf>,
    #[arg(short, long)]
    pub(crate) port: Option<String>,
    #[arg(long = "overrides", value_name = "PATH=VALUE")]
    pub(crate) overrides: Vec<String>,
}

#[derive(Debug, Args)]
pub(crate) struct BackendArgs {
    #[arg(long, conflicts_with_all = ["ducklake_sqlite_catalog", "ducklake_postgres_catalog"])]
    pub(crate) duckdb: Option<String>,
    #[arg(long, conflicts_with = "ducklake_postgres_catalog")]
    pub(crate) ducklake_sqlite_catalog: Option<String>,
    #[arg(long, conflicts_with = "ducklake_sqlite_catalog")]
    pub(crate) ducklake_postgres_catalog: Option<String>,
    #[arg(long)]
    pub(crate) ducklake_data_path: Option<String>,
}

impl From<&BackendArgs> for config::BackendOverride {
    fn from(args: &BackendArgs) -> Self {
        Self {
            duckdb: args.duckdb.clone(),
            ducklake_sqlite_catalog: args.ducklake_sqlite_catalog.clone(),
            ducklake_postgres_catalog: args.ducklake_postgres_catalog.clone(),
            ducklake_data_path: args.ducklake_data_path.clone(),
        }
    }
}
