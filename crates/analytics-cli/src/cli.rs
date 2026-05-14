use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "aux-analytics")]
#[command(about = "Run and operate the Aux Analytics service")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    Schema,
    ConfigSchema,
    Openapi,
    Init {
        #[arg(long)]
        manifest: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long = "overrides", value_name = "PATH=VALUE")]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
    },
    #[command(name = "unscoped-sql-query")]
    UnscopedSqlQuery {
        #[arg(long)]
        sql: String,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long = "overrides", value_name = "PATH=VALUE")]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
    },
    #[command(name = "unscoped-structured-query")]
    UnscopedStructuredQuery {
        #[arg(long)]
        query: PathBuf,
        #[arg(long)]
        manifest: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long = "overrides", value_name = "PATH=VALUE")]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
    },
    #[command(name = "tenant-query")]
    TenantQuery {
        #[arg(long)]
        target_tenant_id: String,
        #[arg(long)]
        query: PathBuf,
        #[arg(long)]
        manifest: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long = "overrides", value_name = "PATH=VALUE")]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
    },
    RepairRetention {
        #[arg(long)]
        manifest: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long = "overrides", value_name = "PATH=VALUE")]
        overrides: Vec<String>,
        #[command(flatten)]
        backend: BackendArgs,
        #[arg(long)]
        table: String,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        batch_size: Option<u64>,
    },
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
