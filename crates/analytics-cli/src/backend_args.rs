use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct BackendArgs {
    #[arg(
        long,
        value_name = "PATH",
        conflicts_with_all = ["ducklake_sqlite_catalog", "ducklake_postgres_catalog"],
        help = "Use a local DuckDB database at PATH"
    )]
    pub(crate) duckdb: Option<String>,
    #[arg(
        long,
        value_name = "PATH",
        conflicts_with = "ducklake_postgres_catalog",
        help = "Use DuckLake with a SQLite catalog file"
    )]
    pub(crate) ducklake_sqlite_catalog: Option<String>,
    #[arg(
        long,
        value_name = "CONNECTION",
        conflicts_with = "ducklake_sqlite_catalog",
        help = "Use DuckLake with a PostgreSQL catalog connection string"
    )]
    pub(crate) ducklake_postgres_catalog: Option<String>,
    #[arg(
        long,
        value_name = "PATH_OR_URI",
        help = "DuckLake data path or object-store URI used with a DuckLake catalog"
    )]
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
