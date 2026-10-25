use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum OperationsCommand {
    #[command(
        about = "Submit one typed maintenance request",
        long_about = "Submit one complete MaintenanceRequest JSON document to the durable \
                      operation store. The request kind and payload are the only maintenance \
                      grammar; authorization uses the CLI caller identity and idempotency is \
                      keyed by operation_id."
    )]
    Submit {
        #[arg(
            long,
            value_name = "PATH",
            help = "Complete MaintenanceRequest JSON file"
        )]
        request: std::path::PathBuf,
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        duckdb: String,
        #[arg(
            long,
            value_enum,
            value_name = "OUTPUT",
            default_value_t = OperationsOutputArg::Json,
            help = "Output format"
        )]
        output: OperationsOutputArg,
    },
    #[command(about = "List durable operations")]
    List {
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        duckdb: String,
        #[arg(
            long,
            value_enum,
            value_name = "OUTPUT",
            default_value_t = OperationsOutputArg::Json,
            help = "Output format"
        )]
        output: OperationsOutputArg,
    },
    #[command(about = "Show one operation status")]
    Status {
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        duckdb: String,
        #[arg(long, value_name = "ID", help = "Operation id to inspect")]
        operation_id: String,
        #[arg(
            long,
            value_enum,
            value_name = "OUTPUT",
            default_value_t = OperationsOutputArg::Json,
            help = "Output format"
        )]
        output: OperationsOutputArg,
    },
    #[command(about = "Show append-only audit events for one operation")]
    Audit {
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        duckdb: String,
        #[arg(long, value_name = "ID", help = "Operation id to inspect")]
        operation_id: String,
        #[arg(
            long,
            value_enum,
            value_name = "OUTPUT",
            default_value_t = OperationsOutputArg::Json,
            help = "Output format"
        )]
        output: OperationsOutputArg,
    },
    #[command(about = "Request cancellation for one operation")]
    Cancel {
        #[arg(long, value_name = "PATH", help = "Operation store DuckDB path")]
        duckdb: String,
        #[arg(long, value_name = "ID", help = "Operation id to cancel")]
        operation_id: String,
        #[arg(
            long,
            value_enum,
            value_name = "OUTPUT",
            default_value_t = OperationsOutputArg::Json,
            help = "Output format"
        )]
        output: OperationsOutputArg,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum OperationsOutputArg {
    Json,
    Text,
}
