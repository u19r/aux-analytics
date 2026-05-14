mod app;
mod cli;
mod error;
mod runtime_config;

#[cfg(test)]
mod app_tests;
#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod quint_query_security_tests;

use clap::Parser;
use cli::Cli;

#[tokio::main]
async fn main() -> error::CliResult<()> {
    let cli = Cli::parse();
    if let Some(output) = app::run(cli).await? {
        println!("{output}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod main_tests;
