use analytics_cli::Cli;
use clap::Parser;

#[tokio::main]
async fn main() -> analytics_cli::CliResult<()> {
    let cli = Cli::parse();
    if let Some(output) = analytics_cli::run(cli).await? {
        println!("{output}");
    }
    Ok(())
}
