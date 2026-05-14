mod cli;
mod error;
mod runtime_config;
mod server;
mod source_polling;

#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod quint_query_security_tests;
#[cfg(test)]
mod quint_source_polling_tests;
#[cfg(test)]
mod server_tests;
#[cfg(test)]
mod source_polling_tests;

use clap::Parser;
use cli::ApiCli;
use error::ApiResult;
use server::serve;

#[tokio::main]
async fn main() -> ApiResult<()> {
    serve(ApiCli::parse()).await
}
