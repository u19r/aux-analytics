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
mod runtime_tests;
#[cfg(test)]
mod server_tests;
#[cfg(test)]
mod source_polling_tests;

use clap::Parser;
use cli::ApiCli;
use error::ApiResult;
use server::serve;

fn main() -> ApiResult<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(async_worker_threads())
        .enable_all()
        .build()?
        .block_on(serve(ApiCli::parse()))
}

fn async_worker_threads() -> usize {
    async_worker_threads_for(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(2),
    )
}

fn async_worker_threads_for(available: usize) -> usize {
    available.max(2)
}
