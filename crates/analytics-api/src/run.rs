use clap::Parser as _;

use crate::{ApiResult, cli, server};

pub fn run() -> ApiResult<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(async_worker_threads())
        .enable_all()
        .build()?
        .block_on(server::serve(cli::ApiCli::parse()))
}

fn async_worker_threads() -> usize {
    async_worker_threads_for(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(2),
    )
}

pub(crate) fn async_worker_threads_for(available: usize) -> usize {
    available.max(2)
}
