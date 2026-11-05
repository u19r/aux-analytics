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
    worker_thread_override().unwrap_or_else(|| {
        async_worker_threads_for(
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(2),
        )
    })
}

fn worker_thread_override() -> Option<usize> {
    std::env::var("TOKIO_WORKER_THREADS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

pub(crate) fn async_worker_threads_for(available: usize) -> usize {
    available.max(2)
}
