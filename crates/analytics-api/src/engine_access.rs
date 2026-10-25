use std::{future::Future, sync::Arc};

use analytics_engine::{AnalyticsEngine, AnalyticsEngineError, QueryCancellation, StorageBackend};
use config::DEFAULT_QUERY_MAX_READ_CONNECTIONS;
use thiserror::Error;
use tokio::{
    sync::{Mutex, Semaphore},
    task::{JoinError, JoinSet},
};

#[derive(Debug, Error)]
pub enum AnalyticsEngineAccessError {
    #[error(transparent)]
    Engine(#[from] AnalyticsEngineError),
    #[error("analytics engine read pool is closed")]
    ReadPoolClosed,
}

#[derive(Clone)]
pub struct AnalyticsEngineAccess {
    writer: Arc<Mutex<AnalyticsEngine>>,
    read_strategy: ReadStrategy,
}

#[derive(Clone)]
enum ReadStrategy {
    SharedWriter,
    DedicatedConnections(Arc<DedicatedReadConnections>),
}

struct DedicatedReadConnections {
    backend: StorageBackend,
    permits: Arc<Semaphore>,
}

impl AnalyticsEngineAccess {
    /// Executes independent queries concurrently and restores the request
    /// order. A failure cancels sibling native work and awaits its settlement.
    /// Each operation must pass its cancellation signal to
    /// `with_cancellable_read` or otherwise observe it before completing.
    pub async fn execute_query_batch<T, R, E, F, Q>(
        items: Vec<T>,
        execute_query: F,
    ) -> Result<Vec<R>, E>
    where
        T: Send + 'static,
        R: Send + 'static,
        E: From<JoinError> + Send + 'static,
        F: Fn(T, QueryCancellation) -> Q,
        Q: Future<Output = Result<R, E>> + Send + 'static,
    {
        let mut tasks = JoinSet::new();
        let cancellation =
            QueryBatchCancellation(items.iter().map(|_| QueryCancellation::default()).collect());
        for (index, (item, signal)) in items.into_iter().zip(&cancellation.0).enumerate() {
            let query = execute_query(item, signal.clone());
            tasks.spawn(async move { query.await.map(|result| (index, result)) });
        }
        let mut results = Vec::with_capacity(tasks.len());
        let mut failure = None;
        while let Some(joined) = tasks.join_next().await {
            match joined.map_err(E::from).and_then(|result| result) {
                Ok(result) => results.push(result),
                Err(error) if failure.is_none() => {
                    failure = Some(error);
                    cancellation.cancel();
                }
                Err(_) => {}
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        results.sort_by_key(|(index, _)| *index);
        Ok(results.into_iter().map(|(_, result)| result).collect())
    }

    #[must_use]
    pub fn shared(engine: AnalyticsEngine) -> Self {
        Self {
            writer: Arc::new(Mutex::new(engine)),
            read_strategy: ReadStrategy::SharedWriter,
        }
    }

    #[must_use]
    pub fn backend_aware(engine: AnalyticsEngine, backend: StorageBackend) -> Self {
        Self::backend_aware_with_max_read_connections(
            engine,
            backend,
            DEFAULT_QUERY_MAX_READ_CONNECTIONS,
        )
    }

    #[must_use]
    pub fn backend_aware_with_max_read_connections(
        engine: AnalyticsEngine,
        backend: StorageBackend,
        max_read_connections: usize,
    ) -> Self {
        let read_strategy = if supports_dedicated_read_connections(&backend) {
            ReadStrategy::DedicatedConnections(Arc::new(DedicatedReadConnections {
                backend,
                permits: Arc::new(Semaphore::new(max_read_connections.max(1))),
            }))
        } else {
            ReadStrategy::SharedWriter
        };
        Self {
            writer: Arc::new(Mutex::new(engine)),
            read_strategy,
        }
    }

    pub async fn with_read<T>(
        &self,
        operation: impl FnOnce(&AnalyticsEngine) -> T + Send + 'static,
    ) -> Result<T, AnalyticsEngineAccessError>
    where
        T: Send + 'static,
    {
        self.with_cancellable_read(QueryCancellation::default(), operation)
            .await
    }

    pub async fn with_cancellable_read<T>(
        &self,
        cancellation: QueryCancellation,
        operation: impl FnOnce(&AnalyticsEngine) -> T + Send + 'static,
    ) -> Result<T, AnalyticsEngineAccessError>
    where
        T: Send + 'static,
    {
        let mut cancel_on_drop = CancelReadOnDrop(Some(cancellation.clone()));
        let result = match &self.read_strategy {
            ReadStrategy::SharedWriter => {
                let engine = Arc::clone(&self.writer).lock_owned().await;
                run_engine_work(move || execute_cancellable_read(&engine, cancellation, operation))
                    .await
            }
            ReadStrategy::DedicatedConnections(read_connections) => {
                let permit = Arc::clone(&read_connections.permits)
                    .acquire_owned()
                    .await
                    .map_err(|_| AnalyticsEngineAccessError::ReadPoolClosed)?;
                let read_connections = Arc::clone(read_connections);
                run_engine_work(move || {
                    let engine = AnalyticsEngine::connect(&read_connections.backend)?;
                    let result = execute_cancellable_read(&engine, cancellation, operation);
                    drop(engine);
                    drop(permit);
                    result
                })
                .await
            }
        };
        cancel_on_drop.0.take();
        result
    }

    pub async fn with_write<T>(
        &self,
        operation: impl FnOnce(&AnalyticsEngine) -> T + Send + 'static,
    ) -> T
    where
        T: Send + 'static,
    {
        let engine = Arc::clone(&self.writer).lock_owned().await;
        run_engine_work(move || operation(&engine)).await
    }
}

async fn run_engine_work<T>(operation: impl FnOnce() -> T + Send + 'static) -> T
where T: Send + 'static {
    match tokio::task::spawn_blocking(operation).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("analytics engine worker was cancelled: {error}"),
    }
}

pub(crate) fn supports_dedicated_read_connections(backend: &StorageBackend) -> bool {
    match backend {
        // DuckDB file connections can keep an old read snapshot when they are
        // pooled across background writes. Use the writer connection for local
        // DuckDB-backed deployments so query results reflect source polling.
        StorageBackend::DuckDb { .. } => false,
        StorageBackend::DuckLake { .. } => true,
    }
}

struct QueryBatchCancellation(Vec<QueryCancellation>);

impl QueryBatchCancellation {
    fn cancel(&self) {
        for signal in &self.0 {
            signal.cancel();
        }
    }
}

impl Drop for QueryBatchCancellation {
    fn drop(&mut self) {
        self.cancel();
    }
}

struct CancelReadOnDrop(Option<QueryCancellation>);

impl Drop for CancelReadOnDrop {
    fn drop(&mut self) {
        if let Some(signal) = &self.0 {
            signal.cancel();
        }
    }
}

fn execute_cancellable_read<T>(
    engine: &AnalyticsEngine,
    cancellation: QueryCancellation,
    operation: impl FnOnce(&AnalyticsEngine) -> T,
) -> Result<T, AnalyticsEngineAccessError> {
    if cancellation.is_cancelled() {
        return Err(AnalyticsEngineError::QueryCancelled.into());
    }
    Ok(engine.with_query_cancellation(cancellation, operation))
}
