use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use analytics_engine::{AnalyticsEngine, AnalyticsEngineError, StorageBackend};
use config::DEFAULT_QUERY_MAX_READ_CONNECTIONS;
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore};

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
    #[cfg(test)]
    write_entries: Arc<AtomicUsize>,
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
    #[must_use]
    pub fn shared(engine: AnalyticsEngine) -> Self {
        Self {
            writer: Arc::new(Mutex::new(engine)),
            read_strategy: ReadStrategy::SharedWriter,
            #[cfg(test)]
            write_entries: Arc::new(AtomicUsize::new(0)),
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
            #[cfg(test)]
            write_entries: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn with_read<T>(
        &self,
        operation: impl FnOnce(&AnalyticsEngine) -> T + Send + 'static,
    ) -> Result<T, AnalyticsEngineAccessError>
    where
        T: Send + 'static,
    {
        match &self.read_strategy {
            ReadStrategy::SharedWriter => {
                let writer = Arc::clone(&self.writer);
                run_engine_work(move || {
                    let engine = writer.blocking_lock();
                    Ok(operation(&engine))
                })
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
                    let result = operation(&engine);
                    drop(permit);
                    Ok(result)
                })
                .await
            }
        }
    }

    pub async fn with_write<T>(
        &self,
        operation: impl FnOnce(&AnalyticsEngine) -> T + Send + 'static,
    ) -> T
    where
        T: Send + 'static,
    {
        #[cfg(test)]
        self.write_entries.fetch_add(1, Ordering::Relaxed);
        let writer = Arc::clone(&self.writer);
        run_engine_work(move || {
            let engine = writer.blocking_lock();
            operation(&engine)
        })
        .await
    }

    #[cfg(test)]
    pub(crate) fn write_entry_count(&self) -> usize {
        self.write_entries.load(Ordering::Relaxed)
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
