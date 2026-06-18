use std::sync::{Arc, Mutex as StdMutex};

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
    #[error("analytics engine read pool lock is poisoned")]
    ReadPoolPoisoned,
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
    idle: StdMutex<Vec<AnalyticsEngine>>,
    permits: Arc<Semaphore>,
}

impl AnalyticsEngineAccess {
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
                idle: StdMutex::new(Vec::new()),
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
        operation: impl FnOnce(&AnalyticsEngine) -> T,
    ) -> Result<T, AnalyticsEngineAccessError> {
        match &self.read_strategy {
            ReadStrategy::SharedWriter => {
                let engine = self.writer.lock().await;
                Ok(operation(&engine))
            }
            ReadStrategy::DedicatedConnections(read_connections) => {
                let permit = Arc::clone(&read_connections.permits)
                    .acquire_owned()
                    .await
                    .map_err(|_| AnalyticsEngineAccessError::ReadPoolClosed)?;
                let engine = read_connections.take_engine()?;
                let result = operation(&engine);
                read_connections.return_engine(engine)?;
                drop(permit);
                Ok(result)
            }
        }
    }

    pub async fn with_write<T>(&self, operation: impl FnOnce(&AnalyticsEngine) -> T) -> T {
        let engine = self.writer.lock().await;
        operation(&engine)
    }
}

impl DedicatedReadConnections {
    fn take_engine(&self) -> Result<AnalyticsEngine, AnalyticsEngineAccessError> {
        if let Some(engine) = self
            .idle
            .lock()
            .map_err(|_| AnalyticsEngineAccessError::ReadPoolPoisoned)?
            .pop()
        {
            return Ok(engine);
        }
        Ok(AnalyticsEngine::connect(&self.backend)?)
    }

    fn return_engine(&self, engine: AnalyticsEngine) -> Result<(), AnalyticsEngineAccessError> {
        self.idle
            .lock()
            .map_err(|_| AnalyticsEngineAccessError::ReadPoolPoisoned)?
            .push(engine);
        Ok(())
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
