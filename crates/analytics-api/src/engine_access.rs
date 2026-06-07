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

fn supports_dedicated_read_connections(backend: &StorageBackend) -> bool {
    match backend {
        StorageBackend::DuckDb { path } => path != ":memory:",
        StorageBackend::DuckLake { .. } => true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use analytics_contract::{QuerySelect, StructuredQuery};
    use analytics_fixtures::{user_item, users_manifest};
    use serde_json::json;
    use tokio::task::JoinSet;

    use super::*;
    use crate::types::IngestStreamRecordRequest;

    #[tokio::test]
    async fn backend_aware_duckdb_file_serves_concurrent_reads_from_writer_data() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let path = tempdir.path().join("analytics.duckdb");
        let backend = StorageBackend::DuckDb {
            path: path.to_string_lossy().to_string(),
        };
        let manifest = users_manifest();
        let writer = AnalyticsEngine::connect(&backend).expect("connect writer");
        writer.ensure_manifest(&manifest).expect("ensure manifest");
        let access =
            AnalyticsEngineAccess::backend_aware_with_max_read_connections(writer, backend, 4);
        let request: IngestStreamRecordRequest = serde_json::from_value(json!({
            "record_key": "user-1",
            "record": {
                "Keys": {},
                "SequenceNumber": "1",
                "NewImage": user_item("user-1", "reader@example.com", "org-a"),
            }
        }))
        .expect("ingest request");
        let (record_key, record) = request.into_contract_record();
        access
            .with_write(|engine| {
                engine.ingest_stream_record(&manifest, "users", record_key.as_bytes(), record)
            })
            .await
            .expect("ingest");

        let access = Arc::new(access);
        let manifest = Arc::new(manifest);
        let mut tasks = JoinSet::new();
        for _ in 0..4 {
            let access = Arc::clone(&access);
            let manifest = Arc::clone(&manifest);
            tasks.spawn(async move {
                access
                    .with_read(|engine| {
                        engine.query_tenant_structured_json(
                            manifest.as_ref(),
                            &StructuredQuery {
                                analytics_table_name: "users".to_string(),
                                select: vec![QuerySelect::Count {
                                    alias: "count".to_string(),
                                }],
                                filters: Vec::new(),
                                group_by: Vec::new(),
                                order_by: Vec::new(),
                                limit: Some(1),
                            },
                            "tenant_01",
                        )
                    })
                    .await
                    .expect("read access")
                    .expect("query")
            });
        }

        while let Some(joined) = tasks.join_next().await {
            let rows = joined.expect("read task");
            assert_eq!(rows[0]["count"], 1);
        }
    }
}
