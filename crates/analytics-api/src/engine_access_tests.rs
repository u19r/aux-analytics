use std::{sync::Arc, time::Duration};

use analytics_contract::{QuerySelect, StructuredQuery};
use analytics_engine::{AnalyticsEngine, CatalogType, StorageBackend};
use analytics_fixtures::{user_item, users_manifest};
use serde_json::json;
use tokio::task::JoinSet;

use crate::{
    engine_access::{AnalyticsEngineAccess, supports_dedicated_read_connections},
    types::IngestStreamRecordRequest,
};

#[test]
fn backend_aware_duckdb_file_uses_shared_writer_reads() {
    let backend = StorageBackend::DuckDb {
        path: "analytics.duckdb".to_string(),
    };

    assert!(!supports_dedicated_read_connections(&backend));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn synchronous_engine_work_does_not_starve_http_runtime() {
    let backend = StorageBackend::DuckDb {
        path: ":memory:".to_string(),
    };
    let engine = AnalyticsEngine::connect(&backend).expect("connect engine");
    let access = Arc::new(AnalyticsEngineAccess::shared(engine));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (heartbeat_tx, heartbeat_rx) = tokio::sync::oneshot::channel();
    let heartbeat = tokio::spawn(async move {
        started_rx.await.expect("blocking work started");
        tokio::time::sleep(Duration::from_millis(10)).await;
        heartbeat_tx.send(()).expect("signal runtime heartbeat");
    });
    let started = std::time::Instant::now();
    let operation = tokio::spawn({
        let access = Arc::clone(&access);
        async move {
            access
                .with_write(|_| {
                    started_tx.send(()).expect("signal blocking work");
                    std::thread::sleep(Duration::from_millis(250));
                })
                .await;
        }
    });

    heartbeat_rx.await.expect("runtime heartbeat completed");

    assert!(
        started.elapsed() < Duration::from_millis(100),
        "synchronous engine work starved the async runtime for {:?}",
        started.elapsed()
    );
    heartbeat.await.expect("runtime heartbeat task");
    operation.await.expect("blocking work task");
}

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
    let access = AnalyticsEngineAccess::backend_aware_with_max_read_connections(writer, backend, 4);
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
    let ingest_manifest = manifest.clone();
    access
        .with_write(move |engine| {
            engine.ingest_stream_record(&ingest_manifest, "users", record_key.as_bytes(), record)
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
                .with_read(move |engine| {
                    engine.query_tenant_structured_json(
                        manifest.as_ref(),
                        &StructuredQuery {
                            analytics_table_name: "users".to_string(),
                            table_alias: None,
                            joins: Vec::new(),
                            select: vec![QuerySelect::Count {
                                alias: "count".to_string(),
                            }],
                            filters: Vec::new(),
                            group_by: Vec::new(),
                            order_by: Vec::new(),
                            limit: Some(1),
                            offset: None,
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

#[tokio::test]
async fn backend_aware_ducklake_reader_observes_external_writer_commit() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let catalog_path = tempdir.path().join("catalog.sqlite");
    let data_path = tempdir.path().join("ducklake-data");
    std::fs::create_dir_all(&data_path).expect("create DuckLake data directory");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: catalog_path.to_string_lossy().to_string(),
        data_path: data_path.to_string_lossy().to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    assert_dedicated_reader_observes_external_write(backend).await;
}

#[tokio::test]
async fn backend_aware_aux_catalog_reader_observes_external_writer_commit() {
    if std::env::var_os("AUX_ANALYTICS_DUCKLAKE_FDB_LIVE").is_none() {
        return;
    }
    let tempdir = tempfile::tempdir().expect("tempdir");
    let catalog_path = tempdir.path().join("catalog.aux");
    let data_path = tempdir.path().join("ducklake-data");
    std::fs::create_dir_all(&data_path).expect("create DuckLake data directory");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::AuxCatalog,
        catalog_path: catalog_path.to_string_lossy().to_string(),
        data_path: data_path.to_string_lossy().to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    assert_dedicated_reader_observes_external_write(backend).await;
}

async fn assert_dedicated_reader_observes_external_write(backend: StorageBackend) {
    let manifest = users_manifest();
    let writer = AnalyticsEngine::connect(&backend).expect("connect writer");
    writer.ensure_manifest(&manifest).expect("ensure manifest");
    let reader = AnalyticsEngine::connect(&backend).expect("connect reader");
    let access = AnalyticsEngineAccess::backend_aware_with_max_read_connections(reader, backend, 1);
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::Count {
            alias: "count".to_string(),
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(1),
        offset: None,
    };
    let initial_manifest = manifest.clone();
    let initial_query = query.clone();
    let initial_rows = access
        .with_read(move |engine| {
            engine.query_tenant_structured_json(&initial_manifest, &initial_query, "tenant_01")
        })
        .await
        .expect("initial read access")
        .expect("initial query");
    assert_eq!(initial_rows[0]["count"], 0);

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
    writer
        .ingest_stream_record(&manifest, "users", record_key.as_bytes(), record)
        .expect("ingest");

    let rows = access
        .with_read(move |engine| {
            engine.query_tenant_structured_json(&manifest, &query, "tenant_01")
        })
        .await
        .expect("read access")
        .expect("query");

    assert_eq!(rows[0]["count"], 1);
}
