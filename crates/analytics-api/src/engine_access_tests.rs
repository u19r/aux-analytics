use std::sync::Arc;

use analytics_contract::{QuerySelect, StructuredQuery};
use analytics_engine::{AnalyticsEngine, CatalogType, StorageBackend};
use analytics_fixtures::{user_item, users_manifest};
use serde_json::json;
use tokio::task::JoinSet;

use crate::{
    engine_access::{AnalyticsEngineAccess, supports_dedicated_read_connections},
    http_contract::IngestStreamRecordRequest,
};

#[test]
fn given_duckdb_file_when_backend_aware_reads_used_then_writer_is_shared() {
    let backend = StorageBackend::DuckDb {
        path: "analytics.duckdb".to_string(),
    };

    assert!(!supports_dedicated_read_connections(&backend));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn given_synchronous_engine_work_when_executed_then_http_runtime_remains_responsive() {
    let backend = StorageBackend::DuckDb {
        path: ":memory:".to_string(),
    };
    let engine = AnalyticsEngine::connect(&backend).expect("connect engine");
    let access = Arc::new(AnalyticsEngineAccess::shared(engine));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (heartbeat_tx, heartbeat_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let heartbeat = tokio::spawn(async move {
        started_rx.await.expect("blocking work started");
        heartbeat_tx.send(()).expect("signal runtime heartbeat");
    });
    let operation = tokio::spawn({
        let access = Arc::clone(&access);
        async move {
            access
                .with_write(|_| {
                    started_tx.send(()).expect("signal blocking work");
                    release_rx.blocking_recv().expect("release blocking work");
                })
                .await;
        }
    });

    heartbeat_rx.await.expect("runtime heartbeat completed");
    release_tx.send(()).expect("release blocking work");
    heartbeat.await.expect("runtime heartbeat task");
    operation.await.expect("blocking work task");
}

#[tokio::test]
async fn given_duckdb_writer_data_when_concurrently_read_then_rows_are_visible() {
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
async fn given_ducklake_external_commit_when_read_then_committed_rows_are_visible() {
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
#[ignore = "requires configured live AuxCatalog service"]
async fn given_aux_catalog_external_commit_when_read_then_committed_rows_are_visible() {
    assert!(
        std::env::var_os("AUX_ANALYTICS_DUCKLAKE_FDB_LIVE").is_some(),
        "configure AUX_ANALYTICS_DUCKLAKE_FDB_LIVE before explicitly running this live-service \
         test"
    );
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

#[tokio::test]
async fn given_one_dedicated_reader_when_caller_is_aborted_then_next_read_waits_for_worker_settlement()
 {
    let directory = tempfile::tempdir().expect("catalog fixture");
    let access = Arc::new(create_dedicated_reader(directory.path(), 1));
    let (started, wait_for_start) = tokio::sync::oneshot::channel();
    let (release, wait_for_release) = tokio::sync::oneshot::channel();
    let (settled, wait_for_settlement) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn({
        let access = Arc::clone(&access);
        async move {
            access
                .with_read(move |engine| {
                    started.send(()).expect("caller observes worker entry");
                    wait_for_release.blocking_recv().expect("release worker");
                    let result = engine.query_unscoped_sql_json("SELECT 1 AS answer");
                    let _ = settled.send(result);
                })
                .await
        }
    });
    wait_for_start.await.expect("worker entered");
    caller.abort();
    assert!(caller.await.expect_err("caller aborted").is_cancelled());

    let next_read =
        access.with_read(|engine| engine.query_unscoped_sql_json("SELECT 42 AS answer"));
    tokio::pin!(next_read);
    let remained_pending =
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut next_read)
            .await
            .is_err();
    release
        .send(())
        .expect("release detached worker before assertions");
    let cancelled = tokio::time::timeout(std::time::Duration::from_secs(10), wait_for_settlement)
        .await
        .expect("cancelled worker settles")
        .expect("worker result");
    let rows = tokio::time::timeout(std::time::Duration::from_secs(10), next_read)
        .await
        .expect("next reader admitted")
        .expect("read access")
        .expect("next query succeeds");
    assert!(
        remained_pending,
        "caller drop must not release native admission early"
    );
    assert!(matches!(
        cancelled,
        Err(analytics_engine::AnalyticsEngineError::QueryCancelled)
    ));
    assert_eq!(rows, [json!({"answer": 42})]);
}

#[tokio::test]
async fn given_one_dedicated_reader_when_worker_panics_then_next_read_is_admitted() {
    let directory = tempfile::tempdir().expect("catalog fixture");
    let access = Arc::new(create_dedicated_reader(directory.path(), 1));
    let failed = tokio::spawn({
        let access = Arc::clone(&access);
        async move {
            access
                .with_read::<()>(|_| panic!("read worker panic"))
                .await
        }
    })
    .await
    .expect_err("worker panic propagates to caller");
    assert!(failed.is_panic());
    let rows = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        access.with_read(|engine| engine.query_unscoped_sql_json("SELECT 42 AS answer")),
    )
    .await
    .expect("panic releases admission")
    .expect("read access")
    .expect("query succeeds");
    assert_eq!(rows, [json!({"answer": 42})]);
}

#[tokio::test]
async fn given_completed_read_when_old_signal_is_cancelled_then_next_shared_read_is_unaffected() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
    let access = AnalyticsEngineAccess::shared(engine);
    let cancellation = analytics_engine::QueryCancellation::default();
    let rows = access
        .with_cancellable_read(cancellation.clone(), |engine| {
            engine.query_unscoped_sql_json("SELECT 1 AS answer")
        })
        .await
        .expect("first read access")
        .expect("first query succeeds");
    assert_eq!(rows, [json!({"answer": 1})]);
    cancellation.cancel();
    let rows = access
        .with_read(|engine| engine.query_unscoped_sql_json("SELECT 42 AS answer"))
        .await
        .expect("second read access")
        .expect("old signal cannot interrupt next query");
    assert_eq!(rows, [json!({"answer": 42})]);
}

fn create_dedicated_reader(
    directory: &std::path::Path,
    maximum_readers: usize,
) -> AnalyticsEngineAccess {
    let data_path = directory.join("data");
    std::fs::create_dir(&data_path).expect("native DuckLake data directory");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: directory
            .join("catalog.sqlite")
            .to_string_lossy()
            .into_owned(),
        data_path: data_path.to_string_lossy().into_owned(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    let writer = AnalyticsEngine::connect(&backend).expect("native DuckLake connection");
    AnalyticsEngineAccess::backend_aware_with_max_read_connections(writer, backend, maximum_readers)
}

const CANCELLATION_QUERY: &str =
    "SELECT sum(sin(a.i + b.i)) AS total FROM range(1000000) a(i), range(1000000) b(i)";

#[derive(Clone, Copy, Debug)]
enum MeasuredReadStrategy {
    Shared,
    Dedicated,
}

impl MeasuredReadStrategy {
    fn create_access(self, directory: &std::path::Path) -> Arc<AnalyticsEngineAccess> {
        Arc::new(match self {
            Self::Shared => AnalyticsEngineAccess::shared(
                AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine"),
            ),
            Self::Dedicated => create_dedicated_reader(directory, 1),
        })
    }
}

#[tokio::test]
async fn given_native_reads_with_each_strategy_when_cancelled_then_workers_settle_and_capacity_recovers()
 {
    for strategy in [
        MeasuredReadStrategy::Shared,
        MeasuredReadStrategy::Dedicated,
    ] {
        let directory = tempfile::tempdir().expect("native catalog fixture");
        let access = strategy.create_access(directory.path());
        measure_native_cancellation(&access).await;
    }
}

async fn measure_native_cancellation(
    access: &Arc<AnalyticsEngineAccess>,
) -> (std::time::Duration, std::time::Duration) {
    let cancellation = analytics_engine::QueryCancellation::default();
    let (started, wait_for_start) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn({
        let access = Arc::clone(access);
        let signal = cancellation.clone();
        async move {
            access
                .with_cancellable_read(signal, move |engine| {
                    started.send(()).expect("signal worker entry");
                    engine.query_unscoped_sql_json_without_timeout(CANCELLATION_QUERY)
                })
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), wait_for_start)
        .await
        .expect("worker admitted")
        .expect("worker entered");
    // Channels bracket the actual native call; this delay lets it progress.
    // No exact native instruction or scheduler interleaving is claimed.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !caller.is_finished(),
        "expensive query is still outstanding"
    );
    let cancelled_at = std::time::Instant::now();
    cancellation.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), caller)
        .await
        .expect("native cancellation settles")
        .expect("caller finishes")
        .expect("read admission succeeds");
    let settlement = cancelled_at.elapsed();
    assert!(matches!(
        result,
        Err(analytics_engine::AnalyticsEngineError::QueryCancelled)
    ));
    // Repeated cancellation after completion must not affect a later native
    // read.
    cancellation.cancel();
    let recovery_at = std::time::Instant::now();
    let next = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        access.with_read(|engine| engine.query_unscoped_sql_json("SELECT 42 AS answer")),
    )
    .await
    .expect("read capacity recovered")
    .expect("read admission succeeds")
    .expect("next native query succeeds");
    let recovery = recovery_at.elapsed();
    assert_eq!(next, [json!({"answer": 42})]);
    (settlement, recovery)
}

#[tokio::test]
#[ignore = "manual native cancellation and read-capacity recovery timing probe"]
async fn given_equal_native_queries_when_cancelled_with_each_strategy_then_settlement_latency_is_measured()
 {
    for strategy in [
        MeasuredReadStrategy::Shared,
        MeasuredReadStrategy::Dedicated,
    ] {
        let directory = tempfile::tempdir().expect("native catalog fixture");
        let access = strategy.create_access(directory.path());
        measure_native_cancellation(&access).await;
        for trial in 0..5 {
            let (settlement, recovery) = measure_native_cancellation(&access).await;
            println!(
                "native-cancellation | strategy={strategy:?} | trial={trial} | settlement_ns={} | \
                 recovery_ns={}",
                settlement.as_nanos(),
                recovery.as_nanos()
            );
        }
    }
}

#[tokio::test]
async fn given_two_dedicated_native_connections_when_one_is_cancelled_then_other_query_is_unaffected()
 {
    let directory = tempfile::tempdir().expect("native catalog fixture");
    let access = Arc::new(create_dedicated_reader(directory.path(), 2));
    let cancellation = analytics_engine::QueryCancellation::default();
    let (started, wait_for_start) = tokio::sync::oneshot::channel();
    let cancelled = tokio::spawn({
        let access = Arc::clone(&access);
        let signal = cancellation.clone();
        async move {
            access
                .with_cancellable_read(signal, move |engine| {
                    started.send(()).expect("first native worker entered");
                    engine.query_unscoped_sql_json_without_timeout(CANCELLATION_QUERY)
                })
                .await
        }
    });
    wait_for_start.await.expect("first native worker entered");
    let (second_started, wait_for_second) = tokio::sync::oneshot::channel();
    let unaffected = tokio::spawn({
        let access = Arc::clone(&access);
        async move {
            access
                .with_read(move |engine| {
                    second_started
                        .send(())
                        .expect("both native connections admitted");
                    engine.query_unscoped_sql_json_without_timeout(
                        "SELECT sum(i) AS total FROM range(1000000000) entries(i)",
                    )
                })
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), wait_for_second)
        .await
        .expect("second read admitted while first runs")
        .expect("second native worker entered");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let other_query_was_outstanding = !unaffected.is_finished();
    cancellation.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), cancelled)
        .await
        .expect("cancelled native worker settles")
        .expect("caller finishes")
        .expect("admission succeeds");
    // The bounded scan's completion time varies with shared CPU load.
    let rows = tokio::time::timeout(std::time::Duration::from_secs(30), unaffected)
        .await
        .expect("other worker settles")
        .expect("other caller finishes")
        .expect("other admission succeeds")
        .expect("other query is unaffected");
    assert!(matches!(
        result,
        Err(analytics_engine::AnalyticsEngineError::QueryCancelled)
    ));
    assert!(
        other_query_was_outstanding,
        "other native query overlaps cancellation"
    );
    assert_eq!(rows, [json!({"total": 499999999500000000_i64})]);
}

#[tokio::test]
async fn given_busy_writer_when_waiting_write_is_cancelled_then_its_callback_is_dropped_before_dispatch()
 {
    use std::{future::Future as _, task::Poll, time::Duration};
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    let access = Arc::new(AnalyticsEngineAccess::shared(engine));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let owner = Arc::clone(&access);
    let held = tokio::spawn(async move {
        owner
            .with_write(move |_| {
                started_tx.send(()).unwrap();
                release_rx.blocking_recv().unwrap();
            })
            .await;
    });
    started_rx.await.unwrap();
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel::<()>();
    let mut queued = Box::pin(access.with_write(move |_| drop(callback_tx)));
    std::future::poll_fn(|cx| {
        assert!(queued.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(queued);
    let cancelled = tokio::time::timeout(Duration::from_millis(100), callback_rx).await;
    release_tx.send(()).unwrap();
    held.await.unwrap();
    assert!(
        matches!(cancelled, Ok(Err(_))),
        "cancelled work must drop before writer admission"
    );
}
