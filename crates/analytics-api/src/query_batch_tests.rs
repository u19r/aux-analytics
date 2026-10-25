use tokio::task::JoinError;

use crate::AnalyticsEngineAccess;

#[derive(Debug, thiserror::Error)]
enum BatchFailure {
    #[error("query rejected")]
    Rejected,
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error(transparent)]
    Access(#[from] crate::AnalyticsEngineAccessError),
    #[error(transparent)]
    Engine(#[from] analytics_engine::AnalyticsEngineError),
}

#[tokio::test]
async fn given_queries_finishing_out_of_order_when_executed_then_request_order_is_preserved() {
    let (release, wait) = tokio::sync::oneshot::channel();
    let items = vec![(Some(wait), None), (None, Some(release))];
    let results =
        AnalyticsEngineAccess::execute_query_batch(items, |(wait, release), _| async move {
            if let Some(wait) = wait {
                wait.await.expect("second query completed");
                Ok::<_, BatchFailure>("first")
            } else {
                release
                    .expect("second query sender")
                    .send(())
                    .expect("first query waits");
                Ok("second")
            }
        })
        .await
        .expect("batch succeeds");
    assert_eq!(results, ["first", "second"]);
}

#[tokio::test]
async fn given_rejected_query_when_batch_executed_then_failure_is_preserved() {
    let result = AnalyticsEngineAccess::execute_query_batch(vec![()], |_, _| async {
        Err::<(), _>(BatchFailure::Rejected)
    })
    .await;
    assert!(matches!(result, Err(BatchFailure::Rejected)));
}

#[tokio::test]
async fn given_panicking_query_when_batch_executed_then_join_failure_is_preserved() {
    let result = AnalyticsEngineAccess::execute_query_batch::<_, (), BatchFailure, _, _>(
        vec![()],
        |_, _| async { panic!("query task panic") },
    )
    .await;
    assert!(matches!(result, Err(BatchFailure::Join(error)) if error.is_panic()));
}

struct CancellationReceipt(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for CancellationReceipt {
    fn drop(&mut self) {
        if let Some(cancelled) = self.0.take() {
            let _ = cancelled.send(());
        }
    }
}

const EXPENSIVE_NATIVE_QUERY: &str =
    "SELECT sum(sin(a.i + b.i)) AS total FROM range(1000000) a(i), range(1000000) b(i)";

enum NativeBatchRequest {
    Query {
        started: tokio::sync::oneshot::Sender<()>,
        settled: tokio::sync::oneshot::Sender<bool>,
    },
    RejectAfterStart(tokio::sync::oneshot::Receiver<()>),
}

#[tokio::test]
async fn given_running_native_query_when_sibling_fails_then_native_work_settles_before_batch_returns()
 {
    let engine =
        analytics_engine::AnalyticsEngine::connect(&analytics_engine::StorageBackend::DuckDb {
            path: ":memory:".to_owned(),
        })
        .expect("in-memory engine");
    let access = std::sync::Arc::new(AnalyticsEngineAccess::shared(engine));
    let (started, wait_for_start) = tokio::sync::oneshot::channel();
    let (settled, mut wait_for_settlement) = tokio::sync::oneshot::channel();
    let requests = vec![
        NativeBatchRequest::Query { started, settled },
        NativeBatchRequest::RejectAfterStart(wait_for_start),
    ];
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        AnalyticsEngineAccess::execute_query_batch(requests, |request, signal| {
            let access = std::sync::Arc::clone(&access);
            async move { execute_native_batch_request(&access, request, signal).await }
        }),
    )
    .await
    .expect("batch settles within bound");
    assert!(matches!(result, Err(BatchFailure::Rejected)));
    assert!(
        wait_for_settlement
            .try_recv()
            .expect("worker completed before batch returned")
    );
    assert_connection_remains_usable(&access).await;
}

async fn execute_native_batch_request(
    access: &AnalyticsEngineAccess,
    request: NativeBatchRequest,
    signal: analytics_engine::QueryCancellation,
) -> Result<(), BatchFailure> {
    match request {
        NativeBatchRequest::Query { started, settled } => {
            access
                .with_cancellable_read(signal, move |engine| {
                    started.send(()).expect("sibling waits for native worker");
                    let result =
                        engine.query_unscoped_sql_json_without_timeout(EXPENSIVE_NATIVE_QUERY);
                    settled
                        .send(matches!(
                            result,
                            Err(analytics_engine::AnalyticsEngineError::QueryCancelled)
                        ))
                        .expect("batch waits for native settlement");
                    result
                })
                .await??;
            Ok(())
        }
        NativeBatchRequest::RejectAfterStart(started) => {
            started.await.expect("native worker entered");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Err(BatchFailure::Rejected)
        }
    }
}

#[tokio::test]
async fn given_running_native_query_when_caller_is_dropped_then_worker_settles_and_connection_is_reusable()
 {
    let engine =
        analytics_engine::AnalyticsEngine::connect(&analytics_engine::StorageBackend::DuckDb {
            path: ":memory:".to_owned(),
        })
        .expect("in-memory engine");
    let access = std::sync::Arc::new(AnalyticsEngineAccess::shared(engine));
    let (started, wait_for_start) = tokio::sync::oneshot::channel();
    let (settled, wait_for_settlement) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn({
        let access = std::sync::Arc::clone(&access);
        async move {
            access
                .with_read(move |engine| {
                    started.send(()).expect("caller waits for native worker");
                    let result =
                        engine.query_unscoped_sql_json_without_timeout(EXPENSIVE_NATIVE_QUERY);
                    let _ = settled.send(result);
                })
                .await
        }
    });
    wait_for_start.await.expect("native worker entered");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    caller.abort();
    assert!(caller.await.expect_err("caller aborted").is_cancelled());
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), wait_for_settlement)
        .await
        .expect("worker settles within bound")
        .expect("native worker reports result");
    assert!(matches!(
        result,
        Err(analytics_engine::AnalyticsEngineError::QueryCancelled)
    ));
    assert_connection_remains_usable(&access).await;
}

#[tokio::test]
async fn given_running_native_query_when_deadline_expires_then_timeout_is_preserved_and_connection_is_reusable()
 {
    let engine =
        analytics_engine::AnalyticsEngine::connect(&analytics_engine::StorageBackend::DuckDb {
            path: ":memory:".to_owned(),
        })
        .expect("in-memory engine");
    let access = AnalyticsEngineAccess::shared(engine);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        access.with_read(|engine| {
            engine.query_unscoped_sql_json_with_timeout(
                EXPENSIVE_NATIVE_QUERY,
                std::time::Duration::from_millis(10),
            )
        }),
    )
    .await
    .expect("worker settles within bound")
    .expect("read access");
    assert!(matches!(
        result,
        Err(analytics_engine::AnalyticsEngineError::QueryTimeout { timeout_ms: 10 })
    ));
    assert_connection_remains_usable(&access).await;
}

async fn assert_connection_remains_usable(access: &AnalyticsEngineAccess) {
    let rows = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        access.with_read(|engine| engine.query_unscoped_sql_json("SELECT 42 AS answer")),
    )
    .await
    .expect("connection and admission released")
    .expect("read access")
    .expect("query succeeds");
    assert_eq!(rows, [serde_json::json!({"answer": 42})]);
}

#[tokio::test]
async fn given_native_worker_entry_when_cancellation_races_query_start_then_each_worker_settles_and_connection_is_reusable()
 {
    let engine =
        analytics_engine::AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
    let access = std::sync::Arc::new(AnalyticsEngineAccess::shared(engine));
    for _ in 0..16 {
        let cancellation = analytics_engine::QueryCancellation::default();
        let (started, wait_for_start) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn({
            let access = std::sync::Arc::clone(&access);
            let signal = cancellation.clone();
            async move {
                access
                    .with_cancellable_read(signal, move |engine| {
                        started.send(()).expect("caller observes worker entry");
                        engine.query_unscoped_sql_json_without_timeout(EXPENSIVE_NATIVE_QUERY)
                    })
                    .await
            }
        });
        wait_for_start
            .await
            .expect("worker entered before cancellation");
        // No delay: cancellation may arrive before prepare, during prepare or
        // during execution. A specific scheduler interleaving is not claimed.
        cancellation.cancel();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), caller)
            .await
            .expect("native worker settles")
            .expect("caller completes")
            .expect("read access");
        assert!(matches!(
            result,
            Err(analytics_engine::AnalyticsEngineError::QueryCancelled)
        ));
        assert_connection_remains_usable(&access).await;
    }
}

#[tokio::test]
async fn given_already_cancelled_read_when_admitted_then_operation_is_not_called_and_connection_is_reusable()
 {
    let engine =
        analytics_engine::AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
    let access = AnalyticsEngineAccess::shared(engine);
    let cancellation = analytics_engine::QueryCancellation::default();
    cancellation.cancel();
    let (entered, mut wait_for_entry) = tokio::sync::oneshot::channel();
    let result = access
        .with_cancellable_read(cancellation, move |_| {
            let _ = entered.send(());
        })
        .await;
    assert!(matches!(
        result,
        Err(crate::AnalyticsEngineAccessError::Engine(
            analytics_engine::AnalyticsEngineError::QueryCancelled,
        ))
    ));
    assert!(matches!(
        wait_for_entry.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    ));
    assert_connection_remains_usable(&access).await;
}

enum BatchRequest {
    Pending {
        started: tokio::sync::oneshot::Sender<()>,
        cancellation: CancellationReceipt,
    },
    RejectAfterStart(tokio::sync::oneshot::Receiver<()>),
}

#[tokio::test]
async fn given_running_query_when_another_query_fails_then_outstanding_task_is_cancelled() {
    let (started, wait_for_start) = tokio::sync::oneshot::channel();
    let (cancelled, wait_for_cancellation) = tokio::sync::oneshot::channel();
    let requests = vec![
        BatchRequest::Pending {
            started,
            cancellation: CancellationReceipt(Some(cancelled)),
        },
        BatchRequest::RejectAfterStart(wait_for_start),
    ];
    let result =
        AnalyticsEngineAccess::execute_query_batch(requests, |request, signal| async move {
            match request {
                BatchRequest::Pending {
                    started,
                    cancellation,
                } => {
                    let _cancellation = cancellation;
                    started
                        .send(())
                        .expect("failure query waits for running task");
                    while !signal.is_cancelled() {
                        tokio::task::yield_now().await;
                    }
                    Err::<(), _>(BatchFailure::Rejected)
                }
                BatchRequest::RejectAfterStart(started) => {
                    started.await.expect("first query starts");
                    Err(BatchFailure::Rejected)
                }
            }
        })
        .await;
    assert!(matches!(result, Err(BatchFailure::Rejected)));
    wait_for_cancellation
        .await
        .expect("outstanding task is cancelled");
}
