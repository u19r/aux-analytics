use std::{
    collections::HashMap,
    future::Future,
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use analytics_contract::{StorageItem, StorageStreamRecord};
use analytics_fixtures::{user_item, users_manifest};
use analytics_storage::{
    PollBatch, PolledRecord, SourceCheckpoint, SourcePoller, SourceTablePlan, SystemStreamBatch,
};
use bg_jobs::{
    DriveStopReason, EffectMode, ExecutionContext, FairnessPolicy, HandlerFuture, JobCatalog,
    JobHandler, JobId, JobKind, JobOutcome, JobSpec, JobState, Namespace, OutcomeCode, Priority,
    QueueName, QueueSelection, RetryClass, RetryPreset, RetrySelection, Timestamp,
    observer::{AttemptBucket, JobEvent, JobObserver, SafeJobErrorEvent, SafeJobErrorKind},
    runtime::{JobSystem, MonotonicDeadline},
    store::{
        CommitOutcome, JobStore, RangePage, RangeRequest, StoreCapabilities, StoreKey,
        StoreTransaction,
    },
    test_support::{
        FakeStore,
        codec::{KeyCodec, KeyTag},
        raw::{
            CancellationResult, ClaimedJob, DriveRequest, DriveRequestInput, ExpiryResult,
            LifecycleObservation, NewJob, OperationId, OutcomeResult, ReconciliationOutcome,
            RedriveExecution, RedriveRequest,
        },
    },
};
use bytes::Bytes;
use config::{
    AnalyticsObjectStorageConfig, AnalyticsSourceConfig, AnalyticsSourceTableConfig,
    AnalyticsStreamType,
};
use storage_types::{SystemStreamCheckpointToken, SystemStreamPosition, SystemStreamScope};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

use crate::{
    background_jobs::{
        BackgroundJobError, BackgroundJobObserver, BackgroundJobRuntime, JobCatalogBundle,
        SourceJobAuthority, SourceJobKind, SourceJobPayload, SourceJobShared, bounded_timeout,
        job_catalog, settle_source_effect, source_deadline_at_ms, source_job_deduplication,
        test_support::{build_system, drive_request},
    },
    constants::{
        MAX_SOURCE_AUTHORIZED_IDENTITIES, MAX_SOURCE_DEADLINE_AT_MS, MAX_SOURCE_JOB_TIMEOUT,
        SOURCE_SCHEMA_VERSION,
    },
};

fn record(source_table_name: &str, shard_id: &str, sequence_number: &str) -> PolledRecord {
    PolledRecord {
        source_table_name: source_table_name.to_string(),
        shard_id: shard_id.to_string(),
        analytics_table_names: vec!["analytics_events".to_string()],
        record_key: format!("key-{sequence_number}"),
        system_position: None,
        record: StorageStreamRecord {
            keys: StorageItem::new(),
            sequence_number: sequence_number.to_string(),
            old_image: None,
            new_image: None,
        },
    }
}

fn checkpoint(source_table_name: &str, shard_id: &str, position: &str) -> SourceCheckpoint {
    SourceCheckpoint {
        source_table_name: source_table_name.to_string(),
        shard_id: shard_id.to_string(),
        position: position.to_string(),
    }
}

fn batch(records: Vec<PolledRecord>, checkpoints: Vec<SourceCheckpoint>) -> PollBatch {
    PollBatch {
        source_response_count: 1,
        source_nonempty_response_count: usize::from(!records.is_empty()),
        source_record_count: records.len(),
        source_encoded_bytes: 0,
        records,
        checkpoints,
        source_predecessor: None,
        system_stream: None,
    }
}

fn test_manifest_digest() -> [u8; analytics_engine::MANIFEST_DIGEST_BYTES] {
    analytics_engine::manifest_digest(&users_manifest())
}

struct SourceHandlerFixture {
    shared: Arc<SourceJobShared>,
    engine: crate::AnalyticsEngineAccess,
    payload: Vec<u8>,
}

async fn source_handler_fixture() -> SourceHandlerFixture {
    let engine = analytics_engine::AnalyticsEngine::connect_duckdb(":memory:")
        .expect("connect source handler test engine");
    let manifest = users_manifest();
    engine
        .ensure_manifest(&manifest)
        .expect("ensure source handler test manifest");
    let engine = crate::AnalyticsEngineAccess::shared(engine);
    let mut source = AnalyticsSourceConfig {
        endpoint_url: Some(describe_endpoint().await),
        ..AnalyticsSourceConfig::default()
    };
    source.poll_request_timeout_ms = 1_000;
    let source_plan =
        SourceTablePlan::aux_storage("tenant_01".to_string(), vec!["users".to_string()]);
    let mut stream_source = source.clone();
    stream_source.stream_type = Some(AnalyticsStreamType::StorageStream);
    stream_source.tables = vec![AnalyticsSourceTableConfig {
        table_name: "tenant_01".to_string(),
        stream_type: None,
        stream_identifier: Some(
            "arn:aws:dynamodb:region:account:table/users/stream/test".to_string(),
        ),
    }];
    let stream_plan = SourceTablePlan::from_source(&stream_source, &manifest)
        .expect("build source handler stream plan")
        .pop()
        .expect("source handler stream plan exists");
    let poller = SourcePoller::from_plans(&source, vec![source_plan.clone()], &[])
        .await
        .expect("build source handler test poller");
    let poller = Arc::new(tokio::sync::Mutex::new(poller));
    let progress_commits = Arc::new(AtomicUsize::new(0));
    let shared = Arc::new(SourceJobShared {
        engine: engine.clone(),
        manifest: Arc::new(tokio::sync::RwLock::new(Arc::new(manifest))),
        source_health: Arc::new(tokio::sync::RwLock::new(crate::SourceHealth::disabled())),
        privacy_policy: None,
        retention: None,
        pollers: tokio::sync::Mutex::new(HashMap::from([
            (SourceJobKind::AuxStorage, Arc::clone(&poller)),
            (SourceJobKind::StorageStream, poller),
        ])),
        authorized_source_identities: std::sync::OnceLock::new(),
        worker_id: "source-handler-test".to_string(),
        progress_commits,
    });
    shared
        .initialize_source_authority(&[source_plan, stream_plan])
        .expect("source handler authority is valid");
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::AuxStorage,
        source_handler_system_batch(),
        MAX_SOURCE_JOB_TIMEOUT.as_millis() as u64,
        test_manifest_digest(),
    )
    .expect("build source handler test payload")
    .encode()
    .expect("encode source handler test payload");

    SourceHandlerFixture {
        shared,
        engine,
        payload,
    }
}

async fn describe_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind describe endpoint");
    let address = listener.local_addr().expect("describe endpoint address");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept describe request");
        let mut request = [0_u8; 4096];
        let _ = socket
            .read(&mut request)
            .await
            .expect("read describe request");
        let body = r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
             {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write describe response");
    });
    format!("http://{address}")
}

async fn source_handler_fixture_without_process_history() -> SourceHandlerFixture {
    source_handler_fixture().await
}

fn source_handler_batch() -> PollBatch {
    let mut source_record = record("tenant_01", "source-shard", "cursor-1");
    source_record.analytics_table_names = vec!["users".to_string()];
    source_record.record.new_image = Some(user_item("user-1", "user@example.test", "org-a"));
    batch(
        vec![source_record],
        vec![checkpoint("tenant_01", "source-shard", "cursor-1")],
    )
}

fn source_handler_system_batch() -> PollBatch {
    let mut source_record = record("tenant_01", "logical-shard", "cursor-1");
    source_record.analytics_table_names = vec!["users".to_string()];
    source_record.system_position = Some(
        SystemStreamPosition::try_new("logical-shard", "lineage-a", "1")
            .expect("source handler system position is valid"),
    );
    source_record.record.keys = StorageItem::from_iter([
        (
            "pk".to_string(),
            analytics_contract::StorageValue::S("user-1".to_string()),
        ),
        (
            "sk".to_string(),
            analytics_contract::StorageValue::S("profile".to_string()),
        ),
    ]);
    source_record.record.new_image = Some(user_item("user-1", "user@example.test", "org-a"));
    PollBatch {
        source_response_count: 1,
        source_nonempty_response_count: 1,
        source_record_count: 1,
        source_encoded_bytes: 0,
        records: vec![source_record],
        checkpoints: Vec::new(),
        source_predecessor: None,
        system_stream: Some(SystemStreamBatch {
            source_scope: SystemStreamScope::try_new("aux-system", 7)
                .expect("source handler system scope is valid"),
            previous_checkpoint: None,
            checkpoint: SystemStreamCheckpointToken::try_from(
                "v1.0123456789abcdef0123456789abcdef",
            )
            .expect("source handler system checkpoint is valid"),
        }),
    }
}

fn source_handler_system_payload_for_table(source_table_name: &str) -> Vec<u8> {
    let mut source_batch = source_handler_system_batch();
    source_batch.records[0].source_table_name = source_table_name.to_string();
    SourceJobPayload::from_batch(
        SourceJobKind::AuxStorage,
        source_batch,
        MAX_SOURCE_JOB_TIMEOUT.as_millis() as u64,
        test_manifest_digest(),
    )
    .expect("build scoped system source handler payload")
    .encode()
    .expect("encode scoped system source handler payload")
}

fn source_handler_payload_for_scope(
    kind: SourceJobKind,
    source_table_name: &str,
    shard_id: &str,
) -> Vec<u8> {
    let mut source_batch = source_handler_batch();
    source_batch.records[0].source_table_name = source_table_name.to_string();
    source_batch.records[0].shard_id = shard_id.to_string();
    source_batch.checkpoints[0].source_table_name = source_table_name.to_string();
    source_batch.checkpoints[0].shard_id = shard_id.to_string();
    SourceJobPayload::from_batch(
        kind,
        source_batch,
        MAX_SOURCE_JOB_TIMEOUT.as_millis() as u64,
        test_manifest_digest(),
    )
    .expect("build scoped source handler payload")
    .encode()
    .expect("encode scoped source handler payload")
}

fn source_registration(fixture: &SourceHandlerFixture) -> JobCatalog {
    job_catalog(Arc::clone(&fixture.shared), None, MAX_SOURCE_JOB_TIMEOUT)
        .expect("source handler catalog is valid")
}

fn source_job_system(catalog: JobCatalog) -> JobSystem<FakeStore> {
    build_system(
        FakeStore::new(),
        catalog,
        b"analytics-source-handler-test",
        0x9a,
        None,
    )
}

#[tokio::test]
async fn given_registered_source_handlers_when_built_then_each_kind_has_a_typed_handle() {
    let fixture = source_handler_fixture().await;
    let bundle =
        JobCatalogBundle::try_new(Arc::clone(&fixture.shared), None, MAX_SOURCE_JOB_TIMEOUT)
            .expect("source handler catalog is valid");

    assert_eq!(bundle.source_handles.len(), SourceJobKind::ALL.len());
    assert!(bundle.retention_handle.is_none());
    for kind in SourceJobKind::ALL {
        let handle = bundle
            .source_handles
            .get(&kind)
            .expect("source handler has a typed handle");
        assert_eq!(handle.kind().as_str(), kind.job_kind());
        assert_eq!(handle.schema_version().get(), SOURCE_SCHEMA_VERSION);
    }
}

#[tokio::test]
async fn given_remote_store_latency_when_source_job_is_driven_then_discovery_does_not_starve_claims()
 {
    for latency in [
        Duration::from_millis(40),
        Duration::from_millis(100),
        Duration::from_secs(1),
    ] {
        let fixture = source_handler_fixture().await;
        let catalog =
            JobCatalogBundle::try_new(Arc::clone(&fixture.shared), None, MAX_SOURCE_JOB_TIMEOUT)
                .expect("source handler catalog");
        let handle = catalog
            .source_handles
            .get(&SourceJobKind::AuxStorage)
            .expect("source handle")
            .clone();
        let clock = Arc::new(bg_jobs_tokio::test_support::TestClock::new(
            Timestamp::ZERO,
            MonotonicDeadline::ZERO,
        ));
        let store = LatencyStore {
            inner: FakeStore::new(),
            clock: Arc::clone(&clock),
            latency,
        };
        let runtime =
            BackgroundJobRuntime::for_test(crate::background_jobs::runtime::TestRuntimeInput {
                store,
                catalog,
                owner: bg_jobs::OwnerId::from([0xb4; 16]),
                shared: Arc::clone(&fixture.shared),
                retention: None,
                health: None,
                source_job_timeout: MAX_SOURCE_JOB_TIMEOUT,
                clock,
            });
        let payload = SourceJobPayload::decode(&fixture.payload).expect("source payload");
        runtime
            .runtime
            .enqueue(
                &handle,
                payload,
                bg_jobs::EnqueueOptions::new(bg_jobs::FairnessClass::low_volume()),
            )
            .await
            .expect("enqueue source job");

        let report = runtime
            .runtime
            .drive_once()
            .await
            .expect("drive source job");

        assert_eq!(
            report.counts.claimed, 1,
            "latency={latency:?}; report={report:?}"
        );
        assert_eq!(
            report.counts.succeeded, 1,
            "latency={latency:?}; report={report:?}"
        );
        assert_eq!(fixture.shared.progress_commits.load(Ordering::Acquire), 1);
    }
}

#[derive(Clone)]
struct LatencyStore {
    inner: FakeStore,
    clock: Arc<bg_jobs_tokio::test_support::TestClock>,
    latency: Duration,
}

impl JobStore for LatencyStore {
    type Error = bg_jobs::test_support::FakeStoreError;

    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    async fn get(&self, key: StoreKey) -> Result<Option<Bytes>, Self::Error> {
        self.clock.advance(self.latency);
        self.inner.get(key).await
    }

    async fn get_many<'a>(
        &'a self,
        keys: &'a [StoreKey],
    ) -> Result<Vec<Option<Bytes>>, Self::Error> {
        if !keys.is_empty() {
            self.clock.advance(self.latency);
        }
        self.inner.get_many(keys).await
    }

    async fn read_range(&self, request: RangeRequest) -> Result<RangePage, Self::Error> {
        self.clock.advance(self.latency);
        self.inner.read_range(request).await
    }

    async fn commit<'a>(
        &'a self,
        transaction: &'a StoreTransaction,
    ) -> Result<CommitOutcome, Self::Error> {
        self.clock.advance(self.latency);
        self.inner.commit(transaction).await
    }
}

#[tokio::test]
async fn given_shutdown_during_source_poll_when_poll_finishes_then_feeder_does_not_enqueue() {
    let (endpoint, request_count, poll_started, release_poll) =
        serve_shutdown_source_endpoint().await;
    let engine = analytics_engine::AnalyticsEngine::connect_duckdb(":memory:")
        .expect("connect source shutdown test engine");
    let manifest = users_manifest();
    engine
        .ensure_manifest(&manifest)
        .expect("ensure source shutdown test manifest");
    let engine = crate::AnalyticsEngineAccess::shared(engine);
    let shared = Arc::new(SourceJobShared {
        engine: engine.clone(),
        manifest: Arc::new(tokio::sync::RwLock::new(Arc::new(manifest))),
        source_health: Arc::new(tokio::sync::RwLock::new(crate::SourceHealth::disabled())),
        privacy_policy: None,
        retention: None,
        pollers: tokio::sync::Mutex::new(HashMap::new()),
        authorized_source_identities: std::sync::OnceLock::new(),
        worker_id: "source-shutdown-test".to_string(),
        progress_commits: Arc::new(AtomicUsize::new(0)),
    });
    let catalog = JobCatalogBundle::try_new(Arc::clone(&shared), None, MAX_SOURCE_JOB_TIMEOUT)
        .expect("source shutdown test catalog");
    let store = FakeStore::new();
    let runtime = crate::background_jobs::BackgroundJobRuntime::for_test(
        crate::background_jobs::runtime::TestRuntimeInput {
            store: store.clone(),
            catalog,
            owner: bg_jobs::OwnerId::from([0xa4; 16]),
            shared: Arc::clone(&shared),
            retention: None,
            health: None,
            source_job_timeout: MAX_SOURCE_JOB_TIMEOUT,
            clock: bg_jobs_tokio::test_support::deterministic_clock(),
        },
    );
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(endpoint),
        poll_interval_ms: 1,
        poll_request_timeout_ms: 1_000,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan::aux_storage("tenant_01".to_string(), vec!["users".to_string()]);

    runtime
        .start_source_polling(source, vec![plan], &[], None)
        .await
        .expect("start source polling");
    poll_started.await.expect("source poll starts");
    let shutdown = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move { runtime.shutdown().await })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while !runtime.feeder_shutdown_requested() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown task starts");
    release_poll.send(()).expect("release source poll");
    shutdown.await.expect("source feeder shutdown");

    assert_eq!(
        request_count.load(Ordering::Acquire),
        2,
        "shutdown must stop the feeder after its in-flight poll"
    );
    let namespace = Namespace::try_new(b"analytics-api:bg-jobs:v1").expect("job namespace");
    let payload_chunks = store
        .snapshot()
        .expect("source shutdown store snapshot")
        .into_iter()
        .filter_map(|(key, _)| KeyCodec::decode(&namespace, &key).ok())
        .filter(|key| key.tag == KeyTag::PayloadChunk)
        .count();
    assert_eq!(payload_chunks, 0, "shutdown must prevent source enqueue");
}

#[tokio::test]
async fn given_shutdown_during_source_enqueue_when_commit_is_blocked_then_shutdown_waits_for_feeder()
 {
    let (endpoint, request_count, poll_started, release_poll) =
        serve_shutdown_source_endpoint().await;
    let engine = analytics_engine::AnalyticsEngine::connect_duckdb(":memory:")
        .expect("connect source enqueue shutdown test engine");
    let manifest = users_manifest();
    engine
        .ensure_manifest(&manifest)
        .expect("ensure source enqueue shutdown test manifest");
    let engine = crate::AnalyticsEngineAccess::shared(engine);
    let shared = Arc::new(SourceJobShared {
        engine: engine.clone(),
        manifest: Arc::new(tokio::sync::RwLock::new(Arc::new(manifest))),
        source_health: Arc::new(tokio::sync::RwLock::new(crate::SourceHealth::disabled())),
        privacy_policy: None,
        retention: None,
        pollers: tokio::sync::Mutex::new(HashMap::new()),
        authorized_source_identities: std::sync::OnceLock::new(),
        worker_id: "source-enqueue-shutdown-test".to_string(),
        progress_commits: Arc::new(AtomicUsize::new(0)),
    });
    let catalog = JobCatalogBundle::try_new(Arc::clone(&shared), None, MAX_SOURCE_JOB_TIMEOUT)
        .expect("source enqueue shutdown test catalog");
    let store = BlockingStore::new(FakeStore::new());
    store.block_next_payload_commit();
    let runtime = crate::background_jobs::BackgroundJobRuntime::for_test(
        crate::background_jobs::runtime::TestRuntimeInput {
            store: store.clone(),
            catalog,
            owner: bg_jobs::OwnerId::from([0xa5; 16]),
            shared: Arc::clone(&shared),
            retention: None,
            health: None,
            source_job_timeout: MAX_SOURCE_JOB_TIMEOUT,
            clock: bg_jobs_tokio::test_support::deterministic_clock(),
        },
    );
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(endpoint),
        poll_interval_ms: 1,
        poll_request_timeout_ms: 1_000,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan::aux_storage("tenant_01".to_string(), vec!["users".to_string()]);

    runtime
        .start_source_polling(source, vec![plan], &[], None)
        .await
        .expect("start source polling");
    poll_started.await.expect("source poll starts");
    release_poll.send(()).expect("release source poll");
    store.wait_for_payload_commit().await;

    let shutdown = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move { runtime.shutdown().await })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while !runtime.feeder_shutdown_requested() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown task starts");
    let mut shutdown = shutdown;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for the in-flight source enqueue"
    );

    store.release_payload_commit();
    shutdown.await.expect("source feeder shutdown");

    assert_eq!(
        request_count.load(Ordering::Acquire),
        2,
        "shutdown must join the feeder before returning"
    );
    let namespace = Namespace::try_new(b"analytics-api:bg-jobs:v1").expect("job namespace");
    let payload_chunks = store
        .inner()
        .snapshot()
        .expect("source enqueue shutdown store snapshot")
        .into_iter()
        .filter_map(|(key, _)| KeyCodec::decode(&namespace, &key).ok())
        .filter(|key| key.tag == KeyTag::PayloadChunk)
        .count();
    assert_eq!(
        payload_chunks, 1,
        "shutdown must allow the already-started enqueue to finish"
    );
}

#[tokio::test]
async fn given_successful_idle_source_poll_when_feeder_runs_then_health_is_healthy_without_durable_state()
 {
    let (endpoint, request_count, poll_finished) = serve_idle_source_endpoint().await;
    let (runtime, shared, store) = source_feeder_runtime(0xa6, "source-idle-test").await;
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(endpoint),
        poll_interval_ms: 60_000,
        poll_request_timeout_ms: 1_000,
        poll_max_responses_per_interval: 1,
        ..AnalyticsSourceConfig::default()
    };
    let plan = SourceTablePlan::aux_storage("tenant_01".to_string(), vec!["users".to_string()]);

    runtime
        .start_source_polling(source, vec![plan], &[], None)
        .await
        .expect("start idle source polling");
    poll_finished.await.expect("idle source poll finishes");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if shared
                .source_health
                .read()
                .await
                .last_success_at_ms
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("idle source poll updates health");
    runtime.shutdown().await;

    assert_eq!(request_count.load(Ordering::Acquire), 2);
    let health = shared.source_health.read().await;
    assert!(matches!(health.status, crate::SourceHealthStatus::Healthy));
    assert_eq!(health.phase, crate::SourcePollingPhase::Healthy);
    assert_eq!(health.total_polls, 1);
    assert!(health.last_poll_started_at_ms.is_some());
    assert!(health.last_success_at_ms.is_some());
    assert_eq!(health.total_poll_errors, 0);
    assert_eq!(health.total_records_ingested, 0);
    assert_eq!(health.total_checkpoints_saved, 0);
    assert!(health.checkpoints.is_empty());
    drop(health);

    let namespace = Namespace::try_new(b"analytics-api:bg-jobs:v1").expect("job namespace");
    let payload_chunks = store
        .snapshot()
        .expect("idle source store snapshot")
        .into_iter()
        .filter_map(|(key, _)| KeyCodec::decode(&namespace, &key).ok())
        .filter(|key| key.tag == KeyTag::PayloadChunk)
        .count();
    assert_eq!(payload_chunks, 0);
}

async fn source_feeder_runtime(
    owner: u8,
    worker_id: &str,
) -> (
    Arc<BackgroundJobRuntime<FakeStore>>,
    Arc<SourceJobShared>,
    FakeStore,
) {
    let engine = analytics_engine::AnalyticsEngine::connect_duckdb(":memory:")
        .expect("connect source feeder test engine");
    let manifest = users_manifest();
    engine
        .ensure_manifest(&manifest)
        .expect("ensure source feeder test manifest");
    let engine = crate::AnalyticsEngineAccess::shared(engine);
    let shared = Arc::new(SourceJobShared {
        engine: engine.clone(),
        manifest: Arc::new(tokio::sync::RwLock::new(Arc::new(manifest))),
        source_health: Arc::new(tokio::sync::RwLock::new(crate::SourceHealth::disabled())),
        privacy_policy: None,
        retention: None,
        pollers: tokio::sync::Mutex::new(HashMap::new()),
        authorized_source_identities: std::sync::OnceLock::new(),
        worker_id: worker_id.to_string(),
        progress_commits: Arc::new(AtomicUsize::new(0)),
    });
    let catalog = JobCatalogBundle::try_new(Arc::clone(&shared), None, MAX_SOURCE_JOB_TIMEOUT)
        .expect("source feeder test catalog");
    let store = FakeStore::new();
    let runtime =
        BackgroundJobRuntime::for_test(crate::background_jobs::runtime::TestRuntimeInput {
            store: store.clone(),
            catalog,
            owner: bg_jobs::OwnerId::from([owner; 16]),
            shared: Arc::clone(&shared),
            retention: None,
            health: None,
            source_job_timeout: MAX_SOURCE_JOB_TIMEOUT,
            clock: bg_jobs_tokio::test_support::deterministic_clock(),
        });
    (runtime, shared, store)
}

async fn claim_source_job(
    system: &JobSystem<FakeStore>,
    job: NewJob,
    now: Timestamp,
) -> ClaimedJob {
    let job_id = job.job_id;
    system
        .enqueue_raw(job, Timestamp::ZERO)
        .await
        .expect("source test job enqueues");
    system
        .claim(job_id, now)
        .await
        .expect("source test job claims")
}

#[derive(Clone)]
struct RecordingBackgroundJobObserver {
    analytics: Arc<BackgroundJobObserver>,
    events: Arc<Mutex<Vec<JobEvent>>>,
}

impl RecordingBackgroundJobObserver {
    fn repaired_events(&self) -> Vec<QueueName> {
        self.events
            .lock()
            .expect("background observer event lock")
            .iter()
            .filter_map(|event| match event {
                JobEvent::Repaired { queue } => Some(queue.clone()),
                _ => None,
            })
            .collect()
    }
}

impl JobObserver for RecordingBackgroundJobObserver {
    fn observe(&self, event: JobEvent) {
        self.analytics.observe(event.clone());
        self.events
            .lock()
            .expect("background observer event lock")
            .push(event);
    }

    fn observe_error(&self, event: SafeJobErrorEvent) {
        self.analytics.observe_error(event);
    }
}

#[test]
fn given_same_checkpoint_anchor_when_batch_identity_is_derived_then_records_remain_distinct() {
    let first = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );
    let second = batch(
        vec![record("events", "shard-1", "record-b")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );

    let first_payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        first,
        1,
        test_manifest_digest(),
    )
    .expect("first payload");
    let second_payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        second,
        1,
        test_manifest_digest(),
    )
    .expect("second payload");

    assert_ne!(first_payload.batch_id(), second_payload.batch_id());
}

#[test]
fn given_iterator_only_batches_when_identity_is_derived_then_positions_remain_distinct() {
    let first = batch(
        Vec::new(),
        vec![checkpoint("events", "__iterator:shard-1", "iterator-a")],
    );
    let second = batch(
        Vec::new(),
        vec![checkpoint("events", "__iterator:shard-1", "iterator-b")],
    );

    let first_payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        first,
        1,
        test_manifest_digest(),
    )
    .expect("first iterator payload");
    let second_payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        second,
        1,
        test_manifest_digest(),
    )
    .expect("second iterator payload");

    assert_ne!(first_payload.batch_id(), second_payload.batch_id());
}

#[test]
fn given_tampered_batch_identity_when_payload_is_validated_then_validation_fails() {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );
    let mut payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        source_batch,
        1,
        test_manifest_digest(),
    )
    .expect("payload");
    let SourceJobPayload::V1 { batch_id, .. } = &mut payload;
    *batch_id = "tampered".to_string();

    assert!(payload.validate().is_err());
}

#[test]
fn given_tampered_record_content_when_payload_is_validated_then_identity_fails_closed() {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        source_batch,
        1,
        test_manifest_digest(),
    )
    .expect("payload");
    let mut encoded = serde_json::to_value(&payload).expect("encode payload");
    encoded["records"][0]["record"]["NewImage"] = serde_json::json!({});

    let encoded = encode_source_json(&encoded);
    assert!(SourceJobPayload::decode(&encoded).is_err());
}

#[test]
fn given_invalid_utf8_record_key_when_payload_is_decoded_then_it_is_rejected_before_batch_conversion()
 {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        source_batch,
        1,
        test_manifest_digest(),
    )
    .expect("payload");
    let mut encoded = serde_json::to_value(&payload).expect("encode payload");
    encoded["records"][0]["record_key"] = serde_json::json!([255]);

    let encoded = encode_source_json(&encoded);
    let error = SourceJobPayload::decode(&encoded)
        .expect_err("invalid UTF-8 keys must fail before into_batch")
        .to_string();
    assert_eq!(
        error,
        "background source job payload is invalid: source record key is not UTF-8"
    );
}

#[test]
fn given_records_without_a_checkpoint_when_payload_is_built_then_it_fails_closed() {
    let source_batch = batch(vec![record("events", "shard-1", "record-a")], Vec::new());

    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            source_batch,
            1,
            test_manifest_digest(),
        )
        .is_err()
    );
}

#[test]
fn given_multiple_durable_checkpoints_when_payload_is_built_then_it_fails_closed() {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![
            checkpoint("events", "shard-1", "checkpoint-1"),
            checkpoint("events", "shard-1", "checkpoint-2"),
        ],
    );

    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            source_batch,
            1,
            test_manifest_digest(),
        )
        .is_err()
    );
}

#[test]
fn given_mixed_source_scopes_when_payload_is_built_then_it_fails_closed() {
    let source_batch = batch(
        vec![
            record("events", "shard-1", "record-a"),
            record("events", "shard-2", "record-b"),
        ],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );

    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            source_batch,
            1,
            test_manifest_digest(),
        )
        .is_err()
    );
}

#[test]
fn given_a_checkpoint_with_a_wrong_shard_when_payload_is_built_then_it_fails_closed() {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-2", "checkpoint-1")],
    );

    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            source_batch,
            1,
            test_manifest_digest(),
        )
        .is_err()
    );
}

#[test]
fn given_an_iterator_checkpoint_when_payload_round_trips_then_scope_and_kind_are_preserved() {
    let source_batch = batch(
        Vec::new(),
        vec![checkpoint("events", "__iterator:shard-1", "iterator-a")],
    );
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        source_batch,
        1,
        test_manifest_digest(),
    )
    .expect("iterator payload");
    let roundtrip = payload.into_batch();
    let scope = roundtrip
        .source_scope()
        .expect("roundtrip scope")
        .expect("scope present");

    assert_eq!(scope.source_table_name(), "events");
    assert_eq!(scope.shard_id(), "shard-1");
    assert!(roundtrip.checkpoints[0].is_iterator());
}

#[test]
fn given_a_payload_when_encoded_then_each_record_keeps_its_logical_source_table() {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        source_batch,
        1,
        test_manifest_digest(),
    )
    .expect("payload");
    let value: serde_json::Value = serde_json::to_value(&payload).expect("payload JSON");
    let record = &value["records"][0];
    let checkpoint = &value["checkpoints"][0];

    assert_eq!(value["version"], "v1");
    assert!(value.get("execution_id").is_none());
    assert!(value.get("operation_id").is_none());
    assert!(value.get("owner").is_none());
    assert!(value.get("job_id").is_none());
    assert_eq!(record["source_table_name"], "events");
    assert!(record.get("shard_id").is_none());
    assert!(checkpoint.get("source_table_name").is_none());
    assert!(checkpoint.get("shard_id").is_none());
}

#[test]
fn given_aux_storage_table_scope_when_payload_is_built_then_kind_mismatch_fails_closed() {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );

    let error = SourceJobPayload::from_batch(
        SourceJobKind::AuxStorage,
        source_batch,
        1,
        test_manifest_digest(),
    )
    .expect_err("AuxStorage jobs must carry a SystemStream scope")
    .to_string();
    assert_eq!(
        error,
        "background source job payload is invalid: source job kind does not match its scope kind"
    );
}

#[test]
fn given_an_unknown_payload_field_when_decoded_then_it_is_rejected() {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        source_batch,
        1,
        test_manifest_digest(),
    )
    .expect("payload");
    let mut value: serde_json::Value = serde_json::to_value(&payload).expect("payload JSON");
    value["unexpected"] = serde_json::Value::Bool(true);

    let encoded = encode_source_json(&value);
    assert!(SourceJobPayload::decode(&encoded).is_err());
}

#[test]
fn given_an_unbounded_deadline_when_payload_is_validated_then_it_is_rejected() {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );

    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            source_batch,
            u64::MAX,
            test_manifest_digest(),
        )
        .is_err()
    );
}

#[test]
fn given_the_supported_deadline_horizon_when_payload_is_validated_then_only_the_boundary_is_accepted()
 {
    let source_batch = batch(
        vec![record("events", "shard-1", "record-a")],
        vec![checkpoint("events", "shard-1", "checkpoint-1")],
    );

    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            source_batch.clone(),
            MAX_SOURCE_DEADLINE_AT_MS,
            test_manifest_digest(),
        )
        .is_ok()
    );
    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            source_batch.clone(),
            MAX_SOURCE_DEADLINE_AT_MS + 1,
            test_manifest_digest(),
        )
        .is_err()
    );
    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            source_batch,
            u64::MAX,
            test_manifest_digest(),
        )
        .is_err()
    );
}

#[test]
fn given_a_source_timeout_when_validated_then_only_the_configured_bound_is_accepted() {
    let maximum_ms = MAX_SOURCE_JOB_TIMEOUT.as_millis() as u64;

    assert_eq!(
        bounded_timeout(maximum_ms).expect("maximum source timeout is valid"),
        MAX_SOURCE_JOB_TIMEOUT
    );
    assert!(bounded_timeout(maximum_ms + 1).is_err());
    assert!(bounded_timeout(u64::MAX).is_err());
    assert!(bounded_timeout(0).is_err());
}

#[test]
fn given_a_feeder_deadline_near_the_horizon_when_calculated_then_enqueue_is_rejected() {
    let maximum_ms = MAX_SOURCE_JOB_TIMEOUT.as_millis() as u64;
    let timeout = MAX_SOURCE_JOB_TIMEOUT;

    assert_eq!(
        source_deadline_at_ms(u128::from(MAX_SOURCE_DEADLINE_AT_MS - maximum_ms), timeout,)
            .expect("the exact horizon is representable"),
        MAX_SOURCE_DEADLINE_AT_MS
    );
    assert!(source_deadline_at_ms(u128::from(MAX_SOURCE_DEADLINE_AT_MS), timeout).is_err());
    assert!(source_deadline_at_ms(0, Duration::from_millis(maximum_ms + 1)).is_err());
}

#[test]
fn given_both_source_kinds_when_registered_then_their_queues_and_kinds_are_distinct() {
    assert_ne!(
        SourceJobKind::AuxStorage.queue_name(),
        SourceJobKind::StorageStream.queue_name()
    );
    assert_ne!(
        SourceJobKind::AuxStorage.job_kind(),
        SourceJobKind::StorageStream.job_kind()
    );
}

#[test]
fn given_duplicate_source_plans_when_authority_is_built_then_kind_and_table_are_deduplicated() {
    let plan = SourceTablePlan::aux_storage("tenant_01".to_string(), vec!["users".to_string()]);
    let authority = SourceJobAuthority::from_plans(&[plan.clone(), plan])
        .expect("duplicate source plans are valid");

    assert!(authority.contains(SourceJobKind::AuxStorage, "tenant_01"));
    assert!(!authority.contains(SourceJobKind::StorageStream, "tenant_01"));
}

#[test]
fn given_too_many_source_plans_when_authority_is_built_then_configuration_fails_closed() {
    let plans = (0..=MAX_SOURCE_AUTHORIZED_IDENTITIES)
        .map(|index| {
            SourceTablePlan::aux_storage(format!("tenant_{index}"), vec!["users".to_string()])
        })
        .collect::<Vec<_>>();

    assert!(SourceJobAuthority::from_plans(&plans).is_err());
}

#[tokio::test]
async fn given_one_logical_event_with_two_policies_when_registered_job_applies_and_replays_then_private_rows_retention_and_progress_are_exact()
 {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let _recorder_guard = metrics::set_default_local_recorder(&recorder);

    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    let router = axum::Router::new().route(
        "/",
        axum::routing::post(move |body: Bytes| {
            let captured = Arc::clone(&captured);
            async move {
                let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let period = match request["TableName"].as_str().unwrap() {
                    "users_policy" => "100",
                    "orders_policy" => "300",
                    table => panic!("unexpected policy table {table}"),
                };
                captured.lock().unwrap().push(request);
                axum::Json(serde_json::json!({"Item": {"period_ms": {"N": period}}}))
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut fixture = source_handler_fixture().await;
    let mut manifest = users_manifest();
    let mut orders = manifest.tables[0].clone();
    orders.analytics_table_name = "orders".into();
    manifest.tables.push(orders);
    let native_manifest = manifest.clone();
    fixture
        .engine
        .with_write(move |engine| {
            engine.ensure_manifest(&native_manifest)?;
            for table in &native_manifest.tables {
                engine.ensure_retention_columns(&table.analytics_table_name)?;
            }
            Ok::<_, analytics_engine::AnalyticsEngineError>(())
        })
        .await
        .unwrap();
    *fixture.shared.manifest.write().await = Arc::new(manifest.clone());
    let tables = ["users", "orders"].map(|table| serde_json::json!({
        "analytics_table_name": table, "default_period_ms": 900, "strict": true,
        "timestamp": {"kind": "attribute", "attribute_path": "created_at_ms"},
        "tenant_policy": {"source": "dynamodb", "endpoint_url": endpoint, "region": "local-test-1",
            "credentials": {"static": {"access_key": "test-access", "secret_key": "test-secret"}},
            "request": {"get_item": {"table_name": format!("{table}_policy"), "key": {"pk": {"S": "${tenant_id}"}}}},
            "duration_selector": {"attribute_path": "period_ms"}, "cache_ttl_ms": 60000}
    }));
    let config: config::AnalyticsRetentionConfig =
        serde_json::from_value(serde_json::json!({"enabled": true, "tables": tables})).unwrap();
    let retention = crate::retention::RetentionRuntime::from_config(&config)
        .await
        .unwrap()
        .unwrap();
    let shared = Arc::get_mut(&mut fixture.shared).unwrap();
    shared.retention = Some(Arc::new(retention));
    shared.privacy_policy = Some(Arc::new(
        analytics_contract::PrivacyPolicy::try_new("source-privacy")
            .unwrap()
            .with_denied_key_name("email")
            .with_denied_key_name("pk"),
    ));
    let mut source_batch = source_handler_system_batch();
    source_batch.records[0].analytics_table_names = vec!["users".into(), "orders".into()];
    let image = source_batch.records[0].record.new_image.as_mut().unwrap();
    image.insert(
        "created_at_ms".into(),
        analytics_contract::StorageValue::N("1000".into()),
    );
    image.insert(
        "pk".into(),
        analytics_contract::StorageValue::S("user-1".into()),
    );
    fixture.payload = SourceJobPayload::from_batch(
        SourceJobKind::AuxStorage,
        source_batch,
        MAX_SOURCE_JOB_TIMEOUT.as_millis() as u64,
        analytics_engine::manifest_digest(&manifest),
    )
    .unwrap()
    .encode()
    .unwrap();
    let system = source_job_system(source_registration(&fixture));
    let claim = claim_source_job(
        &system,
        source_lifecycle_job(0x22, fixture.payload.clone()),
        Timestamp::ZERO,
    )
    .await;
    for expected in ["source_applied", "source_already_applied"] {
        let outcome = system
            .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
            .await
            .unwrap();
        assert!(matches!(outcome, JobOutcome::Complete(ref code) if code.as_str() == expected));
        let rows = fixture
            .engine
            .with_read(|engine| {
                engine.query_unscoped_sql_json(
                    "SELECT 'users' AS destination, user_id, email, org_id, item, __expiry, \
                     __missing_retention FROM users
             UNION ALL SELECT 'orders', user_id, email, org_id, item, __expiry, \
                     __missing_retention FROM orders ORDER BY destination",
                )
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rows, serde_json::json!([
            {"destination": "orders", "user_id": "user-1", "email": null, "org_id": "org-a",
             "item": {"user_id": "user-1", "profile": {}, "organization": {"id": "org-a"}, "created_at_ms": 1000},
             "__expiry": 1300, "__missing_retention": false},
            {"destination": "users", "user_id": "user-1", "email": null, "org_id": "org-a",
             "item": {"user_id": "user-1", "profile": {}, "organization": {"id": "org-a"}, "created_at_ms": 1000},
             "__expiry": 1100, "__missing_retention": false}
        ]).as_array().unwrap().clone());
        let health = fixture.shared.source_health.read().await;
        assert_eq!(health.total_records_ingested, 2);
        assert_eq!(health.total_checkpoints_saved, 1);
        assert!(
            health.checkpoints.is_empty(),
            "SystemStream progress is not a table/shard checkpoint"
        );
        assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 1);
    }
    let exported = recorder.handle().render();
    for sample in [
        "analytics_source_records_committed_total 2",
        "analytics_source_write_transactions_total 1",
        "analytics_source_checkpoints_saved_total 1",
    ] {
        assert!(
            exported.lines().any(|line| line == sample),
            "replay changed committed metrics: {exported}"
        );
    }
    assert_eq!(
        *requests.lock().unwrap(),
        [
            serde_json::json!({"TableName": "users_policy", "Key": {"pk": {"S": "tenant_01"}}}),
            serde_json::json!({"TableName": "orders_policy", "Key": {"pk": {"S": "tenant_01"}}})
        ]
    );
    let checkpoint = fixture
        .engine
        .with_read(|engine| engine.load_system_stream_checkpoint())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.0.scope_id().as_str(), "aux-system");
    assert_eq!(checkpoint.0.generation(), 7);
    assert_eq!(checkpoint.1.as_str(), "v1.0123456789abcdef0123456789abcdef");
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn given_worker_with_older_local_frontier_when_it_applies_another_workers_job_then_next_poll_uses_durable_progress()
 {
    let fixture = source_handler_fixture().await;
    let system = source_job_system(source_registration(&fixture));
    let first = claim_source_job(
        &system,
        source_lifecycle_job(0xb5, fixture.payload.clone()),
        Timestamp::ZERO,
    )
    .await;
    let outcome = system
        .execute_claimed(&first, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .unwrap();
    assert!(matches!(outcome, JobOutcome::Complete(ref code) if code.as_str() == "source_applied"));

    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    let router = axum::Router::new().route(
        "/",
        axum::routing::post(move |body: Bytes| {
            let captured = Arc::clone(&captured);
            async move {
                let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let is_poll = request["SystemStream"] == true;
                captured.lock().unwrap().push(request);
                let mut response = serde_json::json!({
                    "SourceScope": {"Version": 1, "ScopeId": "aux-system", "Generation": 7}
                });
                if is_poll {
                    response["Records"] = serde_json::json!([]);
                }
                axum::Json(response)
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let source = AnalyticsSourceConfig {
        endpoint_url: Some(endpoint),
        ..AnalyticsSourceConfig::default()
    };
    let poller = SourcePoller::from_plans(
        &source,
        vec![SourceTablePlan::aux_storage(
            "tenant_01".into(),
            vec!["users".into()],
        )],
        &[],
    )
    .await
    .unwrap();
    let poller = Arc::new(tokio::sync::Mutex::new(poller));
    fixture
        .shared
        .pollers
        .lock()
        .await
        .insert(SourceJobKind::AuxStorage, Arc::clone(&poller));

    let mut batch = source_handler_system_batch();
    let previous = batch.system_stream.as_ref().unwrap().checkpoint.clone();
    let next =
        SystemStreamCheckpointToken::try_from("v1.22222222222222222222222222222222").unwrap();
    batch.system_stream.as_mut().unwrap().previous_checkpoint = Some(previous);
    batch.system_stream.as_mut().unwrap().checkpoint = next.clone();
    batch.records[0].system_position =
        Some(SystemStreamPosition::try_new("logical-shard", "lineage-a", "2").unwrap());
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::AuxStorage,
        batch,
        MAX_SOURCE_JOB_TIMEOUT.as_millis() as u64,
        test_manifest_digest(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let second = claim_source_job(
        &system,
        source_lifecycle_job(0xb6, payload),
        Timestamp::ZERO,
    )
    .await;

    let outcome = system
        .execute_claimed(&second, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .unwrap();
    assert!(matches!(outcome, JobOutcome::Complete(ref code) if code.as_str() == "source_applied"));
    poller.lock().await.poll_once().await.unwrap();

    assert_eq!(
        requests.lock().unwrap().last().unwrap()["Checkpoint"],
        next.as_str()
    );
    assert_eq!(fixture.shared.progress_commits.load(Ordering::Acquire), 2);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn given_registered_source_handler_replay_when_executed_then_only_applied_commits_progress() {
    let fixture = source_handler_fixture().await;
    let system = source_job_system(source_registration(&fixture));
    let claim = claim_source_job(
        &system,
        source_lifecycle_job(0x21, fixture.payload.clone()),
        Timestamp::ZERO,
    )
    .await;
    let first = system
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("first source execution succeeds");
    let replay = system
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("source replay succeeds");

    assert!(matches!(first, JobOutcome::Complete(ref code) if code.as_str() == "source_applied"));
    assert!(
        matches!(replay, JobOutcome::Complete(ref code) if code.as_str() == "source_already_applied")
    );
    assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 1);

    assert_eq!(
        fixture
            .shared
            .source_health
            .read()
            .await
            .total_checkpoints_saved,
        1,
        "the applied System frontier is counted once and its replay adds no save"
    );
    let checkpoint = fixture
        .engine
        .with_read(|engine| engine.load_system_stream_checkpoint())
        .await
        .expect("read source checkpoints")
        .expect("system source checkpoint load")
        .expect("system source checkpoint present");
    let (scope, checkpoint) = checkpoint;
    assert_eq!(scope.scope_id().as_str(), "aux-system");
    assert_eq!(scope.generation(), 7);
    assert_eq!(checkpoint.as_str(), "v1.0123456789abcdef0123456789abcdef");
}

#[tokio::test]
async fn given_a_stale_registered_source_execution_when_executed_then_destination_and_progress_are_not_replayed()
 {
    let fixture = source_handler_fixture().await;
    let system = source_job_system(source_registration(&fixture));
    let old_claim = claim_source_job(
        &system,
        source_lifecycle_job(0x41, fixture.payload.clone()),
        Timestamp::ZERO,
    )
    .await;
    let new_claim = claim_source_job(
        &system,
        source_lifecycle_job(0x42, fixture.payload.clone()),
        Timestamp::ZERO,
    )
    .await;
    let applied = system
        .execute_claimed(&new_claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("initial source execution succeeds");
    assert!(matches!(applied, JobOutcome::Complete(ref code) if code.as_str() == "source_applied"));
    let stale = system
        .execute_claimed(&old_claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("stale source execution is handled");

    assert!(
        matches!(stale, JobOutcome::Complete(ref code) if code.as_str() == "source_stale_fence")
    );
    assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn given_an_unconfigured_table_or_wrong_kind_when_handler_runs_then_no_effect_or_progress_is_committed()
 {
    for (source_kind, source_table_name) in [
        (SourceJobKind::AuxStorage, "unconfigured-table"),
        (SourceJobKind::StorageStream, "tenant_01"),
    ] {
        let fixture = source_handler_fixture().await;
        let system = source_job_system(source_registration(&fixture));
        let payload = if source_kind == SourceJobKind::AuxStorage {
            source_handler_system_payload_for_table(source_table_name)
        } else {
            source_handler_payload_for_scope(source_kind, source_table_name, "source-shard")
        };
        let claim = claim_source_job(
            &system,
            source_lifecycle_job(0x61, payload),
            Timestamp::ZERO,
        )
        .await;

        let result = system
            .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
            .await;
        assert!(matches!(
            result.expect("unauthorized source job becomes a retry outcome"),
            JobOutcome::Retry { ref code, .. } if code.as_str() == "handler_failed"
        ));
        assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn given_a_durable_source_job_when_runtime_restarts_then_new_instance_executes_once() {
    let instance_a = source_handler_fixture().await;
    let instance_b = source_handler_fixture_without_process_history().await;
    let store = FakeStore::new();
    let system_a = build_system(
        store.clone(),
        job_catalog(Arc::clone(&instance_a.shared), None, MAX_SOURCE_JOB_TIMEOUT)
            .expect("runtime A source handler catalog is valid"),
        b"analytics-source-restart-test",
        0x81,
        None,
    );
    let id = JobId::from([0x82; 16]);
    system_a
        .enqueue_raw(
            source_lifecycle_job(id.as_bytes()[0], instance_a.payload.clone()),
            Timestamp::ZERO,
        )
        .await
        .expect("runtime A enqueues durable source job");
    drop(system_a);

    let system_b = build_system(
        store,
        job_catalog(Arc::clone(&instance_b.shared), None, MAX_SOURCE_JOB_TIMEOUT)
            .expect("runtime B source handler catalog is valid"),
        b"analytics-source-restart-test",
        0x83,
        None,
    );
    let claim = system_b
        .claim(id, Timestamp::ZERO)
        .await
        .expect("runtime B claims the durable source job");
    let outcome = system_b
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("runtime B executes the durable source job");
    assert!(matches!(
        outcome,
        JobOutcome::Complete(ref code) if code.as_str() == "source_applied"
    ));
    system_b
        .complete(claim, outcome, Timestamp::from_millis(1))
        .await
        .expect("runtime B completes the durable source job");

    assert_eq!(
        instance_b.shared.progress_commits.load(Ordering::Relaxed),
        1
    );
    assert!(matches!(
        system_b
            .observe_lifecycle(id)
            .await
            .expect("completed restart job is observable"),
        LifecycleObservation::Terminal(_)
    ));
}

#[tokio::test]
async fn given_multiple_configured_aws_source_shards_when_handlers_run_then_each_scope_is_accepted()
{
    let fixture = source_handler_fixture().await;
    let source_kind = SourceJobKind::StorageStream;
    let system = source_job_system(source_registration(&fixture));

    for (id, shard_id) in [(0x69, "shard-a"), (0x6b, "shard-b")] {
        let payload = source_handler_payload_for_scope(source_kind, "tenant_01", shard_id);
        let claim = claim_source_job(
            &system,
            source_job(id, source_kind, payload),
            Timestamp::ZERO,
        )
        .await;
        let outcome = system
            .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
            .await
            .expect("registered source shard succeeds");
        assert!(
            matches!(outcome, JobOutcome::Complete(ref code) if code.as_str() == "source_applied")
        );
    }

    assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn given_a_scheduled_source_job_when_repaired_then_recovery_runs_once_and_is_observed() {
    let fixture = source_handler_fixture().await;
    let source_health = Arc::clone(&fixture.shared.source_health);
    let observer = Arc::new(RecordingBackgroundJobObserver {
        analytics: Arc::new(BackgroundJobObserver::new(
            Arc::clone(&source_health),
            fixture.shared.worker_id.clone(),
        )),
        events: Arc::new(Mutex::new(Vec::new())),
    });
    let queue = QueueName::try_new(SourceJobKind::AuxStorage.queue_name()).expect("queue");
    let system = build_system(
        FakeStore::new(),
        source_registration(&fixture),
        b"analytics-source-repair-test",
        0x71,
        Some(Arc::clone(&observer) as Arc<dyn JobObserver>),
    );
    let id = JobId::from([0x72; 16]);
    system
        .enqueue_raw(
            source_lifecycle_job(id.as_bytes()[0], fixture.payload.clone()),
            Timestamp::ZERO,
        )
        .await
        .expect("source enqueue commits");
    let initial_claim = system
        .claim(id, Timestamp::ZERO)
        .await
        .expect("source repair seed claims");
    let retry = system
        .apply_outcome(
            initial_claim,
            JobOutcome::Retry {
                class: RetryClass::Transient,
                not_before: Some(Timestamp::from_millis(1)),
                code: OutcomeCode::try_new("repair_seed").expect("retry code"),
            },
            Timestamp::ZERO,
        )
        .await
        .expect("source repair seed is scheduled");
    let scheduled_at = match retry {
        OutcomeResult::Retried { not_before, .. } => not_before,
        other => panic!("expected retry scheduling, got {other:?}"),
    };

    let (revision, operation) = match system
        .observe_lifecycle(id)
        .await
        .expect("scheduled source job is observable")
    {
        LifecycleObservation::Current {
            record,
            operation_id: Some(operation),
            ..
        } => (record.revision, operation),
        other => panic!("expected scheduled source job, got {other:?}"),
    };
    system
        .repair_job(id, revision, operation)
        .await
        .expect("source repair commits");

    match system
        .observe_lifecycle(id)
        .await
        .expect("repaired source job is observable")
    {
        LifecycleObservation::Current { record, .. } => {
            assert_eq!(record.revision, revision + 1);
            assert!(matches!(record.state, JobState::Scheduled));
        }
        other => panic!("expected repaired scheduled source job, got {other:?}"),
    }
    assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 0);
    assert_eq!(observer.repaired_events(), vec![queue.clone()]);
    assert_eq!(
        source_health.read().await.phase,
        crate::SourcePollingPhase::Polling
    );

    let duplicate = system.repair_job(id, revision, operation).await;
    let wrong_operation = system
        .repair_job(id, revision + 1, OperationId::from([0x73; 16]))
        .await;
    assert!(matches!(
        duplicate,
        Err(bg_jobs::JobSystemError::LeaseConflict)
    ));
    assert!(matches!(
        wrong_operation,
        Err(bg_jobs::JobSystemError::LeaseLost)
    ));
    assert_eq!(observer.repaired_events(), vec![queue.clone()]);
    assert_eq!(
        source_health.read().await.phase,
        crate::SourcePollingPhase::Polling
    );

    let claim = system
        .claim(id, scheduled_at)
        .await
        .expect("repaired source job claims");
    let outcome = system
        .execute_claimed(&claim, scheduled_at, || MonotonicDeadline::ZERO)
        .await
        .expect("repaired source handler succeeds");
    system
        .complete(claim, outcome, Timestamp::from_millis(1))
        .await
        .expect("repaired source completion commits");

    assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 1);
}

#[test]
fn given_a_source_batch_when_enqueued_then_deduplication_uses_the_stable_batch_id() {
    let expires_at = Timestamp::from_millis(1_000);
    let deduplication =
        source_job_deduplication("v1-batch-id", expires_at).expect("source deduplication");

    assert_eq!(deduplication.key().as_bytes(), b"v1-batch-id");
    assert_eq!(
        deduplication.retain_until(),
        expires_at
            .checked_add(MAX_SOURCE_JOB_TIMEOUT)
            .expect("retention deadline")
    );
}

#[test]
fn given_the_drive_policy_when_built_then_claim_concurrency_and_store_work_are_bounded() {
    let request = drive_request().expect("bounded drive request");
    assert_eq!(request.max_claims().get(), 8);
    assert_eq!(request.max_concurrency().get(), 4);
    assert_eq!(
        request.max_payload_bytes_in_flight().get(),
        bg_jobs::MAX_INLINE_PAYLOAD_BYTES
    );
    assert_eq!(request.max_completion_details(), 8);
    assert_eq!(request.max_store_operations().get(), 64);
    assert!(request.stop_claiming_at().is_before(request.finish_by()));
}

#[test]
fn given_ducklake_data_paths_when_capability_is_checked_then_only_local_paths_are_supported() {
    use analytics_engine::{CatalogType, StorageBackend};

    let local = StorageBackend::DuckDb {
        path: ":memory:".to_string(),
    };
    let aux_catalog = StorageBackend::DuckLake {
        catalog: CatalogType::AuxCatalog,
        catalog_path: "catalog".to_string(),
        data_path: "data".to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    let motherduck = StorageBackend::DuckLake {
        catalog: CatalogType::MotherDuck,
        catalog_path: "catalog".to_string(),
        data_path: "data".to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    };

    assert!(analytics_engine::AnalyticsEngine::supports_fenced_backend(
        &local
    ));
    assert!(analytics_engine::AnalyticsEngine::supports_fenced_backend(
        &aux_catalog
    ));
    assert!(!analytics_engine::AnalyticsEngine::supports_fenced_backend(
        &motherduck
    ));

    for (data_path, object_storage, expected) in [
        ("s3://bucket/data", None, false),
        ("http://warehouse.example/data", None, false),
        ("https://warehouse.example/data", None, false),
        ("file:///var/lib/analytics/data", None, true),
        ("relative/data", None, true),
        (
            "relative/data",
            Some(Box::new(AnalyticsObjectStorageConfig {
                bucket: Some("analytics-test".to_string()),
                path: Some("warehouse".to_string()),
                ..AnalyticsObjectStorageConfig::default()
            })),
            false,
        ),
    ] {
        let backend = StorageBackend::DuckLake {
            catalog: CatalogType::Sqlite,
            catalog_path: "catalog.db".to_string(),
            data_path: data_path.to_string(),
            object_storage,
            catalog_settings: Default::default(),
        };
        assert_eq!(
            analytics_engine::AnalyticsEngine::supports_fenced_backend(&backend),
            expected,
            "{data_path}"
        );
    }
}

#[derive(Clone, Default)]
struct EventCollector {
    events: Arc<Mutex<Vec<JobEvent>>>,
    errors: Arc<Mutex<Vec<SafeJobErrorEvent>>>,
}

impl JobObserver for EventCollector {
    fn observe(&self, event: JobEvent) {
        self.events
            .lock()
            .expect("event collector lock")
            .push(event);
    }

    fn observe_error(&self, event: SafeJobErrorEvent) {
        self.errors
            .lock()
            .expect("error collector lock")
            .push(event);
    }
}

struct CompletingHandler;

impl JobHandler for CompletingHandler {
    fn execute<'a>(
        &'a self,
        _context: ExecutionContext<'a>,
        _payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        Box::pin(async {
            Ok(JobOutcome::Complete(
                OutcomeCode::try_new("completed").expect("test outcome is valid"),
            ))
        })
    }
}

fn lifecycle_system(store: FakeStore, observer: Arc<dyn JobObserver>) -> JobSystem<FakeStore> {
    let mut catalog = JobCatalog::new();
    catalog
        .register(
            JobSpec::new(
                JobKind::try_new("analytics.test").expect("test kind is valid"),
                NonZeroU16::new(1).expect("schema is non-zero"),
                QueueName::try_new("analytics-test").expect("queue is valid"),
                CompletingHandler,
                |payload: &Vec<u8>| {
                    Ok::<bytes::Bytes, bg_jobs::ValidationError>(bytes::Bytes::from(
                        payload.clone(),
                    ))
                },
            )
            .policy(
                EffectMode::Idempotent,
                FairnessPolicy::low_volume(),
                RetrySelection::Preset(RetryPreset::Interactive),
                Duration::from_secs(1),
            ),
        )
        .expect("handler registration is valid");
    build_system(store, catalog, b"analytics-test", 0x41, Some(observer))
}

fn lifecycle_job(id: u8) -> NewJob {
    NewJob {
        job_id: JobId::from([id; 16]),
        kind: JobKind::try_new("analytics.test").expect("test kind is valid"),
        schema_version: NonZeroU16::new(1).expect("schema is non-zero"),
        payload: axum::body::Bytes::from_static(b"payload"),
        fairness: bg_jobs::FairnessClass::low_volume(),
        ordering_key: None,
        priority: Priority::Default,
        scheduled_at: Timestamp::ZERO,
        expires_at: None,
        retry_override: None,
        deduplication: None,
    }
}

fn source_lifecycle_job(id: u8, payload: Vec<u8>) -> NewJob {
    source_job(id, SourceJobKind::AuxStorage, payload)
}

fn source_job(id: u8, source_kind: SourceJobKind, payload: Vec<u8>) -> NewJob {
    NewJob {
        job_id: JobId::from([id; 16]),
        kind: JobKind::try_new(source_kind.job_kind()).expect("source kind is valid"),
        schema_version: NonZeroU16::new(SOURCE_SCHEMA_VERSION).expect("schema is non-zero"),
        payload: axum::body::Bytes::from(payload),
        fairness: bg_jobs::FairnessClass::low_volume(),
        ordering_key: None,
        priority: Priority::Default,
        scheduled_at: Timestamp::ZERO,
        expires_at: None,
        retry_override: None,
        deduplication: None,
    }
}

fn lifecycle_drive_request() -> DriveRequest {
    DriveRequest::try_from(DriveRequestInput {
        queues: QueueSelection::all(),
        max_claims: NonZeroU32::new(1).expect("claim bound is non-zero"),
        max_concurrency: NonZeroU16::new(1).expect("concurrency bound is non-zero"),
        max_payload_bytes_in_flight: NonZeroUsize::new(1024).expect("payload bound is non-zero"),
        max_completion_details: 1,
        max_store_operations: NonZeroU32::new(64).expect("store bound is non-zero"),
        stop_claiming_at: MonotonicDeadline::from_nanos(1),
        finish_by: MonotonicDeadline::from_nanos(u64::MAX),
    })
    .expect("drive request is valid")
}

fn source_effect_dispositions() -> (
    analytics_engine::SourceEffectDisposition,
    analytics_engine::SourceEffectDisposition,
) {
    use analytics_engine::{
        AnalyticsEngine, RecordCheckpoint, SourceBatchRecord, SourceEffectAuthority,
        SourceEffectRequest, SourceEffectRequestInput, SourceEffectScope, StreamRecordBatchItem,
    };
    use analytics_fixtures::{user_item, users_manifest};

    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect source engine");
    let manifest = users_manifest();
    engine
        .ensure_manifest(&manifest)
        .expect("ensure source manifest");
    let request = || {
        SourceEffectRequest::try_new(SourceEffectRequestInput {
            manifest: &manifest,
            manifest_digest: analytics_engine::manifest_digest(&manifest),
            authority: SourceEffectAuthority::try_new([1; 16], 1).expect("authority is valid"),
            scope: SourceEffectScope::try_new("tenant_01", "source-shard")
                .expect("source scope is valid"),
            records: vec![SourceBatchRecord {
                item: StreamRecordBatchItem {
                    analytics_table_name: "users".to_string(),
                    source_table_name: "tenant_01".to_string(),
                    shard_id: Some("source-shard".to_string()),
                    system_position: None,
                    record_key: b"user-1".to_vec(),
                    record: StorageStreamRecord {
                        keys: StorageItem::new(),
                        sequence_number: "cursor-1".to_string(),
                        old_image: None,
                        new_image: Some(user_item("user-1", "user@example.test", "org-a")),
                    },
                },
                retention: None,
            }],
            checkpoints: vec![
                RecordCheckpoint::try_new("tenant_01", "source-shard", "cursor-1")
                    .expect("source checkpoint is valid"),
            ],
            privacy_policy: None,
        })
        .expect("source request is valid")
    };
    let first = engine
        .ingest_fenced_source_batch(request())
        .expect("first source effect");
    let replay = engine
        .ingest_fenced_source_batch(request())
        .expect("replayed source effect");
    (first, replay)
}

enum SourceSettlementCase {
    Destination(Result<analytics_engine::SourceEffectDisposition, BackgroundJobError>),
    Cancelled,
}

#[tokio::test]
async fn given_source_handler_settlement_matrix_when_resolved_then_only_applied_commits_progress() {
    let (applied, already_applied) = source_effect_dispositions();
    let cases = [
        (
            "Applied",
            SourceSettlementCase::Destination(Ok(applied)),
            Some("source_applied"),
            1,
        ),
        (
            "AlreadyApplied",
            SourceSettlementCase::Destination(Ok(already_applied)),
            Some("source_already_applied"),
            0,
        ),
        (
            "StaleFence",
            SourceSettlementCase::Destination(Ok(
                analytics_engine::SourceEffectDisposition::StaleFence,
            )),
            Some("source_stale_fence"),
            0,
        ),
        (
            "DestinationFailure",
            SourceSettlementCase::Destination(Err(BackgroundJobError::Destination)),
            None,
            0,
        ),
        ("Cancelled", SourceSettlementCase::Cancelled, None, 0),
    ];

    for (label, case, expected_code, expected_commits) in cases {
        match case {
            SourceSettlementCase::Destination(destination) => {
                let commits = Arc::new(AtomicUsize::new(0));
                let result = settle_source_effect(destination, {
                    let commits = Arc::clone(&commits);
                    move || async move {
                        commits.fetch_add(1, Ordering::Relaxed);
                        Ok::<(), BackgroundJobError>(())
                    }
                })
                .await;
                assert_eq!(
                    commits.load(Ordering::Relaxed),
                    expected_commits,
                    "settlement commit invariant for {label}"
                );
                if let Some(expected_code) = expected_code {
                    assert!(matches!(
                        result.expect("destination outcome settles"),
                        JobOutcome::Complete(ref code) if code.as_str() == expected_code
                    ));
                } else {
                    assert!(result.is_err(), "{label} must remain a failed job outcome");
                }
            }
            SourceSettlementCase::Cancelled => {
                let fixture = source_handler_fixture().await;
                let system = source_job_system(source_registration(&fixture));
                let claim = claim_source_job(
                    &system,
                    source_lifecycle_job(0x11, b"must not be decoded".to_vec()),
                    Timestamp::ZERO,
                )
                .await;
                claim.cancellation().cancel();
                let outcome = system
                    .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
                    .await
                    .expect("cancellation is a handled outcome");
                assert!(matches!(outcome, JobOutcome::Cancelled), "{label}");
                assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 0);
            }
        }
    }
}

#[tokio::test]
async fn given_composed_lifecycle_events_when_observed_then_source_health_is_bounded() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let _recorder_guard = metrics::set_default_local_recorder(&recorder);

    let source_health =
        std::sync::Arc::new(tokio::sync::RwLock::new(crate::SourceHealth::disabled()));
    let observer = BackgroundJobObserver::new(
        Arc::clone(&source_health),
        "analytics-test-worker".to_string(),
    );
    observer.observe(JobEvent::Claimed {
        queue: QueueName::try_new("analytics-source-storage-stream").expect("queue is valid"),
        priority: Priority::Default,
        attempt: AttemptBucket::First,
    });
    assert_eq!(
        source_health.read().await.phase,
        crate::SourcePollingPhase::Ingesting
    );

    let exported = recorder.handle().render();
    assert!(
        exported
            .lines()
            .any(|line| line.starts_with("bg_worker_items_processed_total{")
                && line.contains("outcome=\"claimed\"")
                && line.contains("queue=\"analytics-source-storage-stream\"")
                && line.ends_with(" 1")),
        "claim metrics missing: {exported}"
    );

    observer.observe(JobEvent::StoreError {
        kind: SafeJobErrorKind::UnknownCommit,
    });
    let health = source_health.read().await;
    assert_eq!(health.phase, crate::SourcePollingPhase::Degraded);
    assert_eq!(
        health.last_error.as_deref(),
        Some("background source job lifecycle event reported an error")
    );
}

#[tokio::test]
async fn given_normal_drive_stop_reasons_when_observed_then_source_health_remains_unchanged() {
    let source_health =
        std::sync::Arc::new(tokio::sync::RwLock::new(crate::SourceHealth::starting(1)));
    let observer = BackgroundJobObserver::new(
        Arc::clone(&source_health),
        "analytics-test-worker".to_string(),
    );

    for reason in [
        DriveStopReason::ClaimsExhausted,
        DriveStopReason::Deadline,
        DriveStopReason::Shutdown,
        DriveStopReason::NoVisibleWork,
        DriveStopReason::Capacity,
    ] {
        observer.observe(JobEvent::DriveStopped { reason });

        let health = source_health.read().await;
        assert!(matches!(health.status, crate::SourceHealthStatus::Starting));
        assert_eq!(health.phase, crate::SourcePollingPhase::Starting);
        assert_eq!(health.last_error_at_ms, None);
        assert_eq!(health.last_error, None);
        assert_eq!(health.total_poll_errors, 0);
    }
}

#[tokio::test]
async fn given_store_failure_when_drive_stops_then_source_health_is_degraded() {
    let source_health =
        std::sync::Arc::new(tokio::sync::RwLock::new(crate::SourceHealth::starting(1)));
    let observer = BackgroundJobObserver::new(
        Arc::clone(&source_health),
        "analytics-test-worker".to_string(),
    );

    observer.observe(JobEvent::DriveStopped {
        reason: DriveStopReason::StoreFailure,
    });

    let health = source_health.read().await;
    assert!(matches!(health.status, crate::SourceHealthStatus::Degraded));
    assert_eq!(health.phase, crate::SourcePollingPhase::Degraded);
    assert_eq!(
        health.last_error.as_deref(),
        Some("background source job lifecycle event reported an error")
    );
    assert_eq!(health.total_poll_errors, 1);
}

#[tokio::test]
async fn given_claim_and_completion_when_job_system_runs_then_receipt_and_events_are_visible() {
    let collector = EventCollector::default();
    let system = lifecycle_system(FakeStore::new(), Arc::new(collector.clone()));
    let id = JobId::from([1; 16]);
    system
        .enqueue_raw(lifecycle_job(1), Timestamp::ZERO)
        .await
        .expect("enqueue commits");
    let report = system
        .drive(lifecycle_drive_request(), Timestamp::ZERO, || {
            MonotonicDeadline::ZERO
        })
        .await
        .expect("drive commits");
    assert_eq!(report.counts.claimed, 1);

    assert!(matches!(
        system
            .observe_lifecycle(id)
            .await
            .expect("receipt is readable"),
        LifecycleObservation::Terminal(_)
    ));
    let events = collector.events.lock().expect("event collector lock");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, JobEvent::Enqueued { .. }))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, JobEvent::Claimed { .. }))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, JobEvent::Completed { .. }))
    );
}

#[tokio::test]
async fn given_unknown_completion_when_reconciling_then_receipt_controls_replay() {
    let store = FakeStore::new();
    let system = lifecycle_system(store.clone(), Arc::new(EventCollector::default()));
    let id = JobId::from([2; 16]);
    system
        .enqueue_raw(lifecycle_job(2), Timestamp::ZERO)
        .await
        .expect("enqueue commits");
    let claim = system
        .claim(id, Timestamp::ZERO)
        .await
        .expect("claim commits");
    let operation = claim.operation_id();
    store
        .queue_unknown_after_commit()
        .expect("unknown result is queued");

    system
        .complete(
            claim,
            JobOutcome::Complete(OutcomeCode::try_new("completed").expect("outcome is valid")),
            Timestamp::from_millis(1),
        )
        .await
        .expect("receipt proves completion without replay");
    assert_eq!(
        system
            .reconcile(id, operation)
            .await
            .expect("reconciliation succeeds"),
        ReconciliationOutcome::AlreadyCommitted
    );
}

#[tokio::test]
async fn given_registered_source_handler_when_completion_ack_is_unknown_then_receipt_reconciles_without_replay()
 {
    let fixture = source_handler_fixture().await;
    let store = FakeStore::new();
    let system = build_system(
        store.clone(),
        job_catalog(Arc::clone(&fixture.shared), None, MAX_SOURCE_JOB_TIMEOUT)
            .expect("source handler catalog is valid"),
        b"analytics-source-test",
        0x51,
        None,
    );
    let id = JobId::from([0x52; 16]);
    system
        .enqueue_raw(
            source_lifecycle_job(id.as_bytes()[0], fixture.payload.clone()),
            Timestamp::ZERO,
        )
        .await
        .expect("source enqueue commits");
    let claim = system
        .claim(id, Timestamp::ZERO)
        .await
        .expect("source claim commits");
    let operation = claim.operation_id();
    let outcome = system
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("source handler succeeds");
    assert!(matches!(outcome, JobOutcome::Complete(ref code) if code.as_str() == "source_applied"));
    store
        .queue_unknown_after_commit()
        .expect("unknown completion result is queued");
    system
        .complete(claim, outcome, Timestamp::from_millis(1))
        .await
        .expect("source completion reconciles");

    assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 1);
    assert_eq!(
        system
            .reconcile(id, operation)
            .await
            .expect("source reconciliation succeeds"),
        ReconciliationOutcome::AlreadyCommitted
    );
    assert_eq!(fixture.shared.progress_commits.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn given_unknown_before_completion_when_reconciling_then_handler_must_not_be_replayed() {
    let store = FakeStore::new();
    let collector = EventCollector::default();
    let system = lifecycle_system(store.clone(), Arc::new(collector.clone()));
    let id = JobId::from([6; 16]);
    system
        .enqueue_raw(lifecycle_job(6), Timestamp::ZERO)
        .await
        .expect("enqueue commits");
    let claim = system
        .claim(id, Timestamp::ZERO)
        .await
        .expect("claim commits");
    let operation = claim.operation_id();
    store
        .queue_unknown_before_commit()
        .expect("first unknown result is queued");
    store
        .queue_unknown_before_commit()
        .expect("retry unknown result is queued");

    assert!(matches!(
        system
            .complete(
                claim,
                JobOutcome::Complete(OutcomeCode::try_new("completed").expect("outcome is valid"),),
                Timestamp::from_millis(1),
            )
            .await,
        Err(bg_jobs::JobSystemError::UnknownCommit)
    ));
    assert_eq!(
        system
            .reconcile(id, operation)
            .await
            .expect("reconciliation succeeds"),
        ReconciliationOutcome::RetryCommit
    );
    assert!(
        !collector
            .events
            .lock()
            .expect("event collector lock")
            .iter()
            .any(|event| matches!(event, JobEvent::Completed { .. }))
    );
}

#[tokio::test]
async fn given_expired_claim_when_reclaimed_then_fence_advances_and_reclaim_is_observed() {
    let collector = EventCollector::default();
    let system = lifecycle_system(FakeStore::new(), Arc::new(collector.clone()));
    let id = JobId::from([3; 16]);
    system
        .enqueue_raw(lifecycle_job(3), Timestamp::ZERO)
        .await
        .expect("enqueue commits");
    let first = system
        .claim(id, Timestamp::ZERO)
        .await
        .expect("claim commits");
    let first_fence = first.authority().expect("lease authority").fence.get();
    assert_eq!(
        system
            .expire_job(id, Timestamp::from_millis(1_001))
            .await
            .expect("expiry commits"),
        ExpiryResult::LeaseExpired
    );
    let second = system
        .claim(id, Timestamp::from_millis(1_001))
        .await
        .expect("reclaimed claim commits");
    let second_fence = second.authority().expect("lease authority").fence.get();
    assert!(second_fence > first_fence);
    assert!(matches!(
        system.observe_lifecycle(id).await.expect("reclaimed job is observable"),
        LifecycleObservation::Current { record, .. }
            if matches!(record.state, JobState::Leased(ref lease) if lease.fence.get() == second_fence)
    ));
}

#[tokio::test]
async fn given_dead_lettered_claim_when_redriven_then_new_generation_is_observed() {
    let collector = EventCollector::default();
    let system = lifecycle_system(FakeStore::new(), Arc::new(collector.clone()));
    let id = JobId::from([4; 16]);
    system
        .enqueue_raw(lifecycle_job(4), Timestamp::ZERO)
        .await
        .expect("enqueue commits");
    let claim = system
        .claim(id, Timestamp::ZERO)
        .await
        .expect("claim commits");
    system
        .apply_outcome(
            claim,
            JobOutcome::DeadLetter {
                code: OutcomeCode::try_new("permanent").expect("outcome is valid"),
            },
            Timestamp::from_millis(1),
        )
        .await
        .expect("dead-letter commits");
    let (revision, operation) = match system
        .observe_lifecycle(id)
        .await
        .expect("dead letter is observable")
    {
        LifecycleObservation::Current {
            record,
            operation_id: Some(operation),
            ..
        } => (record.revision, operation),
        other => panic!("expected dead-letter record, got {other:?}"),
    };
    let result = system
        .redrive_job(
            id,
            RedriveRequest::new(
                revision,
                operation,
                RedriveExecution::ReuseExecutionId,
                Timestamp::from_millis(2),
            ),
        )
        .await
        .expect("redrive commits");
    assert_eq!(result.generation, 2);
    assert!(
        collector
            .events
            .lock()
            .expect("event collector lock")
            .iter()
            .any(|event| matches!(event, JobEvent::Redriven { .. }))
    );
}

#[tokio::test]
async fn given_a_claim_cancellation_when_acknowledged_then_progress_is_not_replayed() {
    let collector = EventCollector::default();
    let system = lifecycle_system(FakeStore::new(), Arc::new(collector.clone()));
    let id = JobId::from([5; 16]);
    system
        .enqueue_raw(lifecycle_job(5), Timestamp::ZERO)
        .await
        .expect("enqueue commits");
    let claim = system
        .claim(id, Timestamp::ZERO)
        .await
        .expect("claim commits");
    assert_eq!(
        system
            .request_job_cancellation(&claim, Timestamp::from_millis(1))
            .await
            .expect("cancellation request commits"),
        CancellationResult::Requested
    );
    assert_eq!(
        system
            .cancel_claim(claim, Timestamp::from_millis(1))
            .await
            .expect("cancellation acknowledgement commits"),
        CancellationResult::Cancelled
    );
    assert!(matches!(
        system
            .observe_lifecycle(id)
            .await
            .expect("cancelled job is observable"),
        LifecycleObservation::Current { record, .. }
            if matches!(record.state, JobState::Cancelled)
    ));
    let events = collector.events.lock().expect("event collector lock");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, JobEvent::CancellationRequested { .. }))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, JobEvent::Cancelled { .. }))
    );
}

async fn serve_idle_source_endpoint() -> (String, Arc<AtomicUsize>, oneshot::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind idle source endpoint");
    let addr = listener.local_addr().expect("idle source endpoint address");
    let request_count = Arc::new(AtomicUsize::new(0));
    let captured_count = Arc::clone(&request_count);
    let (poll_finished_tx, poll_finished_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut describe_socket, _) = listener
            .accept()
            .await
            .expect("accept idle source describe request");
        read_source_request(&mut describe_socket).await;
        captured_count.fetch_add(1, Ordering::AcqRel);
        write_source_response(
            &mut describe_socket,
            r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
        )
        .await;

        let (mut poll_socket, _) = listener
            .accept()
            .await
            .expect("accept idle source poll request");
        read_source_request(&mut poll_socket).await;
        captured_count.fetch_add(1, Ordering::AcqRel);
        write_source_response(
            &mut poll_socket,
            r#"{"Records":[],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
        )
        .await;
        poll_finished_tx
            .send(())
            .expect("idle source poll receiver is waiting");
    });
    (format!("http://{addr}"), request_count, poll_finished_rx)
}

async fn serve_shutdown_source_endpoint() -> (
    String,
    Arc<AtomicUsize>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind source shutdown endpoint");
    let addr = listener
        .local_addr()
        .expect("source shutdown endpoint address");
    let request_count = Arc::new(AtomicUsize::new(0));
    let captured_count = Arc::clone(&request_count);
    let (poll_started_tx, poll_started_rx) = oneshot::channel();
    let (release_poll_tx, release_poll_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut describe_socket, _) = listener
            .accept()
            .await
            .expect("accept source describe request");
        read_source_request(&mut describe_socket).await;
        captured_count.fetch_add(1, Ordering::AcqRel);
        write_source_response(
            &mut describe_socket,
            r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
        )
        .await;

        let (mut poll_socket, _) = listener.accept().await.expect("accept source poll request");
        read_source_request(&mut poll_socket).await;
        captured_count.fetch_add(1, Ordering::AcqRel);
        poll_started_tx
            .send(())
            .expect("source poll receiver is waiting");
        let _ = release_poll_rx.await;
        write_source_response(
            &mut poll_socket,
            r#"{"Records":[{"SourceTableName":"tenant_01","Keys":{},"SequenceNumber":"seq-1","SystemPosition":{"ShardId":"logical-shard","LineageId":"lineage-a","Offset":"1"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
        )
        .await;
    });
    (
        format!("http://{addr}"),
        request_count,
        poll_started_rx,
        release_poll_tx,
    )
}

#[derive(Clone)]
struct BlockingStore {
    inner: FakeStore,
    state: Arc<BlockingStoreState>,
}

struct BlockingStoreState {
    block_next_payload_commit: std::sync::atomic::AtomicBool,
    commit_started: tokio::sync::Notify,
    release_commit: tokio::sync::Notify,
}

impl BlockingStore {
    fn new(inner: FakeStore) -> Self {
        Self {
            inner,
            state: Arc::new(BlockingStoreState {
                block_next_payload_commit: std::sync::atomic::AtomicBool::new(false),
                commit_started: tokio::sync::Notify::new(),
                release_commit: tokio::sync::Notify::new(),
            }),
        }
    }

    fn inner(&self) -> &FakeStore {
        &self.inner
    }

    fn block_next_payload_commit(&self) {
        self.state
            .block_next_payload_commit
            .store(true, Ordering::Release);
    }

    async fn wait_for_payload_commit(&self) {
        self.state.commit_started.notified().await;
    }

    fn release_payload_commit(&self) {
        self.state.release_commit.notify_one();
    }

    fn is_payload_commit(transaction: &StoreTransaction) -> bool {
        let namespace =
            Namespace::try_new(b"analytics-api:bg-jobs:v1").expect("job namespace is valid");
        transaction.mutations().iter().any(|mutation| {
            KeyCodec::decode(&namespace, mutation.key())
                .is_ok_and(|key| key.tag == KeyTag::PayloadChunk)
        })
    }
}

impl JobStore for BlockingStore {
    type Error = bg_jobs::test_support::FakeStoreError;

    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    fn get(
        &self,
        key: StoreKey,
    ) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send + '_ {
        let inner = self.inner.clone();
        async move { inner.get(key).await }
    }

    fn get_many<'a>(
        &'a self,
        keys: &'a [StoreKey],
    ) -> impl Future<Output = Result<Vec<Option<Bytes>>, Self::Error>> + Send + 'a {
        let inner = self.inner.clone();
        async move { inner.get_many(keys).await }
    }

    fn read_range(
        &self,
        request: RangeRequest,
    ) -> impl Future<Output = Result<RangePage, Self::Error>> + Send + '_ {
        let inner = self.inner.clone();
        async move { inner.read_range(request).await }
    }

    fn commit<'a>(
        &'a self,
        transaction: &'a StoreTransaction,
    ) -> impl Future<Output = Result<CommitOutcome, Self::Error>> + Send + 'a {
        let store = self.clone();
        async move {
            let should_block = Self::is_payload_commit(transaction)
                && store
                    .state
                    .block_next_payload_commit
                    .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
            if should_block {
                store.state.commit_started.notify_one();
                store.state.release_commit.notified().await;
            }
            store.inner.commit(transaction).await
        }
    }
}

async fn read_source_request(socket: &mut tokio::net::TcpStream) {
    let mut buffer = [0_u8; 4096];
    let _ = socket.read(&mut buffer).await.expect("read source request");
}

async fn write_source_response(socket: &mut tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: \
         close\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write source response");
}

#[test]
fn given_one_image_with_many_destinations_when_payload_round_trips_then_image_is_stored_once_and_all_routes_survive()
 {
    let mut source = record("events", "shard-1", "record-a");
    source.record.new_image = Some(
        [(
            "data".to_owned(),
            analytics_contract::StorageValue::S("x".repeat(16_384)),
        )]
        .into_iter()
        .collect(),
    );
    let records = (0..23)
        .map(|index| PolledRecord {
            analytics_table_names: vec![format!("destination_{index}")],
            ..source.clone()
        })
        .collect::<Vec<_>>();
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            records,
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("fan-out payload");
    let encoded = payload.encode().expect("compact inline payload");
    assert!(
        encoded.len() < 32_768,
        "image must not be repeated 23 times: {}",
        encoded.len()
    );
    let json: serde_json::Value = serde_json::to_value(&payload).expect("payload JSON");
    assert_eq!(json["records"].as_array().expect("records").len(), 1);
    assert_eq!(
        json["records"][0]["analytics_table_names"]
            .as_array()
            .expect("destinations")
            .len(),
        23
    );
    let decoded = SourceJobPayload::decode(&encoded)
        .expect("decode")
        .into_batch();
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(
        decoded.records[0].analytics_table_names,
        (0..23)
            .map(|index| format!("destination_{index}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(decoded.records[0].record, source.record);
}

#[test]
fn given_equal_source_keys_with_different_images_when_payload_is_built_then_both_changes_are_preserved()
 {
    let first = record("events", "shard-1", "record-a");
    let mut second = first.clone();
    second.record.new_image = Some(StorageItem::new());
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            vec![first.clone(), second.clone()],
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("distinct source images");
    let roundtrip = SourceJobPayload::decode(&payload.encode().expect("encode"))
        .expect("decode")
        .into_batch();
    assert_eq!(roundtrip.records.len(), 2);
    assert_eq!(roundtrip.records[0].record, first.record);
    assert_eq!(roundtrip.records[1].record, second.record);
}

#[test]
fn given_duplicate_destinations_when_payload_is_built_then_duplicate_effects_are_rejected() {
    let source = record("events", "shard-1", "record-a");
    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            batch(
                vec![source.clone(), source],
                vec![checkpoint("events", "shard-1", "checkpoint-a")]
            ),
            1,
            test_manifest_digest()
        )
        .is_err()
    );
}

#[test]
fn given_compact_records_with_excessive_fanout_when_payload_is_built_then_routed_record_bound_is_preserved()
 {
    let source = record("events", "shard-1", "record-a");
    let records = (0..=crate::constants::MAX_SOURCE_RECORDS)
        .map(|index| PolledRecord {
            analytics_table_names: vec![format!("destination_{index}")],
            ..source.clone()
        })
        .collect();
    assert!(
        SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            batch(
                records,
                vec![checkpoint("events", "shard-1", "checkpoint-a")]
            ),
            1,
            test_manifest_digest()
        )
        .is_err()
    );
}

#[test]
fn given_unsupported_payload_version_when_decoded_then_format_is_rejected() {
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            vec![record("events", "shard-1", "record-a")],
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("payload");
    let mut json: serde_json::Value = serde_json::to_value(&payload).expect("payload JSON");
    assert_eq!(json["version"], "v1");
    assert!(
        json["batch_id"]
            .as_str()
            .expect("batch identity")
            .starts_with("v1-")
    );
    let mut obsolete_envelope = payload.encode().expect("V1 payload encoding");
    assert_eq!(&obsolete_envelope[..4], b"ASJ1");
    obsolete_envelope[..4].copy_from_slice(b"ASJ2");
    assert!(SourceJobPayload::decode(&obsolete_envelope).is_err());
    json["version"] = serde_json::json!("v2");
    assert!(SourceJobPayload::decode(&encode_source_json(&json)).is_err());
}

#[tokio::test]
async fn given_oversized_source_image_when_typed_job_is_enqueued_then_size_error_survives_for_page_recovery()
 {
    let fixture = source_handler_fixture().await;
    let mut bundle =
        JobCatalogBundle::try_new(Arc::clone(&fixture.shared), None, MAX_SOURCE_JOB_TIMEOUT)
            .expect("catalog");
    let handle = bundle
        .source_handles
        .remove(&SourceJobKind::StorageStream)
        .expect("typed source handle");
    let system = source_job_system(bundle.catalog);
    let mut source = record("events", "shard-1", "record-a");
    source.record.new_image = Some(
        [(
            "data".to_owned(),
            analytics_contract::StorageValue::S(source_noise(bg_jobs::MAX_INLINE_PAYLOAD_BYTES)),
        )]
        .into_iter()
        .collect(),
    );
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            vec![source],
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("structurally valid payload");
    let result = system
        .enqueue_typed(
            &handle,
            payload,
            bg_jobs::EnqueueOptions::new(bg_jobs::FairnessClass::low_volume()),
            Timestamp::ZERO,
        )
        .await;
    assert!(
        matches!(result, Err(bg_jobs::JobSystemError::Validation(bg_jobs::ValidationError::TooLong {
        field: "source job payload", maximum: bg_jobs::MAX_INLINE_PAYLOAD_BYTES, actual,
    })) if actual > bg_jobs::MAX_INLINE_PAYLOAD_BYTES)
    );
}

#[tokio::test]
async fn given_inline_payload_above_store_transaction_limit_when_enqueued_then_typed_capacity_error_is_returned()
 {
    let fixture = source_handler_fixture().await;
    let mut bundle =
        JobCatalogBundle::try_new(Arc::clone(&fixture.shared), None, MAX_SOURCE_JOB_TIMEOUT)
            .expect("catalog");
    let handle = bundle
        .source_handles
        .remove(&SourceJobKind::StorageStream)
        .expect("typed source handle");
    let defaults = FakeStore::new().capabilities();
    let capabilities = StoreCapabilities::try_from(bg_jobs::store::StoreCapabilitiesInput {
        level: defaults.level(),
        max_key_bytes: defaults.max_key_bytes(),
        max_value_bytes: defaults.max_value_bytes(),
        max_get_many_keys: defaults.max_get_many_keys(),
        max_get_many_key_bytes: defaults.max_get_many_key_bytes(),
        max_transaction_bytes: NonZeroUsize::new(16_384).expect("transaction limit"),
        max_transaction_keys: defaults.max_transaction_keys(),
        max_range_items: defaults.max_range_items(),
        max_range_bytes: defaults.max_range_bytes(),
        max_transaction_duration: defaults.max_transaction_duration(),
    })
    .expect("bounded store capabilities");
    let system = build_system(
        FakeStore::with_capabilities(capabilities),
        bundle.catalog,
        b"analytics-source-capacity-test",
        0x9b,
        None,
    );
    let mut source = record("events", "shard-1", "record-a");
    source.record.new_image = Some(
        [(
            "data".to_owned(),
            analytics_contract::StorageValue::S(source_noise(20_000)),
        )]
        .into_iter()
        .collect(),
    );
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            vec![source],
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("valid source payload");
    assert!(payload.encode().expect("inline payload").len() < bg_jobs::MAX_INLINE_PAYLOAD_BYTES);
    let result = system
        .enqueue_typed(
            &handle,
            payload,
            bg_jobs::EnqueueOptions::new(bg_jobs::FairnessClass::low_volume()),
            Timestamp::ZERO,
        )
        .await;
    assert!(matches!(
        result,
        Err(bg_jobs::JobSystemError::StoreContract(
            bg_jobs::store::StoreContractError::CapabilityExceeded("transaction bytes")
        ))
    ));
}

fn encode_source_json(value: &serde_json::Value) -> Vec<u8> {
    let json = serde_json::to_vec(value).expect("source payload JSON");
    let mut encoded = crate::constants::SOURCE_PAYLOAD_HEADER.to_vec();
    encoded.extend_from_slice(&(json.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&lz4_flex::block::compress(&json));
    encoded
}

fn source_noise(length: usize) -> String {
    let mut state = 0xabcddcba12345678u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

#[test]
fn given_large_repeated_images_when_payload_is_encoded_then_every_image_roundtrips_within_durable_bounds()
 {
    let mut source = record("events", "shard-1", "record-a");
    let image = [(
        "data".to_owned(),
        analytics_contract::StorageValue::S("x".repeat(512 * 1024)),
    )]
    .into_iter()
    .collect();
    source.record.old_image = Some(image);
    source.record.new_image = source.record.old_image.clone();
    let expected = source.record.clone();
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            vec![source],
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("payload");
    let encoded = payload.encode().expect("bounded compressed payload");
    assert!(encoded.len() < 16_384);
    let decoded = SourceJobPayload::decode(&encoded)
        .expect("decode")
        .into_batch();
    assert_eq!(decoded.records[0].record, expected);
}

#[test]
fn given_maximum_sized_escaped_images_when_compressed_then_complete_record_roundtrips() {
    let mut source = record("events", "shard-1", "record-a");
    let image = [(
        "data".to_owned(),
        analytics_contract::StorageValue::S(
            "\0".repeat(storage_types::MAX_ITEM_SIZE_BYTES - 1_024),
        ),
    )]
    .into_iter()
    .collect();
    source.record.old_image = Some(image);
    source.record.new_image = source.record.old_image.clone();
    let expected = source.record.clone();
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            vec![source],
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("complete source payload");
    let encoded = payload.encode().expect("maximum escaped images fit");
    let decoded_length = u32::from_be_bytes(encoded[4..8].try_into().expect("length"));
    assert!(decoded_length > 4 * 1_024 * 1_024);
    assert!(encoded.len() <= bg_jobs::MAX_INLINE_PAYLOAD_BYTES);
    let decoded = SourceJobPayload::decode(&encoded)
        .expect("bounded decode")
        .into_batch();
    assert_eq!(decoded.records[0].record, expected);
}

#[test]
fn given_invalid_compression_envelopes_when_payload_is_decoded_then_all_fail_closed() {
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            vec![record("events", "shard-1", "record-a")],
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("payload");
    let encoded = payload.encode().expect("encode");
    let mut too_large = encoded.clone();
    too_large[4..8]
        .copy_from_slice(&((crate::constants::MAX_SOURCE_DECODED_BYTES + 1) as u32).to_be_bytes());
    let mut too_small = encoded.clone();
    too_small[4..8].copy_from_slice(&1u32.to_be_bytes());
    let mut wrong_size = encoded.clone();
    let length = u32::from_be_bytes(wrong_size[4..8].try_into().expect("length"));
    wrong_size[4..8].copy_from_slice(&(length + 1).to_be_bytes());
    let mut corrupt = encoded.clone();
    corrupt.truncate(8);
    for invalid in [
        too_large,
        too_small,
        wrong_size,
        corrupt,
        serde_json::to_vec(&payload).expect("obsolete plain JSON"),
    ] {
        assert!(SourceJobPayload::decode(&invalid).is_err());
    }
}

#[test]
fn given_overlarge_compressible_images_when_encoded_then_decoded_memory_bound_is_enforced() {
    let mut source = record("events", "shard-1", "record-a");
    source.record.new_image = Some(
        [(
            "data".to_owned(),
            analytics_contract::StorageValue::S(
                "x".repeat(crate::constants::MAX_SOURCE_DECODED_BYTES),
            ),
        )]
        .into_iter()
        .collect(),
    );
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            vec![source],
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("payload");
    assert!(matches!(
        payload.encode(),
        Err(BackgroundJobError::PayloadTooLarge {
            maximum: crate::constants::MAX_SOURCE_DECODED_BYTES,
            ..
        })
    ));
}

#[test]
fn given_representative_user_changes_when_compressed_then_wire_bytes_shrink_and_records_roundtrip()
{
    let records = (0..128)
        .map(|index| {
            let id = format!("user-{index}");
            let mut source = record("events", "shard-1", &id);
            source.record.old_image = Some(user_item(&id, &format!("{id}@example.test"), "org-a"));
            source.record.new_image = Some(user_item(&id, &format!("{id}@example.test"), "org-b"));
            source
        })
        .collect();
    let payload = SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        batch(
            records,
            vec![checkpoint("events", "shard-1", "checkpoint-a")],
        ),
        1,
        test_manifest_digest(),
    )
    .expect("representative payload");
    let plain = serde_json::to_vec(&payload).expect("plain JSON");
    let encoded = payload.encode().expect("compressed payload");
    assert!(encoded.len() < plain.len() / 2);
    let restored = SourceJobPayload::decode(&encoded).expect("decode");
    assert_eq!(
        serde_json::to_value(&restored).unwrap(),
        serde_json::to_value(&payload).unwrap()
    );
    for sample in 0..3 {
        let started = std::time::Instant::now();
        for _ in 0..20 {
            payload.validate().expect("validate");
            std::hint::black_box(serde_json::to_vec(&payload).expect("plain encode"));
            let decoded: SourceJobPayload = serde_json::from_slice(&plain).expect("plain decode");
            decoded.validate().expect("plain validation");
            std::hint::black_box(decoded);
        }
        let plain_elapsed = started.elapsed();
        let started = std::time::Instant::now();
        for _ in 0..20 {
            let bytes = payload.encode().expect("encode");
            std::hint::black_box(SourceJobPayload::decode(&bytes).expect("decode"));
        }
        eprintln!(
            "source payload sample={sample} records=128 plain_bytes={} compressed_bytes={} \
             plain_roundtrip_us={} compressed_roundtrip_us={}",
            plain.len(),
            encoded.len(),
            plain_elapsed.as_micros(),
            started.elapsed().as_micros()
        );
    }
}
