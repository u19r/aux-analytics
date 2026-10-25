use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use analytics_contract::{
    AnalyticsManifest, RetentionPolicy, RetentionTimestamp, StorageItem, StorageStreamRecord,
    StorageValue,
};
use analytics_engine::{AnalyticsEngine, CatalogType, StorageBackend};
use analytics_fixtures::user_item;
use bg_jobs::{
    DriveStopReason, FairnessClass, JobCatalog, JobId, JobKind, JobOutcome, OwnerId, Priority,
    QueueName, QueueSelection, RetryPreset, RetrySelection, Timestamp,
    observer::{JobEvent, JobObserver},
    runtime::{JobSystem, MonotonicDeadline},
    test_support::{
        FakeStore,
        codec::{ValueCodec, ValueTag},
        raw::{ClaimedJob, DriveRequest, DriveRequestInput, NewJob, ReconciliationOutcome},
    },
};
use config::{
    AnalyticsObjectStorageConfig, AnalyticsRetentionConfig, AnalyticsRetentionTableConfig,
    RetentionDurationSelector, RetentionTimestampConfig, TenantRetentionGetItemRequest,
    TenantRetentionPolicyConfig, TenantRetentionPolicyRequest, TenantRetentionPolicySource,
};
use storage_types::{AttributeValue, KeyAttributes};

use crate::{
    AnalyticsEngineAccess, RetentionHealth, RetentionHealthStatus,
    background_jobs::{
        BackgroundJobObserver, BackgroundJobRuntime, JobCatalogBundle, RetentionJobPayload,
        SourceJobKind, SourceJobShared, job_catalog, test_support::build_system,
    },
    constants::{
        MAX_RETENTION_DEADLINE_AT_MS, MAX_RETENTION_JOB_TIMEOUT, RETENTION_JOB_KIND,
        RETENTION_SCHEMA_VERSION,
    },
    retention::RetentionRuntime,
};

struct RetentionHandlerFixture {
    shared: Arc<SourceJobShared>,
    engine: AnalyticsEngineAccess,
    retention: Arc<RetentionRuntime>,
    health: Arc<tokio::sync::RwLock<RetentionHealth>>,
    payload: Vec<u8>,
}

#[tokio::test]
async fn given_valid_retention_payload_when_round_tripped_then_contract_is_preserved() {
    let payload = retention_payload("users", 2_500, 100, future_deadline(), None, 2_500)
        .expect("valid retention payload");
    let encoded = payload.encode().expect("encode retention payload");
    let decoded = RetentionJobPayload::decode(&encoded).expect("decode retention payload");

    assert_eq!(decoded.analytics_table_name(), "users");
    assert_eq!(decoded.cutoff_ms(), 2_500);
    assert_eq!(decoded.batch_size(), 100);
    assert_eq!(decoded.cursor(), None);
    let value: serde_json::Value = serde_json::from_slice(&encoded).expect("payload JSON");
    assert_eq!(value["version"], "v1");
    assert!(value.get("execution_id").is_none());
    assert!(value.get("operation_id").is_none());
}

#[tokio::test]
async fn given_unknown_or_tampered_retention_payload_fields_when_decoded_then_it_fails_closed() {
    let payload = retention_payload("users", 2_500, 100, future_deadline(), None, 2_500)
        .expect("valid retention payload");
    let mut value: serde_json::Value =
        serde_json::from_slice(&payload.encode().expect("encode payload")).expect("payload JSON");
    value["unexpected"] = serde_json::Value::Bool(true);
    assert!(
        RetentionJobPayload::decode(
            serde_json::to_vec(&value)
                .expect("tampered JSON")
                .as_slice()
        )
        .is_err()
    );

    let mut value: serde_json::Value =
        serde_json::from_slice(&payload.encode().expect("encode payload")).expect("payload JSON");
    value["batch_id"] = serde_json::Value::String("tampered".to_string());
    assert!(
        RetentionJobPayload::decode(
            serde_json::to_vec(&value)
                .expect("tampered JSON")
                .as_slice()
        )
        .is_err()
    );
}

#[tokio::test]
async fn given_out_of_bounds_retention_payload_values_when_built_then_each_is_rejected() {
    assert!(retention_payload("users", -1, 1, future_deadline(), None, 1).is_err());
    assert!(retention_payload("users", 1, 0, future_deadline(), None, 1).is_err());
    assert!(retention_payload("users", 1, 1, 0, None, 1).is_err());
    assert!(retention_payload("users", 1, 1, MAX_RETENTION_DEADLINE_AT_MS + 1, None, 1).is_err());
    assert!(retention_payload("users\n", 1, 1, future_deadline(), None, 1).is_err());
    assert!(retention_payload("users", 1, 1, future_deadline(), Some("\n"), 1).is_err());
}

#[tokio::test]
async fn given_retention_config_when_catalog_is_built_then_it_has_a_typed_handle() {
    let fixture = retention_handler_fixture(false).await;
    let bundle = retention_runtime_registration(&fixture);
    let handle = bundle
        .retention_handle
        .expect("retention handler has a typed handle");

    assert_eq!(bundle.source_handles.len(), SourceJobKind::ALL.len());
    assert_eq!(handle.kind().as_str(), RETENTION_JOB_KIND);
    assert_eq!(handle.schema_version().get(), RETENTION_SCHEMA_VERSION);
}

#[test]
fn given_sqlite_catalog_with_object_storage_when_capability_is_checked_then_it_is_rejected() {
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: "catalog.db".to_string(),
        data_path: "data".to_string(),
        object_storage: Some(Box::new(AnalyticsObjectStorageConfig {
            bucket: Some("analytics-test".to_string()),
            path: Some("warehouse".to_string()),
            ..AnalyticsObjectStorageConfig::default()
        })),
        catalog_settings: Default::default(),
    };

    assert!(!analytics_engine::AnalyticsEngine::supports_fenced_backend(
        &backend
    ));
}

#[tokio::test]
async fn given_incomplete_retention_when_scheduler_restarts_then_batches_and_overdue_occurrences_finish()
 {
    let mut fixture = retention_handler_fixture(true).await;
    let mut config = retention_config();
    config.delete_batch_size = 1;
    fixture.retention = Arc::new(
        RetentionRuntime::from_config(&config)
            .await
            .unwrap()
            .unwrap(),
    );
    seed_configured_expired_user(&fixture, "expired-b").await;
    seed_configured_expired_user(&fixture, "expired-c").await;
    let store = FakeStore::new();
    let base = u64::try_from(crate::clock::now_ms()).unwrap() - 20_000;
    let clock = Arc::new(TestClock::new(base));
    let runtime_a = retention_runtime_fixture(&fixture, store.clone(), 0x81, clock.clone()).await;
    // Recover the occurrence authorized by the fixture before any enqueue.
    assert_eq!(
        runtime_a.materialize_retention_jobs().await,
        Some(Duration::from_secs(1))
    );
    runtime_a.materialize_retention_jobs().await;
    assert_eq!(retention_payloads(&store).len(), 1);
    runtime_a.runtime.drive_once().await.unwrap();
    assert_eq!(fixture.health.read().await.total_rows_deleted, 1);
    seed_configured_expired_user(&fixture, "expired-0").await;
    drop(runtime_a);
    let runtime_b = retention_runtime_fixture(&fixture, store.clone(), 0x82, clock.clone()).await;
    for expected in [2, 3] {
        assert_eq!(
            runtime_b.materialize_retention_jobs().await,
            Some(Duration::from_secs(1))
        );
        runtime_b.runtime.drive_once().await.unwrap();
        assert_eq!(fixture.health.read().await.total_rows_deleted, expected);
    }
    // The late lower key belongs to the next occurrence, which starts at None.
    assert!(
        fixture
            .engine
            .with_read(|engine| engine.find_pending_retention_sweep("users"))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    runtime_b.materialize_retention_jobs().await;
    runtime_b.runtime.drive_once().await.unwrap();
    assert_eq!(fixture.health.read().await.total_rows_deleted, 4);
    clock.set(base + 16_000);
    for _ in 0..3 {
        runtime_b.materialize_retention_jobs().await;
        runtime_b.runtime.drive_once().await.unwrap();
    }
    let schedules = fixture
        .engine
        .with_read(|engine| {
            let mut statement = engine.connection().prepare(
                "SELECT schedule_at_ms FROM __analytics_retention_schedule_occurrences ORDER BY \
                 schedule_at_ms",
            )?;
            statement
                .query_map([], |row| row.get::<_, i64>(0))?
                .collect::<Result<Vec<_>, duckdb::Error>>()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        schedules,
        vec![
            2_500,
            base as i64,
            (base + 5_000) as i64,
            (base + 10_000) as i64,
            (base + 15_000) as i64
        ]
    );
    assert_eq!(
        runtime_b.materialize_retention_jobs().await,
        Some(Duration::from_millis(4_000))
    );
    let remaining = fixture
        .engine
        .with_read(|engine| engine.query_unscoped_sql_json("SELECT user_id FROM users"))
        .await
        .unwrap()
        .unwrap();
    assert!(remaining.is_empty());
    assert_eq!(fixture.health.read().await.total_sweep_errors, 0);
}

#[tokio::test]
async fn given_retention_effect_before_unknown_completion_when_job_system_reconciles_then_delete_and_receipt_progress_once()
 {
    let fixture = retention_handler_fixture(true).await;
    let catalog = retention_registration(&fixture);
    let store = FakeStore::new();
    let system = build_system(
        store.clone(),
        catalog,
        b"analytics-retention-unknown-test",
        0x91,
        None,
    );
    let id = JobId::from([0x92; 16]);
    system
        .enqueue_raw(
            NewJob {
                job_id: id,
                kind: JobKind::try_new(RETENTION_JOB_KIND).expect("retention kind"),
                schema_version: NonZeroU16::new(RETENTION_SCHEMA_VERSION)
                    .expect("schema is non-zero"),
                payload: fixture.payload.clone().into(),
                fairness: FairnessClass::low_volume(),
                ordering_key: None,
                priority: Priority::Default,
                scheduled_at: Timestamp::ZERO,
                expires_at: Some(Timestamp::from_millis(
                    i64::try_from(future_deadline()).expect("deadline fits"),
                )),
                retry_override: Some(RetrySelection::Preset(RetryPreset::Maintenance)),
                deduplication: None,
            },
            Timestamp::ZERO,
        )
        .await
        .expect("retention enqueue commits");
    let claim = system
        .claim(id, Timestamp::ZERO)
        .await
        .expect("retention claim commits");
    let operation = claim.operation_id();
    let outcome = system
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("retention effect succeeds");
    assert!(matches!(
        outcome,
        JobOutcome::Complete(ref code) if code.as_str() == "retention_applied"
    ));
    store
        .queue_unknown_after_commit()
        .expect("unknown completion result is queued");
    system
        .complete(claim, outcome, Timestamp::from_millis(1))
        .await
        .expect("unknown completion reconciles");

    assert_eq!(
        system
            .reconcile(id, operation)
            .await
            .expect("retention reconciliation succeeds"),
        ReconciliationOutcome::AlreadyCommitted
    );
    let health = fixture.health.read().await;
    assert!(matches!(health.status, RetentionHealthStatus::Healthy));
    assert_eq!(health.total_rows_deleted, 1);
    assert_eq!(health.total_sweep_errors, 0);
    drop(health);
    let rows = fixture
        .engine
        .with_read(|engine| engine.query_unscoped_sql_json("SELECT user_id FROM users"))
        .await
        .expect("read retention rows")
        .expect("query retention rows");
    assert!(rows.is_empty());
}

#[tokio::test]
async fn given_stale_retention_execution_when_handler_runs_then_no_delete_or_health_progress_occurs()
 {
    let fixture = retention_handler_fixture(true).await;
    seed_expired_user(&fixture.engine, "expired-b").await;
    let catalog = retention_registration(&fixture);
    let system = retention_job_system(catalog);
    let old_claim = claim_retention_job(
        &system,
        retention_job(0x31, fixture.payload.clone()),
        Timestamp::ZERO,
    )
    .await;
    let new_claim = claim_retention_job(
        &system,
        retention_job(0x32, fixture.payload.clone()),
        Timestamp::ZERO,
    )
    .await;
    let applied = system
        .execute_claimed(&new_claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("newer retention execution");
    assert!(
        matches!(applied, JobOutcome::Complete(ref code) if code.as_str() == "retention_applied")
    );
    let stale = system
        .execute_claimed(&old_claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("stale retention execution");

    assert!(matches!(
        stale,
        JobOutcome::Complete(ref code) if code.as_str() == "retention_stale_fence"
    ));
    assert_eq!(
        fixture
            .engine
            .with_read(|engine| engine.query_unscoped_sql_json("SELECT user_id FROM users"))
            .await
            .expect("read rows")
            .expect("query rows")
            .len(),
        1
    );
    let health = fixture.health.read().await;
    assert_eq!(health.total_rows_deleted, 1);
    assert_eq!(health.total_sweep_errors, 0);
}

#[tokio::test]
async fn given_retention_effect_replay_when_handler_runs_then_health_progress_is_counted_once() {
    let fixture = retention_handler_fixture(true).await;
    let catalog = retention_registration(&fixture);
    let system = retention_job_system(catalog);
    let claim = claim_retention_job(
        &system,
        retention_job(0x41, fixture.payload.clone()),
        Timestamp::ZERO,
    )
    .await;
    let first = system
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("initial retention effect");
    let replay = system
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("retention effect replay");

    assert!(matches!(
        first,
        JobOutcome::Complete(ref code) if code.as_str() == "retention_applied"
    ));
    assert!(matches!(
        replay,
        JobOutcome::Complete(ref code) if code.as_str() == "retention_already_applied"
    ));
    let health = fixture.health.read().await;
    assert!(matches!(health.status, RetentionHealthStatus::Healthy));
    assert_eq!(health.total_rows_deleted, 1);
    assert_eq!(health.total_sweep_errors, 0);
}

#[tokio::test]
async fn given_bounded_retention_effect_when_handler_replays_then_incomplete_health_is_counted_once()
 {
    let fixture = retention_handler_fixture(true).await;
    seed_configured_expired_user(&fixture, "expired-b").await;
    let catalog = retention_registration(&fixture);
    let system = retention_job_system(catalog);
    let claim = claim_retention_job(
        &system,
        retention_job(0x46, fixture.payload.clone()),
        Timestamp::ZERO,
    )
    .await;
    let first = system
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("initial bounded retention effect");
    let first_health = fixture.health.read().await.clone();
    let replay = system
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("bounded retention effect replay");
    let replay_health = fixture.health.read().await.clone();

    assert!(matches!(
        first,
        JobOutcome::Complete(ref code) if code.as_str() == "retention_applied"
    ));
    assert!(matches!(
        replay,
        JobOutcome::Complete(ref code) if code.as_str() == "retention_already_applied"
    ));
    assert!(matches!(
        first_health.status,
        RetentionHealthStatus::Degraded
    ));
    assert!(matches!(
        replay_health.status,
        RetentionHealthStatus::Degraded
    ));
    assert_eq!(first_health.total_rows_deleted, 1);
    assert_eq!(replay_health.total_rows_deleted, 1);
    assert_eq!(first_health.last_sweep_completed_at_ms, None);
    assert_eq!(replay_health.last_sweep_completed_at_ms, None);
}

#[tokio::test]
async fn given_cancelled_or_expired_retention_execution_when_handler_starts_then_destination_is_untouched()
 {
    let fixture = retention_handler_fixture(true).await;
    let catalog = retention_registration(&fixture);
    let system = retention_job_system(catalog);
    let cancelled_claim = claim_retention_job(
        &system,
        retention_job(0x51, b"not decoded".to_vec()),
        Timestamp::ZERO,
    )
    .await;
    cancelled_claim.cancellation().cancel();
    let expired_payload = retention_payload("users", 1, 1, 1, None, 1)
        .expect("expired retention payload")
        .encode()
        .expect("encode expired retention payload");
    let expired_claim = claim_retention_job(
        &system,
        retention_job(0x52, expired_payload),
        Timestamp::ZERO,
    )
    .await;
    let cancelled = system
        .execute_claimed(&cancelled_claim, Timestamp::ZERO, || {
            MonotonicDeadline::ZERO
        })
        .await
        .expect("cancelled retention execution");
    let expired = system
        .execute_claimed(&expired_claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("expired retention execution");

    assert!(matches!(cancelled, JobOutcome::Cancelled));
    assert!(matches!(expired, JobOutcome::Cancelled));
    assert!(matches!(
        fixture.health.read().await.status,
        RetentionHealthStatus::Starting
    ));
}

#[tokio::test]
async fn given_invalid_or_unconfigured_retention_job_when_handler_runs_then_health_degrades_without_counting()
 {
    let fixture = retention_handler_fixture(true).await;
    let catalog = retention_registration(&fixture);
    let system = retention_job_system(catalog);
    let unconfigured = retention_payload("other", 2_500, 1, future_deadline(), None, 2_500)
        .expect("unconfigured payload")
        .encode()
        .expect("encode unconfigured payload");
    let claim =
        claim_retention_job(&system, retention_job(0x71, unconfigured), Timestamp::ZERO).await;
    let outcome = system
        .execute_claimed(&claim, Timestamp::ZERO, || MonotonicDeadline::ZERO)
        .await
        .expect("unconfigured table becomes a retry outcome");
    assert!(matches!(
        outcome,
        JobOutcome::Retry { ref code, .. } if code.as_str() == "handler_failed"
    ));
    let health = fixture.health.read().await;
    assert!(matches!(health.status, RetentionHealthStatus::Degraded));
    assert_eq!(health.total_sweep_errors, 0);
}

#[tokio::test]
async fn given_failed_retention_job_when_job_system_retries_then_error_is_counted_once() {
    let fixture = retention_handler_fixture(false).await;
    let catalog = retention_registration(&fixture);
    let observer = Arc::new(
        BackgroundJobObserver::new(
            Arc::clone(&fixture.shared.source_health),
            fixture.shared.worker_id.clone(),
        )
        .with_retention_health(Arc::clone(&fixture.health)),
    );
    let system = build_system(
        FakeStore::new(),
        catalog,
        b"analytics-retention-retry-test",
        0x73,
        Some(observer),
    );
    let payload = retention_payload("other", 2_500, 1, future_deadline(), None, 2_500)
        .expect("unconfigured payload")
        .encode()
        .expect("encode unconfigured payload");
    system
        .enqueue_raw(retention_job(0x74, payload), Timestamp::ZERO)
        .await
        .expect("retention enqueue commits");

    let report = system
        .drive(retention_drive_request(), Timestamp::ZERO, || {
            MonotonicDeadline::ZERO
        })
        .await
        .expect("retention retry commits");

    assert_eq!(report.counts.retried, 1);
    let health = fixture.health.read().await;
    assert!(matches!(health.status, RetentionHealthStatus::Degraded));
    assert_eq!(health.total_sweep_errors, 1);
}

#[tokio::test]
async fn given_normal_idle_driver_event_when_observed_then_retention_health_does_not_degrade() {
    let source_health = Arc::new(tokio::sync::RwLock::new(crate::SourceHealth::disabled()));
    let retention_health = Arc::new(tokio::sync::RwLock::new(RetentionHealth::starting(1)));
    let observer = BackgroundJobObserver::new(source_health, "retention-idle-test".to_string())
        .with_retention_health(Arc::clone(&retention_health));

    observer.observe(JobEvent::DriveStopped {
        reason: DriveStopReason::NoVisibleWork,
    });

    let health = retention_health.read().await;
    assert!(matches!(health.status, RetentionHealthStatus::Starting));
    assert_eq!(health.total_sweep_errors, 0);
}

#[tokio::test]
async fn given_retention_schedule_configuration_when_runtime_is_built_then_tables_are_sorted_and_bounded()
 {
    let config = retention_config();
    let runtime = RetentionRuntime::from_config(&config)
        .await
        .expect("retention runtime config")
        .expect("retention runtime");
    assert_eq!(runtime.delete_batch_size(), 100);
    assert_eq!(runtime.sweep_interval_ms(), 5_000);
    assert_eq!(runtime.table_names_sorted(), vec!["users".to_string()]);
    assert!(runtime.is_table_configured("users"));
    assert!(!runtime.is_table_configured("other"));
}

fn retention_registration(fixture: &RetentionHandlerFixture) -> JobCatalog {
    job_catalog(
        Arc::clone(&fixture.shared),
        Some((Arc::clone(&fixture.retention), Arc::clone(&fixture.health))),
        MAX_RETENTION_JOB_TIMEOUT,
    )
    .expect("retention handler catalog")
}

fn retention_runtime_registration(fixture: &RetentionHandlerFixture) -> JobCatalogBundle {
    JobCatalogBundle::try_new(
        Arc::clone(&fixture.shared),
        Some((Arc::clone(&fixture.retention), Arc::clone(&fixture.health))),
        MAX_RETENTION_JOB_TIMEOUT,
    )
    .expect("retention runtime catalog")
}

fn retention_job_system(catalog: JobCatalog) -> JobSystem<FakeStore> {
    build_system(
        FakeStore::new(),
        catalog,
        b"analytics-retention-handler-test",
        0x9b,
        None,
    )
}

async fn claim_retention_job(
    system: &JobSystem<FakeStore>,
    job: NewJob,
    now: Timestamp,
) -> ClaimedJob {
    let job_id = job.job_id;
    system
        .enqueue_raw(job, Timestamp::ZERO)
        .await
        .expect("retention test job enqueues");
    system
        .claim(job_id, now)
        .await
        .expect("retention test job claims")
}

fn retention_job(id: u8, payload: Vec<u8>) -> NewJob {
    NewJob {
        job_id: JobId::from([id; 16]),
        kind: JobKind::try_new(RETENTION_JOB_KIND).expect("retention kind is valid"),
        schema_version: NonZeroU16::new(RETENTION_SCHEMA_VERSION)
            .expect("retention schema is non-zero"),
        payload: payload.into(),
        fairness: FairnessClass::low_volume(),
        ordering_key: None,
        priority: Priority::Default,
        scheduled_at: Timestamp::ZERO,
        expires_at: None,
        retry_override: Some(RetrySelection::Preset(RetryPreset::Maintenance)),
        deduplication: None,
    }
}

fn retention_drive_request() -> DriveRequest {
    DriveRequest::try_from(DriveRequestInput {
        queues: QueueSelection::only(vec![
            QueueName::try_new("analytics-retention").expect("retention queue is valid"),
        ])
        .expect("retention queue selection is valid"),
        max_claims: NonZeroU32::new(1).expect("claim bound is non-zero"),
        max_concurrency: NonZeroU16::new(1).expect("concurrency bound is non-zero"),
        max_payload_bytes_in_flight: NonZeroUsize::new(bg_jobs::MAX_INLINE_PAYLOAD_BYTES)
            .expect("payload bound is non-zero"),
        max_completion_details: 1,
        max_store_operations: NonZeroU32::new(64).expect("store bound is non-zero"),
        stop_claiming_at: MonotonicDeadline::from_nanos(1),
        finish_by: MonotonicDeadline::from_nanos(u64::MAX),
    })
    .expect("retention drive request is valid")
}

async fn retention_handler_fixture(with_expired_row: bool) -> RetentionHandlerFixture {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect retention engine");
    let mut manifest = analytics_fixtures::users_manifest();
    manifest.tables[0].retention = Some(RetentionPolicy {
        period_ms: 1_000,
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: "created_at_ms".to_string(),
        },
    });
    engine
        .ensure_manifest(&manifest)
        .expect("ensure retention manifest");
    if with_expired_row {
        seed_expired_engine(&engine, &manifest, "expired-a");
    }
    let retention = Arc::new(
        RetentionRuntime::from_config(&retention_config())
            .await
            .expect("retention runtime")
            .expect("retention runtime enabled"),
    );
    let health = Arc::new(tokio::sync::RwLock::new(RetentionHealth::starting(1)));
    let payload = retention_payload("users", 2_500, 1, future_deadline(), None, 2_500)
        .expect("retention payload");
    let cutoff_ms = payload.cutoff_ms();
    let schedule_at_ms = payload.schedule_at_ms();
    let server_time_ms = payload.server_time_ms();
    engine
        .authorize_retention_schedule_occurrence("users", cutoff_ms, schedule_at_ms, server_time_ms)
        .expect("retention schedule occurrence");
    let engine = AnalyticsEngineAccess::shared(engine);
    let shared = Arc::new(SourceJobShared {
        engine: engine.clone(),
        manifest: Arc::new(tokio::sync::RwLock::new(Arc::new(manifest))),
        source_health: Arc::new(tokio::sync::RwLock::new(crate::SourceHealth::disabled())),
        privacy_policy: None,
        retention: Some(Arc::clone(&retention)),
        pollers: tokio::sync::Mutex::new(HashMap::new()),
        authorized_source_identities: std::sync::OnceLock::new(),
        worker_id: "retention-handler-test".to_string(),
        progress_commits: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    });
    let payload = payload.encode().expect("encode retention payload");
    RetentionHandlerFixture {
        shared,
        engine,
        retention,
        health,
        payload,
    }
}

async fn seed_expired_user(engine: &AnalyticsEngineAccess, user_id: &str) {
    let user_id = user_id.to_string();
    engine
        .with_write(move |engine| {
            let manifest = analytics_fixtures::users_manifest();
            let mut item = user_item(
                user_id.as_str(),
                format!("{user_id}@example.test").as_str(),
                "org-a",
            );
            item.insert(
                "created_at_ms".to_string(),
                StorageValue::N("1000".to_string()),
            );
            engine
                .ingest_stream_record(
                    &manifest,
                    "users",
                    user_id.as_bytes(),
                    StorageStreamRecord {
                        sequence_number: format!("sequence-{user_id}"),
                        keys: StorageItem::new(),
                        old_image: None,
                        new_image: Some(item),
                    },
                )
                .expect("seed expired retention row");
        })
        .await;
}

fn seed_expired_engine(engine: &AnalyticsEngine, manifest: &AnalyticsManifest, user_id: &str) {
    let mut item = user_item(user_id, format!("{user_id}@example.test").as_str(), "org-a");
    item.insert(
        "created_at_ms".to_string(),
        StorageValue::N("1000".to_string()),
    );
    engine
        .ingest_stream_record(
            manifest,
            "users",
            user_id.as_bytes(),
            StorageStreamRecord {
                sequence_number: format!("sequence-{user_id}"),
                keys: StorageItem::new(),
                old_image: None,
                new_image: Some(item),
            },
        )
        .expect("seed expired retention row");
}

async fn seed_configured_expired_user(fixture: &RetentionHandlerFixture, user_id: &str) {
    let user_id = user_id.to_string();
    let manifest = Arc::clone(&*fixture.shared.manifest.read().await);
    fixture
        .engine
        .with_write(move |engine| {
            let mut item = user_item(
                user_id.as_str(),
                format!("{user_id}@example.test").as_str(),
                "org-a",
            );
            item.insert(
                "created_at_ms".to_string(),
                StorageValue::N("1000".to_string()),
            );
            engine
                .ingest_stream_record(
                    manifest.as_ref(),
                    "users",
                    user_id.as_bytes(),
                    StorageStreamRecord {
                        sequence_number: format!("sequence-{user_id}"),
                        keys: StorageItem::new(),
                        old_image: None,
                        new_image: Some(item),
                    },
                )
                .expect("seed configured expired retention row");
        })
        .await;
}

fn retention_payload(
    table_name: &str,
    cutoff_ms: i64,
    batch_size: u64,
    deadline_at_ms: u64,
    cursor: Option<&str>,
    schedule_at_ms: i64,
) -> Result<RetentionJobPayload, crate::background_jobs::BackgroundJobError> {
    RetentionJobPayload::from_schedule(
        table_name,
        cutoff_ms,
        batch_size,
        deadline_at_ms,
        cursor.map(str::to_owned),
        schedule_at_ms,
        u64::try_from(schedule_at_ms).unwrap_or(0),
    )
}

async fn retention_runtime_fixture(
    fixture: &RetentionHandlerFixture,
    store: FakeStore,
    owner: u8,
    clock: Arc<dyn bg_jobs_tokio::test_support::JobClock>,
) -> Arc<BackgroundJobRuntime<FakeStore>> {
    let catalog = retention_runtime_registration(fixture);
    BackgroundJobRuntime::for_test(crate::background_jobs::runtime::TestRuntimeInput {
        store,
        catalog,
        owner: OwnerId::from([owner; 16]),
        shared: Arc::clone(&fixture.shared),
        retention: Some(Arc::clone(&fixture.retention)),
        health: Some(Arc::clone(&fixture.health)),
        source_job_timeout: MAX_RETENTION_JOB_TIMEOUT,
        clock,
    })
}

fn retention_payloads(store: &FakeStore) -> Vec<RetentionJobPayload> {
    store
        .snapshot()
        .expect("retention store snapshot")
        .into_iter()
        .filter_map(|(_, value)| {
            let value = ValueCodec::decode(value.as_ref()).ok()?;
            if value.tag != ValueTag::PayloadChunk {
                return None;
            }
            RetentionJobPayload::decode(value.body.as_ref()).ok()
        })
        .collect()
}

#[derive(Clone)]
struct TestClock {
    now_ms: Arc<AtomicU64>,
}

impl TestClock {
    fn new(now_ms: u64) -> Self {
        Self {
            now_ms: Arc::new(AtomicU64::new(now_ms)),
        }
    }

    fn set(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::Release);
    }
}

impl bg_jobs_tokio::test_support::JobClock for TestClock {
    fn wall_now(&self) -> Timestamp {
        Timestamp::from_millis(
            i64::try_from(self.now_ms.load(Ordering::Acquire))
                .expect("test clock timestamp fits in i64"),
        )
    }

    fn monotonic_now(&self) -> bg_jobs::runtime::MonotonicDeadline {
        let nanos = self
            .now_ms
            .load(Ordering::Acquire)
            .saturating_mul(1_000_000);
        bg_jobs::runtime::MonotonicDeadline::from_nanos(nanos)
    }
}

fn future_deadline() -> u64 {
    (crate::clock::now_ms() + MAX_RETENTION_JOB_TIMEOUT.as_millis()) as u64
}

fn retention_config() -> AnalyticsRetentionConfig {
    AnalyticsRetentionConfig {
        enabled: true,
        sweep_interval_ms: 5_000,
        delete_batch_size: 100,
        tables: vec![AnalyticsRetentionTableConfig {
            analytics_table_name: "users".to_string(),
            default_period_ms: 1_000,
            strict: true,
            timestamp: RetentionTimestampConfig::Attribute {
                attribute_path: "created_at_ms".to_string(),
            },
            tenant_policy: TenantRetentionPolicyConfig {
                source: TenantRetentionPolicySource::AuxStorage,
                endpoint_url: Some("http://127.0.0.1:9".to_string()),
                region: None,
                credentials: None,
                request: TenantRetentionPolicyRequest::GetItem(TenantRetentionGetItemRequest {
                    table_name: "tenant_retention".to_string(),
                    key: KeyAttributes::from_iter([(
                        "tenant_id".to_string(),
                        AttributeValue::S("${tenant_id}".to_string()),
                    )]),
                    consistent_read: Some(true),
                    projection_expression: None,
                    expression_attribute_names: None,
                }),
                duration_selector: RetentionDurationSelector {
                    attribute_path: "retention_ms".to_string(),
                },
                cache_ttl_ms: 1_000,
            },
        }],
    }
}
