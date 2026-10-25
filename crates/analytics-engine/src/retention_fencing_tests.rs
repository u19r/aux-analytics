use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
};

use analytics_contract::{
    AnalyticsManifest, RetentionPolicy, RetentionTimestamp, StorageStreamRecord, StorageValue,
};
use analytics_fixtures::{user_item, users_manifest};
use config::{CatalogType, StorageBackend};
use duckdb::types::Value;
use storage_types::VecMap;

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, MAX_RETENTION_BATCH_SIZE,
    RetentionEffectAuthority, RetentionEffectDisposition, RetentionEffectRequest,
    RetentionEffectRequestInput, RetentionEffectScope,
};

const ANALYTICS_TABLE_NAME: &str = "users";
const NOW_MS: i64 = 2_500;
const RETENTION_PERIOD_MS: u64 = 1_000;

#[test]
fn given_applied_execution_when_reclaimed_with_higher_fence_then_receipt_reconciles_without_deleting_again()
 {
    let (engine, manifest) = retained_users_engine();
    seed_user(&engine, &manifest, "expired-a", 1_000).unwrap();
    apply(&engine, request(1, 1, None, 1)).unwrap();
    let before = destination_state(&engine);
    assert!(matches!(
        apply(&engine, request(1, 2, None, 1)).unwrap(),
        RetentionEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(destination_state(&engine), before);
}

#[test]
fn given_newer_owner_when_stale_worker_commits_then_all_destination_state_is_unchanged() {
    let (engine, manifest) = retained_users_engine();
    seed_user(&engine, &manifest, "expired-a", 1_000).expect("seed first row");
    seed_user(&engine, &manifest, "expired-b", 1_000).expect("seed second row");

    let newer = apply(&engine, request(11, 11, None, 1)).expect("newer owner commit");
    let RetentionEffectDisposition::Applied { summary } = newer else {
        panic!("newer owner must apply");
    };
    assert_eq!(summary.rows_deleted(), 1);
    assert!(!summary.complete());
    let before = destination_state(&engine);

    let stale = apply(&engine, request(10, 10, None, 1)).expect("stale disposition");
    assert_eq!(stale, RetentionEffectDisposition::StaleFence);
    assert_eq!(destination_state(&engine), before);
}

#[test]
fn given_same_execution_when_retention_batch_replayed_then_already_applied_without_duplicates() {
    let (engine, manifest) = retained_users_engine();
    seed_user(&engine, &manifest, "expired-a", 1_000).expect("seed expired row");
    let first = apply(&engine, request(12, 12, None, 1)).expect("initial retention effect");
    let before = destination_state(&engine);
    let replay = apply(&engine, request(12, 12, None, 1)).expect("receipt replay");

    assert!(matches!(first, RetentionEffectDisposition::Applied { .. }));
    assert!(matches!(
        replay,
        RetentionEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(destination_state(&engine), before);
    assert_eq!(receipt_count(&engine), 1);
}

#[test]
fn given_consumed_predecessor_when_different_executions_retry_then_destination_is_unchanged() {
    let (engine, manifest) = retained_users_engine();
    seed_user(&engine, &manifest, "expired-a", 1_000).expect("seed first row");
    seed_user(&engine, &manifest, "expired-b", 1_000).expect("seed second row");
    apply(&engine, request(13, 3, None, 1)).expect("accepted owner");
    let before = destination_state(&engine);

    for (seed, fence) in [(14, 2), (15, 3)] {
        let result = apply(&engine, request(seed, fence, None, 1)).expect("stale disposition");
        assert_eq!(result, RetentionEffectDisposition::StaleFence);
        assert_eq!(destination_state(&engine), before);
    }
}

#[test]
fn given_distinct_jobs_with_equal_fences_when_retention_continues_then_each_batch_deletes_one_ordered_row()
 {
    let (engine, manifest) = retained_users_engine();
    for id in ["expired-a", "expired-b", "expired-c"] {
        seed_user(&engine, &manifest, id, 1_000).expect("seed expired row");
    }

    let first = apply(&engine, request(20, 1, None, 1)).expect("first batch");
    let RetentionEffectDisposition::Applied {
        summary: first_summary,
    } = first
    else {
        panic!("first batch must apply");
    };
    assert_eq!(first_summary.rows_deleted(), 1);
    assert!(!first_summary.complete());
    let first_cursor = first_summary.cursor().map(str::to_owned);

    let second = apply(&engine, request(21, 1, first_cursor, 1)).expect("second batch");
    let RetentionEffectDisposition::Applied {
        summary: second_summary,
    } = second
    else {
        panic!("second batch must apply");
    };
    assert_eq!(second_summary.rows_deleted(), 1);
    assert!(!second_summary.complete());
    let second_cursor = second_summary.cursor().map(str::to_owned);
    assert_eq!(
        authority_state(&engine),
        ("1".to_string(), second_cursor.clone())
    );

    let third = apply(&engine, request(22, 1, second_cursor, 1)).expect("third batch");
    let RetentionEffectDisposition::Applied { summary } = third else {
        panic!("third batch must apply");
    };
    assert_eq!(summary.rows_deleted(), 1);
    assert!(summary.complete());
    assert_eq!(rows(&engine).len(), 0);
    assert_eq!(receipt_count(&engine), 3);
    assert_eq!(authority_state(&engine), ("1".to_string(), None));
}

#[test]
fn given_higher_fence_with_non_contiguous_cursor_when_retention_applies_then_request_fails_closed()
{
    let (engine, manifest) = retained_users_engine();
    for id in ["expired-a", "expired-b"] {
        seed_user(&engine, &manifest, id, 1_000).expect("seed expired row");
    }
    apply(&engine, request(24, 24, None, 1)).expect("first retention batch");
    let before = destination_state(&engine);

    let result = apply(&engine, request(25, 25, Some("expired-z".to_string()), 1));

    assert!(matches!(
        result,
        Err(AnalyticsEngineError::IncompleteRetentionEffectState)
    ));
    assert_eq!(destination_state(&engine), before);
    assert_eq!(rows(&engine).len(), 1, "lower eligible row must remain");
}

#[test]
fn given_late_lower_row_when_current_sweep_finishes_then_next_occurrence_deletes_it() {
    let (engine, manifest) = retained_users_engine();
    for id in ["expired-b", "expired-c"] {
        seed_user(&engine, &manifest, id, 1_000).unwrap();
    }
    let RetentionEffectDisposition::Applied { summary } =
        apply(&engine, request(26, 1, None, 1)).unwrap()
    else {
        panic!("first batch")
    };
    seed_user(&engine, &manifest, "expired-a", 1_000).unwrap();
    let RetentionEffectDisposition::Applied { summary } = apply(
        &engine,
        request(27, 1, summary.cursor().map(str::to_owned), 1),
    )
    .unwrap() else {
        panic!("continuation")
    };
    assert!(summary.complete());
    assert_eq!(rows(&engine).len(), 1);
    let mut next = request(28, 1, None, 1);
    next.cutoff_ms = 3_000;
    next.schedule_at_ms = 3_000;
    next.server_time_ms = 3_000;
    engine
        .authorize_retention_schedule_occurrence(ANALYTICS_TABLE_NAME, 3_000, 3_000, 3_000)
        .unwrap();
    let RetentionEffectDisposition::Applied { summary } = apply(&engine, next).unwrap() else {
        panic!("next occurrence")
    };
    assert!(summary.complete());
    assert!(rows(&engine).is_empty());
}

#[test]
fn given_empty_batch_when_retention_applies_then_complete_receipt_is_durable_and_replayable() {
    let (engine, _manifest) = retained_users_engine();
    let first = apply(&engine, request(23, 23, None, 100)).expect("empty retention batch");
    let replay = apply(&engine, request(23, 23, None, 100)).expect("empty retention replay");

    let RetentionEffectDisposition::Applied { summary } = first else {
        panic!("empty batch must apply");
    };
    assert_eq!(summary.rows_deleted(), 0);
    assert!(summary.complete());
    assert!(summary.cursor().is_none());
    assert!(matches!(
        replay,
        RetentionEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(receipt_count(&engine), 1);
}

#[test]
fn given_future_row_when_later_expired_row_arrives_then_empty_batch_does_not_skip_it() {
    let (engine, manifest) = retained_users_engine();
    seed_user(&engine, &manifest, "future", 5_000).expect("seed future row");
    let first = apply(&engine, request(30, 30, None, 1)).expect("future-only batch");
    let RetentionEffectDisposition::Applied { summary } = first else {
        panic!("future-only batch must apply");
    };
    assert_eq!(summary.rows_deleted(), 0);
    assert!(summary.complete());
    assert_eq!(rows(&engine).len(), 1);

    seed_user(&engine, &manifest, "expired-later", 1_000).expect("seed later expired row");
    engine
        .authorize_retention_schedule_occurrence(ANALYTICS_TABLE_NAME, 3_000, 3_000, 3_000)
        .unwrap();
    let mut next = request(31, 1, None, 1);
    next.cutoff_ms = 3_000;
    next.schedule_at_ms = 3_000;
    next.server_time_ms = 3_000;
    let second = apply(&engine, next).expect("later expired batch");
    let RetentionEffectDisposition::Applied { summary } = second else {
        panic!("later expired row must apply");
    };
    assert_eq!(summary.rows_deleted(), 1);
    assert_eq!(rows(&engine).len(), 1);
}

#[test]
fn given_destination_commit_failure_after_delete_when_applied_then_all_effect_state_rolls_back() {
    let (engine, manifest) = retained_users_engine();
    seed_user(&engine, &manifest, "expired-a", 1_000).expect("seed expired row");
    engine
        .connection()
        .execute_batch(
            "DROP TABLE __analytics_retention_effect_receipts;
             CREATE TABLE __analytics_retention_effect_receipts (
                 receipt_version INTEGER NOT NULL,
                 execution_id BLOB NOT NULL,
                 table_name VARCHAR NOT NULL,
                 fence VARCHAR NOT NULL,
                 cutoff_ms BIGINT NOT NULL,
                 batch_size BIGINT NOT NULL,
                 cursor_before VARCHAR,
                 cursor_after VARCHAR,
                 rows_deleted BIGINT NOT NULL,
                 complete BOOLEAN NOT NULL,
                 committed_at_ms BIGINT NOT NULL,
                 schedule_at_ms BIGINT NOT NULL,
                 server_time_ms BIGINT NOT NULL,
                 CHECK (false)
             )",
        )
        .expect("install failing receipt sink");

    let error = apply(&engine, request(40, 40, None, 1)).expect_err("receipt write must fail");
    assert!(matches!(error, AnalyticsEngineError::DuckDb(_)));
    assert_eq!(rows(&engine).len(), 1);
    assert_eq!(authority_count(&engine), 0);
    assert_eq!(receipt_count(&engine), 0);
}

#[test]
fn given_malformed_authority_or_receipt_state_when_called_then_effect_fails_closed() {
    let (engine, manifest) = retained_users_engine();
    seed_user(&engine, &manifest, "expired-a", 1_000).expect("seed expired row");

    engine
        .connection()
        .execute(
            "INSERT INTO __analytics_retention_effect_authority
             VALUES (1, ?, ?, ?, ?, 1)",
            duckdb::params![
                ANALYTICS_TABLE_NAME,
                "not-canonical",
                Value::Blob(vec![51; 16]),
                Option::<&str>::None,
            ],
        )
        .expect("seed malformed authority");
    let error = apply(&engine, request(51, 51, None, 1)).expect_err("invalid authority");
    assert!(matches!(
        error,
        AnalyticsEngineError::InvalidRetentionEffectFence
    ));
    assert_eq!(rows(&engine).len(), 1);

    engine
        .connection()
        .execute(
            "DELETE FROM __analytics_retention_effect_authority WHERE table_name = ?",
            [ANALYTICS_TABLE_NAME],
        )
        .expect("clear malformed authority");
    engine
        .connection()
        .execute(
            "INSERT INTO __analytics_retention_effect_authority
             VALUES (1, ?, ?, ?, ?, 1)",
            duckdb::params![
                ANALYTICS_TABLE_NAME,
                "52",
                Value::Blob(vec![52; 15]),
                Option::<&str>::None,
            ],
        )
        .expect("seed malformed execution identity");
    let error = apply(&engine, request(52, 52, None, 1)).expect_err("invalid identity");
    assert!(matches!(
        error,
        AnalyticsEngineError::InvalidRetentionEffectExecutionId { actual: 15 }
    ));
    assert_eq!(rows(&engine).len(), 1);

    engine
        .connection()
        .execute(
            "DELETE FROM __analytics_retention_effect_authority WHERE table_name = ?",
            [ANALYTICS_TABLE_NAME],
        )
        .expect("clear malformed authority");
    engine
        .connection()
        .execute(
            "INSERT INTO __analytics_retention_effect_authority
             VALUES (1, ?, ?, ?, ?, 1)",
            duckdb::params![
                ANALYTICS_TABLE_NAME,
                "53",
                Value::Blob(vec![53; 16]),
                Option::<&str>::None,
            ],
        )
        .expect("seed receipt authority");
    insert_receipt(
        &engine,
        ReceiptFixture {
            seed: 53,
            fence: "53",
            table_name: "users",
            cursor_before: None,
            rows_deleted: 1,
            complete: true,
            cursor_after: Some("cursor"),
        },
    );
    let error = apply(&engine, request(53, 53, None, 1)).expect_err("invalid receipt");
    assert!(matches!(
        error,
        AnalyticsEngineError::InvalidRetentionEffectReceipt
    ));
    assert_eq!(rows(&engine).len(), 1);
}

#[test]
fn given_duplicate_durable_authority_or_receipt_when_called_then_ambiguity_fails_closed() {
    let (engine, _manifest) = retained_users_engine();
    engine
        .connection()
        .execute(
            "INSERT INTO __analytics_retention_effect_authority
             VALUES (1, ?, ?, ?, ?, 1), (1, ?, ?, ?, ?, 2)",
            duckdb::params![
                ANALYTICS_TABLE_NAME,
                "54",
                Value::Blob(vec![54; 16]),
                Option::<&str>::None,
                ANALYTICS_TABLE_NAME,
                "55",
                Value::Blob(vec![55; 16]),
                Option::<&str>::None,
            ],
        )
        .expect("seed duplicate authorities");
    let error = apply(&engine, request(56, 56, None, 1)).expect_err("authority ambiguity");
    assert!(matches!(
        error,
        AnalyticsEngineError::AmbiguousRetentionEffectAuthority
    ));

    engine
        .connection()
        .execute(
            "DELETE FROM __analytics_retention_effect_authority WHERE table_name = ?",
            [ANALYTICS_TABLE_NAME],
        )
        .expect("clear authorities");
    let fixture = ReceiptFixture {
        seed: 57,
        fence: "57",
        table_name: ANALYTICS_TABLE_NAME,
        cursor_before: None,
        rows_deleted: 1,
        complete: true,
        cursor_after: None,
    };
    insert_receipt(&engine, fixture);
    insert_receipt(&engine, fixture);
    let error = apply(&engine, request(57, 57, None, 1)).expect_err("receipt ambiguity");
    assert!(matches!(
        error,
        AnalyticsEngineError::AmbiguousRetentionEffectReceipt
    ));
}

#[test]
fn given_receipt_without_authority_when_called_then_incomplete_state_fails_closed() {
    let (engine, _manifest) = retained_users_engine();
    insert_receipt(
        &engine,
        ReceiptFixture {
            seed: 60,
            fence: "60",
            table_name: ANALYTICS_TABLE_NAME,
            cursor_before: None,
            rows_deleted: 1,
            complete: true,
            cursor_after: None,
        },
    );

    let error = apply(&engine, request(60, 60, None, 1)).expect_err("receipt-only state");
    assert!(matches!(
        error,
        AnalyticsEngineError::IncompleteRetentionEffectState
    ));
    assert_eq!(rows(&engine).len(), 0);
}

#[test]
fn given_authority_without_receipt_when_called_then_incomplete_state_fails_closed() {
    let (engine, _manifest) = retained_users_engine();
    engine
        .connection()
        .execute(
            "INSERT INTO __analytics_retention_effect_authority
             VALUES (1, ?, ?, ?, ?, 1)",
            duckdb::params![
                ANALYTICS_TABLE_NAME,
                "61",
                Value::Blob(vec![61; 16]),
                Option::<&str>::None,
            ],
        )
        .expect("seed authority-only state");

    let error = apply(&engine, request(61, 61, None, 1)).expect_err("authority-only state");
    assert!(matches!(
        error,
        AnalyticsEngineError::IncompleteRetentionEffectState
    ));
    assert_eq!(rows(&engine).len(), 0);
}

#[test]
fn given_missing_schedule_occurrence_when_called_then_destination_state_is_unchanged() {
    let (engine, manifest) = retained_users_engine();
    seed_user(&engine, &manifest, "expired-a", 1_000).expect("seed expired row");
    engine
        .connection()
        .execute(
            "DELETE FROM __analytics_retention_schedule_occurrences WHERE table_name = ?",
            [ANALYTICS_TABLE_NAME],
        )
        .expect("remove schedule occurrence");
    let before = destination_state(&engine);

    let error = apply(&engine, request(62, 62, None, 1))
        .expect_err("an unqueued occurrence must fail closed");

    assert!(matches!(
        error,
        AnalyticsEngineError::RetentionEffectScheduleUnavailable
    ));
    assert_eq!(destination_state(&engine), before);
}

#[test]
fn given_future_server_time_when_schedule_is_authorized_then_occurrence_is_not_written() {
    let (engine, _manifest) = retained_users_engine();
    let future_server_time = u64::try_from(i64::MAX).expect("i64 max fits u64");

    let error = engine
        .authorize_retention_schedule_occurrence(
            ANALYTICS_TABLE_NAME,
            NOW_MS,
            NOW_MS,
            future_server_time,
        )
        .expect_err("a future scheduler timestamp must fail closed");

    assert!(matches!(
        error,
        AnalyticsEngineError::InvalidRetentionEffectRequest("server time cannot be in the future")
    ));
    assert_eq!(schedule_occurrence_count(&engine), 1);
}

#[test]
fn given_file_duckdb_when_reopened_then_retention_effect_state_and_replay_are_durable() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let path = tempdir.path().join("analytics.duckdb");
    let backend = StorageBackend::DuckDb {
        path: path.to_string_lossy().into_owned(),
    };
    let (engine, manifest) = retained_engine(&backend);
    seed_user(&engine, &manifest, "expired-a", 1_000).expect("seed expired row");
    apply(&engine, request(70, 70, None, 1)).expect("file retention effect");
    let before = destination_state(&engine);
    drop(engine);

    let reopened = AnalyticsEngine::connect(&backend).expect("reopen file DuckDB");
    let replay = apply(&reopened, request(70, 70, None, 1)).expect("file retention replay");
    assert!(matches!(
        replay,
        RetentionEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(destination_state(&reopened), before);
}

#[test]
fn given_sqlite_catalog_ducklake_when_reopened_then_retention_effect_state_is_durable() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let (backend, _) = sqlite_ducklake_backend(tempdir.path());
    let (engine, manifest) = retained_engine(&backend);
    seed_user(&engine, &manifest, "expired-a", 1_000).expect("seed expired row");
    apply(&engine, request(71, 71, None, 1)).expect("DuckLake retention effect");
    let before = destination_state(&engine);
    drop(engine);

    let reopened = AnalyticsEngine::connect(&backend).expect("reopen SQLite-catalog DuckLake");
    let replay = apply(&reopened, request(71, 71, None, 1)).expect("DuckLake retention replay");
    assert!(matches!(
        replay,
        RetentionEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(destination_state(&reopened), before);
}

#[test]
fn given_sqlite_catalog_ducklake_with_remote_data_path_when_fenced_then_rejected_without_credentials()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: tempdir
            .path()
            .join("catalog.sqlite")
            .to_string_lossy()
            .into_owned(),
        data_path: "s3://analytics-bucket/retention".to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    let engine = AnalyticsEngine::connect(&backend).expect("connect remote-path backend");

    let error = apply(&engine, request(73, 73, None, 1))
        .expect_err("remote SQLite-catalog DuckLake cannot claim local fencing");

    assert!(
        matches!(error, AnalyticsEngineError::RetentionEffectFenceUnavailable),
        "actual remote-path result: {error:?}"
    );
}

#[test]
fn given_invalid_retention_request_bounds_when_constructed_then_each_invalid_state_is_rejected() {
    let authority = RetentionEffectAuthority::try_new([80; 16], 80).expect("valid authority");
    let scope = RetentionEffectScope::try_new(ANALYTICS_TABLE_NAME).expect("valid scope");
    for input in [
        RetentionEffectRequestInput {
            authority: authority.clone(),
            scope: scope.clone(),
            cutoff_ms: -1,
            schedule_at_ms: -1,
            server_time_ms: 1,
            batch_size: 1,
            cursor: None,
        },
        RetentionEffectRequestInput {
            authority: authority.clone(),
            scope: scope.clone(),
            cutoff_ms: 1,
            schedule_at_ms: 1,
            server_time_ms: 1,
            batch_size: 0,
            cursor: None,
        },
        RetentionEffectRequestInput {
            authority: authority.clone(),
            scope: scope.clone(),
            cutoff_ms: 1,
            schedule_at_ms: 1,
            server_time_ms: 1,
            batch_size: MAX_RETENTION_BATCH_SIZE + 1,
            cursor: None,
        },
        RetentionEffectRequestInput {
            authority: authority.clone(),
            scope: scope.clone(),
            cutoff_ms: 1,
            schedule_at_ms: 1,
            server_time_ms: 1,
            batch_size: 1,
            cursor: Some("\n".to_string()),
        },
    ] {
        assert!(matches!(
            RetentionEffectRequest::try_new(input),
            Err(AnalyticsEngineError::InvalidRetentionEffectRequest(_))
        ));
    }
    assert!(matches!(
        RetentionEffectAuthority::try_new([80; 15], 80),
        Err(AnalyticsEngineError::InvalidRetentionEffectExecutionId { actual: 15 })
    ));
    assert!(matches!(
        RetentionEffectAuthority::try_new([80; 16], 0),
        Err(AnalyticsEngineError::InvalidRetentionEffectFence)
    ));
    assert!(matches!(
        RetentionEffectScope::try_new(""),
        Err(AnalyticsEngineError::InvalidRetentionEffectScope)
    ));
}

#[test]
fn given_old_worker_paused_before_effect_when_newer_fence_commits_then_old_has_no_effect() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let backend = StorageBackend::DuckDb {
        path: tempdir
            .path()
            .join("analytics.duckdb")
            .to_string_lossy()
            .into_owned(),
    };
    let (setup, manifest) = retained_engine(&backend);
    seed_user(&setup, &manifest, "expired-a", 1_000).expect("seed first expired row");
    seed_user(&setup, &manifest, "expired-b", 1_000).expect("seed second expired row");
    drop(setup);

    let old_engine = AnalyticsEngine::connect(&backend).expect("connect old worker");
    let new_engine = AnalyticsEngine::connect(&backend).expect("connect new worker");
    let ready = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let old_ready = Arc::clone(&ready);
    let old_release = Arc::clone(&release);
    let old_worker = thread::spawn(move || {
        old_engine
            .with_retention_effect_barriers(old_ready, old_release)
            .delete_expired_rows_fenced(request(74, 74, None, 1))
    });

    ready.wait();
    let newer = apply(&new_engine, request(75, 75, None, 1)).expect("newer fence commits");
    assert!(matches!(newer, RetentionEffectDisposition::Applied { .. }));
    let before_old = (
        rows(&new_engine),
        authority_state(&new_engine),
        receipt_count(&new_engine),
    );
    release.wait();

    let old = old_worker.join().expect("join old worker");

    assert!(
        matches!(
            &old,
            Ok(RetentionEffectDisposition::StaleFence) | Err(AnalyticsEngineError::DuckDb(_))
        ),
        "old worker must be rejected after the newer commit: {old:?}"
    );
    assert_eq!(
        (
            rows(&new_engine),
            authority_state(&new_engine),
            receipt_count(&new_engine),
        ),
        before_old
    );
    assert_eq!(
        receipt_count(&new_engine),
        1,
        "old worker cannot add a receipt"
    );
}

fn retained_users_engine() -> (AnalyticsEngine, AnalyticsManifest) {
    retained_engine(&StorageBackend::DuckDb {
        path: ":memory:".to_string(),
    })
}

fn retained_engine(backend: &StorageBackend) -> (AnalyticsEngine, AnalyticsManifest) {
    let engine = AnalyticsEngine::connect(backend).expect("connect analytics engine");
    let mut manifest = users_manifest();
    manifest.tables[0].retention = Some(RetentionPolicy {
        period_ms: RETENTION_PERIOD_MS,
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: "created_at_ms".to_string(),
        },
    });
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    engine
        .authorize_retention_schedule_occurrence(
            ANALYTICS_TABLE_NAME,
            NOW_MS,
            NOW_MS,
            u64::try_from(NOW_MS).expect("schedule time fits"),
        )
        .expect("authorize retention schedule occurrence");
    (engine, manifest)
}

fn request(
    seed: u8,
    fence: u64,
    cursor: Option<String>,
    batch_size: u64,
) -> RetentionEffectRequest {
    RetentionEffectRequest::try_new(RetentionEffectRequestInput {
        authority: RetentionEffectAuthority::try_new([seed; 16], fence).expect("valid authority"),
        scope: RetentionEffectScope::try_new(ANALYTICS_TABLE_NAME).expect("valid scope"),
        cutoff_ms: NOW_MS,
        schedule_at_ms: NOW_MS,
        server_time_ms: u64::try_from(NOW_MS).expect("schedule time fits"),
        batch_size,
        cursor,
    })
    .expect("valid retention request")
}

fn apply(
    engine: &AnalyticsEngine,
    request: RetentionEffectRequest,
) -> AnalyticsEngineResult<RetentionEffectDisposition> {
    engine.delete_expired_rows_fenced(request)
}

fn seed_user(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    user_id: &str,
    created_at_ms: i64,
) -> AnalyticsEngineResult<()> {
    let email = format!("{user_id}@example.test");
    let mut item = user_item(user_id, email.as_str(), "org-a");
    item.insert(
        "created_at_ms".to_string(),
        StorageValue::N(created_at_ms.to_string()),
    );
    engine.ingest_stream_record(
        manifest,
        ANALYTICS_TABLE_NAME,
        user_id.as_bytes(),
        StorageStreamRecord {
            sequence_number: format!("sequence-{user_id}"),
            keys: VecMap::new(),
            old_image: None,
            new_image: Some(item),
        },
    )?;
    Ok(())
}

fn rows(engine: &AnalyticsEngine) -> Vec<serde_json::Value> {
    engine
        .query_unscoped_sql_json("SELECT user_id, __expiry FROM users ORDER BY user_id")
        .expect("query retention rows")
}

fn destination_state(engine: &AnalyticsEngine) -> (Vec<serde_json::Value>, u64, u64) {
    (rows(engine), authority_count(engine), receipt_count(engine))
}

fn authority_count(engine: &AnalyticsEngine) -> u64 {
    engine
        .query_unscoped_sql_json(
            "SELECT table_name FROM __analytics_retention_effect_authority ORDER BY table_name",
        )
        .expect("query retention authority")
        .len() as u64
}

fn receipt_count(engine: &AnalyticsEngine) -> u64 {
    engine
        .query_unscoped_sql_json(
            "SELECT execution_id FROM __analytics_retention_effect_receipts ORDER BY execution_id",
        )
        .expect("query retention receipts")
        .len() as u64
}

fn schedule_occurrence_count(engine: &AnalyticsEngine) -> u64 {
    engine
        .query_unscoped_sql_json(
            "SELECT table_name FROM __analytics_retention_schedule_occurrences ORDER BY table_name",
        )
        .expect("query retention schedule occurrences")
        .len() as u64
}

fn authority_state(engine: &AnalyticsEngine) -> (String, Option<String>) {
    engine
        .connection()
        .query_row(
            "SELECT fence, cursor
             FROM __analytics_retention_effect_authority
             WHERE table_name = ?",
            [ANALYTICS_TABLE_NAME],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("query retention authority state")
}

fn insert_receipt(engine: &AnalyticsEngine, fixture: ReceiptFixture<'_>) {
    engine
        .connection()
        .execute(
            "INSERT INTO __analytics_retention_effect_receipts
             VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
            duckdb::params![
                Value::Blob(vec![fixture.seed; 16]),
                fixture.table_name,
                fixture.fence,
                NOW_MS,
                1_i64,
                fixture.cursor_before,
                fixture.cursor_after,
                fixture.rows_deleted,
                fixture.complete,
                NOW_MS,
                NOW_MS,
            ],
        )
        .expect("seed retention receipt");
}

#[derive(Clone, Copy)]
struct ReceiptFixture<'a> {
    seed: u8,
    fence: &'a str,
    table_name: &'a str,
    cursor_before: Option<&'a str>,
    rows_deleted: i64,
    complete: bool,
    cursor_after: Option<&'a str>,
}

fn sqlite_ducklake_backend(root: &std::path::Path) -> (StorageBackend, (PathBuf, PathBuf)) {
    let catalog_path = root.join("catalog.sqlite");
    let data_path = root.join("ducklake-data");
    fs::create_dir_all(&data_path).expect("create DuckLake data directory");
    (
        StorageBackend::DuckLake {
            catalog: CatalogType::Sqlite,
            catalog_path: catalog_path.to_string_lossy().into_owned(),
            data_path: data_path.to_string_lossy().into_owned(),
            object_storage: None,
            catalog_settings: Default::default(),
        },
        (catalog_path, data_path),
    )
}
