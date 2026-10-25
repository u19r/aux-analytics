use analytics_contract::{SourcePosition, StorageItem, StorageStreamRecord};
use analytics_fixtures::{user_item, users_manifest};

use super::{
    AnalyticsEngine, AnalyticsEngineError, IngestExecution, IngestOutcome, RecordCheckpoint,
    immediate_ingest::StreamRecordIngest, source_record_authority::SourceRecordAuthority,
    transaction::EngineTransaction,
};

#[test]
fn given_written_rows_when_transaction_unwinds_then_rows_roll_back_and_connection_remains_usable() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
    engine
        .connection()
        .execute_batch("CREATE TABLE effects (value INTEGER)")
        .expect("create table");
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _transaction = EngineTransaction::begin(&engine).expect("begin");
        engine
            .connection()
            .execute("INSERT INTO effects VALUES (?)", [1])
            .expect("write row");
        panic!("operation panicked after writing");
    }));
    assert!(panic.is_err());
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT count(*) AS count FROM effects")
            .expect("query"),
        [serde_json::json!({"count": 0})]
    );

    let transaction = EngineTransaction::begin(&engine).expect("begin after unwind");
    engine
        .connection()
        .execute("INSERT INTO effects VALUES (?)", [2])
        .expect("write following row");
    transaction.commit().expect("following commit");
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT value FROM effects")
            .expect("query"),
        [serde_json::json!({"value": 2})]
    );
}

#[test]
fn given_rolled_back_native_transaction_when_commit_fails_then_error_is_preserved_and_next_transaction_succeeds()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
    let transaction = EngineTransaction::begin(&engine).expect("begin");
    engine
        .connection()
        .execute_batch("ROLLBACK")
        .expect("native rollback");
    let error = transaction.commit().expect_err("commit must fail");
    assert!(error.to_string().contains("no transaction"));
    EngineTransaction::begin(&engine)
        .expect("begin after failed commit")
        .commit()
        .expect("following commit");
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT 42 AS answer")
            .expect("query"),
        [serde_json::json!({"answer": 42})]
    );
}

#[test]
fn given_caller_owned_transaction_when_rows_and_checkpoint_are_written_then_finish_commits_or_rolls_back_all_state()
 {
    for commit in [true, false] {
        let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
        let manifest = users_manifest();
        engine
            .ensure_manifest(&manifest)
            .expect("registered tables");
        let checkpoint = RecordCheckpoint::try_new("tenant_01", "shard", "1").unwrap();
        let position = SourcePosition::AuxStorageStreamCursor {
            stream_name: "users".into(),
            cursor: "1".into(),
        };
        let mut transaction = EngineTransaction::begin(&engine).expect("caller starts transaction");
        for (id, authority) in [
            ("plain", SourceRecordAuthority::Unpositioned),
            ("positioned", SourceRecordAuthority::Positioned(&position)),
        ] {
            let outcome = transaction
                .ingest_record(StreamRecordIngest {
                    manifest: &manifest,
                    analytics_table_name: "users",
                    record_key: id.as_bytes(),
                    record: StorageStreamRecord {
                        keys: StorageItem::new(),
                        sequence_number: "1".into(),
                        old_image: None,
                        new_image: Some(user_item(id, "visible", "org-a")),
                    },
                    execution: IngestExecution {
                        authority,
                        ..Default::default()
                    },
                })
                .expect("inner ingest borrows caller transaction without nested BEGIN");
            assert_eq!(outcome, IngestOutcome::Inserted);
        }
        engine
            .write_record_checkpoint(&checkpoint)
            .expect("same transaction writes checkpoint");
        assert_eq!(
            engine
                .query_unscoped_sql_json("SELECT count(*) AS count FROM users")
                .unwrap(),
            [serde_json::json!({"count": 2})]
        );
        if commit {
            transaction.commit().expect("commit all effects");
        } else {
            let error = transaction
                .finish::<()>(Err(AnalyticsEngineError::TableNotRegistered(
                    "late_failure".into(),
                )))
                .expect_err("late caller failure rolls back before returning");
            assert!(
                matches!(error, AnalyticsEngineError::TableNotRegistered(name) if name == "late_failure")
            );
        }
        let rows = engine
            .query_unscoped_sql_json("SELECT user_id, email FROM users ORDER BY user_id")
            .unwrap();
        let expected = if commit {
            vec![
                serde_json::json!({"user_id": "plain", "email": "visible"}),
                serde_json::json!({"user_id": "positioned", "email": "visible"}),
            ]
        } else {
            vec![]
        };
        assert_eq!(rows, expected);
        assert_eq!(
            engine.load_source_checkpoints().unwrap(),
            if commit { vec![checkpoint] } else { vec![] }
        );
        assert_eq!(
            engine
                .query_unscoped_sql_json(
                    "SELECT count(*) AS count FROM __analytics_row_source_positions"
                )
                .unwrap(),
            [serde_json::json!({"count": usize::from(commit)})]
        );
        engine
            .ingest_stream_record(
                &manifest,
                "users",
                b"following",
                StorageStreamRecord {
                    keys: StorageItem::new(),
                    sequence_number: "2".into(),
                    old_image: None,
                    new_image: Some(user_item("following", "after", "org-a")),
                },
            )
            .expect("next public ingestion owns a new transaction");
        assert_eq!(
            engine
                .query_unscoped_sql_json("SELECT email FROM users WHERE user_id = 'following'")
                .unwrap(),
            [serde_json::json!({"email": "after"})]
        );
    }
}

#[test]
#[ignore = "manual equal-work single-row transaction guard overhead probe"]
fn given_equal_native_single_row_transactions_when_guarded_then_output_and_overhead_are_measured() {
    use std::time::Instant;

    use alloc_counter::AllocationGuard;
    use sha2::{Digest, Sha256};

    let allocations = std::env::var_os("ANALYTICS_TRANSACTION_PROBE_ALLOCATIONS").is_some();
    let expected = (0..128)
        .map(|id| serde_json::json!({"id": id, "payload": "literal payload"}))
        .collect::<Vec<_>>();
    let expected_digest = Sha256::digest(serde_json::to_vec(&expected).unwrap());
    for pair in 0..5 {
        let modes = if pair % 2 == 0 {
            ["manual", "guarded"]
        } else {
            ["guarded", "manual"]
        };
        for mode in modes {
            let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
            engine
                .connection()
                .execute_batch("CREATE TABLE effects (id INTEGER, payload VARCHAR)")
                .unwrap();
            engine
                .connection()
                .execute_batch("BEGIN; INSERT INTO effects VALUES (-1, 'warmup'); ROLLBACK")
                .unwrap();
            let guard = allocations.then(|| {
                AllocationGuard::start(
                    module_path!(),
                    "single-row transactions",
                    file!(),
                    line!(),
                    None,
                )
            });
            let started = Instant::now();
            for id in 0..128 {
                match mode {
                    "manual" => {
                        engine
                            .connection()
                            .execute_batch("BEGIN TRANSACTION")
                            .unwrap();
                        engine
                            .connection()
                            .execute(
                                "INSERT INTO effects VALUES (?, ?)",
                                duckdb::params![id, "literal payload"],
                            )
                            .unwrap();
                        engine.connection().execute_batch("COMMIT").unwrap();
                    }
                    _ => {
                        let transaction = EngineTransaction::begin(&engine).unwrap();
                        let result = engine.connection().execute(
                            "INSERT INTO effects VALUES (?, ?)",
                            duckdb::params![id, "literal payload"],
                        );
                        transaction
                            .finish(result.map_err(AnalyticsEngineError::from))
                            .unwrap();
                    }
                }
            }
            let elapsed = started.elapsed();
            let report = guard.map(AllocationGuard::finish);
            let output = engine
                .query_unscoped_sql_json("SELECT id, payload FROM effects ORDER BY id")
                .unwrap();
            assert_eq!(output, expected);
            assert_eq!(
                Sha256::digest(serde_json::to_vec(&output).unwrap()),
                expected_digest
            );
            println!(
                "transaction-guard | mode={mode} | pair={pair} | rows=128 | elapsed_ns={} | \
                 allocations={:?} | bytes={:?} | sha256_bytes={expected_digest:?}",
                elapsed.as_nanos(),
                report.as_ref().map(|report| report.allocation_count),
                report.as_ref().map(|report| report.allocated_bytes)
            );
        }
    }
}
