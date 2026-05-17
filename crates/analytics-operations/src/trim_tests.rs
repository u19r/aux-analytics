use crate::{
    OperationActor, OperationEventKind, OperationId, OperationKind, OperationPhase,
    OperationRequest, OperationStatus, OperationStore, RateLimitPolicy, TrimCandidateSource,
    TrimDestination, TrimError, TrimExecutor, TrimRequest, TrimTarget,
};

#[test]
fn given_trim_request_when_serialized_then_target_is_internally_tagged() {
    let request = trim_request(true, TrimTarget::FullTableDrop);

    let value = serde_json::to_value(&request).unwrap();

    assert_eq!(value["target"]["type"], "full_table_drop");
}

#[test]
fn given_lifecycle_trim_request_when_serialized_then_target_is_internally_tagged() {
    let snapshot_request = trim_request(
        true,
        TrimTarget::DuckLakeSnapshotExpiry {
            older_than_ms: 86_400_000,
        },
    );
    let raw_backup_request = trim_request(
        true,
        TrimTarget::RawBackupExpiration {
            older_than_ms: 86_400_000,
        },
    );

    let snapshot = serde_json::to_value(&snapshot_request).unwrap();
    let raw_backup = serde_json::to_value(&raw_backup_request).unwrap();

    assert_eq!(snapshot["target"]["type"], "duck_lake_snapshot_expiry");
    assert_eq!(raw_backup["target"]["type"], "raw_backup_expiration");
}

#[test]
fn given_dry_run_trim_request_when_validated_then_confirmation_is_not_required() {
    let request = trim_request(
        true,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
    );

    request.validate().unwrap();
}

#[test]
fn given_apply_trim_request_without_confirmation_when_validated_then_it_is_rejected() {
    let request = trim_request(
        false,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
    );

    let error = request.validate().unwrap_err();

    assert!(matches!(error, TrimError::MissingConfirmationToken));
}

#[test]
fn given_empty_row_selection_when_trim_request_validated_then_it_is_rejected() {
    let request = trim_request(true, TrimTarget::RowKeys { keys: Vec::new() });

    let error = request.validate().unwrap_err();

    assert!(matches!(error, TrimError::EmptyRowSelection));
}

#[test]
fn given_empty_predicate_when_trim_request_validated_then_it_is_rejected() {
    let request = trim_request(
        true,
        TrimTarget::RowPredicate {
            predicate_sql: " ".to_string(),
        },
    );

    let error = request.validate().unwrap_err();

    assert!(matches!(error, TrimError::EmptyPredicate));
}

#[test]
fn given_empty_lifecycle_horizon_when_trim_request_validated_then_it_is_rejected() {
    let request = trim_request(true, TrimTarget::RawBackupExpiration { older_than_ms: 0 });

    let error = request.validate().unwrap_err();

    assert!(matches!(error, TrimError::EmptyHorizon));
}

#[test]
fn given_scoped_row_predicate_when_trim_request_validated_then_it_is_accepted() {
    let request = trim_request(
        true,
        TrimTarget::RowPredicate {
            predicate_sql: "tenant_id = 'tenant-a' and deleted_at_ms < 1000".to_string(),
        },
    );

    request.validate().unwrap();
}

#[test]
fn given_unscoped_row_predicate_when_trim_request_validated_then_it_is_rejected() {
    let request = trim_request(
        true,
        TrimTarget::RowPredicate {
            predicate_sql: "1 = 1".to_string(),
        },
    );

    let error = request.validate().unwrap_err();

    assert!(matches!(error, TrimError::UnsafePredicate));
}

#[test]
fn given_row_predicate_with_dml_when_trim_request_validated_then_it_is_rejected() {
    let request = trim_request(
        true,
        TrimTarget::RowPredicate {
            predicate_sql: "tenant_id = 'tenant-a'; drop table users".to_string(),
        },
    );

    let error = request.validate().unwrap_err();

    assert!(matches!(error, TrimError::UnsafePredicate));
}

#[test]
fn given_trim_dry_run_when_executed_then_report_counts_candidates_without_mutation() {
    let request = trim_request(
        true,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec(), b"user-2".to_vec()],
        },
    );
    let source = FixedCandidateSource { count: 2 };

    let report = TrimExecutor
        .execute_dry_run(&request, &source)
        .expect("trim report");

    assert_eq!(report.candidate_rows, 2);
    assert_eq!(report.rows_deleted, 0);
    assert!(!report.destination_mutated);
}

#[test]
fn given_apply_trim_when_executor_runs_then_it_fails_closed() {
    let mut request = trim_request(
        false,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
    );
    request.confirmation_token = Some("trim-confirmed".to_string());
    let source = FixedCandidateSource { count: 1 };

    let error = TrimExecutor.execute_dry_run(&request, &source).unwrap_err();

    assert!(matches!(error, TrimError::ApplyNotSupported));
}

#[test]
fn given_confirmed_row_key_trim_when_applied_then_bounded_rows_are_deleted() {
    let mut request = trim_request(
        false,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec(), b"user-2".to_vec()],
        },
    );
    request.confirmation_token = Some("trim-confirmed".to_string());
    let source = FixedCandidateSource { count: 2 };
    let mut destination = CapturingTrimDestination::new(2);

    let report = TrimExecutor
        .execute_apply(&request, &source, &mut destination)
        .expect("trim apply report");

    assert_eq!(report.candidate_rows, 2);
    assert_eq!(report.rows_deleted, 2);
    assert!(report.destination_mutated);
    assert_eq!(destination.calls, 1);
    assert_eq!(destination.max_rows_seen, Some(2));
}

#[test]
fn given_destination_deletes_more_than_candidate_count_when_trim_applies_then_it_fails_closed() {
    let mut request = trim_request(
        false,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
    );
    request.confirmation_token = Some("trim-confirmed".to_string());
    let source = FixedCandidateSource { count: 1 };
    let mut destination = CapturingTrimDestination::new(2);

    let error = TrimExecutor
        .execute_apply(&request, &source, &mut destination)
        .unwrap_err();

    assert!(matches!(
        error,
        TrimError::DeleteCountExceededDryRun {
            candidate_rows: 1,
            deleted_rows: 2
        }
    ));
}

#[test]
fn given_duckdb_trim_rows_when_counting_candidates_then_row_keys_are_matched_by_hex_id() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let source = crate::DuckDbTrimRows::new(database_path.to_string_lossy().to_string());
    let request = trim_request(
        true,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec(), b"missing".to_vec()],
        },
    );

    let candidate_rows = source
        .count_candidates(request.table_name.as_str(), &request.target)
        .unwrap();

    assert_eq!(candidate_rows, 1);
}

#[test]
fn given_duckdb_trim_rows_when_apply_runs_then_only_selected_rows_are_deleted() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = crate::DuckDbTrimRows::new(duckdb_path.clone());
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let mut request = trim_request(
        false,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
    );
    request.confirmation_token = Some("trim-confirmed".to_string());

    let report = TrimExecutor
        .execute_apply(&request, &source, &mut destination)
        .unwrap();

    assert_eq!(report.candidate_rows, 1);
    assert_eq!(report.rows_deleted, 1);
    assert_eq!(trim_table_count(database_path.as_path()), 1);
}

#[test]
fn given_duckdb_trim_rows_when_full_table_drop_applies_then_table_is_dropped_with_count() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = crate::DuckDbTrimRows::new(duckdb_path.clone());
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let mut request = trim_request(false, TrimTarget::FullTableDrop);
    request.confirmation_token = Some("trim-confirmed".to_string());

    let report = TrimExecutor
        .execute_apply(&request, &source, &mut destination)
        .unwrap();

    assert_eq!(report.candidate_rows, 2);
    assert_eq!(report.rows_deleted, 2);
    assert!(trim_table_missing(database_path.as_path()));
}

#[test]
fn given_duckdb_trim_rows_when_horizon_trim_applies_then_only_expired_rows_are_deleted() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = crate::DuckDbTrimRows::new(duckdb_path.clone());
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let mut request = trim_request(
        false,
        TrimTarget::HorizonTrim {
            older_than_ms: 1_000,
        },
    );
    request.confirmation_token = Some("trim-confirmed".to_string());

    let report = TrimExecutor
        .execute_apply(&request, &source, &mut destination)
        .unwrap();

    assert_eq!(report.candidate_rows, 1);
    assert_eq!(report.rows_deleted, 1);
    assert_eq!(trim_table_count(database_path.as_path()), 1);
}

#[test]
fn given_duckdb_trim_rows_when_row_predicate_counts_then_only_matching_rows_are_candidates() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let source = crate::DuckDbTrimRows::new(database_path.to_string_lossy().to_string());

    let candidate_rows = source
        .count_candidates(
            "users",
            &TrimTarget::RowPredicate {
                predicate_sql: "email = 'one@example.com'".to_string(),
            },
        )
        .unwrap();

    assert_eq!(candidate_rows, 1);
}

#[test]
fn given_duckdb_trim_rows_when_row_predicate_applies_then_only_matching_rows_are_deleted() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = crate::DuckDbTrimRows::new(duckdb_path.clone());
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let mut request = trim_request(
        false,
        TrimTarget::RowPredicate {
            predicate_sql: "email = 'one@example.com'".to_string(),
        },
    );
    request.confirmation_token = Some("trim-confirmed".to_string());

    let report = TrimExecutor
        .execute_apply(&request, &source, &mut destination)
        .unwrap();

    assert_eq!(report.candidate_rows, 1);
    assert_eq!(report.rows_deleted, 1);
    assert_eq!(trim_table_count(database_path.as_path()), 1);
}

#[test]
fn given_duckdb_trim_rows_when_target_is_unsafe_row_predicate_then_it_fails_closed() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let mut destination = crate::DuckDbTrimRows::new(database_path.to_string_lossy().to_string());

    let error = destination
        .delete_candidates(
            "users",
            &TrimTarget::RowPredicate {
                predicate_sql: "1 = 1".to_string(),
            },
            1,
        )
        .unwrap_err();

    assert!(matches!(error, TrimError::UnsafePredicate));
}

#[test]
fn given_duckdb_trim_rows_when_target_is_lifecycle_expiration_then_it_fails_closed() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let destination = crate::DuckDbTrimRows::new(database_path.to_string_lossy().to_string());

    let error = destination
        .count_candidates(
            "users",
            &TrimTarget::DuckLakeSnapshotExpiry {
                older_than_ms: 86_400_000,
            },
        )
        .unwrap_err();

    assert!(matches!(error, TrimError::UnsupportedTarget));
}

#[test]
fn given_trim_report_when_persisted_then_audit_counts_exclude_raw_values() {
    let tempdir = tempfile::tempdir().unwrap();
    let operation_id = OperationId::new("trim_audit").unwrap();
    let store = OperationStore::connect_duckdb(tempdir.path().join("operations.duckdb")).unwrap();
    let request = trim_request(
        true,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
    );
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: OperationKind::Trim,
            actor: OperationActor::new("test").unwrap(),
            target_tables: vec!["users".to_string()],
            dry_run: true,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::to_value(&request).unwrap(),
        })
        .unwrap();
    let report = TrimExecutor
        .execute_dry_run(&request, &FixedCandidateSource { count: 1 })
        .unwrap();

    store.save_trim_report(&operation_id, &report).unwrap();
    store
        .transition_with_counts(
            &operation_id,
            OperationPhase::Completed,
            OperationStatus::Succeeded,
            None,
            OperationEventKind::Succeeded,
            report.operation_counts(),
            Some("trim dry-run completed"),
        )
        .unwrap();
    let events = store.audit_events(&operation_id).unwrap();
    let counts = &events.last().unwrap().counts;

    assert_eq!(counts["candidate_rows"], 1);
    assert!(counts.get("user-1").is_none());
    assert_eq!(store.trim_report(&operation_id).unwrap().candidate_rows, 1);
}

struct FixedCandidateSource {
    count: u64,
}

impl TrimCandidateSource for FixedCandidateSource {
    fn count_candidates(&self, _table_name: &str, _target: &TrimTarget) -> crate::TrimResult<u64> {
        Ok(self.count)
    }
}

#[derive(Default)]
struct CapturingTrimDestination {
    rows_deleted: u64,
    calls: u64,
    max_rows_seen: Option<u64>,
}

impl CapturingTrimDestination {
    const fn new(rows_deleted: u64) -> Self {
        Self {
            rows_deleted,
            calls: 0,
            max_rows_seen: None,
        }
    }
}

impl TrimDestination for CapturingTrimDestination {
    fn delete_candidates(
        &mut self,
        _table_name: &str,
        _target: &TrimTarget,
        max_rows: u64,
    ) -> crate::TrimResult<u64> {
        self.calls += 1;
        self.max_rows_seen = Some(max_rows);
        Ok(self.rows_deleted)
    }
}

fn seed_trim_table(database_path: &std::path::Path) {
    let connection = duckdb::Connection::open(database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE users (__id VARCHAR NOT NULL, email VARCHAR, __expiry BIGINT);
             INSERT INTO users VALUES ('757365722d31', 'one@example.com', 500);
             INSERT INTO users VALUES ('757365722d32', 'two@example.com', 1500);",
        )
        .unwrap();
}

fn trim_table_count(database_path: &std::path::Path) -> u64 {
    let connection = duckdb::Connection::open(database_path).unwrap();
    let count: i64 = connection
        .query_row("SELECT count(*) FROM users", [], |row| row.get(0))
        .unwrap();
    u64::try_from(count).unwrap()
}

fn trim_table_missing(database_path: &std::path::Path) -> bool {
    let connection = duckdb::Connection::open(database_path).unwrap();
    connection
        .query_row("SELECT count(*) FROM users", [], |row| row.get::<_, i64>(0))
        .is_err()
}

fn trim_request(dry_run: bool, target: TrimTarget) -> TrimRequest {
    TrimRequest {
        operation_id: OperationId::new("trim_1").unwrap(),
        table_name: "users".to_string(),
        dry_run,
        target,
        confirmation_token: None,
    }
}
