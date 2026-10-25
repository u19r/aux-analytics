use crate::{
    MaintenanceExecutionAuthority, OperationActor, OperationEventKind, OperationId, OperationKind,
    OperationPhase, OperationRateLimiter, OperationRequest, OperationStatus, OperationStore,
    RateLimitClock, RateLimitPolicy, TrimCandidateSnapshot, TrimCandidateSource, TrimDestination,
    TrimError, TrimPredicate, TrimPredicateValue, TrimRequest, TrimTarget,
    store_test_support::OperationStoreTestFixture, trim::TrimExecution,
};

fn test_authority(operation_id: &OperationId) -> MaintenanceExecutionAuthority {
    test_authority_at(operation_id, 1)
}

fn test_authority_at(operation_id: &OperationId, generation: u64) -> MaintenanceExecutionAuthority {
    MaintenanceExecutionAuthority::try_new(operation_id.clone(), generation).unwrap()
}

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
fn given_blank_trim_confirmation_when_validated_then_it_is_rejected() {
    let mut request = trim_request(
        false,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
    );
    request.confirmation_token = Some("  \n".to_string());

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
fn given_empty_all_predicate_when_trim_request_is_validated_then_it_is_rejected() {
    let request = trim_request(
        true,
        TrimTarget::RowPredicate {
            predicate: TrimPredicate::All {
                predicates: Vec::new(),
            },
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
fn given_typed_row_predicate_when_trim_request_validated_then_it_is_accepted() {
    let request = trim_request(
        true,
        TrimTarget::RowPredicate {
            predicate: TrimPredicate::All {
                predicates: vec![
                    text_equals("tenant_id", "tenant-a"),
                    integer_less_than("deleted_at_ms", 1_000),
                ],
            },
        },
    );

    request.validate().unwrap();
}

#[test]
fn given_typed_row_predicate_with_unknown_field_when_deserialized_then_it_is_rejected() {
    let request = trim_request(
        true,
        TrimTarget::RowPredicate {
            predicate: text_equals("email", "one@example.com"),
        },
    );
    let mut encoded = serde_json::to_value(request).unwrap();
    encoded["target"]["predicate"]["legacy_mode"] = serde_json::json!(true);

    assert!(serde_json::from_value::<TrimRequest>(encoded).is_err());
}

#[test]
fn given_typed_predicate_literal_with_sql_delimiters_when_compiling_predicate_then_it_is_bound_as_a_value()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let source = crate::DuckDbTrimRows::new(database_path.to_string_lossy().to_string());
    let target = TrimTarget::RowPredicate {
        predicate: text_equals("email", "one@example.com' OR TRUE"),
    };

    assert_eq!(source.count_candidates("users", &target).unwrap(), 0);
    assert_eq!(trim_table_count(database_path.as_path()), 2);
}

#[test]
fn given_empty_any_predicate_when_trim_request_is_validated_then_it_is_rejected() {
    let request = trim_request(
        true,
        TrimTarget::RowPredicate {
            predicate: TrimPredicate::Any {
                predicates: Vec::new(),
            },
        },
    );

    let error = request.validate().unwrap_err();

    assert!(matches!(error, TrimError::EmptyPredicate));
}

#[test]
fn given_typed_row_predicate_with_invalid_column_when_validated_then_it_is_rejected() {
    let request = trim_request(
        true,
        TrimTarget::RowPredicate {
            predicate: text_equals("email; drop table users", "one@example.com"),
        },
    );

    let error = request.validate().unwrap_err();

    assert!(matches!(error, TrimError::UnsafePredicate));
}

#[test]
fn given_typed_row_predicate_that_exceeds_tree_depth_when_compiling_predicate_then_it_is_rejected()
{
    let predicate = (0..10).fold(text_equals("email", "one@example.com"), |predicate, _| {
        TrimPredicate::All {
            predicates: vec![predicate],
        }
    });
    let request = trim_request(true, TrimTarget::RowPredicate { predicate });

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

    let report = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut CapturingTrimDestination::new(0)))
    .expect("trim report");

    assert_eq!(report.candidate_rows, 2);
    assert_eq!(report.rows_deleted, 0);
    assert!(!report.destination_mutated);
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

    let report = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .expect("trim apply report");

    assert_eq!(report.candidate_rows, 2);
    assert_eq!(report.rows_deleted, 2);
    assert!(report.destination_mutated);
    assert_eq!(destination.calls, 1);
    assert_eq!(destination.max_rows_seen, Some(2));
}

#[test]
fn given_trim_candidate_rows_when_execution_budget_is_exhausted_then_destination_is_not_called() {
    let mut request = trim_request(
        false,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec(), b"user-2".to_vec()],
        },
    );
    request.confirmation_token = Some("trim-confirmed".to_string());
    let source = FixedCandidateSource { count: 2 };
    let mut destination = CapturingTrimDestination::new(2);
    let policy = RateLimitPolicy {
        max_rows_per_second: Some(2),
        ..RateLimitPolicy::default()
    };

    let error = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(policy.clone()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap_err();

    assert!(matches!(error, TrimError::RateLimit(_)));
    assert_eq!(destination.calls, 0);
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

    let error = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap_err();

    assert!(matches!(
        error,
        TrimError::DeleteCountExceededDryRun {
            candidate_rows: 1,
            deleted_rows: 0
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

    let report = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap();

    assert_eq!(report.candidate_rows, 1);
    assert_eq!(report.rows_deleted, 1);
    assert_eq!(trim_table_count(database_path.as_path()), 1);
}

#[test]
fn given_trim_destination_commit_when_report_retry_occurs_then_same_key_is_reconciled() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = StaticCandidateSource {
        revision: "sha256:106255974f8487af9a1cd71da819a70fa63e6b4e8f8a63df191e5e1ec0de95a2"
            .to_string(),
        candidate_keys: vec!["757365722d31".to_string()],
    };
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let mut request = trim_request(
        false,
        TrimTarget::RowPredicate {
            predicate: text_equals("email", "one@example.com"),
        },
    );
    request.operation_id = OperationId::try_new("trim-idempotent").unwrap();
    request.confirmation_token = Some("trim-confirmed".to_string());

    let first = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap();
    let second = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap();

    assert_eq!(second, first);
    assert_eq!(first.candidate_rows, 1);
    assert_eq!(first.rows_deleted, 1);
    assert_eq!(trim_table_count(database_path.as_path()), 1);
    assert_eq!(trim_effect_count(database_path.as_path()), 1);
}

#[test]
fn given_duckdb_trim_rows_when_lower_fence_uses_new_effect_key_then_write_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = crate::DuckDbTrimRows::new(duckdb_path.clone());
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let operation_id = OperationId::try_new("trim-authority-order").unwrap();
    let high_authority = test_authority_at(&operation_id, 2);
    let low_authority = test_authority_at(&operation_id, 1);
    let first_target = TrimTarget::RowKeys {
        keys: vec![b"user-1".to_vec()],
    };
    let second_target = TrimTarget::RowKeys {
        keys: vec![b"user-2".to_vec()],
    };
    let first_snapshot = source.candidate_snapshot("users", &first_target).unwrap();
    let second_snapshot = source.candidate_snapshot("users", &second_target).unwrap();
    let first_key = crate::OperationChunkKey::for_operation(&operation_id, "trim", 0);
    let second_key = crate::OperationChunkKey::for_operation(&operation_id, "trim", 1);

    destination
        .delete_candidate_snapshot_with_key(
            &high_authority,
            &first_key,
            "users",
            &first_target,
            &first_snapshot,
        )
        .unwrap();
    let error = destination
        .delete_candidate_snapshot_with_key(
            &low_authority,
            &second_key,
            "users",
            &second_target,
            &second_snapshot,
        )
        .unwrap_err();

    assert!(matches!(error, TrimError::StaleDestinationAuthority));
    assert_eq!(trim_table_count(database_path.as_path()), 1);
    assert_eq!(trim_effect_count(database_path.as_path()), 1);
    assert_eq!(
        trim_authority_fence(database_path.as_path(), &operation_id),
        Some(2)
    );
}

#[test]
fn given_duckdb_trim_rows_when_higher_fence_replaces_authority_then_new_effect_applies_once() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = crate::DuckDbTrimRows::new(duckdb_path.clone());
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let operation_id = OperationId::try_new("trim-authority-advance").unwrap();
    let first_authority = test_authority_at(&operation_id, 1);
    let second_authority = test_authority_at(&operation_id, 2);
    let first_target = TrimTarget::RowKeys {
        keys: vec![b"user-1".to_vec()],
    };
    let second_target = TrimTarget::RowKeys {
        keys: vec![b"user-2".to_vec()],
    };
    let first_snapshot = source.candidate_snapshot("users", &first_target).unwrap();
    let first_key = crate::OperationChunkKey::for_operation(&operation_id, "trim", 0);
    let first_outcome = destination
        .delete_candidate_snapshot_with_key(
            &first_authority,
            &first_key,
            "users",
            &first_target,
            &first_snapshot,
        )
        .unwrap();

    let second_snapshot = source.candidate_snapshot("users", &second_target).unwrap();
    let second_key = crate::OperationChunkKey::for_operation(&operation_id, "trim", 1);
    let second_outcome = destination
        .delete_candidate_snapshot_with_key(
            &second_authority,
            &second_key,
            "users",
            &second_target,
            &second_snapshot,
        )
        .unwrap();
    let replay_outcome = destination
        .delete_candidate_snapshot_with_key(
            &second_authority,
            &second_key,
            "users",
            &second_target,
            &second_snapshot,
        )
        .unwrap();

    assert_eq!(first_outcome.rows_deleted, 1);
    assert_eq!(second_outcome.rows_deleted, 1);
    assert_eq!(replay_outcome, second_outcome);
    assert_eq!(trim_table_count(database_path.as_path()), 0);
    assert_eq!(trim_effect_count(database_path.as_path()), 2);
    assert_eq!(
        trim_authority_fence(database_path.as_path(), &operation_id),
        Some(2)
    );
}

#[test]
fn given_duckdb_trim_rows_when_execution_key_or_content_mismatch_then_write_fails_closed() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = crate::DuckDbTrimRows::new(duckdb_path.clone());
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let operation_id = OperationId::try_new("trim-authority-mismatch").unwrap();
    let authority = test_authority(&operation_id);
    let target = TrimTarget::RowKeys {
        keys: vec![b"user-1".to_vec()],
    };
    let expected = source.candidate_snapshot("users", &target).unwrap();
    let key = crate::OperationChunkKey::for_operation(&operation_id, "trim", 0);
    destination
        .delete_candidate_snapshot_with_key(&authority, &key, "users", &target, &expected)
        .unwrap();

    let wrong_execution = OperationId::try_new("trim-other-execution").unwrap();
    let wrong_authority = test_authority(&wrong_execution);
    let mut wrong_content = expected.clone();
    wrong_content.revision = "tampered-revision".to_string();
    let execution_error = destination
        .delete_candidate_snapshot_with_key(
            &wrong_authority,
            &key,
            "users",
            &target,
            &wrong_content,
        )
        .unwrap_err();
    assert!(matches!(
        execution_error,
        TrimError::ConflictingDestinationReceipt
    ));

    let wrong_key = crate::OperationChunkKey::for_operation(&operation_id, "wrong", 0);
    let key_error = destination
        .delete_candidate_snapshot_with_key(&authority, &wrong_key, "users", &target, &expected)
        .unwrap_err();
    assert!(matches!(key_error, TrimError::StaleCandidateSnapshot));

    let content_error = destination
        .delete_candidate_snapshot_with_key(&authority, &key, "users", &target, &wrong_content)
        .unwrap_err();
    assert!(matches!(
        content_error,
        TrimError::ConflictingDestinationReceipt
    ));

    assert_eq!(trim_table_count(database_path.as_path()), 1);
    assert_eq!(trim_effect_count(database_path.as_path()), 1);
}

#[test]
fn given_trim_candidate_bound_is_stale_when_apply_runs_then_destination_is_not_mutated() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = FixedCandidateSource { count: 1 };
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let mut request = trim_request(
        false,
        TrimTarget::RowPredicate {
            predicate: TrimPredicate::Any {
                predicates: vec![
                    text_equals("email", "one@example.com"),
                    text_equals("email", "two@example.com"),
                ],
            },
        },
    );
    request.operation_id = OperationId::try_new("trim-stale-bound").unwrap();
    request.confirmation_token = Some("trim-confirmed".to_string());

    let error = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap_err();

    assert!(matches!(error, TrimError::StaleCandidateSnapshot));
    assert_eq!(trim_table_count(database_path.as_path()), 2);
}

#[test]
fn given_invalid_trim_request_when_execution_is_prepared_then_source_and_destination_are_untouched()
{
    for (table_name, keys, confirmation, expected) in [
        ("", vec![b"entry-1".to_vec()], Some("confirmed"), "table"),
        ("entries", Vec::new(), Some("confirmed"), "selection"),
        ("entries", vec![b"entry-1".to_vec()], None, "confirmation"),
    ] {
        let request = TrimRequest {
            operation_id: OperationId::try_new("invalid-trim-preparation").unwrap(),
            table_name: table_name.to_string(),
            dry_run: false,
            target: TrimTarget::RowKeys { keys },
            confirmation_token: confirmation.map(str::to_owned),
        };
        let source = CountingCandidateSource {
            count: 1,
            calls: std::cell::Cell::new(0),
        };
        let mut destination = CapturingTrimDestination::new(1);
        let error = TrimExecution::try_new(
            &request,
            &test_authority(&request.operation_id),
            &OperationRateLimiter::new(RateLimitPolicy::default()),
            &FixedRateLimitClock,
        )
        .and_then(|execution| execution.execute(&source, &mut destination))
        .unwrap_err();
        assert!(matches!(
            (expected, error),
            ("table", TrimError::EmptyTableName)
                | ("selection", TrimError::EmptyRowSelection)
                | ("confirmation", TrimError::MissingConfirmationToken)
        ));
        assert_eq!(source.calls.get(), 0);
        assert_eq!(destination.calls, 0);
    }
}

#[test]
fn given_trim_rate_limit_denies_source_when_apply_starts_then_no_io_is_performed() {
    let mut request = trim_request(
        false,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
    );
    request.confirmation_token = Some("trim-confirmed".to_string());
    let source = CountingCandidateSource {
        count: 1,
        calls: std::cell::Cell::new(0),
    };
    let mut destination = CapturingTrimDestination::new(1);
    let policy = RateLimitPolicy {
        max_source_requests_per_second: Some(0),
        ..RateLimitPolicy::default()
    };

    let error = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(policy.clone()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap_err();

    assert!(matches!(error, TrimError::RateLimit(_)));
    assert_eq!(source.calls.get(), 0);
    assert_eq!(destination.calls, 0);
}

#[test]
fn given_trim_rate_limit_denies_source_when_dry_run_starts_then_no_read_is_performed() {
    let request = trim_request(
        true,
        TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
    );
    let source = CountingCandidateSource {
        count: 1,
        calls: std::cell::Cell::new(0),
    };
    let policy = RateLimitPolicy {
        max_source_requests_per_second: Some(0),
        ..RateLimitPolicy::default()
    };

    let error = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(policy.clone()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut CapturingTrimDestination::new(0)))
    .unwrap_err();

    assert!(matches!(error, TrimError::RateLimit(_)));
    assert_eq!(source.calls.get(), 0);
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

    let report = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap();

    assert_eq!(report.candidate_rows, 2);
    assert_eq!(report.rows_deleted, 2);
    assert!(trim_table_missing(database_path.as_path()));
}

#[test]
fn given_empty_full_table_drop_when_applied_then_it_is_a_reported_noop() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    let connection = duckdb::Connection::open(database_path.as_path()).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE users (__id VARCHAR NOT NULL, email VARCHAR, __expiry BIGINT);",
        )
        .unwrap();
    drop(connection);
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = crate::DuckDbTrimRows::new(duckdb_path.clone());
    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let mut request = trim_request(false, TrimTarget::FullTableDrop);
    request.confirmation_token = Some("trim-confirmed".to_string());

    let report = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap();

    assert_eq!(report.candidate_rows, 0);
    assert_eq!(report.rows_deleted, 0);
    assert!(!report.destination_mutated);
    assert!(!trim_table_missing(database_path.as_path()));
    assert_eq!(trim_table_count(database_path.as_path()), 0);
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

    let report = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap();

    assert_eq!(report.candidate_rows, 1);
    assert_eq!(report.rows_deleted, 1);
    assert_eq!(trim_table_count(database_path.as_path()), 1);
    assert_eq!(
        read_trim_rows(database_path.as_path()),
        [(
            "757365722d32".to_string(),
            "two@example.com".to_string(),
            1500
        )]
    );
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
                predicate: text_equals("email", "one@example.com"),
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
            predicate: text_equals("email", "two@example.com"),
        },
    );
    request.confirmation_token = Some("trim-confirmed".to_string());

    let report = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| execution.execute(&source, &mut destination))
    .unwrap();

    assert_eq!(report.candidate_rows, 1);
    assert_eq!(report.rows_deleted, 1);
    assert_eq!(trim_table_count(database_path.as_path()), 1);
    assert_eq!(
        read_trim_rows(database_path.as_path()),
        [(
            "757365722d31".to_string(),
            "one@example.com".to_string(),
            500
        )]
    );
}

#[test]
fn given_duckdb_trim_rows_when_target_has_empty_predicate_then_it_fails_closed() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let mut destination = crate::DuckDbTrimRows::new(database_path.to_string_lossy().to_string());
    let expected = TrimCandidateSnapshot {
        revision: "unused".to_string(),
        candidate_keys: Vec::new(),
    };
    let key = crate::OperationChunkKey::for_operation(
        &OperationId::try_new("trim-unsafe-predicate").unwrap(),
        "trim",
        0,
    );

    let error = destination
        .delete_candidate_snapshot_with_key(
            &test_authority(&OperationId::try_new("trim-unsafe-predicate").unwrap()),
            &key,
            "users",
            &TrimTarget::RowPredicate {
                predicate: TrimPredicate::Any {
                    predicates: Vec::new(),
                },
            },
            &expected,
        )
        .unwrap_err();

    assert!(matches!(error, TrimError::EmptyPredicate));
}

#[test]
fn given_equal_count_but_different_trim_membership_when_applying_trim_then_destination_is_not_mutated()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    seed_trim_table(database_path.as_path());
    let duckdb_path = database_path.to_string_lossy().to_string();
    let source = crate::DuckDbTrimRows::new(duckdb_path.clone());
    let target = TrimTarget::RowPredicate {
        predicate: TrimPredicate::Any {
            predicates: vec![
                text_equals("email", "one@example.com"),
                text_equals("email", "two@example.com"),
            ],
        },
    };
    let expected = source.candidate_snapshot("users", &target).unwrap();

    let connection = duckdb::Connection::open(database_path.as_path()).unwrap();
    connection
        .execute_batch(
            "UPDATE users SET email = 'three@example.com' WHERE __id = '757365722d31';
             INSERT INTO users VALUES ('757365722d33', 'two@example.com', 2000);",
        )
        .unwrap();
    drop(connection);

    let mut destination = crate::DuckDbTrimRows::new(duckdb_path);
    let key = crate::OperationChunkKey::for_operation(
        &OperationId::try_new("trim-membership-stale").unwrap(),
        "trim",
        0,
    );
    let error = destination
        .delete_candidate_snapshot_with_key(
            &test_authority(&OperationId::try_new("trim-membership-stale").unwrap()),
            &key,
            "users",
            &target,
            &expected,
        )
        .unwrap_err();

    assert!(matches!(error, TrimError::StaleCandidateSnapshot));
    assert_eq!(trim_table_count(database_path.as_path()), 3);
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
    let operation_id = OperationId::try_new("trim_audit").unwrap();
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
            actor: OperationActor::try_new("test").unwrap(),
            target_tables: vec!["users".to_string()],
            dry_run: true,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::to_value(&request).unwrap(),
        })
        .unwrap();
    let store_fixture =
        OperationStoreTestFixture::for_database(tempdir.path().join("operations.duckdb"));
    store_fixture
        .prepare(
            &store,
            &operation_id,
            &OperationActor::try_new("test").unwrap(),
        )
        .unwrap();
    let report = TrimExecution::try_new(
        &request,
        &test_authority(&request.operation_id),
        &OperationRateLimiter::new(RateLimitPolicy::default()),
        &FixedRateLimitClock,
    )
    .and_then(|execution| {
        execution.execute(
            &FixedCandidateSource { count: 1 },
            &mut CapturingTrimDestination::new(0),
        )
    })
    .unwrap();

    store_fixture
        .save_report(
            &operation_id,
            crate::store::OperationReportKind::Trim,
            &report,
        )
        .unwrap();
    let expected = store.show_operation(&operation_id).unwrap();
    store
        .transition_with_counts_from(
            &expected,
            crate::OperationTransition {
                phase: OperationPhase::Completed,
                status: OperationStatus::Succeeded,
                cursor: None,
                event_kind: OperationEventKind::Succeeded,
                counts: report.operation_counts(),
                message: Some("trim dry-run completed"),
            },
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
    fn candidate_snapshot(
        &self,
        _table_name: &str,
        _target: &TrimTarget,
    ) -> crate::TrimResult<TrimCandidateSnapshot> {
        Ok(TrimCandidateSnapshot {
            revision: format!("fixed:{}", self.count),
            candidate_keys: (0..self.count)
                .map(|index| format!("candidate-{index}"))
                .collect(),
        })
    }
}

struct CountingCandidateSource {
    count: u64,
    calls: std::cell::Cell<u32>,
}

struct StaticCandidateSource {
    revision: String,
    candidate_keys: Vec<String>,
}

impl TrimCandidateSource for StaticCandidateSource {
    fn candidate_snapshot(
        &self,
        _table_name: &str,
        _target: &TrimTarget,
    ) -> crate::TrimResult<TrimCandidateSnapshot> {
        Ok(TrimCandidateSnapshot {
            revision: self.revision.clone(),
            candidate_keys: self.candidate_keys.clone(),
        })
    }
}

impl TrimCandidateSource for CountingCandidateSource {
    fn candidate_snapshot(
        &self,
        _table_name: &str,
        _target: &TrimTarget,
    ) -> crate::TrimResult<TrimCandidateSnapshot> {
        self.calls.set(self.calls.get() + 1);
        Ok(TrimCandidateSnapshot {
            revision: format!("counting:{}", self.count),
            candidate_keys: (0..self.count)
                .map(|index| format!("candidate-{index}"))
                .collect(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct FixedRateLimitClock;

impl RateLimitClock for FixedRateLimitClock {
    fn minute_utc(&self) -> u16 {
        0
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
    fn delete_candidate_snapshot_with_key(
        &mut self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &crate::OperationChunkKey,
        _table_name: &str,
        _target: &TrimTarget,
        expected: &TrimCandidateSnapshot,
    ) -> crate::TrimResult<crate::TrimDeletionOutcome> {
        self.calls += 1;
        self.max_rows_seen = Some(expected.candidate_rows());
        Ok(crate::TrimDeletionOutcome {
            candidate_rows: expected.candidate_rows(),
            rows_deleted: self.rows_deleted,
        })
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

fn read_trim_rows(database_path: &std::path::Path) -> Vec<(String, String, i64)> {
    let connection = duckdb::Connection::open(database_path).unwrap();
    let mut statement = connection
        .prepare("SELECT __id, email, __expiry FROM users ORDER BY __id")
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap()
}

fn trim_table_count(database_path: &std::path::Path) -> u64 {
    let connection = duckdb::Connection::open(database_path).unwrap();
    let count: i64 = connection
        .query_row("SELECT count(*) FROM users", [], |row| row.get(0))
        .unwrap();
    u64::try_from(count).unwrap()
}

fn trim_effect_count(database_path: &std::path::Path) -> u64 {
    let connection = duckdb::Connection::open(database_path).unwrap();
    let count: i64 = connection
        .query_row(
            "SELECT count(*) FROM __analytics_trim_effects_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    u64::try_from(count).unwrap()
}

fn trim_authority_fence(
    database_path: &std::path::Path,
    operation_id: &OperationId,
) -> Option<u64> {
    let connection = duckdb::Connection::open(database_path).unwrap();
    connection
        .query_row(
            "SELECT fence FROM __analytics_trim_authority_v2 WHERE execution_id = ?",
            duckdb::params![operation_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .and_then(|fence| fence.parse().ok())
}

fn trim_table_missing(database_path: &std::path::Path) -> bool {
    let connection = duckdb::Connection::open(database_path).unwrap();
    connection
        .query_row("SELECT count(*) FROM users", [], |row| row.get::<_, i64>(0))
        .is_err()
}

fn trim_request(dry_run: bool, target: TrimTarget) -> TrimRequest {
    TrimRequest {
        operation_id: OperationId::try_new("trim_1").unwrap(),
        table_name: "users".to_string(),
        dry_run,
        target,
        confirmation_token: None,
    }
}

fn text_equals(column: &str, value: &str) -> TrimPredicate {
    TrimPredicate::Eq {
        column: column.to_string(),
        value: TrimPredicateValue::Text(value.to_string()),
    }
}

fn integer_less_than(column: &str, value: i64) -> TrimPredicate {
    TrimPredicate::Lt {
        column: column.to_string(),
        value: TrimPredicateValue::Integer(value),
    }
}
