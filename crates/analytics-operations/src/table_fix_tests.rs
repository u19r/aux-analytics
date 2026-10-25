use std::{cell::RefCell, collections::HashMap, rc::Rc};

use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};

use crate::{
    CheckComparisonStrategy, CheckMetrics, CheckOutcome, CheckReport, CheckRowProjection,
    CheckSamples, CurrentRowPageRequest, CurrentRowStreamOrder, FilesystemRawBackupStore,
    MaintenanceExecutionAuthority, OperationActor, OperationChunkKey, OperationEventKind,
    OperationId, OperationKind, OperationPhase, OperationRateLimiter, OperationRequest,
    OperationStatus, OperationStore, ProductionCurrentRowReader, ProjectedCurrentRow,
    ProjectedCurrentRowPage, ProjectionBackedTableFixCurrentSource, RateLimitClock,
    RateLimitPolicy, RawBackupObjectId, RawBackupRecord, RawBackupWriteRequest,
    SystemRateLimitClock, TableFixError, TableFixExecutor, TableFixRequest, TableFixSelection,
    TableFixSource, TableFixSourceSnapshot, TableFixTruthRecord, VerifiedTableFixDestination,
    VersionedTableFixCurrentSource, store_test_support::OperationStoreTestFixture,
};

fn test_authority(operation_id: &OperationId) -> MaintenanceExecutionAuthority {
    test_authority_at(operation_id, 1)
}

fn test_authority_at(operation_id: &OperationId, generation: u64) -> MaintenanceExecutionAuthority {
    MaintenanceExecutionAuthority::try_new(operation_id.clone(), generation).unwrap()
}

fn run_current_source_fix<Store: ?Sized, Source, Destination>(
    executor: &TableFixExecutor<'_, Store>,
    request: &TableFixRequest,
    source: &Source,
    destination: &mut Destination,
) -> crate::TableFixResult<crate::TableFixReport>
where
    Source: VersionedTableFixCurrentSource,
    Destination: VerifiedTableFixDestination,
{
    let limiter = OperationRateLimiter::new(RateLimitPolicy::default());
    executor.execute_current_source_verified_with_limiter_at(
        request,
        source,
        destination,
        &limiter,
        &SystemRateLimitClock,
    )
}

fn run_raw_backup_fix<Store, Destination>(
    executor: &TableFixExecutor<'_, Store>,
    request: &TableFixRequest,
    indexes: &[crate::RawBackupObjectIndex],
    destination: &mut Destination,
) -> crate::TableFixResult<crate::TableFixReport>
where
    Store: ?Sized + crate::RawBackupObjectStore,
    Destination: VerifiedTableFixDestination,
{
    let limiter = OperationRateLimiter::new(RateLimitPolicy::default());
    executor.execute_raw_backup_verified_with_limiter_at(
        request,
        indexes,
        destination,
        &limiter,
        &SystemRateLimitClock,
    )
}

#[test]
fn given_raw_backup_table_fix_request_when_serialized_then_shape_is_internally_tagged() {
    let request = TableFixRequest {
        operation_id: OperationId::try_new("fix_1").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        source: TableFixSource::RawBackup {
            object_ids: vec![RawBackupObjectId::try_new("backup_1").unwrap()],
        },
        validation: crate::TableFixValidation::default(),
    };

    request.validate().unwrap();
    let value = serde_json::to_value(&request).unwrap();

    assert_eq!(value["table_name"], "users");
    assert_eq!(value["dry_run"], true);
    assert_eq!(value["selection"]["type"], "row_keys");
    assert_eq!(value["source"]["type"], "raw_backup");
    assert_eq!(value["validation"]["require_post_fix_check"], true);
}

#[test]
fn given_empty_row_selection_when_table_fix_request_is_validated_then_it_is_rejected() {
    let error = (TableFixRequest {
        operation_id: OperationId::try_new("fix_empty").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::RowKeys { keys: Vec::new() },
        source: TableFixSource::CurrentSource {
            source_identity: "users".to_string(),
        },
        validation: crate::TableFixValidation::default(),
    })
    .validate()
    .unwrap_err();

    assert!(matches!(error, TableFixError::EmptyRowSelection));
}

#[test]
fn given_empty_raw_backup_objects_when_table_fix_request_is_validated_then_it_is_rejected() {
    let error = (TableFixRequest {
        operation_id: OperationId::try_new("fix_empty_backup").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        source: TableFixSource::RawBackup {
            object_ids: Vec::new(),
        },
        validation: crate::TableFixValidation::default(),
    })
    .validate()
    .unwrap_err();

    assert!(matches!(error, TableFixError::EmptyRawBackupObjects));
}

#[test]
fn given_raw_backup_fix_dry_run_when_executed_then_destination_is_not_mutated() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let index = store
        .write_object(&raw_backup_request(
            "fix_dry_run_backup",
            "backup_dry",
            "user-1",
        ))
        .unwrap();
    let request = table_fix_request(true, "fix_dry_run", "backup_dry", b"user-1".to_vec());
    let mut destination = CapturingDestination::default();

    let report = run_raw_backup_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &[index],
        &mut destination,
    )
    .unwrap();

    assert_eq!(report.selected_rows, 1);
    assert_eq!(report.repaired_rows, 0);
    assert_eq!(report.missing_rows, 0);
    assert!(!report.destination_mutated);
    assert!(destination.applied.is_empty());
}

#[test]
fn given_raw_backup_fix_apply_when_executed_then_selected_row_is_applied() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let index = store
        .write_object(&raw_backup_request(
            "fix_apply_backup",
            "backup_apply",
            "user-1",
        ))
        .unwrap();
    let request = table_fix_request(false, "fix_apply", "backup_apply", b"user-1".to_vec());
    let mut destination = CapturingDestination::default();

    let report = run_raw_backup_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &[index],
        &mut destination,
    )
    .unwrap();

    assert_eq!(report.repaired_rows, 1);
    assert!(report.destination_mutated);
    assert_eq!(destination.applied.len(), 1);
    assert_eq!(destination.applied[0].0, "users");
    assert_eq!(destination.applied[0].1, b"user-1");
}

#[test]
fn given_duplicate_raw_backup_indexes_when_table_fix_executes_then_first_index_is_used() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let index = store
        .write_object(&raw_backup_request(
            "fix_duplicate_backup",
            "backup_duplicate",
            "user-1",
        ))
        .unwrap();
    let mut duplicate = index.clone();
    duplicate.checksum = "not-the-first-checksum".to_string();
    let original_checksum = index.checksum.clone();
    let duplicate_checksum = duplicate.checksum.clone();
    let request = table_fix_request(
        false,
        "fix_duplicate",
        "backup_duplicate",
        b"user-1".to_vec(),
    );
    let mut destination = VerifiedCapturingDestination {
        verify: true,
        ..VerifiedCapturingDestination::default()
    };

    let report = run_raw_backup_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &[index, duplicate],
        &mut destination,
    )
    .unwrap();

    assert_eq!(report.repaired_rows, 1);
    assert_eq!(destination.applied.len(), 1);
    assert_eq!(destination.revisions.len(), 1);
    assert!(destination.revisions[0].contains(format!(":{original_checksum}:").as_str()));
    assert!(!destination.revisions[0].contains(format!(":{duplicate_checksum}:").as_str()));
}

#[test]
fn given_duplicate_requested_raw_backup_object_when_table_fix_executes_then_duplicate_truth_is_rejected()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let index = store
        .write_object(&raw_backup_request(
            "fix_duplicate_request_backup",
            "backup_duplicate_request",
            "user-1",
        ))
        .unwrap();
    let mut request = table_fix_request(
        false,
        "fix_duplicate_request",
        index.object_id.as_str(),
        b"user-1".to_vec(),
    );
    request.source = TableFixSource::RawBackup {
        object_ids: vec![index.object_id.clone(), index.object_id.clone()],
    };
    let mut destination = CapturingDestination::default();

    let error = run_raw_backup_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        std::slice::from_ref(&index),
        &mut destination,
    )
    .unwrap_err();

    assert!(matches!(error, TableFixError::DuplicateTruthRecord));
    assert!(destination.applied.is_empty());
}

#[test]
fn given_missing_raw_backup_index_when_table_fix_executes_then_it_is_rejected_before_mutation() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let missing = RawBackupObjectId::try_new("backup_missing_index").unwrap();
    let request = table_fix_request(
        false,
        "fix_missing_index",
        missing.as_str(),
        b"user-1".to_vec(),
    );
    let mut destination = CapturingDestination::default();

    let error = run_raw_backup_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &[],
        &mut destination,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        TableFixError::MissingRawBackupIndex(object_id) if object_id == missing
    ));
    assert!(destination.applied.is_empty());
}

#[test]
fn given_table_fix_report_when_persisted_then_audit_counts_exclude_raw_rows() {
    let tempdir = tempfile::tempdir().unwrap();
    let operation_id = OperationId::try_new("fix_audit").unwrap();
    let store = OperationStore::connect_duckdb(tempdir.path().join("operations.duckdb")).unwrap();
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: OperationKind::TableFix,
            actor: OperationActor::try_new("test").unwrap(),
            target_tables: vec!["users".to_string()],
            dry_run: false,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::json!({}),
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
    let report = table_fix_report(&operation_id);

    store_fixture
        .save_report(
            &operation_id,
            crate::store::OperationReportKind::TableFix,
            &report,
        )
        .unwrap();
    store_fixture
        .transition(
            &store,
            &operation_id,
            crate::OperationTransition {
                phase: OperationPhase::Completed,
                status: OperationStatus::Succeeded,
                cursor: None,
                event_kind: OperationEventKind::Succeeded,
                counts: report.operation_counts(),
                message: Some("table fix applied"),
            },
        )
        .unwrap();
    let events = store.audit_events(&operation_id).unwrap();
    let counts = &events.last().unwrap().counts;

    assert_eq!(counts["repaired_rows"], 1);
    assert_eq!(counts["sample_keys"][0], "user-1");
    assert!(counts.get("record").is_none());
    assert_eq!(
        store.table_fix_report(&operation_id).unwrap().repaired_rows,
        1
    );
}

#[test]
fn given_current_source_fix_apply_when_executed_then_truth_row_is_applied() {
    let store = ();
    let source = FakeCurrentSource {
        records: vec![truth_record("users", "user-1", false)],
    };
    let request = current_source_fix_request(false, "fix_current", b"user-1".to_vec());
    let mut destination = CapturingDestination::default();

    let report = run_current_source_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &source,
        &mut destination,
    )
    .unwrap();

    assert_eq!(report.repaired_rows, 1);
    assert!(report.destination_mutated);
    assert_eq!(destination.applied.len(), 1);
}

#[test]
fn given_multiple_current_source_repairs_when_destination_budget_is_exhausted_then_later_repair_is_rejected()
 {
    let store = ();
    let source = VersionedFakeCurrentSource {
        snapshot: TableFixSourceSnapshot {
            source_revision: "source-revision-1".to_string(),
            records: vec![
                truth_record("users", "user-1", false),
                truth_record("users", "user-2", false),
            ],
        },
    };
    let mut request = TableFixRequest {
        operation_id: OperationId::try_new("fix_rate_limited_repairs").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::RowKeys {
            keys: vec![b"user-1".to_vec(), b"user-2".to_vec()],
        },
        source: TableFixSource::CurrentSource {
            source_identity: "users".to_string(),
        },
        validation: crate::TableFixValidation::default(),
    };
    request.dry_run = false;
    request.validation.confirmation =
        Some(crate::TableFixConfirmation::try_new("fix-confirmed").unwrap());
    request.validate().unwrap();
    let policy = RateLimitPolicy {
        max_destination_writes_per_second: Some(3),
        ..RateLimitPolicy::default()
    };
    let mut destination = CapturingDestination::default();

    let limiter = OperationRateLimiter::new(policy);
    let error = TableFixExecutor::new(&store, test_authority(&request.operation_id))
        .execute_current_source_verified_with_limiter_at(
            &request,
            &source,
            &mut destination,
            &limiter,
            &FixedRateLimitClock,
        )
        .unwrap_err();

    assert!(matches!(error, TableFixError::RateLimit(_)));
    assert_eq!(destination.applied.len(), 1);
}

#[test]
fn given_table_fix_exceeds_max_rows_when_executed_then_it_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let index = store
        .write_object(&raw_backup_request(
            "fix_too_many_backup",
            "backup_too_many",
            "user-1",
        ))
        .unwrap();
    let mut request =
        table_fix_request(false, "fix_too_many", "backup_too_many", b"user-1".to_vec());
    request.validation.max_rows = 0;
    let mut destination = CapturingDestination::default();

    let error = run_raw_backup_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &[index],
        &mut destination,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        TableFixError::RepairSetTooLarge {
            selected_rows: 1,
            max_rows: 0
        }
    ));
    assert!(destination.applied.is_empty());
}

#[test]
fn given_required_post_fix_check_has_missing_rows_when_executed_then_it_is_rejected() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = FilesystemRawBackupStore::new(tempdir.path());
    let index = store
        .write_object(&raw_backup_request(
            "fix_missing_backup",
            "backup_missing",
            "other-user",
        ))
        .unwrap();
    let request = table_fix_request(false, "fix_missing", "backup_missing", b"user-1".to_vec());
    let mut destination = CapturingDestination::default();

    let error = run_raw_backup_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &[index],
        &mut destination,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        TableFixError::PostFixValidationFailed { missing_rows: 1 }
    ));
    assert!(destination.applied.is_empty());
}

#[test]
fn given_check_report_mismatch_fix_when_executed_then_mismatched_key_is_repaired() {
    let store = ();
    let check_operation_id = OperationId::try_new("check_mismatch").unwrap();
    let source = FakeCurrentSource {
        records: vec![truth_record("users", "user-1", false)],
    };
    let mut request = TableFixRequest {
        operation_id: OperationId::try_new("fix_from_check").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::CheckReport {
            operation_id: check_operation_id.clone(),
        },
        source: TableFixSource::CheckReportMismatchSet {
            operation_id: check_operation_id.clone(),
        },
        validation: crate::TableFixValidation::default(),
    };
    request.dry_run = false;
    request.validation.confirmation =
        Some(crate::TableFixConfirmation::try_new("fix-confirmed").unwrap());
    request.validate().unwrap();
    let mut destination = CapturingDestination::default();

    let limiter = OperationRateLimiter::new(RateLimitPolicy::default());
    let report = TableFixExecutor::new(&store, test_authority(&request.operation_id))
        .execute_check_report_mismatches_verified_with_limiter_at(
            &request,
            &check_report(check_operation_id, "users:user-1"),
            &source,
            &mut destination,
            &limiter,
            &SystemRateLimitClock,
        )
        .unwrap();

    assert_eq!(report.selected_rows, 1);
    assert_eq!(report.repaired_rows, 1);
    assert_eq!(destination.applied.len(), 1);
}

#[test]
fn given_verified_check_report_fix_when_executed_then_revision_is_verified() {
    let store = ();
    let check_operation_id = OperationId::try_new("check_verified_mismatch").unwrap();
    let source = VersionedFakeCurrentSource {
        snapshot: TableFixSourceSnapshot {
            source_revision: "source-revision-1".to_string(),
            records: vec![truth_record("users", "user-1", false)],
        },
    };
    let mut request = TableFixRequest {
        operation_id: OperationId::try_new("fix_verified_check").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::CheckReport {
            operation_id: check_operation_id.clone(),
        },
        source: TableFixSource::CheckReportMismatchSet {
            operation_id: check_operation_id.clone(),
        },
        validation: crate::TableFixValidation::default(),
    };
    request.dry_run = false;
    request.validation.confirmation =
        Some(crate::TableFixConfirmation::try_new("fix-confirmed").unwrap());
    let mut destination = VerifiedCapturingDestination {
        verify: true,
        ..VerifiedCapturingDestination::default()
    };

    let limiter = OperationRateLimiter::new(RateLimitPolicy::default());
    let report = TableFixExecutor::new(&store, test_authority(&request.operation_id))
        .execute_check_report_mismatches_verified_with_limiter_at(
            &request,
            &check_report(check_operation_id, "users:user-1"),
            &source,
            &mut destination,
            &limiter,
            &SystemRateLimitClock,
        )
        .unwrap();

    assert_eq!(report.repaired_rows, 1);
    assert_eq!(destination.applied.len(), 1);
    assert_eq!(destination.revisions, vec!["source-revision-1"]);
}

#[test]
fn given_projection_backed_current_source_when_fix_executes_then_selected_production_row_is_applied()
 {
    let store = ();
    let source = ProjectionBackedTableFixCurrentSource::new(
        check_projection(),
        ProductionCurrentRowReader::new(
            OrderedProductionPageReader::new(
                vec![ProjectedCurrentRowPage::terminal(vec![
                    projected_row("user-1", "ada@example.test", 7),
                    projected_row("user-2", "grace@example.test", 8),
                ])],
                CurrentRowStreamOrder::GloballySortedByKey,
            ),
            100,
        ),
    );
    let request = current_source_fix_request(false, "fix_projected_current", b"user-1".to_vec());
    let mut destination = CapturingDestination::default();

    let report = run_current_source_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &source,
        &mut destination,
    )
    .unwrap();

    assert_eq!(report.selected_rows, 1);
    assert_eq!(report.repaired_rows, 1);
    assert_eq!(report.sample_keys, vec!["user-1"]);
    assert_eq!(destination.applied.len(), 1);
    assert_eq!(destination.applied[0].0, "users");
    assert_eq!(destination.applied[0].1, b"user-1");
    assert_eq!(
        serde_json::from_str::<analytics_contract::SourcePosition>(
            &destination.applied[0].2.sequence_number,
        )
        .expect("canonical source position"),
        analytics_contract::SourcePosition::RawBackupObjectOffset {
            object_id: "check-fixture".to_owned(),
            offset: 7,
        },
    );
    assert_eq!(
        destination.applied[0]
            .2
            .new_image
            .as_ref()
            .and_then(|item| item.get("email"))
            .expect("email"),
        &StorageValue::S("ada@example.test".to_string())
    );
}

#[test]
fn given_projection_backed_current_source_for_other_table_when_fix_executes_then_row_is_missing() {
    let store = ();
    let source = ProjectionBackedTableFixCurrentSource::new(
        CheckRowProjection::try_new("orders", "__id", vec!["email".to_string()]).unwrap(),
        ProductionCurrentRowReader::new(
            OrderedProductionPageReader::new(
                vec![ProjectedCurrentRowPage::terminal(vec![projected_row(
                    "user-1",
                    "ada@example.test",
                    7,
                )])],
                CurrentRowStreamOrder::GloballySortedByKey,
            ),
            100,
        ),
    );
    let mut request =
        current_source_fix_request(false, "fix_projected_current_missing", b"user-1".to_vec());
    request.validation.require_post_fix_check = false;
    let mut destination = CapturingDestination::default();

    let report = run_current_source_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &source,
        &mut destination,
    )
    .unwrap();

    assert_eq!(report.selected_rows, 1);
    assert_eq!(report.repaired_rows, 0);
    assert_eq!(report.missing_rows, 1);
    assert!(destination.applied.is_empty());
}

#[test]
fn given_verified_table_fix_when_destination_post_write_check_fails_then_success_is_rejected() {
    let store = ();
    let source = VersionedFakeCurrentSource {
        snapshot: TableFixSourceSnapshot {
            source_revision: "source-revision-1".to_string(),
            records: vec![truth_record("users", "user-1", false)],
        },
    };
    let request = current_source_fix_request(false, "fix_verified_failure", b"user-1".to_vec());
    let mut destination = VerifiedCapturingDestination {
        verify: false,
        ..VerifiedCapturingDestination::default()
    };

    let error = run_current_source_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &source,
        &mut destination,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        TableFixError::DestinationVerificationFailed(key) if key == "user-1"
    ));
    assert_eq!(destination.applied.len(), 1);
    assert_eq!(destination.revisions, vec!["source-revision-1"]);
}

#[test]
fn given_verified_table_fix_dry_run_when_executed_then_no_repairs_are_applied() {
    let store = ();
    let source = VersionedFakeCurrentSource {
        snapshot: TableFixSourceSnapshot {
            source_revision: "source-revision-1".to_string(),
            records: vec![truth_record("users", "user-1", false)],
        },
    };
    let request = current_source_fix_request(true, "fix_verified_dry_run", b"user-1".to_vec());
    let mut destination = VerifiedCapturingDestination::default();

    let report = run_current_source_fix(
        &TableFixExecutor::new(&store, test_authority(&request.operation_id)),
        &request,
        &source,
        &mut destination,
    )
    .unwrap();

    assert_eq!(report.repaired_rows, 0);
    assert!(!report.destination_mutated);
    assert!(destination.applied.is_empty());
}

#[derive(Debug, Clone)]
struct OrderedProductionPageReader {
    pages: Rc<RefCell<Vec<ProjectedCurrentRowPage>>>,
    stream_order: CurrentRowStreamOrder,
}

impl OrderedProductionPageReader {
    fn new(pages: Vec<ProjectedCurrentRowPage>, stream_order: CurrentRowStreamOrder) -> Self {
        Self {
            pages: Rc::new(RefCell::new(pages)),
            stream_order,
        }
    }
}

impl crate::ProductionCurrentRowPageReader for OrderedProductionPageReader {
    fn current_row_page(
        &self,
        _request: &CurrentRowPageRequest,
    ) -> crate::CheckResult<ProjectedCurrentRowPage> {
        Ok(self
            .pages
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| ProjectedCurrentRowPage::terminal(Vec::new())))
    }

    fn stream_order(&self) -> CurrentRowStreamOrder {
        self.stream_order
    }
}

#[derive(Default)]
struct CapturingDestination {
    applied: Vec<(String, Vec<u8>, StorageStreamRecord)>,
}

impl VerifiedTableFixDestination for CapturingDestination {
    fn apply_repair_with_key(
        &mut self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &crate::OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        _expected_source_revision: &str,
    ) -> crate::TableFixResult<crate::TableFixApplyOutcome> {
        self.applied
            .push((table_name.to_string(), record_key.to_vec(), record));
        Ok(crate::TableFixApplyOutcome::Applied)
    }

    fn verify_repair(
        &self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &crate::OperationChunkKey,
        _table_name: &str,
        _record_key: &[u8],
        _expected: &StorageStreamRecord,
        _expected_source_revision: &str,
    ) -> crate::TableFixResult<bool> {
        Ok(true)
    }
}

struct FakeCurrentSource {
    records: Vec<TableFixTruthRecord>,
}

impl VersionedTableFixCurrentSource for FakeCurrentSource {
    fn read_truth_snapshot(
        &self,
        _source_identity: &str,
        table_name: &str,
        row_keys: &[Vec<u8>],
    ) -> crate::TableFixResult<TableFixSourceSnapshot> {
        let records = self
            .records
            .iter()
            .filter(|record| {
                record.table_name == table_name
                    && row_keys
                        .iter()
                        .any(|key| key.as_slice() == record.record_key.as_slice())
            })
            .cloned()
            .collect();
        Ok(TableFixSourceSnapshot {
            source_revision: "fake-test-source".to_string(),
            records,
        })
    }
}

struct VersionedFakeCurrentSource {
    snapshot: TableFixSourceSnapshot,
}

impl VersionedTableFixCurrentSource for VersionedFakeCurrentSource {
    fn read_truth_snapshot(
        &self,
        _source_identity: &str,
        _table_name: &str,
        _row_keys: &[Vec<u8>],
    ) -> crate::TableFixResult<TableFixSourceSnapshot> {
        Ok(self.snapshot.clone())
    }
}

#[derive(Default)]
struct VerifiedCapturingDestination {
    verify: bool,
    applied: Vec<(String, Vec<u8>, StorageStreamRecord)>,
    revisions: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct FixedRateLimitClock;

impl RateLimitClock for FixedRateLimitClock {
    fn minute_utc(&self) -> u16 {
        0
    }
}

impl VerifiedTableFixDestination for VerifiedCapturingDestination {
    fn apply_repair_with_key(
        &mut self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &crate::OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        expected_source_revision: &str,
    ) -> crate::TableFixResult<crate::TableFixApplyOutcome> {
        self.applied
            .push((table_name.to_string(), record_key.to_vec(), record));
        self.revisions.push(expected_source_revision.to_string());
        Ok(crate::TableFixApplyOutcome::Applied)
    }

    fn verify_repair(
        &self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &crate::OperationChunkKey,
        _table_name: &str,
        _record_key: &[u8],
        _expected: &StorageStreamRecord,
        _expected_source_revision: &str,
    ) -> crate::TableFixResult<bool> {
        Ok(self.verify)
    }
}

#[test]
fn given_table_fix_destination_when_lower_fence_uses_new_effect_key_then_write_is_rejected() {
    let operation_id = OperationId::try_new("table-fix-authority-order").unwrap();
    let high_authority = test_authority_at(&operation_id, 2);
    let low_authority = test_authority_at(&operation_id, 1);
    let first = truth_record("users", "user-1", false);
    let second = truth_record("users", "user-2", false);
    let first_key = OperationChunkKey::for_repair(
        &operation_id,
        first.table_name.as_str(),
        first.record_key.as_slice(),
    );
    let second_key = OperationChunkKey::for_repair(
        &operation_id,
        second.table_name.as_str(),
        second.record_key.as_slice(),
    );
    let mut destination = TransactionTableFixDestination::new(operation_id.clone());

    assert_eq!(
        destination
            .apply_repair_with_key(
                &high_authority,
                &first_key,
                first.table_name.as_str(),
                first.record_key.as_slice(),
                first.record.clone(),
                "revision-1",
            )
            .unwrap(),
        crate::TableFixApplyOutcome::Applied
    );
    let error = destination
        .apply_repair_with_key(
            &low_authority,
            &second_key,
            second.table_name.as_str(),
            second.record_key.as_slice(),
            second.record,
            "revision-2",
        )
        .unwrap_err();

    assert!(matches!(error, TableFixError::Destination(_)));
    assert_eq!(destination.mutation_count, 1);
    assert_eq!(destination.current_fence(), Some(2));
}

#[test]
fn given_table_fix_destination_when_same_authority_replays_same_key_then_mutation_is_idempotent() {
    let operation_id = OperationId::try_new("table-fix-authority-replay").unwrap();
    let authority = test_authority(&operation_id);
    let truth = truth_record("users", "user-1", false);
    let key = OperationChunkKey::for_repair(
        &operation_id,
        truth.table_name.as_str(),
        truth.record_key.as_slice(),
    );
    let mut destination = TransactionTableFixDestination::new(operation_id);

    let first = destination
        .apply_repair_with_key(
            &authority,
            &key,
            truth.table_name.as_str(),
            truth.record_key.as_slice(),
            truth.record.clone(),
            "revision-1",
        )
        .unwrap();
    let replay = destination
        .apply_repair_with_key(
            &authority,
            &key,
            truth.table_name.as_str(),
            truth.record_key.as_slice(),
            truth.record.clone(),
            "revision-1",
        )
        .unwrap();

    assert_eq!(first, crate::TableFixApplyOutcome::Applied);
    assert_eq!(replay, crate::TableFixApplyOutcome::AlreadyApplied);
    assert_eq!(destination.mutation_count, 1);
    assert!(
        destination
            .verify_repair(
                &authority,
                &key,
                truth.table_name.as_str(),
                truth.record_key.as_slice(),
                &truth.record,
                "revision-1",
            )
            .unwrap()
    );
}

#[test]
fn given_table_fix_destination_when_higher_fence_replaces_authority_then_new_effect_applies_once() {
    let operation_id = OperationId::try_new("table-fix-authority-advance").unwrap();
    let first_authority = test_authority_at(&operation_id, 1);
    let second_authority = test_authority_at(&operation_id, 2);
    let first = truth_record("users", "user-1", false);
    let second = truth_record("users", "user-2", false);
    let first_key = OperationChunkKey::for_repair(
        &operation_id,
        first.table_name.as_str(),
        first.record_key.as_slice(),
    );
    let second_key = OperationChunkKey::for_repair(
        &operation_id,
        second.table_name.as_str(),
        second.record_key.as_slice(),
    );
    let mut destination = TransactionTableFixDestination::new(operation_id);

    let first_outcome = destination
        .apply_repair_with_key(
            &first_authority,
            &first_key,
            first.table_name.as_str(),
            first.record_key.as_slice(),
            first.record,
            "revision-1",
        )
        .unwrap();
    let second_outcome = destination
        .apply_repair_with_key(
            &second_authority,
            &second_key,
            second.table_name.as_str(),
            second.record_key.as_slice(),
            second.record.clone(),
            "revision-2",
        )
        .unwrap();
    let replay_outcome = destination
        .apply_repair_with_key(
            &second_authority,
            &second_key,
            second.table_name.as_str(),
            second.record_key.as_slice(),
            second.record,
            "revision-2",
        )
        .unwrap();

    assert_eq!(first_outcome, crate::TableFixApplyOutcome::Applied);
    assert_eq!(second_outcome, crate::TableFixApplyOutcome::Applied);
    assert_eq!(replay_outcome, crate::TableFixApplyOutcome::AlreadyApplied);
    assert_eq!(destination.mutation_count, 2);
    assert_eq!(destination.current_fence(), Some(2));
}

#[test]
fn given_table_fix_destination_when_execution_key_or_content_mismatch_then_write_fails_closed() {
    let operation_id = OperationId::try_new("table-fix-authority-mismatch").unwrap();
    let authority = test_authority(&operation_id);
    let truth = truth_record("users", "user-1", false);
    let key = OperationChunkKey::for_repair(
        &operation_id,
        truth.table_name.as_str(),
        truth.record_key.as_slice(),
    );
    let mut destination = TransactionTableFixDestination::new(operation_id.clone());
    destination
        .apply_repair_with_key(
            &authority,
            &key,
            truth.table_name.as_str(),
            truth.record_key.as_slice(),
            truth.record.clone(),
            "revision-1",
        )
        .unwrap();

    let wrong_execution = OperationId::try_new("table-fix-other-execution").unwrap();
    let wrong_authority = test_authority(&wrong_execution);
    let wrong_execution_key = OperationChunkKey::for_repair(
        &wrong_execution,
        truth.table_name.as_str(),
        truth.record_key.as_slice(),
    );
    let execution_error = destination
        .apply_repair_with_key(
            &wrong_authority,
            &wrong_execution_key,
            truth.table_name.as_str(),
            truth.record_key.as_slice(),
            truth.record.clone(),
            "revision-1",
        )
        .unwrap_err();
    assert!(matches!(execution_error, TableFixError::Destination(_)));

    let wrong_key = OperationChunkKey::for_operation(&operation_id, "wrong", 0);
    let key_error = destination
        .apply_repair_with_key(
            &authority,
            &wrong_key,
            truth.table_name.as_str(),
            truth.record_key.as_slice(),
            truth.record.clone(),
            "revision-1",
        )
        .unwrap_err();
    assert!(matches!(key_error, TableFixError::Destination(_)));

    let mut wrong_record = truth.record.clone();
    wrong_record.sequence_number = "tampered-sequence".to_string();
    let content_error = destination
        .apply_repair_with_key(
            &authority,
            &key,
            truth.table_name.as_str(),
            truth.record_key.as_slice(),
            wrong_record,
            "revision-1",
        )
        .unwrap_err();
    assert!(matches!(content_error, TableFixError::Destination(_)));

    assert_eq!(destination.mutation_count, 1);
}

struct TransactionTableFixDestination {
    execution_id: OperationId,
    authority: Option<MaintenanceExecutionAuthority>,
    receipts: HashMap<OperationChunkKey, AppliedTableFix>,
    mutation_count: usize,
}

struct AppliedTableFix {
    table_name: String,
    record_key: Vec<u8>,
    record: StorageStreamRecord,
    source_revision: String,
}

impl TransactionTableFixDestination {
    fn new(execution_id: OperationId) -> Self {
        Self {
            execution_id,
            authority: None,
            receipts: HashMap::new(),
            mutation_count: 0,
        }
    }

    fn current_fence(&self) -> Option<u64> {
        self.authority
            .as_ref()
            .map(|authority| authority.fence().get())
    }

    fn validate_authority(
        &self,
        authority: &MaintenanceExecutionAuthority,
    ) -> crate::TableFixResult<()> {
        if authority.execution_id() != &self.execution_id {
            return Err(TableFixError::Destination(
                "execution authority does not match destination".to_string(),
            ));
        }
        if self
            .authority
            .as_ref()
            .is_some_and(|current| current.fence() > authority.fence())
        {
            return Err(TableFixError::Destination(
                "stale execution authority".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_key(
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
    ) -> crate::TableFixResult<()> {
        let expected =
            OperationChunkKey::for_repair(authority.execution_id(), table_name, record_key);
        if key != &expected {
            return Err(TableFixError::Destination(
                "operation chunk key does not match repair identity".to_string(),
            ));
        }
        Ok(())
    }

    fn advance_authority(&mut self, authority: &MaintenanceExecutionAuthority) {
        if self
            .authority
            .as_ref()
            .is_none_or(|current| current.fence() < authority.fence())
        {
            self.authority = Some(authority.clone());
        }
    }

    fn matches(
        applied: &AppliedTableFix,
        table_name: &str,
        record_key: &[u8],
        record: &StorageStreamRecord,
        source_revision: &str,
    ) -> bool {
        applied.table_name == table_name
            && applied.record_key == record_key
            && applied.record == record.clone()
            && applied.source_revision == source_revision
    }
}

impl VerifiedTableFixDestination for TransactionTableFixDestination {
    fn apply_repair_with_key(
        &mut self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        expected_source_revision: &str,
    ) -> crate::TableFixResult<crate::TableFixApplyOutcome> {
        self.validate_authority(authority)?;
        Self::validate_key(authority, key, table_name, record_key)?;
        self.advance_authority(authority);
        if let Some(applied) = self.receipts.get(key) {
            if Self::matches(
                applied,
                table_name,
                record_key,
                &record,
                expected_source_revision,
            ) {
                return Ok(crate::TableFixApplyOutcome::AlreadyApplied);
            }
            return Err(TableFixError::Destination(
                "repair receipt does not match requested content".to_string(),
            ));
        }
        self.receipts.insert(
            key.clone(),
            AppliedTableFix {
                table_name: table_name.to_string(),
                record_key: record_key.to_vec(),
                record,
                source_revision: expected_source_revision.to_string(),
            },
        );
        self.mutation_count += 1;
        Ok(crate::TableFixApplyOutcome::Applied)
    }

    fn verify_repair(
        &self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        expected: &StorageStreamRecord,
        expected_source_revision: &str,
    ) -> crate::TableFixResult<bool> {
        self.validate_authority(authority)?;
        Self::validate_key(authority, key, table_name, record_key)?;
        Ok(self.receipts.get(key).is_some_and(|applied| {
            Self::matches(
                applied,
                table_name,
                record_key,
                expected,
                expected_source_revision,
            )
        }))
    }
}

fn table_fix_request(
    dry_run: bool,
    operation_id: &str,
    object_id: &str,
    key: Vec<u8>,
) -> TableFixRequest {
    let mut request = TableFixRequest {
        operation_id: OperationId::try_new(operation_id).unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::RowKeys { keys: vec![key] },
        source: TableFixSource::RawBackup {
            object_ids: vec![RawBackupObjectId::try_new(object_id).unwrap()],
        },
        validation: crate::TableFixValidation::default(),
    };
    request.dry_run = dry_run;
    if !dry_run {
        request.validation.confirmation =
            Some(crate::TableFixConfirmation::try_new("fix-confirmed").unwrap());
    }
    request.validate().unwrap();
    request
}

fn current_source_fix_request(dry_run: bool, operation_id: &str, key: Vec<u8>) -> TableFixRequest {
    let mut request = TableFixRequest {
        operation_id: OperationId::try_new(operation_id).unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::RowKeys { keys: vec![key] },
        source: TableFixSource::CurrentSource {
            source_identity: "users".to_string(),
        },
        validation: crate::TableFixValidation::default(),
    };
    request.dry_run = dry_run;
    if !dry_run {
        request.validation.confirmation =
            Some(crate::TableFixConfirmation::try_new("fix-confirmed").unwrap());
    }
    request.validate().unwrap();
    request
}

fn table_fix_report(operation_id: &OperationId) -> crate::TableFixReport {
    crate::TableFixReport {
        operation_id: operation_id.clone(),
        table_name: "users".to_string(),
        dry_run: false,
        selected_rows: 1,
        repaired_rows: 1,
        missing_rows: 0,
        destination_mutated: true,
        sample_keys: vec!["user-1".to_string()],
    }
}

fn check_report(operation_id: OperationId, mismatched_key: &str) -> CheckReport {
    CheckReport {
        operation_id,
        outcome: CheckOutcome::Mismatched,
        strategy: CheckComparisonStrategy::FullRows,
        metrics: CheckMetrics {
            mismatched_rows: 1,
            ..CheckMetrics::default()
        },
        samples: CheckSamples {
            mismatched_keys: vec![mismatched_key.to_string()],
            ..CheckSamples::default()
        },
    }
}

fn check_projection() -> CheckRowProjection {
    CheckRowProjection::try_new("users", "__id", vec!["email".to_string()]).unwrap()
}

fn projected_row(key: &str, email: &str, source_position: u64) -> ProjectedCurrentRow {
    ProjectedCurrentRow::new(
        key,
        [("email".to_string(), Some(email.to_string()))]
            .into_iter()
            .collect(),
        crate::check_test_support::backup_position(source_position),
    )
}

fn raw_backup_request(
    operation_id: &str,
    object_id: &str,
    record_key: &str,
) -> RawBackupWriteRequest {
    RawBackupWriteRequest::try_new(
        OperationId::try_new(operation_id).unwrap(),
        RawBackupObjectId::try_new(object_id).unwrap(),
        "users",
        Some("policy-v1".to_string()),
        vec![
            RawBackupRecord::try_new(
                "users",
                record_key,
                None,
                StorageStreamRecord {
                    keys: storage_item([("pk", StorageValue::S(record_key.to_string()))]),
                    sequence_number: record_key.to_string(),
                    old_image: None,
                    new_image: Some(storage_item([(
                        "pk",
                        StorageValue::S(record_key.to_string()),
                    )])),
                },
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

fn truth_record(table_name: &str, record_key: &str, stale: bool) -> TableFixTruthRecord {
    TableFixTruthRecord {
        table_name: table_name.to_string(),
        record_key: record_key.as_bytes().to_vec(),
        record: StorageStreamRecord {
            keys: storage_item([("pk", StorageValue::S(record_key.to_string()))]),
            sequence_number: record_key.to_string(),
            old_image: None,
            new_image: Some(storage_item([(
                "pk",
                StorageValue::S(record_key.to_string()),
            )])),
        },
        stale,
    }
}

fn storage_item<const N: usize>(values: [(&str, StorageValue); N]) -> StorageItem {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

#[test]
fn given_invalid_current_source_snapshots_when_repair_runs_then_no_destination_writes_occur() {
    for (revision, records, expected_error) in [
        (
            " ",
            vec![truth_record("users", "user-1", false)],
            TableFixError::MissingSourceRevision,
        ),
        (
            "revision",
            vec![
                truth_record("users", "user-1", false),
                truth_record("users", "user-1", false),
            ],
            TableFixError::DuplicateTruthRecord,
        ),
        (
            "revision",
            vec![truth_record("orders", "user-1", false)],
            TableFixError::TruthRecordOutsideSelection,
        ),
        // Staleness takes precedence over scope rejection, before any repair.
        (
            "revision",
            vec![
                truth_record("orders", "user-1", false),
                truth_record("users", "user-1", true),
            ],
            TableFixError::StaleSourceTruth,
        ),
    ] {
        let source = VersionedFakeCurrentSource {
            snapshot: TableFixSourceSnapshot {
                source_revision: revision.to_string(),
                records,
            },
        };
        let request = current_source_fix_request(false, "fix_invalid_snapshot", b"user-1".to_vec());
        let mut destination = CapturingDestination::default();
        let executor = TableFixExecutor::new(&(), test_authority(&request.operation_id));
        let error =
            run_current_source_fix(&executor, &request, &source, &mut destination).unwrap_err();
        assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&expected_error)
        );
        assert!(destination.applied.is_empty());
    }
}
