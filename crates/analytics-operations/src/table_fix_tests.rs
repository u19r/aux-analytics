use std::{cell::RefCell, collections::HashMap, rc::Rc};

use analytics_contract::{StorageStreamRecord, StorageValue};

use crate::{
    CheckComparisonStrategy, CheckMetrics, CheckOutcome, CheckReport, CheckRowProjection,
    CheckSamples, CurrentRowPageRequest, CurrentRowStreamOrder, FilesystemRawBackupStore,
    OperationActor, OperationEventKind, OperationId, OperationKind, OperationPhase,
    OperationRequest, OperationStatus, OperationStore, ProductionCurrentRowReader,
    ProjectedCurrentRow, ProjectedCurrentRowPage, ProjectionBackedTableFixCurrentSource,
    RateLimitPolicy, RawBackupObjectId, RawBackupRecord, RawBackupWriteRequest,
    TableFixDestination, TableFixError, TableFixExecutor, TableFixRequest, TableFixSelection,
    TableFixSource, TableFixTruthRecord,
};

#[test]
fn given_raw_backup_table_fix_request_when_serialized_then_shape_is_internally_tagged() {
    let request = TableFixRequest::new(
        OperationId::new("fix_1").unwrap(),
        "users",
        true,
        TableFixSelection::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        TableFixSource::RawBackup {
            object_ids: vec![RawBackupObjectId::new("backup_1").unwrap()],
        },
    )
    .unwrap();

    let value = serde_json::to_value(&request).unwrap();

    assert_eq!(value["table_name"], "users");
    assert_eq!(value["dry_run"], true);
    assert_eq!(value["selection"]["type"], "row_keys");
    assert_eq!(value["source"]["type"], "raw_backup");
    assert_eq!(value["validation"]["require_post_fix_check"], true);
}

#[test]
fn given_empty_row_selection_when_table_fix_request_built_then_it_is_rejected() {
    let error = TableFixRequest::new(
        OperationId::new("fix_empty").unwrap(),
        "users",
        true,
        TableFixSelection::RowKeys { keys: Vec::new() },
        TableFixSource::CurrentSource {
            source_identity: "users".to_string(),
        },
    )
    .unwrap_err();

    assert!(matches!(error, TableFixError::EmptyRowSelection));
}

#[test]
fn given_empty_raw_backup_objects_when_table_fix_request_built_then_it_is_rejected() {
    let error = TableFixRequest::new(
        OperationId::new("fix_empty_backup").unwrap(),
        "users",
        true,
        TableFixSelection::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        TableFixSource::RawBackup {
            object_ids: Vec::new(),
        },
    )
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

    let report = TableFixExecutor::new(&store)
        .execute_raw_backup(&request, &[index], &mut destination)
        .unwrap();

    assert_eq!(report.selected_rows, 1);
    assert_eq!(report.repaired_rows, 1);
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

    let report = TableFixExecutor::new(&store)
        .execute_raw_backup(&request, &[index], &mut destination)
        .unwrap();

    assert_eq!(report.repaired_rows, 1);
    assert!(report.destination_mutated);
    assert_eq!(destination.applied.len(), 1);
    assert_eq!(destination.applied[0].0, "users");
    assert_eq!(destination.applied[0].1, b"user-1");
}

#[test]
fn given_table_fix_report_when_persisted_then_audit_counts_exclude_raw_rows() {
    let tempdir = tempfile::tempdir().unwrap();
    let operation_id = OperationId::new("fix_audit").unwrap();
    let store = OperationStore::connect_duckdb(tempdir.path().join("operations.duckdb")).unwrap();
    store
        .create_operation(&OperationRequest {
            operation_id: operation_id.clone(),
            kind: OperationKind::TableFix,
            actor: OperationActor::new("test").unwrap(),
            target_tables: vec!["users".to_string()],
            dry_run: false,
            rate_limit: RateLimitPolicy::default(),
            payload: serde_json::json!({}),
        })
        .unwrap();
    let report = table_fix_report(&operation_id);

    store.save_table_fix_report(&operation_id, &report).unwrap();
    store
        .transition_with_counts(
            &operation_id,
            OperationPhase::Completed,
            OperationStatus::Succeeded,
            None,
            OperationEventKind::Succeeded,
            report.operation_counts(),
            Some("table fix applied"),
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
    let store = FilesystemRawBackupStore::new(tempfile::tempdir().unwrap().path());
    let source = FakeCurrentSource {
        records: vec![truth_record("users", "user-1", false)],
    };
    let request = current_source_fix_request(false, "fix_current", b"user-1".to_vec());
    let mut destination = CapturingDestination::default();

    let report = TableFixExecutor::new(&store)
        .execute_current_source(&request, &source, &mut destination)
        .unwrap();

    assert_eq!(report.repaired_rows, 1);
    assert!(report.destination_mutated);
    assert_eq!(destination.applied.len(), 1);
}

#[test]
fn given_current_source_returns_stale_truth_when_fix_executes_then_it_is_rejected() {
    let store = FilesystemRawBackupStore::new(tempfile::tempdir().unwrap().path());
    let source = FakeCurrentSource {
        records: vec![truth_record("users", "user-1", true)],
    };
    let request = current_source_fix_request(false, "fix_stale", b"user-1".to_vec());
    let mut destination = CapturingDestination::default();

    let error = TableFixExecutor::new(&store)
        .execute_current_source(&request, &source, &mut destination)
        .unwrap_err();

    assert!(matches!(error, TableFixError::StaleSourceTruth));
    assert!(destination.applied.is_empty());
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
    let request = table_fix_request(false, "fix_too_many", "backup_too_many", b"user-1".to_vec())
        .with_validation(crate::TableFixValidation {
            require_post_fix_check: true,
            max_rows: 0,
        });
    let mut destination = CapturingDestination::default();

    let error = TableFixExecutor::new(&store)
        .execute_raw_backup(&request, &[index], &mut destination)
        .unwrap_err();

    assert!(matches!(
        error,
        TableFixError::RepairSetTooLarge {
            selected_rows: 1,
            max_rows: 0
        }
    ));
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

    let error = TableFixExecutor::new(&store)
        .execute_raw_backup(&request, &[index], &mut destination)
        .unwrap_err();

    assert!(matches!(
        error,
        TableFixError::PostFixValidationFailed { missing_rows: 1 }
    ));
    assert!(destination.applied.is_empty());
}

#[test]
fn given_check_report_mismatch_fix_when_executed_then_mismatched_key_is_repaired() {
    let store = FilesystemRawBackupStore::new(tempfile::tempdir().unwrap().path());
    let check_operation_id = OperationId::new("check_mismatch").unwrap();
    let source = FakeCurrentSource {
        records: vec![truth_record("users", "user-1", false)],
    };
    let request = TableFixRequest::new(
        OperationId::new("fix_from_check").unwrap(),
        "users",
        false,
        TableFixSelection::CheckReport {
            operation_id: check_operation_id.clone(),
        },
        TableFixSource::CheckReportMismatchSet {
            operation_id: check_operation_id.clone(),
        },
    )
    .unwrap();
    let mut destination = CapturingDestination::default();

    let report = TableFixExecutor::new(&store)
        .execute_check_report_mismatches(
            &request,
            &check_report(check_operation_id, "users:user-1"),
            &source,
            &mut destination,
        )
        .unwrap();

    assert_eq!(report.selected_rows, 1);
    assert_eq!(report.repaired_rows, 1);
    assert_eq!(destination.applied.len(), 1);
}

#[test]
fn given_projection_backed_current_source_when_fix_executes_then_selected_production_row_is_applied()
 {
    let store = FilesystemRawBackupStore::new(tempfile::tempdir().unwrap().path());
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

    let report = TableFixExecutor::new(&store)
        .execute_current_source(&request, &source, &mut destination)
        .unwrap();

    assert_eq!(report.selected_rows, 1);
    assert_eq!(report.repaired_rows, 1);
    assert_eq!(report.sample_keys, vec!["user-1"]);
    assert_eq!(destination.applied.len(), 1);
    assert_eq!(destination.applied[0].0, "users");
    assert_eq!(destination.applied[0].1, b"user-1");
    assert_eq!(destination.applied[0].2.sequence_number, "7");
    assert_eq!(
        destination.applied[0].2.new_image.as_ref().unwrap()["email"],
        StorageValue::S("ada@example.test".to_string())
    );
}

#[test]
fn given_projection_backed_current_source_for_other_table_when_fix_executes_then_row_is_missing() {
    let store = FilesystemRawBackupStore::new(tempfile::tempdir().unwrap().path());
    let source = ProjectionBackedTableFixCurrentSource::new(
        CheckRowProjection::new("orders", "__id", vec!["email".to_string()]).unwrap(),
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
    let request =
        current_source_fix_request(false, "fix_projected_current_missing", b"user-1".to_vec())
            .with_validation(crate::TableFixValidation {
                require_post_fix_check: false,
                max_rows: 1_000,
            });
    let mut destination = CapturingDestination::default();

    let report = TableFixExecutor::new(&store)
        .execute_current_source(&request, &source, &mut destination)
        .unwrap();

    assert_eq!(report.selected_rows, 1);
    assert_eq!(report.repaired_rows, 0);
    assert_eq!(report.missing_rows, 1);
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

impl TableFixDestination for CapturingDestination {
    fn apply_repair(
        &mut self,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
    ) -> crate::TableFixResult<()> {
        self.applied
            .push((table_name.to_string(), record_key.to_vec(), record));
        Ok(())
    }
}

struct FakeCurrentSource {
    records: Vec<TableFixTruthRecord>,
}

impl crate::TableFixCurrentSource for FakeCurrentSource {
    fn read_truth(
        &self,
        _source_identity: &str,
        table_name: &str,
        row_keys: &[Vec<u8>],
    ) -> crate::TableFixResult<Vec<TableFixTruthRecord>> {
        Ok(self
            .records
            .iter()
            .filter(|record| {
                record.table_name == table_name
                    && row_keys
                        .iter()
                        .any(|key| key.as_slice() == record.record_key.as_slice())
            })
            .cloned()
            .collect())
    }
}

fn table_fix_request(
    dry_run: bool,
    operation_id: &str,
    object_id: &str,
    key: Vec<u8>,
) -> TableFixRequest {
    TableFixRequest::new(
        OperationId::new(operation_id).unwrap(),
        "users",
        dry_run,
        TableFixSelection::RowKeys { keys: vec![key] },
        TableFixSource::RawBackup {
            object_ids: vec![RawBackupObjectId::new(object_id).unwrap()],
        },
    )
    .unwrap()
}

fn current_source_fix_request(dry_run: bool, operation_id: &str, key: Vec<u8>) -> TableFixRequest {
    TableFixRequest::new(
        OperationId::new(operation_id).unwrap(),
        "users",
        dry_run,
        TableFixSelection::RowKeys { keys: vec![key] },
        TableFixSource::CurrentSource {
            source_identity: "users".to_string(),
        },
    )
    .unwrap()
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
    CheckRowProjection::new("users", "__id", vec!["email".to_string()]).unwrap()
}

fn projected_row(key: &str, email: &str, source_position: u64) -> ProjectedCurrentRow {
    ProjectedCurrentRow::new(
        key,
        [("email".to_string(), Some(email.to_string()))]
            .into_iter()
            .collect(),
        source_position,
    )
}

fn raw_backup_request(
    operation_id: &str,
    object_id: &str,
    record_key: &str,
) -> RawBackupWriteRequest {
    RawBackupWriteRequest::new(
        OperationId::new(operation_id).unwrap(),
        RawBackupObjectId::new(object_id).unwrap(),
        "users",
        Some("policy-v1".to_string()),
        vec![
            RawBackupRecord::new(
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

fn storage_item<const N: usize>(
    values: [(&str, StorageValue); N],
) -> HashMap<String, StorageValue> {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}
