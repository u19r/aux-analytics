use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use analytics_contract::{ProjectionColumn, SourcePosition, TableRegistration};

use crate::{
    AuxStorageCheckRows, CheckBackendKind, CheckComparisonStrategy, CheckError, CheckOutcome,
    CheckOutputFormat, CheckRequest, CheckRow, CheckRowProjection, CurrentRowStreamOrder,
    DuckDbAnalyticalCheckRows, DuckDbCheckRows, DuckDbTableScanCurrentRowReader, DuckLakeCheckRows,
    DynamoDbCheckRows, FixtureCurrentRowReader, FixturePagedCurrentRowReader, LocalCheckDataset,
    LocalCheckExecutor, LocalFixtureCheckRows, OperationActor, OperationId, OperationStatus,
    OperationStore, ProductionCurrentRowPageReader, ProjectedCurrentRow, ProjectedCurrentRowPage,
    ProjectedCurrentRowStream, ProjectionBackedCheckRows, RateLimitPolicy, SourceTruthRows,
};

fn request(id: &str, sample_limit: usize) -> CheckRequest {
    CheckRequest {
        operation_id: OperationId::new(id).unwrap(),
        actor: OperationActor::new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        strategy: CheckComparisonStrategy::FullRows,
        sample_limit,
        privacy_policy_version: Some("policy-v1".to_string()),
        output: CheckOutputFormat::Json,
        rate_limit: RateLimitPolicy::default(),
    }
}

fn row(key: &str, row_hash: u64, source_position: u64) -> CheckRow {
    CheckRow {
        table: "users".to_string(),
        key: key.to_string(),
        row_hash,
        source_position,
        contains_private_data: false,
    }
}

fn projected_row(
    key: &str,
    email: &str,
    org_id: &str,
    source_position: u64,
) -> ProjectedCurrentRow {
    ProjectedCurrentRow::new(
        key,
        [
            ("email".to_string(), Some(email.to_string())),
            ("org_id".to_string(), Some(org_id.to_string())),
        ]
        .into_iter()
        .collect(),
        source_position,
    )
}

#[derive(Debug, Clone)]
struct RecordingCurrentRowStreamReader {
    rows: Vec<ProjectedCurrentRow>,
    calls: Rc<Cell<usize>>,
}

impl RecordingCurrentRowStreamReader {
    fn new(rows: Vec<ProjectedCurrentRow>, calls: Rc<Cell<usize>>) -> Self {
        Self { rows, calls }
    }
}

impl crate::CurrentRowStreamReader for RecordingCurrentRowStreamReader {
    fn current_row_stream(
        &self,
        _projection: &CheckRowProjection,
    ) -> crate::CheckResult<ProjectedCurrentRowStream> {
        self.calls.set(self.calls.get() + 1);
        Ok(ProjectedCurrentRowStream::from_unsorted_rows(
            self.rows.clone(),
        ))
    }
}

#[derive(Debug, Clone)]
struct RecordingProductionPageReader {
    pages: Rc<RefCell<Vec<ProjectedCurrentRowPage>>>,
    requests: Rc<RefCell<Vec<crate::CurrentRowPageRequest>>>,
}

impl RecordingProductionPageReader {
    fn new(
        pages: Vec<ProjectedCurrentRowPage>,
        requests: Rc<RefCell<Vec<crate::CurrentRowPageRequest>>>,
    ) -> Self {
        Self {
            pages: Rc::new(RefCell::new(pages)),
            requests,
        }
    }
}

impl crate::ProductionCurrentRowPageReader for RecordingProductionPageReader {
    fn current_row_page(
        &self,
        request: &crate::CurrentRowPageRequest,
    ) -> crate::CheckResult<ProjectedCurrentRowPage> {
        self.requests.borrow_mut().push(request.clone());
        Ok(self
            .pages
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| ProjectedCurrentRowPage::terminal(Vec::new())))
    }

    fn stream_order(&self) -> CurrentRowStreamOrder {
        CurrentRowStreamOrder::GloballySortedByKey
    }
}

fn check_projection() -> CheckRowProjection {
    CheckRowProjection::new(
        "users",
        "__id",
        vec!["email".to_string(), "org_id".to_string()],
    )
    .unwrap()
}

#[test]
fn given_matching_source_and_destination_when_check_runs_then_report_is_complete_and_persisted() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = LocalCheckDataset::new(vec![row("u1", 11, 3), row("u2", 22, 4)]);
    let destination = LocalCheckDataset::new(vec![row("u1", 11, 3), row("u2", 22, 4)]);
    let executor = LocalCheckExecutor;
    let request = request("check_complete_1", 3);

    let report = executor
        .execute(&store, &request, &source, &destination)
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.checked_rows, 2);
    assert_eq!(report.metrics.mismatched_rows, 0);
    assert_eq!(report.metrics.unsupported_comparisons, 0);
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Succeeded
    );
    assert_eq!(
        store.check_report(&request.operation_id).unwrap().outcome,
        CheckOutcome::Complete
    );
}

#[test]
fn given_missing_extra_mismatched_stale_and_private_rows_when_check_runs_then_evidence_is_bounded()
{
    let store = OperationStore::connect_in_memory().unwrap();
    let mut private_destination = row("u5", 55, 5);
    private_destination.contains_private_data = true;
    let source = LocalCheckDataset::new(vec![
        row("u1", 11, 3),
        row("u2", 22, 4),
        row("u3", 33, 9),
        row("u5", 55, 5),
    ]);
    let destination = LocalCheckDataset::new(vec![
        row("u1", 99, 3),
        row("u3", 33, 7),
        row("u4", 44, 4),
        private_destination,
    ]);
    let executor = LocalCheckExecutor;

    let report = executor
        .execute(
            &store,
            &request("check_mismatch_1", 1),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::PrivacyViolation);
    assert_eq!(report.metrics.missing_keys, 1);
    assert_eq!(report.metrics.extra_keys, 1);
    assert_eq!(report.metrics.mismatched_rows, 1);
    assert_eq!(report.metrics.stale_rows, 1);
    assert_eq!(report.metrics.privacy_violations, 1);
    assert_eq!(report.samples.missing_keys, vec!["users:u2"]);
    assert_eq!(report.samples.extra_keys, vec!["users:u4"]);
    assert_eq!(report.samples.mismatched_keys, vec!["users:u1"]);
    assert_eq!(report.samples.stale_keys, vec!["users:u3"]);
    assert_eq!(report.samples.privacy_violation_keys, vec!["users:u5"]);
    assert_eq!(
        store
            .show_operation(&OperationId::new("check_mismatch_1").unwrap())
            .unwrap()
            .status,
        OperationStatus::Failed
    );
}

#[test]
fn given_unsorted_rows_when_check_runs_then_sorted_merge_reports_stable_samples() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = LocalCheckDataset::new(vec![row("u3", 33, 9), row("u1", 11, 3), row("u2", 22, 4)]);
    let destination =
        LocalCheckDataset::new(vec![row("u4", 44, 4), row("u3", 33, 7), row("u1", 99, 3)]);

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_sorted_merge_1", 1),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Stale);
    assert_eq!(report.metrics.checked_rows, 4);
    assert_eq!(report.metrics.missing_keys, 1);
    assert_eq!(report.metrics.extra_keys, 1);
    assert_eq!(report.metrics.mismatched_rows, 1);
    assert_eq!(report.metrics.stale_rows, 1);
    assert_eq!(report.samples.missing_keys, vec!["users:u2"]);
    assert_eq!(report.samples.extra_keys, vec!["users:u4"]);
    assert_eq!(report.samples.mismatched_keys, vec!["users:u1"]);
    assert_eq!(report.samples.stale_keys, vec!["users:u3"]);
}

#[test]
fn given_vector_dataset_when_row_stream_requested_then_rows_are_sorted_by_table_and_key() {
    let dataset = LocalCheckDataset::new(vec![
        CheckRow {
            table: "users".to_string(),
            key: "u2".to_string(),
            row_hash: 22,
            source_position: 4,
            contains_private_data: false,
        },
        CheckRow {
            table: "orders".to_string(),
            key: "o1".to_string(),
            row_hash: 11,
            source_position: 3,
            contains_private_data: false,
        },
    ]);
    let mut stream = dataset
        .source_row_stream(&["users".to_string(), "orders".to_string()])
        .unwrap();

    assert_eq!(
        stream.next_row().unwrap().map(|row| row.table),
        Some("orders".to_string())
    );
    assert_eq!(
        stream.next_row().unwrap().map(|row| row.table),
        Some("users".to_string())
    );
    assert!(stream.next_row().unwrap().is_none());
}

#[test]
fn given_invalid_check_request_when_check_runs_then_it_is_rejected_before_operation_create() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = LocalCheckDataset::new(vec![]);
    let destination = LocalCheckDataset::new(vec![]);
    let executor = LocalCheckExecutor;
    let mut request = request("check_invalid_1", 0);
    request.target_tables.clear();

    let error = executor
        .execute(&store, &request, &source, &destination)
        .unwrap_err();

    assert!(matches!(error, CheckError::EmptyTargetTables));
    assert!(store.show_operation(&request.operation_id).is_err());
}

#[test]
fn given_counts_only_strategy_when_hashes_differ_then_only_counts_are_checked() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = LocalCheckDataset::new(vec![row("u1", 11, 3)]);
    let destination = LocalCheckDataset::new(vec![row("u1", 99, 1)]);
    let executor = LocalCheckExecutor;
    let mut request = request("check_counts_only_1", 3);
    request.strategy = CheckComparisonStrategy::CountsOnly;

    let report = executor
        .execute(&store, &request, &source, &destination)
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.mismatched_rows, 0);
    assert_eq!(report.metrics.stale_rows, 0);
}

#[test]
fn given_local_fixture_adapter_when_check_runs_then_rows_are_read_through_adapter_boundary() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = LocalFixtureCheckRows::new(LocalCheckDataset::new(vec![
        row("u1", 11, 3),
        CheckRow {
            table: "orders".to_string(),
            key: "o1".to_string(),
            row_hash: 44,
            source_position: 4,
            contains_private_data: false,
        },
    ]));
    let destination = LocalFixtureCheckRows::new(LocalCheckDataset::new(vec![row("u1", 11, 3)]));
    let executor = LocalCheckExecutor;

    let report = executor
        .execute(
            &store,
            &request("check_local_adapter_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.source_rows, 1);
    assert_eq!(report.metrics.destination_rows, 1);
}

#[test]
fn given_unsupported_backend_when_check_runs_then_operation_is_rejected_before_report_persist() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = AuxStorageCheckRows::unsupported(
        CheckBackendKind::AuxStorage,
        "current-row enumeration is not configured",
    );
    let destination = LocalFixtureCheckRows::new(LocalCheckDataset::new(vec![]));
    let request = request("check_unsupported_source_1", 3);

    let error = LocalCheckExecutor
        .execute(&store, &request, &source, &destination)
        .unwrap_err();

    assert!(matches!(
        error,
        CheckError::UnsupportedBackend {
            backend: CheckBackendKind::AuxStorage,
            ..
        }
    ));
    assert!(store.check_report(&request.operation_id).is_err());
}

#[test]
fn given_partially_supported_backend_when_check_runs_then_operation_is_rejected_before_report_persist()
 {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = DynamoDbCheckRows::partial(
        CheckBackendKind::DynamoDb,
        "consistent current-row reads require table scan permission",
    );
    let destination = LocalFixtureCheckRows::new(LocalCheckDataset::new(vec![]));
    let request = request("check_partial_source_1", 3);

    let error = LocalCheckExecutor
        .execute(&store, &request, &source, &destination)
        .unwrap_err();

    assert!(matches!(
        error,
        CheckError::PartiallySupportedBackend {
            backend: CheckBackendKind::DynamoDb,
            ..
        }
    ));
    assert!(store.check_report(&request.operation_id).is_err());
}

#[test]
fn given_ducklake_backend_without_reader_when_check_runs_then_it_fails_closed() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = DuckLakeCheckRows::unsupported(
        CheckBackendKind::DuckLake,
        "DuckLake check row extraction requires manifest-aware table scans",
    );
    let destination = LocalFixtureCheckRows::new(LocalCheckDataset::new(vec![]));
    let request = request("check_ducklake_unsupported_1", 3);

    let error = LocalCheckExecutor
        .execute(&store, &request, &source, &destination)
        .unwrap_err();

    assert!(matches!(
        error,
        CheckError::UnsupportedBackend {
            backend: CheckBackendKind::DuckLake,
            ..
        }
    ));
    assert!(store.check_report(&request.operation_id).is_err());
}

#[test]
fn given_aux_storage_current_row_reader_when_check_runs_then_projection_shapes_hashes() {
    let store = OperationStore::connect_in_memory().unwrap();
    let projection = check_projection();
    let source = ProjectionBackedCheckRows::fixture_aux_storage(
        projection.clone(),
        FixtureCurrentRowReader::new(vec![
            projected_row("u1", "a@example.test", "org-a", 10),
            projected_row("u2", "b@example.test", "org-b", 11),
        ]),
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::fixture_aux_storage(
        projection,
        FixtureCurrentRowReader::new(vec![
            projected_row("u1", "a@example.test", "org-a", 10),
            projected_row("u2", "b@example.test", "org-b", 11),
        ]),
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_aux_current_rows_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.checked_rows, 2);
}

#[test]
fn given_dynamodb_current_row_reader_when_projected_values_differ_then_mismatch_is_reported() {
    let store = OperationStore::connect_in_memory().unwrap();
    let projection = check_projection();
    let source = ProjectionBackedCheckRows::fixture_dynamodb(
        projection.clone(),
        FixtureCurrentRowReader::new(vec![projected_row("u1", "a@example.test", "org-a", 10)]),
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::fixture_dynamodb(
        projection,
        FixtureCurrentRowReader::new(vec![projected_row("u1", "a@example.test", "org-z", 10)]),
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_dynamodb_current_rows_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Mismatched);
    assert_eq!(report.metrics.mismatched_rows, 1);
    assert_eq!(report.samples.mismatched_keys, vec!["users:u1"]);
}

#[test]
fn given_current_row_reader_missing_projected_column_when_check_runs_then_it_fails_closed() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = ProjectionBackedCheckRows::fixture_dynamodb(
        check_projection(),
        FixtureCurrentRowReader::new(vec![ProjectedCurrentRow::new(
            "u1",
            [("email".to_string(), Some("a@example.test".to_string()))]
                .into_iter()
                .collect(),
            10,
        )]),
    )
    .unwrap();
    let destination = LocalFixtureCheckRows::new(LocalCheckDataset::new(vec![]));

    let error = LocalCheckExecutor
        .execute(
            &store,
            &request("check_current_rows_missing_column_1", 3),
            &source,
            &destination,
        )
        .unwrap_err();

    assert!(
        matches!(error, CheckError::InvalidBackendConfiguration(message) if message.contains("org_id"))
    );
}

#[test]
fn given_paged_current_row_reader_when_check_runs_then_pages_are_flattened_through_projection() {
    let store = OperationStore::connect_in_memory().unwrap();
    let projection = check_projection();
    let source = ProjectionBackedCheckRows::fixture_dynamodb(
        projection.clone(),
        FixturePagedCurrentRowReader::new(vec![
            ProjectedCurrentRowPage::with_next_cursor(
                vec![projected_row("u1", "a@example.test", "org-a", 10)],
                "page-2",
            ),
            ProjectedCurrentRowPage::terminal(vec![projected_row(
                "u2",
                "b@example.test",
                "org-b",
                11,
            )]),
        ]),
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::fixture_dynamodb(
        projection,
        FixturePagedCurrentRowReader::new(vec![ProjectedCurrentRowPage::terminal(vec![
            projected_row("u1", "a@example.test", "org-a", 10),
            projected_row("u2", "b@example.test", "org-b", 11),
        ])]),
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_paged_current_rows_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.checked_rows, 2);
}

#[test]
fn given_projection_backed_reader_when_check_runs_then_direct_stream_path_is_used() {
    let store = OperationStore::connect_in_memory().unwrap();
    let projection = check_projection();
    let source_calls = Rc::new(Cell::new(0));
    let destination_calls = Rc::new(Cell::new(0));
    let source = ProjectionBackedCheckRows::fixture_aux_storage(
        projection.clone(),
        RecordingCurrentRowStreamReader::new(
            vec![projected_row("u2", "b@example.test", "org-b", 11)],
            Rc::clone(&source_calls),
        ),
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::fixture_aux_storage(
        projection,
        RecordingCurrentRowStreamReader::new(
            vec![projected_row("u2", "b@example.test", "org-b", 11)],
            Rc::clone(&destination_calls),
        ),
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_projection_stream_path_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(source_calls.get(), 1);
    assert_eq!(destination_calls.get(), 1);
}

#[test]
fn given_paged_current_row_stream_when_requested_then_rows_are_sorted_by_key() {
    let source = ProjectionBackedCheckRows::fixture_dynamodb(
        check_projection(),
        FixturePagedCurrentRowReader::new(vec![
            ProjectedCurrentRowPage::with_next_cursor(
                vec![projected_row("u2", "b@example.test", "org-b", 11)],
                "page-2",
            ),
            ProjectedCurrentRowPage::terminal(vec![projected_row(
                "u1",
                "a@example.test",
                "org-a",
                10,
            )]),
        ]),
    )
    .unwrap();

    let mut stream = source
        .source_row_stream(&["users".to_string()])
        .expect("source row stream");

    let first_key = stream
        .next_row()
        .expect("stream read")
        .expect("first row")
        .key;
    let second_key = stream
        .next_row()
        .expect("stream read")
        .expect("second row")
        .key;

    assert_eq!(first_key, "u1");
    assert_eq!(second_key, "u2");
    assert!(stream.next_row().unwrap().is_none());
}

#[test]
fn given_production_aux_storage_reader_when_check_runs_then_pages_are_requested_lazily() {
    let store = OperationStore::connect_in_memory().unwrap();
    let projection = check_projection();
    let source_requests = Rc::new(RefCell::new(Vec::new()));
    let destination_requests = Rc::new(RefCell::new(Vec::new()));
    let source = ProjectionBackedCheckRows::full_table_aux_storage(
        projection.clone(),
        RecordingProductionPageReader::new(
            vec![
                ProjectedCurrentRowPage::terminal(vec![projected_row(
                    "u2",
                    "b@example.test",
                    "org-b",
                    11,
                )]),
                ProjectedCurrentRowPage::with_next_cursor(
                    vec![projected_row("u1", "a@example.test", "org-a", 10)],
                    "page-2",
                ),
            ],
            Rc::clone(&source_requests),
        ),
        100,
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::full_table_aux_storage(
        projection,
        RecordingProductionPageReader::new(
            vec![
                ProjectedCurrentRowPage::terminal(vec![projected_row(
                    "u2",
                    "b@example.test",
                    "org-b",
                    11,
                )]),
                ProjectedCurrentRowPage::with_next_cursor(
                    vec![projected_row("u1", "a@example.test", "org-a", 10)],
                    "page-2",
                ),
            ],
            Rc::clone(&destination_requests),
        ),
        100,
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_production_aux_storage_reader_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(
        source_requests
            .borrow()
            .iter()
            .map(|request| request.cursor.clone())
            .collect::<Vec<_>>(),
        vec![None, Some("page-2".to_string())]
    );
    assert_eq!(destination_requests.borrow().len(), 2);
}

#[test]
fn given_production_dynamodb_reader_when_rows_are_unsorted_within_page_then_stream_sorts_page() {
    let source = ProjectionBackedCheckRows::full_table_dynamodb(
        check_projection(),
        RecordingProductionPageReader::new(
            vec![ProjectedCurrentRowPage::terminal(vec![
                projected_row("u2", "b@example.test", "org-b", 11),
                projected_row("u1", "a@example.test", "org-a", 10),
            ])],
            Rc::new(RefCell::new(Vec::new())),
        ),
        50,
    )
    .unwrap();

    let mut stream = source.source_row_stream(&["users".to_string()]).unwrap();
    let first_key = stream.next_row().unwrap().unwrap().key;
    let second_key = stream.next_row().unwrap().unwrap().key;

    assert_eq!(first_key, "u1");
    assert_eq!(second_key, "u2");
    assert!(stream.next_row().unwrap().is_none());
}

#[test]
fn given_production_ducklake_reader_when_check_runs_then_table_scan_facade_is_supported() {
    let store = OperationStore::connect_in_memory().unwrap();
    let projection = check_projection();
    let source = ProjectionBackedCheckRows::full_table_ducklake(
        projection.clone(),
        RecordingProductionPageReader::new(
            vec![ProjectedCurrentRowPage::terminal(vec![projected_row(
                "u1",
                "a@example.test",
                "org-a",
                10,
            )])],
            Rc::new(RefCell::new(Vec::new())),
        ),
        100,
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::full_table_ducklake(
        projection,
        RecordingProductionPageReader::new(
            vec![ProjectedCurrentRowPage::terminal(vec![projected_row(
                "u1",
                "a@example.test",
                "org-a",
                10,
            )])],
            Rc::new(RefCell::new(Vec::new())),
        ),
        100,
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_production_ducklake_reader_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.checked_rows, 1);
}

#[test]
fn given_duckdb_table_scan_reader_when_pages_requested_then_projected_rows_are_returned_in_key_order()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    let connection = duckdb::Connection::open(database_path.as_path()).unwrap();
    connection
        .execute_batch(
            "create table users (
                user_id varchar not null,
                email varchar not null,
                org_id varchar not null,
                __source_position json
            );
            insert into users values
                ('u2', 'b@example.test', 'org-b', \
             '{\"type\":\"dynamo_db_scan_chunk\",\"table_name\":\"users\",\"chunk_id\":\"c1\",\"\
             item_index\":11}'),
                ('u1', 'a@example.test', 'org-a', \
             '{\"type\":\"dynamo_db_scan_chunk\",\"table_name\":\"users\",\"chunk_id\":\"c1\",\"\
             item_index\":10}');",
        )
        .unwrap();
    drop(connection);
    let projection = CheckRowProjection::new_with_optional_columns(
        "users",
        "user_id",
        vec!["email".to_string(), "org_id".to_string()],
        Some("__source_position".to_string()),
        None,
    )
    .unwrap();
    let reader = DuckDbTableScanCurrentRowReader::new(database_path.to_string_lossy());

    assert_eq!(
        reader.stream_order(),
        CurrentRowStreamOrder::GloballySortedByKey
    );

    let first_page = reader
        .current_row_page(&crate::CurrentRowPageRequest {
            projection: projection.clone(),
            cursor: None,
            page_size: 1,
        })
        .unwrap();
    let second_page = reader
        .current_row_page(&crate::CurrentRowPageRequest {
            projection,
            cursor: first_page.next_cursor.clone(),
            page_size: 1,
        })
        .unwrap();

    assert_eq!(first_page.rows[0].key, "u1");
    assert_eq!(first_page.rows[0].source_position, 10);
    assert_eq!(first_page.next_cursor, Some("1".to_string()));
    assert_eq!(second_page.rows[0].key, "u2");
    assert_eq!(second_page.rows[0].source_position, 11);
}

#[test]
fn given_duckdb_table_scan_reader_when_used_as_ducklake_facade_then_check_runs() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    let connection = duckdb::Connection::open(database_path.as_path()).unwrap();
    connection
        .execute_batch(
            "create table users (
                user_id varchar not null,
                email varchar not null,
                org_id varchar not null
            );
            insert into users values
                ('u1', 'a@example.test', 'org-a'),
                ('u2', 'b@example.test', 'org-b');",
        )
        .unwrap();
    drop(connection);
    let store = OperationStore::connect_in_memory().unwrap();
    let projection = CheckRowProjection::new(
        "users",
        "user_id",
        vec!["email".to_string(), "org_id".to_string()],
    )
    .unwrap();
    let source = ProjectionBackedCheckRows::full_table_ducklake(
        projection.clone(),
        DuckDbTableScanCurrentRowReader::new(database_path.to_string_lossy()),
        1,
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::full_table_ducklake(
        projection,
        DuckDbTableScanCurrentRowReader::new(database_path.to_string_lossy()),
        1,
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_ducklake_table_scan_reader_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.checked_rows, 2);
}

#[test]
fn given_ducklake_current_row_reader_when_check_runs_then_backend_kind_is_supported() {
    let store = OperationStore::connect_in_memory().unwrap();
    let projection = check_projection();
    let source = ProjectionBackedCheckRows::fixture_ducklake(
        projection.clone(),
        FixtureCurrentRowReader::new(vec![projected_row("u1", "a@example.test", "org-a", 10)]),
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::fixture_ducklake(
        projection,
        FixtureCurrentRowReader::new(vec![projected_row("u1", "a@example.test", "org-a", 10)]),
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_ducklake_current_rows_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.checked_rows, 1);
}

#[test]
fn given_duckdb_analytical_table_when_check_runs_then_hashes_are_derived_from_projected_columns() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    let connection = duckdb::Connection::open(database_path.as_path()).unwrap();
    connection
        .execute_batch(
            "create table users (
                user_id varchar not null,
                email varchar not null,
                org_id varchar not null
            );
            insert into users values
                ('u1', 'a@example.test', 'org-a'),
                ('u2', 'b@example.test', 'org-b');",
        )
        .unwrap();
    drop(connection);
    let store = OperationStore::connect_in_memory().unwrap();
    let source = DuckDbAnalyticalCheckRows::new(
        database_path.to_string_lossy(),
        "users",
        "user_id",
        vec!["email".to_string(), "org_id".to_string()],
    )
    .unwrap();
    let destination = DuckDbAnalyticalCheckRows::new(
        database_path.to_string_lossy(),
        "users",
        "user_id",
        vec!["email".to_string(), "org_id".to_string()],
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_duckdb_analytical_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.checked_rows, 2);
}

#[test]
fn given_duckdb_analytical_table_with_different_projection_when_check_runs_then_mismatch_is_reported()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    let connection = duckdb::Connection::open(database_path.as_path()).unwrap();
    connection
        .execute_batch(
            "create table users (
                user_id varchar not null,
                email varchar not null,
                org_id varchar not null,
                alternate_org_id varchar not null
            );
            insert into users values
                ('u1', 'a@example.test', 'org-a', 'org-z');",
        )
        .unwrap();
    drop(connection);
    let store = OperationStore::connect_in_memory().unwrap();
    let source = DuckDbAnalyticalCheckRows::new(
        database_path.to_string_lossy(),
        "users",
        "user_id",
        vec!["email".to_string(), "org_id".to_string()],
    )
    .unwrap();
    let destination = DuckDbAnalyticalCheckRows::new(
        database_path.to_string_lossy(),
        "users",
        "user_id",
        vec!["email".to_string(), "alternate_org_id".to_string()],
    )
    .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_duckdb_analytical_mismatch_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Mismatched);
    assert_eq!(report.metrics.mismatched_rows, 1);
    assert_eq!(report.samples.mismatched_keys, vec!["users:u1"]);
}

#[test]
fn given_duckdb_analytical_projection_with_source_position_and_private_flag_when_check_runs_then_metadata_is_used()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("analytics.duckdb");
    let source_position = SourcePosition::DynamoDbExportSnapshot {
        export_arn: "export-1".to_string(),
        exported_at_ms: 123,
    };
    let source_position_json = serde_json::to_string(&source_position).unwrap();
    let connection = duckdb::Connection::open(database_path.as_path()).unwrap();
    connection
        .execute_batch(
            "create table users (
                user_id varchar not null,
                email varchar not null,
                __source_position json,
                has_private_data boolean not null
            );",
        )
        .unwrap();
    connection
        .prepare("insert into users values ('u1', 'a@example.test', ?::json, true)")
        .unwrap()
        .execute(duckdb::params![source_position_json])
        .unwrap();
    drop(connection);
    let store = OperationStore::connect_in_memory().unwrap();
    let projection = CheckRowProjection::new_with_optional_columns(
        "users",
        "user_id",
        vec!["email".to_string()],
        Some("__source_position".to_string()),
        Some("has_private_data".to_string()),
    )
    .unwrap();
    let source = DuckDbAnalyticalCheckRows::new_with_projection(
        database_path.to_string_lossy(),
        projection.clone(),
    )
    .unwrap();
    let destination =
        DuckDbAnalyticalCheckRows::new_with_projection(database_path.to_string_lossy(), projection)
            .unwrap();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_duckdb_metadata_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::PrivacyViolation);
    assert_eq!(report.metrics.stale_rows, 0);
    assert_eq!(report.metrics.privacy_violations, 1);
    assert_eq!(report.metrics.checked_rows, 1);
}

#[test]
fn given_table_registration_when_projection_is_built_then_manifest_columns_define_check_shape() {
    let table = TableRegistration {
        source_table_name: "source_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant-a".to_string()),
        tenant_selector: analytics_contract::TenantSelector::TableName,
        row_identity: analytics_contract::RowIdentity::RecordKey,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(vec![ProjectionColumn {
            column_name: "email".to_string(),
            attribute_path: "profile.email".to_string(),
            column_type: None,
        }]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    };

    let projection = CheckRowProjection::from_table_registration(&table).unwrap();

    assert_eq!(projection.table_name, "users");
    assert_eq!(projection.key_column, "__id");
    assert_eq!(projection.hash_columns, vec!["email"]);
    assert_eq!(
        projection.source_position_column.as_deref(),
        Some("__source_position")
    );
}

#[test]
fn given_projection_without_hash_columns_when_constructed_then_it_is_rejected() {
    let error = CheckRowProjection::new("users", "user_id", Vec::new()).unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message)
            if message.contains("at least one hash column")
    ));
}

#[test]
fn given_duckdb_check_rows_when_check_runs_then_rows_are_loaded_from_adapter_table() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("check_rows.duckdb");
    let connection = duckdb::Connection::open(database_path.as_path()).unwrap();
    connection
        .execute_batch(
            "create table check_rows (
                table_name varchar not null,
                row_key varchar not null,
                row_hash bigint not null,
                source_position bigint not null,
                contains_private_data boolean not null
            );
            insert into check_rows values
                ('users', 'u1', 11, 3, false),
                ('users', 'u2', 22, 4, false),
                ('orders', 'o1', 33, 5, false);",
        )
        .unwrap();
    drop(connection);
    let store = OperationStore::connect_in_memory().unwrap();
    let source = DuckDbCheckRows::new(database_path.to_string_lossy(), "check_rows").unwrap();
    let destination = LocalFixtureCheckRows::new(LocalCheckDataset::new(vec![
        row("u1", 11, 3),
        row("u2", 22, 4),
    ]));

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_duckdb_adapter_1", 3),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.source_rows, 2);
    assert_eq!(report.metrics.destination_rows, 2);
}

#[test]
fn given_duckdb_check_rows_when_row_stream_requested_then_database_ordering_is_used() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("check_rows.duckdb");
    let connection = duckdb::Connection::open(database_path.as_path()).unwrap();
    connection
        .execute_batch(
            "create table check_rows (
                table_name varchar not null,
                row_key varchar not null,
                row_hash bigint not null,
                source_position bigint not null,
                contains_private_data boolean not null
            );
            insert into check_rows values
                ('users', 'u2', 22, 4, false),
                ('orders', 'o1', 33, 5, false),
                ('users', 'u1', 11, 3, false);",
        )
        .unwrap();
    drop(connection);
    let source = DuckDbCheckRows::new(database_path.to_string_lossy(), "check_rows").unwrap();
    let mut stream = source
        .source_row_stream(&["users".to_string(), "orders".to_string()])
        .unwrap();

    let first = stream.next_row().unwrap().unwrap();
    assert_eq!((first.table.as_str(), first.key.as_str()), ("orders", "o1"));
    let second = stream.next_row().unwrap().unwrap();
    assert_eq!(
        (second.table.as_str(), second.key.as_str()),
        ("users", "u1")
    );
    let third = stream.next_row().unwrap().unwrap();
    assert_eq!((third.table.as_str(), third.key.as_str()), ("users", "u2"));
    assert!(stream.next_row().unwrap().is_none());
}
