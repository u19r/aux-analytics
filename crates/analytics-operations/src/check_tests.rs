use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use analytics_contract::{ProjectionColumn, SourcePosition, TableRegistration};
use storage_types::{SystemStreamPosition, SystemStreamScope};

use crate::{
    AuxStorageCheckRows, CheckBackendKind, CheckComparisonStrategy, CheckError, CheckOutcome,
    CheckOutputFormat, CheckRequest, CheckRow, CheckRowProjection, CheckRowStream,
    CheckSourcePosition, CurrentRowStreamOrder, DestinationRows, DuckDbAnalyticalCheckRows,
    DuckDbCheckRows, DuckDbTableScanCurrentRowReader, DuckLakeCheckRows, DynamoDbCheckRows,
    FixtureCurrentRowReader, FixturePagedCurrentRowReader, LocalCheckDataset, LocalCheckExecutor,
    LocalFixtureCheckRows, OperationActor, OperationId, OperationStatus, OperationStore,
    ProductionCurrentRowPageReader, ProjectedCurrentRow, ProjectedCurrentRowPage,
    ProjectedCurrentRowStream, ProjectionBackedCheckRows, RateLimitDenied, RateLimitPolicy,
    SourceTruthRows, store_test_support::OperationStoreTestFixture,
};

fn request(id: &str, sample_limit: usize) -> CheckRequest {
    CheckRequest {
        operation_id: OperationId::try_new(id).unwrap(),
        actor: OperationActor::try_new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        strategy: CheckComparisonStrategy::FullRows,
        sample_limit,
        privacy_policy_version: Some("policy-v1".to_string()),
        output: CheckOutputFormat::Json,
        rate_limit: RateLimitPolicy::default(),
    }
}

struct CancelThenFailSource {
    store: Rc<OperationStore>,
    operation_id: OperationId,
    actor: OperationActor,
}

impl SourceTruthRows for CancelThenFailSource {
    fn source_rows(&self, _target_tables: &[String]) -> crate::CheckResult<Vec<CheckRow>> {
        self.store
            .request_cancellation_as(&self.operation_id, &self.actor)
            .unwrap();
        Err(CheckError::SourceRead(
            "source became unavailable".to_string(),
        ))
    }
}

fn take_over_check_attempt(
    fixture: &OperationStoreTestFixture,
    store: &OperationStore,
    operation_id: &OperationId,
    actor: &OperationActor,
) {
    fixture
        .prepare_at(store, operation_id, actor, i64::MAX)
        .expect("expired check attempt can be taken over");
}

struct TakeoverThenFailSource {
    fixture: OperationStoreTestFixture,
    store: Rc<OperationStore>,
    operation_id: OperationId,
    actor: OperationActor,
}

impl SourceTruthRows for TakeoverThenFailSource {
    fn source_rows(&self, _target_tables: &[String]) -> crate::CheckResult<Vec<CheckRow>> {
        take_over_check_attempt(&self.fixture, &self.store, &self.operation_id, &self.actor);
        Err(CheckError::SourceRead(
            "source became unavailable".to_string(),
        ))
    }
}

struct TakeoverThenRowsDestination {
    fixture: OperationStoreTestFixture,
    store: Rc<OperationStore>,
    operation_id: OperationId,
    actor: OperationActor,
}

impl DestinationRows for TakeoverThenRowsDestination {
    fn destination_rows(&self, _target_tables: &[String]) -> crate::CheckResult<Vec<CheckRow>> {
        take_over_check_attempt(&self.fixture, &self.store, &self.operation_id, &self.actor);
        Ok(vec![row("u1", 11, 3)])
    }
}

struct TakeoverRateLimitClock {
    fixture: OperationStoreTestFixture,
    store: Rc<OperationStore>,
    operation_id: OperationId,
    actor: OperationActor,
    took_over: Cell<bool>,
}

impl crate::RateLimitClock for TakeoverRateLimitClock {
    fn minute_utc(&self) -> u16 {
        0
    }

    fn now_ms(&self) -> u64 {
        if !self.took_over.replace(true) {
            take_over_check_attempt(&self.fixture, &self.store, &self.operation_id, &self.actor);
        }
        0
    }
}

struct FixedRateLimitClock;

impl crate::RateLimitClock for FixedRateLimitClock {
    fn minute_utc(&self) -> u16 {
        0
    }

    fn now_ms(&self) -> u64 {
        0
    }
}

fn row(key: &str, row_hash: u64, source_position: u64) -> CheckRow {
    CheckRow {
        table: "users".to_string(),
        key: key.to_string(),
        row_hash,
        source_position: crate::check_test_support::backup_position(source_position),
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
        crate::check_test_support::backup_position(source_position),
    )
}

fn system_position(offset: &str) -> CheckSourcePosition {
    CheckSourcePosition::system_stream(
        SystemStreamScope::try_new("scope", 1).unwrap(),
        SystemStreamPosition::try_new("shard", "lineage", offset).unwrap(),
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
    CheckRowProjection::try_new(
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
            .show_operation(&OperationId::try_new("check_mismatch_1").unwrap())
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
fn given_system_versionstamp_suffixes_when_rows_are_compared_then_full_order_is_used() {
    let store = OperationStore::connect_in_memory().unwrap();
    let mut source_row = row("u1", 11, 0);
    source_row.source_position = system_position("00000000000000000010-vs00000001");
    let mut newer_destination_row = row("u1", 11, 0);
    newer_destination_row.source_position = system_position("00000000000000000010-vs00000002");

    let newer_report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_system_suffix_newer", 1),
            &LocalCheckDataset::new(vec![source_row.clone()]),
            &LocalCheckDataset::new(vec![newer_destination_row.clone()]),
        )
        .unwrap();
    assert_eq!(newer_report.outcome, CheckOutcome::Complete);
    assert_eq!(newer_report.metrics.stale_rows, 0);
    assert_eq!(newer_report.metrics.unsupported_comparisons, 0);

    let stale_report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_system_suffix_stale", 1),
            &LocalCheckDataset::new(vec![newer_destination_row]),
            &LocalCheckDataset::new(vec![source_row]),
        )
        .unwrap();
    assert_eq!(stale_report.outcome, CheckOutcome::Stale);
    assert_eq!(stale_report.metrics.stale_rows, 1);
    assert_eq!(stale_report.metrics.unsupported_comparisons, 0);
}

#[test]
fn given_backup_and_system_positions_when_rows_are_compared_then_check_fails_closed() {
    let store = OperationStore::connect_in_memory().unwrap();
    let backup_source = LocalCheckDataset::new(vec![row("u1", 11, 11)]);
    let mut system_destination_row = row("u1", 11, 0);
    system_destination_row.source_position = system_position("10");
    let system_destination = LocalCheckDataset::new(vec![system_destination_row]);

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_backup_system_incomparable", 1),
            &backup_source,
            &system_destination,
        )
        .unwrap();
    assert_eq!(report.outcome, CheckOutcome::Mismatched);
    assert_eq!(report.metrics.stale_rows, 0);
    assert_eq!(report.metrics.unsupported_comparisons, 1);
}

#[test]
fn given_system_versionstamp_suffixes_when_chunk_hashes_are_compared_then_they_remain_distinct() {
    let store = OperationStore::connect_in_memory().unwrap();
    let mut request = request("check_system_suffix_hash", 1);
    request.strategy = CheckComparisonStrategy::ChunkHashes;
    let mut source_row = row("u1", 11, 0);
    source_row.source_position = system_position("00000000000000000010-vs00000001");
    let mut destination_row = row("u1", 11, 0);
    destination_row.source_position = system_position("00000000000000000010-vs00000002");

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request,
            &LocalCheckDataset::new(vec![source_row]),
            &LocalCheckDataset::new(vec![destination_row]),
        )
        .unwrap();
    assert_eq!(report.outcome, CheckOutcome::Mismatched);
    assert_eq!(report.metrics.deterministic_chunk_hash_mismatches, 1);
}

#[test]
fn given_vector_dataset_when_row_stream_requested_then_rows_are_sorted_by_table_and_key() {
    let dataset = LocalCheckDataset::new(vec![
        CheckRow {
            table: "users".to_string(),
            key: "u2".to_string(),
            row_hash: 22,
            source_position: crate::check_test_support::backup_position(4),
            contains_private_data: false,
        },
        CheckRow {
            table: "orders".to_string(),
            key: "o1".to_string(),
            row_hash: 11,
            source_position: crate::check_test_support::backup_position(3),
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
fn given_multi_table_filter_when_rows_are_requested_then_order_and_membership_match_baseline() {
    let rows = vec![
        CheckRow {
            table: "orders".to_string(),
            key: "o1".to_string(),
            row_hash: 1,
            source_position: crate::check_test_support::backup_position(1),
            contains_private_data: false,
        },
        CheckRow {
            table: "users".to_string(),
            key: "u1".to_string(),
            row_hash: 2,
            source_position: crate::check_test_support::backup_position(2),
            contains_private_data: false,
        },
        CheckRow {
            table: "events".to_string(),
            key: "e1".to_string(),
            row_hash: 3,
            source_position: crate::check_test_support::backup_position(3),
            contains_private_data: false,
        },
    ];
    let targets = vec!["users".to_string(), "orders".to_string()];
    let dataset = LocalCheckDataset::new(rows.clone());

    let filtered = dataset.source_rows(&targets).unwrap();
    let baseline = crate::filter_target_tables_baseline(&rows, &targets);

    assert_eq!(filtered, baseline);
    assert_eq!(
        filtered
            .iter()
            .map(|row| row.key.as_str())
            .collect::<Vec<_>>(),
        ["o1", "u1"]
    );
    assert!(
        dataset
            .source_rows(&["missing".to_string()])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn given_all_target_tables_when_check_rows_are_filtered_then_order_and_clone_count_match_baseline()
{
    let rows = vec![
        CheckRow {
            table: "orders".to_string(),
            key: "o1".to_string(),
            row_hash: 1,
            source_position: crate::check_test_support::backup_position(1),
            contains_private_data: false,
        },
        CheckRow {
            table: "users".to_string(),
            key: "u1".to_string(),
            row_hash: 2,
            source_position: crate::check_test_support::backup_position(2),
            contains_private_data: false,
        },
        CheckRow {
            table: "events".to_string(),
            key: "e1".to_string(),
            row_hash: 3,
            source_position: crate::check_test_support::backup_position(3),
            contains_private_data: false,
        },
    ];
    let targets = vec![
        "events".to_string(),
        "users".to_string(),
        "orders".to_string(),
    ];
    let dataset = LocalCheckDataset::new(rows);

    crate::check::reset_check_row_clone_count();
    let baseline = crate::filter_target_tables_baseline(&dataset.rows, &targets);
    let baseline_clones = crate::check::check_row_clone_count();
    crate::check::reset_check_row_clone_count();
    let filtered = dataset.source_rows(&targets).unwrap();
    let candidate_clones = crate::check::check_row_clone_count();

    assert_eq!(filtered, baseline);
    assert_eq!(
        filtered
            .iter()
            .map(|row| row.key.as_str())
            .collect::<Vec<_>>(),
        ["o1", "u1", "e1"]
    );
    assert_eq!(baseline_clones, baseline.len());
    assert_eq!(candidate_clones, filtered.len());
    assert_eq!(candidate_clones, baseline_clones);
}

#[test]
fn given_duplicate_row_keys_when_unsorted_stream_requested_then_input_order_is_stable() {
    let mut stream = CheckRowStream::from_unsorted(vec![row("u1", 11, 3), row("u1", 22, 4)]);

    assert_eq!(stream.next_row().unwrap().map(|row| row.row_hash), Some(11));
    assert_eq!(stream.next_row().unwrap().map(|row| row.row_hash), Some(22));
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
fn given_rate_limit_denial_when_check_starts_then_no_source_or_destination_is_read() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source_calls = Rc::new(Cell::new(0));
    let destination_calls = Rc::new(Cell::new(0));
    let source = ProjectionBackedCheckRows::fixture_aux_storage(
        check_projection(),
        RecordingCurrentRowStreamReader::new(Vec::new(), Rc::clone(&source_calls)),
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::fixture_aux_storage(
        check_projection(),
        RecordingCurrentRowStreamReader::new(Vec::new(), Rc::clone(&destination_calls)),
    )
    .unwrap();
    let mut request = request("check_rate_limit_1", 3);
    request.rate_limit.max_source_requests_per_second = Some(0);

    let error = LocalCheckExecutor
        .execute(&store, &request, &source, &destination)
        .unwrap_err();

    assert!(matches!(
        error,
        CheckError::RateLimit(RateLimitDenied { .. })
    ));
    assert_eq!(source_calls.get(), 0);
    assert_eq!(destination_calls.get(), 0);
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Paused
    );
}

#[test]
fn given_one_source_request_budget_when_check_reads_both_sides_then_second_read_is_denied() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source_calls = Rc::new(Cell::new(0));
    let destination_calls = Rc::new(Cell::new(0));
    let source = ProjectionBackedCheckRows::fixture_aux_storage(
        check_projection(),
        RecordingCurrentRowStreamReader::new(Vec::new(), Rc::clone(&source_calls)),
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::fixture_aux_storage(
        check_projection(),
        RecordingCurrentRowStreamReader::new(Vec::new(), Rc::clone(&destination_calls)),
    )
    .unwrap();
    let mut request = request("check_rate_limit_cumulative_1", 3);
    request.rate_limit.max_source_requests_per_second = Some(1);

    let error = LocalCheckExecutor
        .execute_at(
            &store,
            &request,
            &source,
            &destination,
            &FixedRateLimitClock,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        CheckError::RateLimit(RateLimitDenied { .. })
    ));
    assert_eq!(source_calls.get(), 1);
    assert_eq!(destination_calls.get(), 0);
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Paused
    );
    assert!(store.check_report(&request.operation_id).is_err());
    assert_eq!(
        store
            .audit_events(&request.operation_id)
            .unwrap()
            .iter()
            .filter(|event| event.kind == crate::OperationEventKind::Paused)
            .count(),
        1
    );
    assert!(
        store
            .audit_events(&request.operation_id)
            .unwrap()
            .iter()
            .all(|event| !matches!(
                event.kind,
                crate::OperationEventKind::Failed | crate::OperationEventKind::Succeeded
            ))
    );
}

#[test]
fn given_cancellation_during_check_source_read_when_failure_is_recorded_then_persistence_error_is_not_ignored()
 {
    let store = Rc::new(OperationStore::connect_in_memory().unwrap());
    let request = request("check_failure_persistence_1", 3);
    let source = CancelThenFailSource {
        store: Rc::clone(&store),
        operation_id: request.operation_id.clone(),
        actor: request.actor.clone(),
    };
    let destination = LocalCheckDataset::new(Vec::new());

    let error = LocalCheckExecutor
        .execute(&store, &request, &source, &destination)
        .unwrap_err();

    assert!(matches!(error, CheckError::FailurePersistence { .. }));
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Cancelling
    );
}

#[test]
fn given_source_failure_after_takeover_when_check_records_failure_then_replacement_stays_running() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("operations.duckdb");
    let store = Rc::new(OperationStore::connect_duckdb(database_path.as_path()).unwrap());
    let fixture = OperationStoreTestFixture::for_database(database_path.as_path());
    let request = request("check_stale_source_failure_1", 3);
    let source = TakeoverThenFailSource {
        fixture,
        store: Rc::clone(&store),
        operation_id: request.operation_id.clone(),
        actor: request.actor.clone(),
    };
    let destination = LocalCheckDataset::new(Vec::new());

    let error = LocalCheckExecutor
        .execute(&store, &request, &source, &destination)
        .unwrap_err();

    assert!(matches!(error, CheckError::FailurePersistence { .. }));
    let current = store.show_operation(&request.operation_id).unwrap();
    assert_eq!(current.status, OperationStatus::Running);
    assert_eq!(current.execution_generation, 2);
    assert!(store.check_report(&request.operation_id).is_err());
    assert!(
        store
            .audit_events(&request.operation_id)
            .unwrap()
            .iter()
            .all(|event| event.kind != crate::OperationEventKind::Failed)
    );
}

#[test]
fn given_rate_limit_denial_after_takeover_when_check_pauses_then_replacement_stays_running() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("operations.duckdb");
    let store = Rc::new(OperationStore::connect_duckdb(database_path.as_path()).unwrap());
    let fixture = OperationStoreTestFixture::for_database(database_path.as_path());
    let mut request = request("check_stale_rate_limit_1", 3);
    request.rate_limit.max_source_requests_per_second = Some(0);
    let clock = TakeoverRateLimitClock {
        fixture,
        store: Rc::clone(&store),
        operation_id: request.operation_id.clone(),
        actor: request.actor.clone(),
        took_over: Cell::new(false),
    };
    let source = LocalCheckDataset::new(Vec::new());
    let destination = LocalCheckDataset::new(Vec::new());

    let error = LocalCheckExecutor
        .execute_at(&store, &request, &source, &destination, &clock)
        .unwrap_err();

    assert!(matches!(error, CheckError::FailurePersistence { .. }));
    let current = store.show_operation(&request.operation_id).unwrap();
    assert_eq!(current.status, OperationStatus::Running);
    assert_eq!(current.execution_generation, 2);
    assert!(
        store
            .audit_events(&request.operation_id)
            .unwrap()
            .iter()
            .all(|event| event.kind != crate::OperationEventKind::Paused)
    );
}

#[test]
fn given_matching_rows_after_takeover_when_check_persists_report_then_replacement_stays_running() {
    let tempdir = tempfile::tempdir().unwrap();
    let database_path = tempdir.path().join("operations.duckdb");
    let store = Rc::new(OperationStore::connect_duckdb(database_path.as_path()).unwrap());
    let fixture = OperationStoreTestFixture::for_database(database_path.as_path());
    let request = request("check_stale_report_1", 3);
    let source = LocalCheckDataset::new(vec![row("u1", 11, 3)]);
    let destination = TakeoverThenRowsDestination {
        fixture,
        store: Rc::clone(&store),
        operation_id: request.operation_id.clone(),
        actor: request.actor.clone(),
    };

    let error = LocalCheckExecutor
        .execute(&store, &request, &source, &destination)
        .unwrap_err();

    assert!(matches!(error, CheckError::Store(_)));
    let current = store.show_operation(&request.operation_id).unwrap();
    assert_eq!(current.status, OperationStatus::Running);
    assert_eq!(current.execution_generation, 2);
    assert!(store.check_report(&request.operation_id).is_err());
    assert!(
        store
            .audit_events(&request.operation_id)
            .unwrap()
            .iter()
            .all(|event| event.kind != crate::OperationEventKind::Succeeded)
    );
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
            source_position: crate::check_test_support::backup_position(4),
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
            crate::check_test_support::backup_position(10),
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
fn given_production_reader_returns_unchanged_cursor_when_reading_rows_then_stream_fails_closed() {
    let source = ProjectionBackedCheckRows::full_table_aux_storage(
        check_projection(),
        RecordingProductionPageReader::new(
            vec![
                ProjectedCurrentRowPage::with_next_cursor(
                    vec![projected_row("u2", "b@example.test", "org-b", 11)],
                    "page-2",
                ),
                ProjectedCurrentRowPage::with_next_cursor(
                    vec![projected_row("u1", "a@example.test", "org-a", 10)],
                    "page-2",
                ),
            ],
            Rc::new(RefCell::new(Vec::new())),
        ),
        100,
    )
    .unwrap();

    let mut stream = source.source_row_stream(&["users".to_string()]).unwrap();
    assert_eq!(stream.next_row().unwrap().unwrap().key, "u1");
    let error = stream.next_row().unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message) if message.contains("unchanged cursor")
    ));
}

#[test]
fn given_production_reader_returns_cyclic_cursor_when_reading_rows_then_stream_fails_closed() {
    let source = ProjectionBackedCheckRows::full_table_aux_storage(
        check_projection(),
        RecordingProductionPageReader::new(
            vec![
                ProjectedCurrentRowPage::with_next_cursor(
                    vec![projected_row("u3", "c@example.test", "org-c", 12)],
                    "page-2",
                ),
                ProjectedCurrentRowPage::with_next_cursor(
                    vec![projected_row("u2", "b@example.test", "org-b", 11)],
                    "page-3",
                ),
                ProjectedCurrentRowPage::with_next_cursor(
                    vec![projected_row("u1", "a@example.test", "org-a", 10)],
                    "page-2",
                ),
            ],
            Rc::new(RefCell::new(Vec::new())),
        ),
        100,
    )
    .unwrap();

    let mut stream = source.source_row_stream(&["users".to_string()]).unwrap();
    assert_eq!(stream.next_row().unwrap().unwrap().key, "u1");
    assert_eq!(stream.next_row().unwrap().unwrap().key, "u2");
    let error = stream.next_row().unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message) if message.contains("cyclic cursor")
    ));
}

#[test]
fn given_production_reader_returns_too_many_empty_pages_when_reading_rows_then_stream_fails_closed()
{
    let pages = (0..=crate::constants::MAX_CONSECUTIVE_EMPTY_PAGES)
        .rev()
        .map(|index| ProjectedCurrentRowPage::with_next_cursor(Vec::new(), format!("page-{index}")))
        .collect();
    let source = ProjectionBackedCheckRows::full_table_aux_storage(
        check_projection(),
        RecordingProductionPageReader::new(pages, Rc::new(RefCell::new(Vec::new()))),
        100,
    )
    .unwrap();

    let mut stream = source.source_row_stream(&["users".to_string()]).unwrap();
    let error = stream.next_row().unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message)
            if message.contains("consecutive empty page budget")
    ));
}

#[test]
fn given_production_reader_returns_a_blank_cursor_when_reading_rows_then_stream_fails_closed() {
    let source = ProjectionBackedCheckRows::full_table_aux_storage(
        check_projection(),
        RecordingProductionPageReader::new(
            vec![ProjectedCurrentRowPage::with_next_cursor(
                vec![projected_row("u1", "a@example.test", "org-a", 10)],
                "   ",
            )],
            Rc::new(RefCell::new(Vec::new())),
        ),
        100,
    )
    .unwrap();

    let mut stream = source.source_row_stream(&["users".to_string()]).unwrap();
    let error = stream.next_row().unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message) if message.contains("empty cursor")
    ));
}

#[test]
fn given_production_reader_returns_an_empty_page_before_rows_when_reading_rows_then_order_and_rows_are_retained()
 {
    let requests = Rc::new(RefCell::new(Vec::new()));
    let source = ProjectionBackedCheckRows::full_table_aux_storage(
        check_projection(),
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
                    "page-3",
                ),
                ProjectedCurrentRowPage::with_next_cursor(Vec::new(), "page-2"),
            ],
            Rc::clone(&requests),
        ),
        100,
    )
    .unwrap();

    let mut stream = source.source_row_stream(&["users".to_string()]).unwrap();
    assert_eq!(stream.next_row().unwrap().unwrap().key, "u1");
    assert_eq!(stream.next_row().unwrap().unwrap().key, "u2");
    assert!(stream.next_row().unwrap().is_none());
    assert_eq!(
        requests
            .borrow()
            .iter()
            .map(|request| request.cursor.clone())
            .collect::<Vec<_>>(),
        vec![None, Some("page-2".to_string()), Some("page-3".to_string())]
    );
}

#[test]
fn given_production_reader_returns_unique_non_empty_pages_when_reading_rows_then_total_work_is_bounded()
 {
    let requests = Rc::new(RefCell::new(Vec::new()));
    let pages = (0..crate::constants::MAX_PAGE_WORK)
        .rev()
        .map(|index| {
            let key = format!("u{index:04}");
            let email = format!("{index}@example.test");
            ProjectedCurrentRowPage::with_next_cursor(
                vec![projected_row(&key, &email, "org-a", index as u64)],
                format!("page-{index}"),
            )
        })
        .collect();
    let source = ProjectionBackedCheckRows::full_table_aux_storage(
        check_projection(),
        RecordingProductionPageReader::new(pages, Rc::clone(&requests)),
        100,
    )
    .unwrap();

    let mut stream = source.source_row_stream(&["users".to_string()]).unwrap();
    for index in 0..crate::constants::MAX_PAGE_WORK {
        assert_eq!(
            stream.next_row().unwrap().unwrap().key,
            format!("u{index:04}")
        );
    }
    let error = stream.next_row().unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message)
            if message.contains("page/cursor work budget")
    ));
    assert_eq!(requests.borrow().len(), crate::constants::MAX_PAGE_WORK);
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
             '{\"type\":\"dynamo_db_scan_chunk\",\"table_name\":\"users\",\"scan_generation\":1,\"\
             segment\":0,\"item_index\":11}'),
                ('u1', 'a@example.test', 'org-a', \
             '{\"type\":\"dynamo_db_scan_chunk\",\"table_name\":\"users\",\"scan_generation\":1,\"\
             segment\":0,\"item_index\":10}');",
        )
        .unwrap();
    drop(connection);
    let projection = CheckRowProjection::try_new_with_optional_columns(
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
    assert_eq!(
        first_page.rows[0].source_position,
        CheckSourcePosition::from_source_position(
            analytics_contract::SourcePosition::DynamoDbScanChunk {
                table_name: "users".to_string(),
                scan_generation: 1,
                segment: 0,
                item_index: 10
            }
        )
        .unwrap()
    );
    assert_eq!(first_page.next_cursor, Some("1".to_string()));
    assert_eq!(second_page.rows[0].key, "u2");
    assert_eq!(
        second_page.rows[0].source_position,
        CheckSourcePosition::from_source_position(
            analytics_contract::SourcePosition::DynamoDbScanChunk {
                table_name: "users".to_string(),
                scan_generation: 1,
                segment: 0,
                item_index: 11
            }
        )
        .unwrap()
    );
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
    let projection = CheckRowProjection::try_new(
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
    let source = DuckDbAnalyticalCheckRows::try_new(
        database_path.to_string_lossy(),
        "users",
        "user_id",
        vec!["email".to_string(), "org_id".to_string()],
    )
    .unwrap();
    let destination = DuckDbAnalyticalCheckRows::try_new(
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
    let source = DuckDbAnalyticalCheckRows::try_new(
        database_path.to_string_lossy(),
        "users",
        "user_id",
        vec!["email".to_string(), "org_id".to_string()],
    )
    .unwrap();
    let destination = DuckDbAnalyticalCheckRows::try_new(
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
    let projection = CheckRowProjection::try_new_with_optional_columns(
        "users",
        "user_id",
        vec!["email".to_string()],
        Some("__source_position".to_string()),
        Some("has_private_data".to_string()),
    )
    .unwrap();
    let source = DuckDbAnalyticalCheckRows::try_new_with_projection(
        database_path.to_string_lossy(),
        projection.clone(),
    )
    .unwrap();
    let destination = DuckDbAnalyticalCheckRows::try_new_with_projection(
        database_path.to_string_lossy(),
        projection,
    )
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
        row_expansion: None,
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
    let error = CheckRowProjection::try_new("users", "user_id", Vec::new()).unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message)
            if message.contains("at least one hash column")
    ));
}

#[test]
fn given_projection_backed_rows_with_empty_hash_columns_when_constructing_projection_then_constructor_rejects_it()
 {
    let projection = CheckRowProjection {
        table_name: "users".to_string(),
        key_column: "user_id".to_string(),
        hash_columns: Vec::new(),
        source_position_column: None,
        private_data_column: None,
    };
    let error = ProjectionBackedCheckRows::try_new(
        CheckBackendKind::AuxStorage,
        projection,
        FixtureCurrentRowReader::new(Vec::new()),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message)
            if message.contains("at least one hash column")
    ));
}

#[test]
fn given_duckdb_projection_with_invalid_identifier_when_constructing_projection_then_constructor_rejects_it()
 {
    let projection = CheckRowProjection {
        table_name: "users".to_string(),
        key_column: "user id".to_string(),
        hash_columns: vec!["email".to_string()],
        source_position_column: None,
        private_data_column: None,
    };
    let error =
        DuckDbAnalyticalCheckRows::try_new_with_projection(":memory:", projection).unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message)
            if message.contains("invalid DuckDB check rows table identifier")
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
                source_position json,
                contains_private_data boolean not null
            );
            insert into check_rows values
                ('users', 'u1', 11, \
             '{\"type\":\"raw_backup_object_offset\",\"object_id\":\"check-fixture\",\"offset\":\
             3}', false),
                ('users', 'u2', 22, \
             '{\"type\":\"raw_backup_object_offset\",\"object_id\":\"check-fixture\",\"offset\":\
             4}', false),
                ('orders', 'o1', 33, \
             '{\"type\":\"raw_backup_object_offset\",\"object_id\":\"check-fixture\",\"offset\":\
             5}', false);",
        )
        .unwrap();
    drop(connection);
    let store = OperationStore::connect_in_memory().unwrap();
    let source = DuckDbCheckRows::try_new(database_path.to_string_lossy(), "check_rows").unwrap();
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
                source_position json,
                contains_private_data boolean not null
            );
            insert into check_rows values
                ('users', 'u2', 22, \
             '{\"type\":\"raw_backup_object_offset\",\"object_id\":\"check-fixture\",\"offset\":\
             4}', false),
                ('orders', 'o1', 33, \
             '{\"type\":\"raw_backup_object_offset\",\"object_id\":\"check-fixture\",\"offset\":\
             5}', false),
                ('users', 'u1', 11, \
             '{\"type\":\"raw_backup_object_offset\",\"object_id\":\"check-fixture\",\"offset\":\
             3}', false);",
        )
        .unwrap();
    drop(connection);
    let source = DuckDbCheckRows::try_new(database_path.to_string_lossy(), "check_rows").unwrap();
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

#[test]
fn given_ambiguous_field_boundaries_or_null_and_empty_when_checked_then_content_mismatch_is_reported()
 {
    for (index, (left, right)) in [
        ([Some("ab"), Some("c")], [Some("a"), Some("bc")]),
        ([None, Some("value")], [Some(""), Some("value")]),
    ]
    .into_iter()
    .enumerate()
    {
        let make_source = |values: [Option<&str>; 2]| {
            ProjectionBackedCheckRows::fixture_dynamodb(
                check_projection(),
                FixtureCurrentRowReader::new(vec![ProjectedCurrentRow::new(
                    "u1",
                    ["email", "org_id"]
                        .into_iter()
                        .zip(values)
                        .map(|(column, value)| (column.to_string(), value.map(str::to_string)))
                        .collect(),
                    CheckSourcePosition::default(),
                )]),
            )
            .unwrap()
        };
        let store = OperationStore::connect_in_memory().unwrap();
        let report = LocalCheckExecutor
            .execute(
                &store,
                &request(&format!("check_hash_framing_{index}"), 3),
                &make_source(left),
                &make_source(right),
            )
            .unwrap();
        assert_eq!(report.outcome, CheckOutcome::Mismatched);
        assert_eq!(report.metrics.mismatched_rows, 1);
        assert_eq!(report.samples.mismatched_keys, vec!["users:u1"]);
    }
}

#[test]
fn given_large_provider_sequences_when_destination_is_behind_then_check_reports_stale_without_mutation()
 {
    let position = |sequence: &str| {
        CheckSourcePosition::from_source_position(SourcePosition::DynamoDbStreamSequence {
            stream_arn: "stream".to_string(),
            shard_id: "shard".to_string(),
            sequence_number: sequence.to_string(),
        })
        .unwrap()
    };
    let mut source_row = row("u1", 11, 0);
    source_row.source_position = position("100000000000000000000000000000000000000000000000004");
    let mut destination_row = source_row.clone();
    destination_row.source_position =
        position("100000000000000000000000000000000000000000000000003");
    let source = LocalCheckDataset::new(vec![source_row]);
    let destination = LocalCheckDataset::new(vec![destination_row]);
    let before = destination.clone();
    let report = LocalCheckExecutor
        .execute(
            &OperationStore::connect_in_memory().unwrap(),
            &request("check_large_sequences", 1),
            &source,
            &destination,
        )
        .unwrap();
    assert_eq!(report.outcome, CheckOutcome::Stale);
    assert_eq!(report.metrics.stale_rows, 1);
    assert_eq!(destination, before);
}

#[test]
fn given_different_provider_lanes_when_rows_are_checked_then_equal_offsets_do_not_establish_order()
{
    let positions = [
        SourcePosition::DynamoDbStreamSequence {
            stream_arn: "stream-a".to_string(),
            shard_id: "shard-a".to_string(),
            sequence_number: "10".to_string(),
        },
        SourcePosition::DynamoDbStreamSequence {
            stream_arn: "stream-a".to_string(),
            shard_id: "shard-b".to_string(),
            sequence_number: "10".to_string(),
        },
        SourcePosition::DynamoDbStreamSequence {
            stream_arn: "stream-b".to_string(),
            shard_id: "shard-a".to_string(),
            sequence_number: "10".to_string(),
        },
        SourcePosition::AuxStorageStreamCursor {
            stream_name: "stream-a".to_string(),
            cursor: "10".to_string(),
        },
        SourcePosition::AuxStorageStreamCursor {
            stream_name: "stream-b".to_string(),
            cursor: "10".to_string(),
        },
        SourcePosition::DynamoDbExportSnapshot {
            export_arn: "export-a".to_string(),
            exported_at_ms: 10,
        },
        SourcePosition::DynamoDbExportSnapshot {
            export_arn: "export-b".to_string(),
            exported_at_ms: 10,
        },
        SourcePosition::DynamoDbScanChunk {
            table_name: "users".to_string(),
            scan_generation: 1,
            segment: 0,
            item_index: 10,
        },
        SourcePosition::DynamoDbScanChunk {
            table_name: "users".to_string(),
            scan_generation: 2,
            segment: 0,
            item_index: 10,
        },
        SourcePosition::DynamoDbScanChunk {
            table_name: "users".to_string(),
            scan_generation: 1,
            segment: 1,
            item_index: 10,
        },
        SourcePosition::RawBackupObjectOffset {
            object_id: "object-a".to_string(),
            offset: 10,
        },
        SourcePosition::RawBackupObjectOffset {
            object_id: "object-b".to_string(),
            offset: 10,
        },
    ];
    let store = OperationStore::connect_in_memory().unwrap();
    for (left_index, left) in positions.iter().enumerate() {
        for (right_index, right) in positions.iter().enumerate().skip(left_index + 1) {
            let mut source_row = row("u1", 11, 0);
            source_row.source_position =
                CheckSourcePosition::from_source_position(left.clone()).unwrap();
            let mut destination_row = source_row.clone();
            destination_row.source_position =
                CheckSourcePosition::from_source_position(right.clone()).unwrap();
            let source = LocalCheckDataset::new(vec![source_row]);
            let destination = LocalCheckDataset::new(vec![destination_row]);
            let before = destination.clone();
            let report = LocalCheckExecutor
                .execute(
                    &store,
                    &request(&format!("check_lanes_{left_index}_{right_index}"), 1),
                    &source,
                    &destination,
                )
                .unwrap();
            assert_eq!(report.outcome, CheckOutcome::Mismatched);
            assert_eq!(report.metrics.unsupported_comparisons, 1);
            assert_eq!(report.metrics.stale_rows, 0);
            assert_eq!(destination, before);
        }
    }
}

#[test]
fn given_unpositioned_rows_when_checked_then_only_two_absent_positions_allow_content_comparison() {
    for (index, destination_position) in [
        CheckSourcePosition::default(),
        crate::check_test_support::backup_position(0),
    ]
    .into_iter()
    .enumerate()
    {
        let mut source_row = row("u1", 11, 0);
        source_row.source_position = CheckSourcePosition::default();
        let mut destination_row = source_row.clone();
        destination_row.source_position = destination_position;
        let report = LocalCheckExecutor
            .execute(
                &OperationStore::connect_in_memory().unwrap(),
                &request(&format!("check_absence_{index}"), 1),
                &LocalCheckDataset::new(vec![source_row]),
                &LocalCheckDataset::new(vec![destination_row]),
            )
            .unwrap();
        assert_eq!(report.metrics.unsupported_comparisons, index as u64);
        assert_eq!(
            report.outcome,
            if index == 0 {
                CheckOutcome::Complete
            } else {
                CheckOutcome::Mismatched
            }
        );
    }
}
