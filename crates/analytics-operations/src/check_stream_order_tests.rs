use std::{cell::RefCell, rc::Rc};

use crate::{
    CheckComparisonStrategy, CheckError, CheckOutcome, CheckOutputFormat, CheckRequest,
    CheckRowProjection, CurrentRowPageRequest, CurrentRowStreamOrder, LocalCheckExecutor,
    OperationActor, OperationId, OperationStore, ProjectedCurrentRow, ProjectedCurrentRowPage,
    ProjectionBackedCheckRows, RateLimitPolicy,
};

fn request(id: &str) -> CheckRequest {
    CheckRequest {
        operation_id: OperationId::new(id).unwrap(),
        actor: OperationActor::new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        strategy: CheckComparisonStrategy::FullRows,
        sample_limit: 3,
        privacy_policy_version: Some("policy-v1".to_string()),
        output: CheckOutputFormat::Json,
        rate_limit: RateLimitPolicy::default(),
    }
}

fn projection() -> CheckRowProjection {
    CheckRowProjection::new(
        "users",
        "__id",
        vec!["email".to_string(), "org_id".to_string()],
    )
    .unwrap()
}

fn row(key: &str, email: &str, org_id: &str, source_position: u64) -> ProjectedCurrentRow {
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

#[test]
fn given_unordered_production_reader_when_check_runs_then_it_fails_closed() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = ProjectionBackedCheckRows::full_table_aux_storage(
        projection(),
        OrderedProductionPageReader::new(
            vec![ProjectedCurrentRowPage::terminal(vec![row(
                "u1",
                "a@example.test",
                "org-a",
                10,
            )])],
            CurrentRowStreamOrder::Unordered,
        ),
        100,
    )
    .unwrap();
    let destination = ProjectionBackedCheckRows::full_table_aux_storage(
        projection(),
        OrderedProductionPageReader::new(
            vec![ProjectedCurrentRowPage::terminal(vec![row(
                "u1",
                "a@example.test",
                "org-a",
                10,
            )])],
            CurrentRowStreamOrder::GloballySortedByKey,
        ),
        100,
    )
    .unwrap();

    let error = LocalCheckExecutor
        .execute(
            &store,
            &request("check_unordered_production_reader_1"),
            &source,
            &destination,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message)
            if message.contains("globally sorted or partition-sorted rows")
                && message.contains("declared Unordered")
    ));
}

#[test]
fn given_partition_sorted_production_readers_when_check_runs_then_partitions_are_compared() {
    let store = OperationStore::connect_in_memory().unwrap();
    let source = partition_sorted_reader();
    let destination = partition_sorted_reader();

    let report = LocalCheckExecutor
        .execute(
            &store,
            &request("check_partition_sorted_production_reader_1"),
            &source,
            &destination,
        )
        .unwrap();

    assert_eq!(report.outcome, CheckOutcome::Complete);
    assert_eq!(report.metrics.checked_rows, 2);
}

#[test]
fn given_partition_sorted_production_reader_without_partition_when_check_runs_then_it_fails_closed()
{
    let store = OperationStore::connect_in_memory().unwrap();
    let source = ProjectionBackedCheckRows::full_table_dynamodb(
        projection(),
        OrderedProductionPageReader::new(
            vec![ProjectedCurrentRowPage::terminal(vec![row(
                "u1",
                "a@example.test",
                "org-a",
                10,
            )])],
            CurrentRowStreamOrder::PartitionSortedByKey,
        ),
        100,
    )
    .unwrap();
    let destination = partition_sorted_reader();

    let error = LocalCheckExecutor
        .execute(
            &store,
            &request("check_partition_sorted_missing_partition_1"),
            &source,
            &destination,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        CheckError::InvalidBackendConfiguration(message)
            if message.contains("page without a partition key")
    ));
}

fn partition_sorted_reader()
-> ProjectionBackedCheckRows<crate::ProductionCurrentRowReader<OrderedProductionPageReader>> {
    ProjectionBackedCheckRows::full_table_dynamodb(
        projection(),
        OrderedProductionPageReader::new(
            vec![
                ProjectedCurrentRowPage::terminal_partition(
                    "tenant-b",
                    vec![row("u1", "b@example.test", "org-b", 20)],
                ),
                ProjectedCurrentRowPage::partition_with_next_cursor(
                    "tenant-a",
                    vec![row("u1", "a@example.test", "org-a", 10)],
                    "tenant-b",
                ),
            ],
            CurrentRowStreamOrder::PartitionSortedByKey,
        ),
        100,
    )
    .unwrap()
}
