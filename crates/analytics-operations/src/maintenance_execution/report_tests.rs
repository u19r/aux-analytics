use serde::{Serialize, Serializer};

use super::*;
use crate::{
    OperationId, OperationKind, OperationRequest, RateLimitPolicy, TrimReport, TrimTarget,
};

struct UnserializableReport;

impl Serialize for UnserializableReport {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("report encoding failed"))
    }
}

#[test]
fn given_report_serialization_failure_when_retried_then_effect_can_commit_and_report_is_persisted()
{
    let store = OperationStore::connect_in_memory().unwrap();
    let request = OperationRequest {
        operation_id: OperationId::try_new("report-retry").unwrap(),
        kind: OperationKind::Trim,
        actor: OperationActor::try_new("operator").unwrap(),
        target_tables: vec!["entries".to_owned()],
        dry_run: true,
        rate_limit: RateLimitPolicy::default(),
        payload: serde_json::json!({}),
    };
    store.create_operation(&request).unwrap();
    let attempt = store
        .prepare_for_execution(&request.operation_id, &request.actor)
        .unwrap();
    let key = OperationChunkKey::for_operation(&request.operation_id, "trim-report", 0);
    let operations = MaintenanceOperations::new(&store);
    let error = operations
        .finish_execution_effect(
            &attempt,
            &key,
            ExecutionReport {
                kind: OperationReportKind::Trim,
                report: &UnserializableReport,
                counts: serde_json::json!({}),
                message: "trim completed",
            },
        )
        .unwrap_err();
    assert!(matches!(error, MaintenanceError::Serialization(_)));
    let report = TrimReport {
        operation_id: request.operation_id.clone(),
        table_name: "entries".to_owned(),
        dry_run: true,
        target: TrimTarget::FullTableDrop,
        candidate_rows: 3,
        rows_deleted: 0,
        destination_mutated: false,
    };
    operations
        .finish_execution_effect(
            &attempt,
            &key,
            ExecutionReport {
                kind: OperationReportKind::Trim,
                report: &report,
                counts: serde_json::json!({}),
                message: "trim completed",
            },
        )
        .unwrap();
    assert_eq!(
        store.show_operation(&request.operation_id).unwrap().status,
        OperationStatus::Succeeded
    );
    assert_eq!(store.trim_report(&request.operation_id).unwrap(), report);
}
