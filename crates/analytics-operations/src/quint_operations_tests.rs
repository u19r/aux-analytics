use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    OperationActor, OperationCursor, OperationEventKind, OperationId, OperationKind,
    OperationPhase, OperationRequest, OperationStatus, OperationStore, RateLimitPolicy,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum OperationAction {
    Create,
    DuplicateCreate,
    Start,
    AdvanceCursor,
    MoveCursorBackwards,
    RequestCancel,
    AcceptCancel,
    Succeed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum OperationResult {
    NotChecked,
    Submitted,
    DuplicateRejected,
    Running,
    CursorAdvanced,
    BackwardsCursorRejected,
    Cancelling,
    Cancelled,
    Succeeded,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct OperationMbtState {
    #[serde(rename = "lastResult")]
    last_result: OperationResult,
}

impl State<OperationMbtDriver> for OperationMbtState {
    fn from_driver(driver: &OperationMbtDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct OperationMbtDriver {
    last_result: OperationResult,
}

impl Default for OperationMbtDriver {
    fn default() -> Self {
        Self {
            last_result: OperationResult::NotChecked,
        }
    }
}

impl Driver for OperationMbtDriver {
    type State = OperationMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = OperationResult::NotChecked,
            CheckOperation(action: OperationAction) => {
                self.last_result = check_operation_action(action)?;
            },
            step(opAction: OperationAction?) => {
                if let Some(op_action) = opAction {
                    self.last_result = check_operation_action(op_action)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/operations_mbt.qnt",
    max_samples = 32,
    max_steps = 4,
    seed = "0x5163"
)]
fn quint_operations_mbt_compares_store_result_state() -> impl Driver {
    OperationMbtDriver::default()
}

fn check_operation_action(action: OperationAction) -> Result<OperationResult> {
    match action {
        OperationAction::Create => check_create(),
        OperationAction::DuplicateCreate => check_duplicate_create(),
        OperationAction::Start => check_start(),
        OperationAction::AdvanceCursor => check_advance_cursor(),
        OperationAction::MoveCursorBackwards => check_move_cursor_backwards(),
        OperationAction::RequestCancel => check_request_cancel(),
        OperationAction::AcceptCancel => check_accept_cancel(),
        OperationAction::Succeed => check_succeed(),
    }
}

fn check_create() -> Result<OperationResult> {
    let store = OperationStore::connect_in_memory()?;
    let request = request("op_create")?;
    store.create_operation(&request)?;
    let operation = store.show_operation(&request.operation_id)?;
    if operation.status == OperationStatus::Submitted
        && store.audit_events(&request.operation_id)?.len() == 1
    {
        Ok(OperationResult::Submitted)
    } else {
        Err(anyhow::anyhow!(
            "operation was not submitted with one audit event"
        ))
    }
}

fn check_duplicate_create() -> Result<OperationResult> {
    let store = OperationStore::connect_in_memory()?;
    let request = request("op_duplicate")?;
    store.create_operation(&request)?;
    if store.create_operation(&request).is_err()
        && store.audit_events(&request.operation_id)?.len() == 1
    {
        Ok(OperationResult::DuplicateRejected)
    } else {
        Err(anyhow::anyhow!(
            "duplicate operation was not rejected without audit append"
        ))
    }
}

fn check_start() -> Result<OperationResult> {
    let store = operation_store_with_request("op_start")?;
    let operation_id = OperationId::new("op_start")?;
    transition(
        &store,
        &operation_id,
        OperationPhase::Executing,
        OperationStatus::Running,
        None,
        OperationEventKind::Started,
    )?;
    Ok(OperationResult::Running)
}

fn check_advance_cursor() -> Result<OperationResult> {
    let store = operation_store_with_request("op_cursor")?;
    let operation_id = OperationId::new("op_cursor")?;
    transition(
        &store,
        &operation_id,
        OperationPhase::Executing,
        OperationStatus::Running,
        Some(OperationCursor {
            label: "chunk".to_string(),
            position: 1,
        }),
        OperationEventKind::CursorAdvanced,
    )?;
    let operation = store.show_operation(&operation_id)?;
    if operation.cursor.as_ref().map(|cursor| cursor.position) == Some(1) {
        Ok(OperationResult::CursorAdvanced)
    } else {
        Err(anyhow::anyhow!("cursor did not advance"))
    }
}

fn check_move_cursor_backwards() -> Result<OperationResult> {
    let store = operation_store_with_request("op_backwards")?;
    let operation_id = OperationId::new("op_backwards")?;
    transition(
        &store,
        &operation_id,
        OperationPhase::Executing,
        OperationStatus::Running,
        Some(OperationCursor {
            label: "chunk".to_string(),
            position: 2,
        }),
        OperationEventKind::CursorAdvanced,
    )?;
    if transition(
        &store,
        &operation_id,
        OperationPhase::Executing,
        OperationStatus::Running,
        Some(OperationCursor {
            label: "chunk".to_string(),
            position: 1,
        }),
        OperationEventKind::CursorAdvanced,
    )
    .is_err()
    {
        Ok(OperationResult::BackwardsCursorRejected)
    } else {
        Err(anyhow::anyhow!("backwards cursor transition was accepted"))
    }
}

fn check_request_cancel() -> Result<OperationResult> {
    let store = operation_store_with_request("op_cancel")?;
    let operation_id = OperationId::new("op_cancel")?;
    store.request_cancellation(&operation_id)?;
    let operation = store.show_operation(&operation_id)?;
    if operation.status == OperationStatus::Cancelling {
        Ok(OperationResult::Cancelling)
    } else {
        Err(anyhow::anyhow!("operation was not cancelling"))
    }
}

fn check_accept_cancel() -> Result<OperationResult> {
    let store = operation_store_with_request("op_cancel_accept")?;
    let operation_id = OperationId::new("op_cancel_accept")?;
    store.request_cancellation(&operation_id)?;
    transition(
        &store,
        &operation_id,
        OperationPhase::Completed,
        OperationStatus::Cancelled,
        None,
        OperationEventKind::CancellationAccepted,
    )?;
    Ok(OperationResult::Cancelled)
}

fn check_succeed() -> Result<OperationResult> {
    let store = operation_store_with_request("op_succeed")?;
    let operation_id = OperationId::new("op_succeed")?;
    transition(
        &store,
        &operation_id,
        OperationPhase::Completed,
        OperationStatus::Succeeded,
        None,
        OperationEventKind::Succeeded,
    )?;
    Ok(OperationResult::Succeeded)
}

fn operation_store_with_request(operation_id: &str) -> Result<OperationStore> {
    let store = OperationStore::connect_in_memory()?;
    let request = request(operation_id)?;
    store.create_operation(&request)?;
    Ok(store)
}

fn request(operation_id: &str) -> Result<OperationRequest> {
    Ok(OperationRequest {
        operation_id: OperationId::new(operation_id)?,
        kind: OperationKind::Backfill,
        actor: OperationActor::new("operator")?,
        target_tables: vec!["users".to_string()],
        dry_run: true,
        rate_limit: RateLimitPolicy::default(),
        payload: serde_json::json!({ "mode": "mbt" }),
    })
}

fn transition(
    store: &OperationStore,
    operation_id: &OperationId,
    phase: OperationPhase,
    status: OperationStatus,
    cursor: Option<OperationCursor>,
    event_kind: OperationEventKind,
) -> Result<()> {
    store.transition(operation_id, phase, status, cursor, event_kind, Some("mbt"))?;
    Ok(())
}
