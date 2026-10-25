use quint_connect::{Driver, Result, State, Step, switch};
use serde::{Deserialize, Serialize};

use crate::{
    CancellationState as StoreCancellationState, OperationActor, OperationCursor,
    OperationEventKind, OperationId, OperationKind, OperationPhase as StoreOperationPhase,
    OperationRequest, OperationStatus as StoreOperationStatus, OperationStore, OperationStoreError,
    RateLimitPolicy, store_test_support::OperationStoreTestFixture,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum OperationAction {
    Create,
    DuplicateCreate,
    Plan,
    Start,
    AdvanceCursor,
    MoveCursorBackwards,
    Pause,
    Resume,
    RequestCancel,
    AcceptCancel,
    Succeed,
    Crash,
    Retry,
    StaleResume,
    StaleCancel,
    Observe,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum OperationResult {
    NotChecked,
    Submitted,
    DuplicateRejected,
    Planned,
    Running,
    CursorAdvanced,
    BackwardsCursorRejected,
    Paused,
    Resumed,
    Cancelling,
    Cancelled,
    Succeeded,
    Crashed,
    Retried,
    StaleRejected,
    Observed,
    Rejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelOperationPhase {
    SubmittedPhase,
    Planning,
    Executing,
    Validating,
    Completed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelOperationStatus {
    #[serde(rename = "SubmittedStatus")]
    Submitted,
    #[serde(rename = "PlannedStatus")]
    Planned,
    #[serde(rename = "RunningStatus")]
    Running,
    #[serde(rename = "PausedStatus")]
    Paused,
    #[serde(rename = "CancellingStatus")]
    Cancelling,
    #[serde(rename = "CancelledStatus")]
    Cancelled,
    #[serde(rename = "FailedStatus")]
    Failed,
    #[serde(rename = "SucceededStatus")]
    Succeeded,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelCancellationState {
    NotRequested,
    Requested,
    Accepted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DurableOperationState {
    exists: bool,
    operation_identity: i64,
    actor_authorized: bool,
    phase: ModelOperationPhase,
    status: ModelOperationStatus,
    cancellation_state: ModelCancellationState,
    cursor_present: bool,
    cursor_position: i64,
    cursor_label: String,
    progress_rows: i64,
    progress_retries: i64,
    revision: i64,
    audit_length: i64,
    node_alive: bool,
    restart_count: i64,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct OperationMbtState {
    #[serde(rename = "lastAction")]
    last_action: OperationAction,
    #[serde(rename = "lastResult")]
    last_result: OperationResult,
    operation: DurableOperationState,
    previous_operation: DurableOperationState,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
struct OperationProgress {
    rows: u64,
    retries: u64,
}

impl State<OperationMbtDriver> for OperationMbtState {
    fn from_driver(driver: &OperationMbtDriver) -> Result<Self> {
        Ok(Self {
            last_action: driver.last_action,
            last_result: driver.last_result,
            operation: driver.durable_state()?,
            previous_operation: driver.previous_operation.clone(),
        })
    }
}

struct OperationMbtDriver {
    store: Option<OperationStore>,
    operation_id: OperationId,
    actor: OperationActor,
    request: OperationRequest,
    last_action: OperationAction,
    last_result: OperationResult,
    previous_operation: DurableOperationState,
    node_alive: bool,
    restart_count: u64,
}

impl Default for OperationMbtDriver {
    fn default() -> Self {
        let operation_id = OperationId::try_new("quint-operation-mbt").expect("valid MBT id");
        let actor = OperationActor::try_new("operator").expect("valid MBT actor");
        let request = operation_request(operation_id.clone(), actor.clone());
        Self {
            store: None,
            operation_id,
            actor,
            request,
            last_action: OperationAction::Observe,
            last_result: OperationResult::NotChecked,
            previous_operation: empty_durable_operation(true, 0),
            node_alive: true,
            restart_count: 0,
        }
    }
}

impl Driver for OperationMbtDriver {
    type State = OperationMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.initialize()?,
            CheckOperation(action: OperationAction) => self.apply(action)?,
            step(opAction: OperationAction?) => {
                if let Some(action) = opAction {
                    self.apply(action)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/operations_mbt.qnt",
    max_samples = 32,
    max_steps = 8,
    seed = "0x5163"
)]
fn given_quint_operations_model_when_replayed_then_store_result_state_matches() -> impl Driver {
    OperationMbtDriver::default()
}

impl OperationMbtDriver {
    fn initialize(&mut self) -> Result<()> {
        self.store = Some(OperationStore::connect_in_memory()?);
        self.last_action = OperationAction::Observe;
        self.last_result = OperationResult::NotChecked;
        self.previous_operation = empty_durable_operation(true, 0);
        self.node_alive = true;
        self.restart_count = 0;
        Ok(())
    }

    fn apply(&mut self, action: OperationAction) -> Result<()> {
        let previous_operation = self.durable_state()?;
        let result = match action {
            OperationAction::Create => self.create(),
            OperationAction::DuplicateCreate => self.duplicate_create(),
            OperationAction::Plan => self.plan(),
            OperationAction::Start => self.start(),
            OperationAction::AdvanceCursor => self.advance_cursor(),
            OperationAction::MoveCursorBackwards => self.move_cursor_backwards(),
            OperationAction::Pause => self.pause(),
            OperationAction::Resume => self.resume(),
            OperationAction::RequestCancel => self.request_cancel(),
            OperationAction::AcceptCancel => self.accept_cancel(),
            OperationAction::Succeed => self.succeed(),
            OperationAction::Crash => self.crash(),
            OperationAction::Retry => self.retry(),
            OperationAction::StaleResume => self.stale_resume(),
            OperationAction::StaleCancel => self.stale_cancel(),
            OperationAction::Observe => self.observe(),
        }?;
        self.previous_operation = previous_operation;
        self.last_action = action;
        self.last_result = result;
        Ok(())
    }

    fn create(&self) -> Result<OperationResult> {
        let store = self.store()?;
        match store.create_operation(&self.request) {
            Ok(()) => {
                let (operation, audit_length) = self.current()?;
                if operation.status == StoreOperationStatus::Submitted && audit_length == 1 {
                    Ok(OperationResult::Submitted)
                } else {
                    Err(anyhow::anyhow!(
                        "create returned an unexpected durable state"
                    ))
                }
            }
            Err(OperationStoreError::DuplicateOperation(_)) => Ok(OperationResult::Rejected),
            Err(error) => Err(error.into()),
        }
    }

    fn duplicate_create(&self) -> Result<OperationResult> {
        let store = self.store()?;
        match store.create_operation(&self.request) {
            Ok(()) => Err(anyhow::anyhow!(
                "duplicate action unexpectedly created an operation"
            )),
            Err(OperationStoreError::DuplicateOperation(_)) => {
                let (_, audit_length) = self.current()?;
                if audit_length == 1 {
                    Ok(OperationResult::DuplicateRejected)
                } else {
                    Err(anyhow::anyhow!(
                        "duplicate create changed the operation audit"
                    ))
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    fn plan(&self) -> Result<OperationResult> {
        let (operation, audit_length) = match self.current() {
            Ok(value) => value,
            Err(error)
                if matches!(
                    error.downcast_ref::<OperationStoreError>(),
                    Some(OperationStoreError::OperationNotFound(_))
                ) =>
            {
                return Ok(OperationResult::Rejected);
            }
            Err(error) => return Err(error),
        };
        match self.transition(
            &operation,
            crate::OperationTransition {
                phase: StoreOperationPhase::Planning,
                status: StoreOperationStatus::Planned,
                cursor: None,
                event_kind: OperationEventKind::Planned,
                counts: serde_json::json!({}),
                message: Some("quint operations plan"),
            },
        ) {
            Ok(()) => self.expect_status(
                StoreOperationPhase::Planning,
                StoreOperationStatus::Planned,
                audit_length + 1,
                OperationResult::Planned,
            ),
            Err(error) => Err(error),
        }
    }

    fn start(&self) -> Result<OperationResult> {
        let (operation, audit_length) = match self.current() {
            Ok(value) => value,
            Err(error)
                if matches!(
                    error.downcast_ref::<OperationStoreError>(),
                    Some(OperationStoreError::OperationNotFound(_))
                ) =>
            {
                return Ok(OperationResult::Rejected);
            }
            Err(error) => return Err(error),
        };
        match self.transition(
            &operation,
            crate::OperationTransition {
                phase: StoreOperationPhase::Executing,
                status: StoreOperationStatus::Running,
                cursor: None,
                event_kind: OperationEventKind::Started,
                counts: serde_json::json!({}),
                message: Some("quint operations start"),
            },
        ) {
            Ok(()) => self.expect_status(
                StoreOperationPhase::Executing,
                StoreOperationStatus::Running,
                audit_length + 1,
                OperationResult::Running,
            ),
            Err(error) => Err(error),
        }
    }

    fn advance_cursor(&self) -> Result<OperationResult> {
        let store = self.store()?;
        let (operation, audit_length) = self.current()?;
        let position = operation
            .cursor
            .as_ref()
            .map_or(1, |cursor| cursor.position.saturating_add(1));
        let mut progress: OperationProgress = serde_json::from_value(operation.progress.clone())?;
        progress.rows = progress.rows.saturating_add(1);
        let cursor = OperationCursor {
            label: "chunk".to_string(),
            position,
        };
        match store.transition_with_counts_as(
            &operation,
            &self.actor,
            crate::OperationTransition {
                phase: StoreOperationPhase::Executing,
                status: StoreOperationStatus::Running,
                cursor: Some(cursor),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: serde_json::to_value(progress)?,
                message: Some("quint operations cursor advance"),
            },
        ) {
            Ok(()) => {
                let (next, next_audit_length) = self.current()?;
                if next.phase == StoreOperationPhase::Executing
                    && next.status == StoreOperationStatus::Running
                    && next.cursor.as_ref().map(|value| value.position) == Some(position)
                    && next_audit_length == audit_length + 1
                {
                    Ok(OperationResult::CursorAdvanced)
                } else {
                    Err(anyhow::anyhow!(
                        "cursor transition returned an unexpected durable state"
                    ))
                }
            }
            Err(OperationStoreError::OperationNotFound(_)) => Ok(OperationResult::Rejected),
            Err(error) => Err(error.into()),
        }
    }

    fn move_cursor_backwards(&self) -> Result<OperationResult> {
        let store = self.store()?;
        let (operation, audit_length) = self.current()?;
        let Some(cursor) = operation.cursor.as_ref() else {
            return Ok(OperationResult::Rejected);
        };
        if cursor.position == 0 {
            return Ok(OperationResult::Rejected);
        }
        let backwards = OperationCursor {
            label: cursor.label.clone(),
            position: cursor.position - 1,
        };
        let error = store
            .transition_with_counts_as(
                &operation,
                &self.actor,
                crate::OperationTransition {
                    phase: StoreOperationPhase::Executing,
                    status: StoreOperationStatus::Running,
                    cursor: Some(backwards),
                    event_kind: OperationEventKind::CursorAdvanced,
                    counts: operation.progress.clone(),
                    message: Some("quint operations backwards cursor"),
                },
            )
            .expect_err("a backwards cursor transition must be rejected");
        let (unchanged, next_audit_length) = self.current()?;
        if matches!(error, OperationStoreError::Type(_))
            && unchanged == operation
            && next_audit_length == audit_length
        {
            Ok(OperationResult::BackwardsCursorRejected)
        } else {
            Err(anyhow::anyhow!(
                "backwards cursor rejection did not preserve durable state"
            ))
        }
    }

    fn pause(&self) -> Result<OperationResult> {
        let (operation, audit_length) = self.current()?;
        match self.transition(
            &operation,
            crate::OperationTransition {
                phase: StoreOperationPhase::Executing,
                status: StoreOperationStatus::Paused,
                cursor: operation.cursor.clone(),
                event_kind: OperationEventKind::Paused,
                counts: serde_json::json!({}),
                message: Some("quint operations pause"),
            },
        ) {
            Ok(()) => self.expect_after_audit(
                StoreOperationStatus::Paused,
                audit_length + 1,
                OperationResult::Paused,
            ),
            Err(error) => Err(error),
        }
    }

    fn resume(&self) -> Result<OperationResult> {
        let (_, audit_length) = self.current()?;
        match self
            .store()?
            .resume_operation(&self.operation_id, &self.actor)
        {
            Ok(()) => self.expect_after_audit(
                StoreOperationStatus::Running,
                audit_length + 1,
                OperationResult::Resumed,
            ),
            Err(error) => Err(error.into()),
        }
    }

    fn request_cancel(&self) -> Result<OperationResult> {
        let (_, audit_length) = self.current()?;
        match self
            .store()?
            .request_cancellation_as(&self.operation_id, &self.actor)
        {
            Ok(()) => {
                let (operation, next_audit_length) = self.current()?;
                if operation.status == StoreOperationStatus::Cancelling
                    && operation.cancellation_state == StoreCancellationState::Requested
                    && next_audit_length == audit_length + 1
                {
                    Ok(OperationResult::Cancelling)
                } else {
                    Err(anyhow::anyhow!(
                        "cancellation request returned an unexpected durable state"
                    ))
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    fn accept_cancel(&self) -> Result<OperationResult> {
        let (_, audit_length) = self.current()?;
        match self
            .store()?
            .accept_cancellation_as(&self.operation_id, &self.actor)
        {
            Ok(()) => {
                let (operation, next_audit_length) = self.current()?;
                if operation.phase == StoreOperationPhase::Completed
                    && operation.status == StoreOperationStatus::Cancelled
                    && operation.cancellation_state == StoreCancellationState::Accepted
                    && next_audit_length == audit_length + 1
                {
                    Ok(OperationResult::Cancelled)
                } else {
                    Err(anyhow::anyhow!(
                        "cancellation acceptance returned an unexpected durable state"
                    ))
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    fn succeed(&self) -> Result<OperationResult> {
        let (operation, audit_length) = self.current()?;
        match self.transition(
            &operation,
            crate::OperationTransition {
                phase: StoreOperationPhase::Completed,
                status: StoreOperationStatus::Succeeded,
                cursor: operation.cursor.clone(),
                event_kind: OperationEventKind::Succeeded,
                counts: serde_json::json!({}),
                message: Some("quint operations succeed"),
            },
        ) {
            Ok(()) => self.expect_after_audit(
                StoreOperationStatus::Succeeded,
                audit_length + 1,
                OperationResult::Succeeded,
            ),
            Err(error) => Err(error),
        }
    }

    fn crash(&mut self) -> Result<OperationResult> {
        let (operation, _) = self.current()?;
        if operation.status != StoreOperationStatus::Running || !self.node_alive {
            return Ok(OperationResult::Rejected);
        }
        self.node_alive = false;
        Ok(OperationResult::Crashed)
    }

    fn retry(&mut self) -> Result<OperationResult> {
        let (operation, audit_length) = self.current()?;
        if operation.status != StoreOperationStatus::Running || self.node_alive {
            return Ok(OperationResult::Rejected);
        }
        let mut progress: OperationProgress = serde_json::from_value(operation.progress.clone())?;
        progress.retries = progress.retries.saturating_add(1);
        self.transition(
            &operation,
            crate::OperationTransition {
                phase: StoreOperationPhase::Executing,
                status: StoreOperationStatus::Running,
                cursor: operation.cursor.clone(),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: serde_json::to_value(progress)?,
                message: Some("quint operations retry"),
            },
        )?;
        self.node_alive = true;
        self.restart_count = self.restart_count.saturating_add(1);
        let (next, next_audit_length) = self.current()?;
        if next.status != StoreOperationStatus::Running || next_audit_length != audit_length + 1 {
            return Err(anyhow::anyhow!(
                "retry did not preserve the durable running operation"
            ));
        }
        Ok(OperationResult::Retried)
    }

    fn stale_resume(&self) -> Result<OperationResult> {
        let (stale, audit_length) = self.current()?;
        if stale.status != StoreOperationStatus::Paused {
            return Ok(OperationResult::Rejected);
        }
        self.store()?
            .resume_operation(&self.operation_id, &self.actor)?;
        let error = self
            .store()?
            .transition_with_counts_as(
                &stale,
                &self.actor,
                crate::OperationTransition {
                    phase: StoreOperationPhase::Executing,
                    status: StoreOperationStatus::Running,
                    cursor: stale.cursor.clone(),
                    event_kind: OperationEventKind::Resumed,
                    counts: serde_json::json!({}),
                    message: Some("quint stale resume"),
                },
            )
            .expect_err("stale resume must fail its durable CAS");
        let (current, next_audit_length) = self.current()?;
        if matches!(error, OperationStoreError::StaleOperationState { .. })
            && current.status == StoreOperationStatus::Running
            && next_audit_length == audit_length + 1
        {
            Ok(OperationResult::StaleRejected)
        } else {
            Err(anyhow::anyhow!(
                "stale resume changed the wrong durable state"
            ))
        }
    }

    fn stale_cancel(&self) -> Result<OperationResult> {
        let (stale, audit_length) = self.current()?;
        if stale.status != StoreOperationStatus::Running {
            return Ok(OperationResult::Rejected);
        }
        self.transition(
            &stale,
            crate::OperationTransition {
                phase: StoreOperationPhase::Executing,
                status: StoreOperationStatus::Paused,
                cursor: stale.cursor.clone(),
                event_kind: OperationEventKind::Paused,
                counts: serde_json::json!({}),
                message: Some("quint stale cancellation pause"),
            },
        )?;
        let error = self
            .store()?
            .transition_with_counts_as(
                &stale,
                &self.actor,
                crate::OperationTransition {
                    phase: StoreOperationPhase::Executing,
                    status: StoreOperationStatus::Cancelling,
                    cursor: stale.cursor.clone(),
                    event_kind: OperationEventKind::CancellationRequested,
                    counts: serde_json::json!({}),
                    message: Some("quint stale cancellation"),
                },
            )
            .expect_err("stale cancellation must fail its durable CAS");
        let (current, next_audit_length) = self.current()?;
        if matches!(error, OperationStoreError::StaleOperationState { .. })
            && current.status == StoreOperationStatus::Paused
            && next_audit_length == audit_length + 1
        {
            Ok(OperationResult::StaleRejected)
        } else {
            Err(anyhow::anyhow!("stale cancellation changed durable state"))
        }
    }

    fn observe(&self) -> Result<OperationResult> {
        let _ = self.durable_state()?;
        Ok(OperationResult::Observed)
    }

    fn store(&self) -> Result<&OperationStore> {
        self.store
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("operation MBT store is not initialized"))
    }

    fn current(&self) -> Result<(crate::StoredOperation, usize)> {
        let operation = self.store()?.show_operation(&self.operation_id)?;
        let audit_length = self.store()?.audit_events(&self.operation_id)?.len();
        Ok((operation, audit_length))
    }

    fn durable_state(&self) -> Result<DurableOperationState> {
        let (operation, audit_length) = match self.current() {
            Ok(value) => value,
            Err(error) => {
                if matches!(
                    error.downcast_ref::<OperationStoreError>(),
                    Some(OperationStoreError::OperationNotFound(_))
                ) {
                    return Ok(empty_durable_operation(self.node_alive, self.restart_count));
                }
                return Err(error);
            }
        };
        let progress: OperationProgress = serde_json::from_value(operation.progress.clone())?;
        Ok(DurableOperationState {
            exists: true,
            operation_identity: i64::from(
                operation.operation_id.as_str() == self.operation_id.as_str(),
            ),
            actor_authorized: operation.actor == self.actor,
            phase: model_phase(operation.phase),
            status: model_status(operation.status),
            cancellation_state: model_cancellation(operation.cancellation_state),
            cursor_present: operation.cursor.is_some(),
            cursor_position: operation
                .cursor
                .as_ref()
                .map_or(0, |cursor| u64_to_i64(cursor.position)),
            cursor_label: operation
                .cursor
                .as_ref()
                .map_or_else(String::new, |cursor| cursor.label.clone()),
            progress_rows: u64_to_i64(progress.rows),
            progress_retries: u64_to_i64(progress.retries),
            revision: u64_to_i64(operation.revision),
            audit_length: u64_to_i64(audit_length as u64),
            node_alive: self.node_alive,
            restart_count: u64_to_i64(self.restart_count),
        })
    }

    fn transition(
        &self,
        expected: &crate::StoredOperation,
        transition: crate::OperationTransition<'_>,
    ) -> Result<()> {
        self.store()?
            .transition_with_counts_as(expected, &self.actor, transition)?;
        Ok(())
    }

    fn expect_status(
        &self,
        phase: StoreOperationPhase,
        status: StoreOperationStatus,
        audit_length: usize,
        result: OperationResult,
    ) -> Result<OperationResult> {
        let (operation, actual_audit_length) = self.current()?;
        if operation.phase == phase
            && operation.status == status
            && actual_audit_length == audit_length
        {
            Ok(result)
        } else {
            Err(anyhow::anyhow!("unexpected operation lifecycle poststate"))
        }
    }

    fn expect_after_audit(
        &self,
        status: StoreOperationStatus,
        audit_length: usize,
        result: OperationResult,
    ) -> Result<OperationResult> {
        let (operation, actual_audit_length) = self.current()?;
        if operation.status == status && actual_audit_length == audit_length {
            Ok(result)
        } else {
            Err(anyhow::anyhow!("unexpected operation poststate"))
        }
    }
}

fn empty_durable_operation(node_alive: bool, restart_count: u64) -> DurableOperationState {
    DurableOperationState {
        exists: false,
        operation_identity: 0,
        actor_authorized: false,
        phase: ModelOperationPhase::SubmittedPhase,
        status: ModelOperationStatus::Submitted,
        cancellation_state: ModelCancellationState::NotRequested,
        cursor_present: false,
        cursor_position: 0,
        cursor_label: String::new(),
        progress_rows: 0,
        progress_retries: 0,
        revision: 0,
        audit_length: 0,
        node_alive,
        restart_count: u64_to_i64(restart_count),
    }
}

fn operation_request(operation_id: OperationId, actor: OperationActor) -> OperationRequest {
    OperationRequest {
        operation_id,
        kind: OperationKind::Backfill,
        actor,
        target_tables: vec!["users".to_string()],
        dry_run: true,
        rate_limit: RateLimitPolicy::default(),
        payload: serde_json::json!({ "mode": "quint-mbt" }),
    }
}

fn model_phase(phase: StoreOperationPhase) -> ModelOperationPhase {
    match phase {
        StoreOperationPhase::Submitted => ModelOperationPhase::SubmittedPhase,
        StoreOperationPhase::Planning => ModelOperationPhase::Planning,
        StoreOperationPhase::Executing => ModelOperationPhase::Executing,
        StoreOperationPhase::Validating => ModelOperationPhase::Validating,
        StoreOperationPhase::Completed => ModelOperationPhase::Completed,
    }
}

fn model_status(status: StoreOperationStatus) -> ModelOperationStatus {
    match status {
        StoreOperationStatus::Submitted => ModelOperationStatus::Submitted,
        StoreOperationStatus::Planned => ModelOperationStatus::Planned,
        StoreOperationStatus::Running => ModelOperationStatus::Running,
        StoreOperationStatus::Paused => ModelOperationStatus::Paused,
        StoreOperationStatus::Cancelling => ModelOperationStatus::Cancelling,
        StoreOperationStatus::Cancelled => ModelOperationStatus::Cancelled,
        StoreOperationStatus::Failed => ModelOperationStatus::Failed,
        StoreOperationStatus::Succeeded => ModelOperationStatus::Succeeded,
    }
}

fn model_cancellation(state: StoreCancellationState) -> ModelCancellationState {
    match state {
        StoreCancellationState::NotRequested => ModelCancellationState::NotRequested,
        StoreCancellationState::Requested => ModelCancellationState::Requested,
        StoreCancellationState::Accepted => ModelCancellationState::Accepted,
    }
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod hostile_trace_tests {
    use super::*;

    fn running_driver() -> OperationMbtDriver {
        let mut driver = OperationMbtDriver::default();
        driver.initialize().expect("initialize operation store");
        assert_eq!(
            driver.create().expect("create operation"),
            OperationResult::Submitted
        );
        assert_eq!(
            driver.plan().expect("plan operation"),
            OperationResult::Planned
        );
        assert_eq!(
            driver.start().expect("start operation"),
            OperationResult::Running
        );
        driver
    }

    #[test]
    fn given_crashed_operation_when_retried_then_retry_progress_is_durable() {
        let mut driver = running_driver();
        assert_eq!(
            driver.crash().expect("crash operation"),
            OperationResult::Crashed
        );
        assert_eq!(
            driver.retry().expect("retry operation"),
            OperationResult::Retried
        );
        let state = driver.durable_state().expect("read retried operation");
        assert_eq!(state.progress_retries, 1);
        assert_eq!(state.revision, 3);
        assert_eq!(state.audit_length, 4);
        assert!(state.node_alive);
        assert_eq!(state.restart_count, 1);
    }

    #[test]
    fn given_reused_driver_when_trace_initializes_then_operation_store_is_fresh() {
        let mut driver = OperationMbtDriver::default();
        driver
            .initialize()
            .expect("initialize first operation trace");
        assert_eq!(
            driver.create().expect("create first operation"),
            OperationResult::Submitted
        );

        driver
            .initialize()
            .expect("initialize second operation trace");
        let state = driver.durable_state().expect("read fresh operation trace");
        assert!(!state.exists);
        assert_eq!(state.operation_identity, 0);
        assert_eq!(state.audit_length, 0);
        assert_eq!(state.revision, 0);
    }

    fn running_operation() -> (OperationStore, OperationRequest) {
        let store = OperationStore::connect_in_memory().expect("in-memory store");
        let operation_id = OperationId::try_new("hostile-operation").expect("operation id");
        let actor = OperationActor::try_new("hostile-actor").expect("actor");
        let request = operation_request(operation_id, actor.clone());
        store.create_operation(&request).expect("create operation");
        store
            .prepare_for_execution(&request.operation_id, &actor)
            .expect("start operation");
        (store, request)
    }

    #[test]
    fn given_paused_operation_when_stale_resume_follows_current_resume_then_cas_rejects() {
        let (store, request) = running_operation();
        OperationStoreTestFixture::new()
            .transition(
                &store,
                &request.operation_id,
                crate::OperationTransition {
                    phase: StoreOperationPhase::Executing,
                    status: StoreOperationStatus::Paused,
                    cursor: None,
                    event_kind: OperationEventKind::Paused,
                    counts: serde_json::json!({}),
                    message: Some("pause before stale resume"),
                },
            )
            .expect("pause operation");
        let stale = store
            .show_operation(&request.operation_id)
            .expect("read stale snapshot");
        store
            .resume_operation(&request.operation_id, &request.actor)
            .expect("current resume");
        let error = store
            .transition_with_counts_as(
                &stale,
                &request.actor,
                crate::OperationTransition {
                    phase: StoreOperationPhase::Executing,
                    status: StoreOperationStatus::Running,
                    cursor: stale.cursor.clone(),
                    event_kind: OperationEventKind::Resumed,
                    counts: serde_json::json!({}),
                    message: Some("stale resume"),
                },
            )
            .expect_err("stale resume must fail");
        assert!(matches!(
            error,
            OperationStoreError::StaleOperationState { .. }
        ));
        let current = store
            .show_operation(&request.operation_id)
            .expect("read resumed operation");
        assert_eq!(current.status, StoreOperationStatus::Running);
        assert_eq!(store.audit_events(&request.operation_id).unwrap().len(), 5);
    }

    #[test]
    fn given_running_operation_when_stale_cancel_follows_current_pause_then_cas_rejects() {
        let (store, request) = running_operation();
        let stale = store
            .show_operation(&request.operation_id)
            .expect("read stale snapshot");
        OperationStoreTestFixture::new()
            .transition(
                &store,
                &request.operation_id,
                crate::OperationTransition {
                    phase: StoreOperationPhase::Executing,
                    status: StoreOperationStatus::Paused,
                    cursor: None,
                    event_kind: OperationEventKind::Paused,
                    counts: serde_json::json!({}),
                    message: Some("pause before stale cancellation"),
                },
            )
            .expect("current pause");
        let error = store
            .transition_with_counts_as(
                &stale,
                &request.actor,
                crate::OperationTransition {
                    phase: StoreOperationPhase::Executing,
                    status: StoreOperationStatus::Cancelling,
                    cursor: stale.cursor.clone(),
                    event_kind: OperationEventKind::CancellationRequested,
                    counts: serde_json::json!({}),
                    message: Some("stale cancellation"),
                },
            )
            .expect_err("stale cancellation must fail");
        assert!(matches!(
            error,
            OperationStoreError::StaleOperationState { .. }
        ));
        let current = store
            .show_operation(&request.operation_id)
            .expect("read paused operation");
        assert_eq!(current.status, StoreOperationStatus::Paused);
        assert_eq!(store.audit_events(&request.operation_id).unwrap().len(), 4);
    }
}
