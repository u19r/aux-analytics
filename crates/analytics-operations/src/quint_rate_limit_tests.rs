use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    BackfillExecutionProgress, CancellationState as StoreCancellationState, MaintenanceWindow,
    OperationActor, OperationEventKind, OperationId, OperationKind, OperationPhase,
    OperationRateLimiter, OperationRequest, OperationStatus, OperationStore, RateLimitClock,
    RateLimitPolicy, RateLimitUsage, enforce_rate_limit,
};

const WITHIN_BATCH_ROWS: u64 = 25;
const OVER_BATCH_ROWS: u64 = 250;
const OVER_SOURCE_REQUESTS: u64 = 2;
const LOWER_BATCH_ROWS: u64 = 75;
const BATCH_SOURCE_REQUESTS: u64 = 1;
const RUNNING_MINUTE_UTC: u16 = 60;
const OUTSIDE_WINDOW_MINUTE_UTC: u16 = 30;
const BASE_ROWS_PER_SECOND: u64 = 100;
const BASE_SOURCE_REQUESTS_PER_SECOND: u64 = 1;
const LOWER_ROWS_PER_SECOND: u64 = 50;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum RateLimitCase {
    WithinBudget,
    OverRowsBudget,
    OverSourceRequestsBudget,
    RetryBackoff,
    OutsideMaintenanceWindow,
    LowerBudgetTakesEffect,
    CancelBeforeBatch,
    PauseBeforeBatch,
    ResumeAfterPause,
    AcceptCancellation,
    Observe,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RateLimitReport {
    allowed: bool,
    wait_ms: i64,
    retry_delay_ms: i64,
    batch_may_start: bool,
    policy_rows_limit: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelRateOperationStatus {
    Running,
    Paused,
    Cancelling,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelRateCancellationState {
    NotRequested,
    Requested,
    Accepted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DurableRateState {
    operation_identity: i64,
    actor_authorized: bool,
    phase_executing: bool,
    status: ModelRateOperationStatus,
    cancellation_state: ModelRateCancellationState,
    completed_batches: i64,
    rows_scanned: i64,
    retry_count: i64,
    revision: i64,
    audit_length: i64,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RateLimitMbtState {
    last_case: RateLimitCase,
    last_report: RateLimitReport,
    previous_operation: DurableRateState,
    operation: DurableRateState,
}

impl State<RateLimitDriver> for RateLimitMbtState {
    fn from_driver(driver: &RateLimitDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case,
            last_report: driver.last_report,
            previous_operation: driver
                .previous_operation
                .clone()
                .ok_or_else(|| anyhow::anyhow!("rate-limit MBT has no previous operation"))?,
            operation: driver.durable_state()?,
        })
    }
}

struct RateLimitDriver {
    store: Option<OperationStore>,
    operation_id: OperationId,
    lower_operation_id: OperationId,
    actor: OperationActor,
    request: OperationRequest,
    last_case: RateLimitCase,
    last_report: RateLimitReport,
    previous_operation: Option<DurableRateState>,
}

impl Default for RateLimitDriver {
    fn default() -> Self {
        let operation_id = OperationId::try_new("quint-rate-limit-mbt").expect("valid MBT id");
        let lower_operation_id =
            OperationId::try_new("quint-rate-limit-mbt-lower").expect("valid lower MBT id");
        let actor = OperationActor::try_new("rate-limit-mbt").expect("valid MBT actor");
        let request = operation_request(
            operation_id.clone(),
            actor.clone(),
            BASE_ROWS_PER_SECOND,
            BASE_SOURCE_REQUESTS_PER_SECOND,
        );
        Self {
            store: None,
            operation_id,
            lower_operation_id,
            actor,
            request,
            last_case: RateLimitCase::WithinBudget,
            last_report: RateLimitReport::default(),
            previous_operation: None,
        }
    }
}

impl Driver for RateLimitDriver {
    type State = RateLimitMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.initialize()?,
            RateLimitScenario(rateLimitCase: RateLimitCase) => self.apply(rateLimitCase)?,
            step(rateLimitCase: RateLimitCase?) => {
                if let Some(rate_limit_case) = rateLimitCase {
                    self.apply(rate_limit_case)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/rate_limit_mbt.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x711"
)]
fn given_quint_rate_limit_model_when_replayed_then_full_state_matches() -> impl Driver {
    RateLimitDriver::default()
}

impl RateLimitDriver {
    fn initialize(&mut self) -> Result<()> {
        self.store = Some(OperationStore::connect_in_memory()?);
        self.store()?.create_operation(&self.request)?;
        self.store()?
            .prepare_for_execution(&self.operation_id, &self.actor)?;
        self.previous_operation = Some(self.durable_state()?);
        self.last_case = RateLimitCase::Observe;
        self.last_report = self.report_for_operation(
            &self.operation_id,
            RateLimitUsage {
                rows: WITHIN_BATCH_ROWS,
                source_requests: BATCH_SOURCE_REQUESTS,
                ..RateLimitUsage::default()
            },
            RUNNING_MINUTE_UTC,
        )?;
        Ok(())
    }

    fn apply(&mut self, rate_limit_case: RateLimitCase) -> Result<()> {
        self.previous_operation = Some(self.durable_state()?);
        let report = match rate_limit_case {
            RateLimitCase::WithinBudget => self.within_budget()?,
            RateLimitCase::OverRowsBudget => self.over_rows_budget()?,
            RateLimitCase::OverSourceRequestsBudget => self.over_source_requests_budget()?,
            RateLimitCase::RetryBackoff => self.retry_backoff()?,
            RateLimitCase::OutsideMaintenanceWindow => self.outside_maintenance_window()?,
            RateLimitCase::LowerBudgetTakesEffect => self.lower_budget()?,
            RateLimitCase::CancelBeforeBatch => self.cancel_before_batch()?,
            RateLimitCase::PauseBeforeBatch => self.pause_before_batch()?,
            RateLimitCase::ResumeAfterPause => self.resume_after_pause()?,
            RateLimitCase::AcceptCancellation => self.accept_cancellation()?,
            RateLimitCase::Observe => self.observe()?,
        };
        self.last_case = rate_limit_case;
        self.last_report = report;
        Ok(())
    }

    fn within_budget(&self) -> Result<RateLimitReport> {
        let (operation, _) = self.current()?;
        let progress = self.progress(&operation)?;
        let usage = RateLimitUsage {
            rows: WITHIN_BATCH_ROWS,
            source_requests: BATCH_SOURCE_REQUESTS,
            ..RateLimitUsage::default()
        };
        let report = self.report_for_operation(&self.operation_id, usage, RUNNING_MINUTE_UTC)?;
        if report.batch_may_start {
            let mut next_progress = progress;
            next_progress.metrics.chunks_completed =
                next_progress.metrics.chunks_completed.saturating_add(1);
            next_progress.metrics.rows_scanned = next_progress
                .metrics
                .rows_scanned
                .saturating_add(WITHIN_BATCH_ROWS);
            next_progress.metrics.rows_written = next_progress
                .metrics
                .rows_written
                .saturating_add(WITHIN_BATCH_ROWS);
            self.persist_progress(&operation, next_progress, "record allowed rate-limit usage")?;
        }
        Ok(report)
    }

    fn over_rows_budget(&self) -> Result<RateLimitReport> {
        let usage = RateLimitUsage {
            rows: OVER_BATCH_ROWS,
            source_requests: BATCH_SOURCE_REQUESTS,
            ..RateLimitUsage::default()
        };
        self.report_for_operation(&self.operation_id, usage, RUNNING_MINUTE_UTC)
    }

    fn over_source_requests_budget(&self) -> Result<RateLimitReport> {
        let usage = RateLimitUsage {
            rows: WITHIN_BATCH_ROWS,
            source_requests: OVER_SOURCE_REQUESTS,
            ..RateLimitUsage::default()
        };
        self.report_for_operation(&self.operation_id, usage, RUNNING_MINUTE_UTC)
    }

    fn retry_backoff(&self) -> Result<RateLimitReport> {
        let (operation, _) = self.current()?;
        let progress = self.progress(&operation)?;
        let report = self.report_for_operation(
            &self.operation_id,
            RateLimitUsage {
                rows: WITHIN_BATCH_ROWS,
                source_requests: BATCH_SOURCE_REQUESTS,
                ..RateLimitUsage::default()
            },
            RUNNING_MINUTE_UTC,
        )?;
        let retry_count = progress.metrics.chunks_retried.saturating_add(1);
        let retry_delay_ms =
            OperationRateLimiter::new(operation.rate_limit.clone()).retry_backoff_ms(retry_count);
        if report.batch_may_start {
            let mut next_progress = progress;
            next_progress.metrics.chunks_retried = retry_count;
            next_progress.metrics.retry_delay_ms = next_progress
                .metrics
                .retry_delay_ms
                .saturating_add(retry_delay_ms);
            next_progress.metrics.throttle_pause_ms = next_progress
                .metrics
                .throttle_pause_ms
                .saturating_add(retry_delay_ms);
            self.persist_progress(&operation, next_progress, "record rate-limit retry")?;
        }
        Ok(RateLimitReport {
            retry_delay_ms: u64_to_i64(retry_delay_ms),
            ..report
        })
    }

    fn outside_maintenance_window(&self) -> Result<RateLimitReport> {
        let usage = RateLimitUsage {
            rows: WITHIN_BATCH_ROWS,
            source_requests: BATCH_SOURCE_REQUESTS,
            ..RateLimitUsage::default()
        };
        self.report_for_operation(&self.operation_id, usage, OUTSIDE_WINDOW_MINUTE_UTC)
    }

    fn lower_budget(&self) -> Result<RateLimitReport> {
        self.lower_operation()?;
        let usage = RateLimitUsage {
            rows: LOWER_BATCH_ROWS,
            source_requests: BATCH_SOURCE_REQUESTS,
            ..RateLimitUsage::default()
        };
        self.report_for_operation(&self.lower_operation_id, usage, RUNNING_MINUTE_UTC)
    }

    fn observe(&self) -> Result<RateLimitReport> {
        self.report_for_operation(
            &self.operation_id,
            RateLimitUsage {
                rows: WITHIN_BATCH_ROWS,
                source_requests: BATCH_SOURCE_REQUESTS,
                ..RateLimitUsage::default()
            },
            RUNNING_MINUTE_UTC,
        )
    }

    fn cancel_before_batch(&self) -> Result<RateLimitReport> {
        let (_operation, before_audit_length) = self.current()?;
        self.store()?
            .request_cancellation_as(&self.operation_id, &self.actor)?;
        let (current, after_audit_length) = self.current()?;
        if current.status != OperationStatus::Cancelling
            || current.cancellation_state != StoreCancellationState::Requested
            || after_audit_length != before_audit_length + 1
        {
            return Err(anyhow::anyhow!(
                "cancellation did not persist rate-limit state"
            ));
        }
        self.report_for_operation(
            &self.operation_id,
            RateLimitUsage {
                rows: WITHIN_BATCH_ROWS,
                source_requests: BATCH_SOURCE_REQUESTS,
                ..RateLimitUsage::default()
            },
            RUNNING_MINUTE_UTC,
        )
    }

    fn pause_before_batch(&self) -> Result<RateLimitReport> {
        let (operation, audit_length) = self.current()?;
        self.transition(
            &operation,
            OperationPhase::Executing,
            OperationStatus::Paused,
            OperationEventKind::Paused,
            "pause before rate-limited batch",
        )?;
        let (current, next_audit_length) = self.current()?;
        if current.status != OperationStatus::Paused || next_audit_length != audit_length + 1 {
            return Err(anyhow::anyhow!("pause did not persist rate-limit state"));
        }
        self.report_for_operation(
            &self.operation_id,
            RateLimitUsage {
                rows: WITHIN_BATCH_ROWS,
                source_requests: BATCH_SOURCE_REQUESTS,
                ..RateLimitUsage::default()
            },
            RUNNING_MINUTE_UTC,
        )
    }

    fn resume_after_pause(&self) -> Result<RateLimitReport> {
        let (operation, _) = self.current()?;
        self.store()?
            .resume_operation(&self.operation_id, &self.actor)?;
        let (current, _) = self.current()?;
        if operation.status != OperationStatus::Paused || current.status != OperationStatus::Running
        {
            return Err(anyhow::anyhow!("resume did not persist rate-limit state"));
        }
        self.report_for_operation(
            &self.operation_id,
            RateLimitUsage {
                rows: WITHIN_BATCH_ROWS,
                source_requests: BATCH_SOURCE_REQUESTS,
                ..RateLimitUsage::default()
            },
            RUNNING_MINUTE_UTC,
        )
    }

    fn accept_cancellation(&self) -> Result<RateLimitReport> {
        let (operation, audit_length) = self.current()?;
        self.store()?
            .accept_cancellation_as(&self.operation_id, &self.actor)?;
        let (current, next_audit_length) = self.current()?;
        if operation.status != OperationStatus::Cancelling
            || current.status != OperationStatus::Cancelled
            || current.cancellation_state != StoreCancellationState::Accepted
            || next_audit_length != audit_length + 1
        {
            return Err(anyhow::anyhow!(
                "cancellation acceptance did not persist rate-limit state"
            ));
        }
        self.report_for_operation(
            &self.operation_id,
            RateLimitUsage {
                rows: WITHIN_BATCH_ROWS,
                source_requests: BATCH_SOURCE_REQUESTS,
                ..RateLimitUsage::default()
            },
            RUNNING_MINUTE_UTC,
        )
    }

    fn store(&self) -> Result<&OperationStore> {
        self.store
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("rate-limit MBT store is not initialized"))
    }

    fn current(&self) -> Result<(crate::StoredOperation, usize)> {
        let operation = self.store()?.show_operation(&self.operation_id)?;
        let audit_length = self.store()?.audit_events(&self.operation_id)?.len();
        Ok((operation, audit_length))
    }

    fn progress(&self, operation: &crate::StoredOperation) -> Result<BackfillExecutionProgress> {
        Ok(serde_json::from_value(operation.progress.clone())?)
    }

    fn report_for_operation(
        &self,
        operation_id: &OperationId,
        usage: RateLimitUsage,
        minute_utc: u16,
    ) -> Result<RateLimitReport> {
        let operation = self.store()?.show_operation(operation_id)?;
        let decision = self.enforced_decision(&operation, usage, minute_utc);
        Ok(report_from_decision(
            decision,
            operation.status,
            operation.rate_limit.max_rows_per_second,
        ))
    }

    fn enforced_decision(
        &self,
        operation: &crate::StoredOperation,
        usage: RateLimitUsage,
        minute_utc: u16,
    ) -> crate::RateLimitDecision {
        let clock = FixedRateLimitClock { minute_utc };
        match enforce_rate_limit(&operation.rate_limit, usage, &clock) {
            Ok(()) => crate::RateLimitDecision {
                allowed: true,
                wait_ms: 0,
            },
            Err(error) => crate::RateLimitDecision {
                allowed: false,
                wait_ms: error.wait_ms,
            },
        }
    }

    fn lower_operation(&self) -> Result<crate::StoredOperation> {
        let request = operation_request(
            self.lower_operation_id.clone(),
            self.actor.clone(),
            LOWER_ROWS_PER_SECOND,
            BATCH_SOURCE_REQUESTS,
        );
        self.store()?.create_or_verify_operation(&request)?;
        let operation = self.store()?.show_operation(&self.lower_operation_id)?;
        if operation.status != OperationStatus::Running {
            self.store()?
                .prepare_for_execution(&self.lower_operation_id, &self.actor)?;
            return Ok(self.store()?.show_operation(&self.lower_operation_id)?);
        }
        Ok(operation)
    }

    fn persist_progress(
        &self,
        operation: &crate::StoredOperation,
        progress: BackfillExecutionProgress,
        message: &str,
    ) -> Result<()> {
        self.store()?.transition_with_counts_as(
            operation,
            &self.actor,
            crate::OperationTransition {
                phase: OperationPhase::Executing,
                status: OperationStatus::Running,
                cursor: operation.cursor.clone(),
                event_kind: OperationEventKind::CursorAdvanced,
                counts: serde_json::to_value(progress)?,
                message: Some(message),
            },
        )?;
        Ok(())
    }

    fn transition(
        &self,
        operation: &crate::StoredOperation,
        phase: OperationPhase,
        status: OperationStatus,
        event_kind: OperationEventKind,
        message: &str,
    ) -> Result<()> {
        self.store()?.transition_with_counts_as(
            operation,
            &self.actor,
            crate::OperationTransition {
                phase,
                status,
                cursor: operation.cursor.clone(),
                event_kind,
                counts: serde_json::json!({}),
                message: Some(message),
            },
        )?;
        Ok(())
    }

    fn durable_state(&self) -> Result<DurableRateState> {
        let (operation, audit_length) = self.current()?;
        let progress = self.progress(&operation)?;
        Ok(DurableRateState {
            operation_identity: i64::from(
                operation.operation_id.as_str() == self.operation_id.as_str(),
            ),
            actor_authorized: operation.actor == self.actor,
            phase_executing: operation.phase == OperationPhase::Executing,
            status: model_status(operation.status),
            cancellation_state: model_cancellation(operation.cancellation_state),
            completed_batches: u64_to_i64(progress.metrics.chunks_completed),
            rows_scanned: u64_to_i64(progress.metrics.rows_scanned),
            retry_count: u64_to_i64(progress.metrics.chunks_retried),
            revision: u64_to_i64(operation.revision),
            audit_length: u64_to_i64(audit_length as u64),
        })
    }
}

#[derive(Clone, Copy)]
struct FixedRateLimitClock {
    minute_utc: u16,
}

impl RateLimitClock for FixedRateLimitClock {
    fn minute_utc(&self) -> u16 {
        self.minute_utc
    }
}

fn operation_request(
    operation_id: OperationId,
    actor: OperationActor,
    max_rows_per_second: u64,
    max_source_requests_per_second: u64,
) -> OperationRequest {
    OperationRequest {
        operation_id,
        kind: OperationKind::Backfill,
        actor,
        target_tables: vec!["users".to_string()],
        dry_run: true,
        rate_limit: RateLimitPolicy {
            max_rows_per_second: Some(max_rows_per_second),
            max_bytes_per_second: None,
            max_source_requests_per_second: Some(max_source_requests_per_second),
            max_destination_writes_per_second: None,
            max_parallel_chunks: 1,
            batch_size: 25,
            pause_ms: 100,
            retry_backoff_ms: 1_000,
            maintenance_windows_utc: vec![MaintenanceWindow {
                start_minute_utc: 60,
                end_minute_utc: 120,
            }],
        },
        payload: serde_json::json!({ "source": "quint-rate-limit-mbt" }),
    }
}

fn report_from_decision(
    decision: crate::RateLimitDecision,
    operation_status: OperationStatus,
    policy_rows_limit: Option<u64>,
) -> RateLimitReport {
    RateLimitReport {
        allowed: decision.allowed,
        wait_ms: u64_to_i64(decision.wait_ms),
        retry_delay_ms: 0,
        batch_may_start: operation_status == OperationStatus::Running && decision.allowed,
        policy_rows_limit: policy_rows_limit.map_or(0, u64_to_i64),
    }
}

fn model_status(status: OperationStatus) -> ModelRateOperationStatus {
    match status {
        OperationStatus::Running => ModelRateOperationStatus::Running,
        OperationStatus::Paused => ModelRateOperationStatus::Paused,
        OperationStatus::Cancelling => ModelRateOperationStatus::Cancelling,
        OperationStatus::Cancelled => ModelRateOperationStatus::Cancelled,
        other => panic!("unexpected rate-limit MBT operation status: {other:?}"),
    }
}

fn model_cancellation(state: StoreCancellationState) -> ModelRateCancellationState {
    match state {
        StoreCancellationState::NotRequested => ModelRateCancellationState::NotRequested,
        StoreCancellationState::Requested => ModelRateCancellationState::Requested,
        StoreCancellationState::Accepted => ModelRateCancellationState::Accepted,
    }
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod hostile_trace_tests {
    use super::*;

    fn driver_running() -> RateLimitDriver {
        let mut driver = RateLimitDriver::default();
        driver
            .initialize()
            .expect("initialize rate-limit operation");
        driver
    }

    #[test]
    fn given_cumulative_usage_when_lower_policy_uses_separate_operation_then_usage_is_retained() {
        let driver = driver_running();
        for _ in 0..3 {
            driver.within_budget().expect("allow cumulative request");
        }
        let (before, before_audit_length) = driver.current().expect("read retained usage");
        let before_progress = driver.progress(&before).expect("decode retained usage");
        assert_eq!(before_progress.metrics.chunks_completed, 3);
        assert_eq!(before_progress.metrics.rows_scanned, 75);

        let report = driver
            .lower_budget()
            .expect("evaluate lower-policy request");
        assert!(!report.allowed);
        assert_eq!(report.wait_ms, 1_500);
        assert_eq!(report.policy_rows_limit, 50);
        let (after, audit_length) = driver.current().expect("read lowered row");
        let after_progress = driver.progress(&after).expect("decode retained progress");
        assert_eq!(after_progress, before_progress);
        assert_eq!(after.rate_limit, before.rate_limit);
        assert_eq!(after.revision, before.revision);
        assert_eq!(audit_length, before_audit_length);

        let lower = driver
            .lower_operation()
            .expect("read separately persisted lower-policy request");
        assert_eq!(
            lower.rate_limit.max_rows_per_second,
            Some(LOWER_ROWS_PER_SECOND)
        );
        assert_eq!(lower.revision, 2);
        assert_eq!(
            driver
                .progress(&lower)
                .expect("decode lower-policy progress")
                .metrics
                .chunks_completed,
            0
        );
    }

    #[test]
    fn given_persisted_source_request_limit_when_reserved_then_operation_is_unchanged() {
        let driver = driver_running();
        let (before, before_audit_length) = driver.current().expect("read persisted policy");
        assert_eq!(
            before.rate_limit.max_source_requests_per_second,
            Some(BASE_SOURCE_REQUESTS_PER_SECOND)
        );

        let report = driver
            .over_source_requests_budget()
            .expect("evaluate source-request budget");
        assert!(!report.allowed);
        assert_eq!(report.wait_ms, 2_000);
        assert!(!report.batch_may_start);
        assert_eq!(report.policy_rows_limit, BASE_ROWS_PER_SECOND as i64);

        let (after, after_audit_length) = driver.current().expect("read unchanged operation");
        assert_eq!(after.rate_limit, before.rate_limit);
        assert_eq!(after.progress, before.progress);
        assert_eq!(after.revision, before.revision);
        assert_eq!(after_audit_length, before_audit_length);
    }

    #[test]
    fn given_retry_progression_when_reserved_then_usage_is_retained_and_delay_accumulates() {
        let driver = driver_running();
        driver.within_budget().expect("record completed batch");
        let (before, before_audit_length) = driver.current().expect("read completed batch");
        let before_progress = driver.progress(&before).expect("decode completed batch");

        let first = driver.retry_backoff().expect("record first retry");
        assert_eq!(first.retry_delay_ms, 1_000);
        let (after_first, first_audit_length) = driver.current().expect("read first retry");
        let first_progress = driver.progress(&after_first).expect("decode first retry");
        assert_eq!(first_progress.metrics.chunks_completed, 1);
        assert_eq!(first_progress.metrics.rows_scanned, 25);
        assert_eq!(first_progress.metrics.chunks_retried, 1);
        assert_eq!(first_progress.metrics.retry_delay_ms, 1_000);
        assert_eq!(after_first.revision, before.revision + 1);
        assert_eq!(first_audit_length, before_audit_length + 1);

        let second = driver.retry_backoff().expect("record second retry");
        assert_eq!(second.retry_delay_ms, 2_000);
        let (after_second, _) = driver.current().expect("read second retry");
        let second_progress = driver.progress(&after_second).expect("decode second retry");
        assert_eq!(second_progress.metrics.chunks_completed, 1);
        assert_eq!(second_progress.metrics.rows_scanned, 25);
        assert_eq!(second_progress.metrics.chunks_retried, 2);
        assert_eq!(second_progress.metrics.retry_delay_ms, 3_000);
        assert_eq!(second_progress.metrics.throttle_pause_ms, 3_000);
        assert_eq!(
            second_progress.metrics.chunks_completed,
            before_progress.metrics.chunks_completed
        );
        assert_eq!(
            second_progress.metrics.rows_scanned,
            before_progress.metrics.rows_scanned
        );
    }

    #[test]
    fn given_reused_driver_when_trace_initializes_then_rate_state_is_fresh() {
        let mut driver = RateLimitDriver::default();
        driver
            .initialize()
            .expect("initialize first rate-limit trace");
        driver.within_budget().expect("record first trace usage");
        let first = driver.durable_state().expect("read first trace state");
        assert_eq!(first.completed_batches, 1);
        assert_eq!(first.rows_scanned, 25);

        driver
            .initialize()
            .expect("initialize second rate-limit trace");
        let second = driver.durable_state().expect("read fresh rate-limit state");
        assert_eq!(second.operation_identity, 1);
        assert!(second.actor_authorized);
        assert_eq!(second.completed_batches, 0);
        assert_eq!(second.rows_scanned, 0);
        assert_eq!(second.retry_count, 0);
        assert_eq!(second.revision, 2);
        assert_eq!(second.audit_length, 3);
        assert_eq!(second.status, ModelRateOperationStatus::Running);
        assert_eq!(
            second.cancellation_state,
            ModelRateCancellationState::NotRequested
        );
    }

    #[test]
    fn given_running_operation_when_paused_resumed_and_cancelled_then_identity_is_retained() {
        let driver = driver_running();
        let (before, _) = driver.current().expect("read running operation");
        driver.pause_before_batch().expect("pause operation");
        driver.resume_after_pause().expect("resume operation");
        let (resumed, _) = driver.current().expect("read resumed operation");
        assert_eq!(resumed.operation_id, before.operation_id);
        driver.cancel_before_batch().expect("request cancellation");
        driver.accept_cancellation().expect("accept cancellation");
        let (cancelled, _) = driver.current().expect("read cancelled operation");
        assert_eq!(cancelled.operation_id, before.operation_id);
        assert_eq!(cancelled.status, OperationStatus::Cancelled);
        assert_eq!(
            cancelled.cancellation_state,
            StoreCancellationState::Accepted
        );
    }
}
