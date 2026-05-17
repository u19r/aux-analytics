use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{MaintenanceWindow, OperationRateLimiter, RateLimitPolicy, RateLimitUsage};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum RateLimitCase {
    WithinBudget,
    OverRowsBudget,
    RetryBackoff,
    OutsideMaintenanceWindow,
    LowerBudgetTakesEffect,
    CancelBeforeBatch,
    PauseBeforeBatch,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RateLimitReport {
    allowed: bool,
    wait_ms: i64,
    retry_delay_ms: i64,
    batch_may_start: bool,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RateLimitMbtState {
    last_case: RateLimitCase,
    last_report: RateLimitReport,
}

impl State<RateLimitDriver> for RateLimitMbtState {
    fn from_driver(driver: &RateLimitDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case,
            last_report: driver.last_report,
        })
    }
}

struct RateLimitDriver {
    last_case: RateLimitCase,
    last_report: RateLimitReport,
}

impl Default for RateLimitDriver {
    fn default() -> Self {
        Self {
            last_case: RateLimitCase::WithinBudget,
            last_report: RateLimitReport {
                allowed: true,
                wait_ms: 0,
                retry_delay_ms: 0,
                batch_may_start: true,
            },
        }
    }
}

impl Driver for RateLimitDriver {
    type State = RateLimitMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            RateLimitScenario(rateLimitCase: RateLimitCase) => {
                self.last_case = rateLimitCase;
                self.last_report = report_for(rateLimitCase);
            },
            step(rateLimitCase: RateLimitCase?) => {
                if let Some(rate_limit_case) = rateLimitCase {
                    self.last_case = rate_limit_case;
                    self.last_report = report_for(rate_limit_case);
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/rate_limit_mbt.qnt",
    max_samples = 50,
    max_steps = 4,
    seed = "0x711"
)]
fn quint_rate_limit_mbt_compares_full_state() -> impl Driver {
    RateLimitDriver::default()
}

fn report_for(rate_limit_case: RateLimitCase) -> RateLimitReport {
    match rate_limit_case {
        RateLimitCase::WithinBudget => limiter_report(
            RateLimitPolicy {
                max_rows_per_second: Some(100),
                ..RateLimitPolicy::default()
            },
            RateLimitUsage {
                rows: 100,
                ..RateLimitUsage::default()
            },
        ),
        RateLimitCase::OverRowsBudget => limiter_report(
            RateLimitPolicy {
                max_rows_per_second: Some(100),
                ..RateLimitPolicy::default()
            },
            RateLimitUsage {
                rows: 250,
                ..RateLimitUsage::default()
            },
        ),
        RateLimitCase::RetryBackoff => retry_report(),
        RateLimitCase::OutsideMaintenanceWindow => maintenance_window_report(),
        RateLimitCase::LowerBudgetTakesEffect => limiter_report(
            RateLimitPolicy {
                max_rows_per_second: Some(50),
                ..RateLimitPolicy::default()
            },
            RateLimitUsage {
                rows: 500,
                ..RateLimitUsage::default()
            },
        ),
        RateLimitCase::CancelBeforeBatch | RateLimitCase::PauseBeforeBatch => {
            boundary_report(false)
        }
    }
}

fn limiter_report(policy: RateLimitPolicy, usage: RateLimitUsage) -> RateLimitReport {
    let decision = OperationRateLimiter::new(policy).reserve(usage);
    RateLimitReport {
        allowed: decision.allowed,
        wait_ms: u64_to_i64(decision.wait_ms),
        retry_delay_ms: 0,
        batch_may_start: decision.allowed,
    }
}

fn retry_report() -> RateLimitReport {
    let retry_delay_ms = OperationRateLimiter::new(RateLimitPolicy {
        retry_backoff_ms: 250,
        ..RateLimitPolicy::default()
    })
    .retry_backoff_ms(3);
    RateLimitReport {
        allowed: true,
        wait_ms: 0,
        retry_delay_ms: u64_to_i64(retry_delay_ms),
        batch_may_start: true,
    }
}

fn maintenance_window_report() -> RateLimitReport {
    let decision = OperationRateLimiter::new(RateLimitPolicy {
        maintenance_windows_utc: vec![MaintenanceWindow {
            start_minute_utc: 60,
            end_minute_utc: 120,
        }],
        ..RateLimitPolicy::default()
    })
    .reserve_at_minute(RateLimitUsage::default(), 30);
    RateLimitReport {
        allowed: decision.allowed,
        wait_ms: u64_to_i64(decision.wait_ms),
        retry_delay_ms: 0,
        batch_may_start: decision.allowed,
    }
}

const fn boundary_report(batch_may_start: bool) -> RateLimitReport {
    RateLimitReport {
        allowed: true,
        wait_ms: 0,
        retry_delay_ms: 0,
        batch_may_start,
    }
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
