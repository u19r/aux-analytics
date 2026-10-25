use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::RateLimitPolicy;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("operation rate limit denied; retry after {wait_ms} ms")]
pub struct RateLimitDenied {
    pub wait_ms: u64,
}

pub trait RateLimitClock {
    fn minute_utc(&self) -> u16;

    fn now_ms(&self) -> u64 {
        u64::from(self.minute_utc()) * 60_000
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemRateLimitClock;

impl RateLimitClock for SystemRateLimitClock {
    fn minute_utc(&self) -> u16 {
        let millis = self.now_ms();
        u16::try_from((millis / 1_000) % 86_400 / 60).unwrap_or(0)
    }

    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            })
    }
}

pub fn enforce_rate_limit<C: RateLimitClock>(
    policy: &RateLimitPolicy,
    usage: RateLimitUsage,
    clock: &C,
) -> Result<(), RateLimitDenied> {
    let decision = OperationRateLimiter::new(policy.clone()).reserve_at_ms(usage, clock.now_ms());
    if decision.allowed {
        Ok(())
    } else {
        Err(RateLimitDenied {
            wait_ms: decision.wait_ms,
        })
    }
}

/// Reserve usage against a limiter shared by one operation. Unlike the
/// one-shot compatibility helper above, this keeps a cumulative bucket across
/// source and destination calls in the same execution.
pub fn enforce_rate_limit_with_limiter<C: RateLimitClock>(
    limiter: &OperationRateLimiter,
    usage: RateLimitUsage,
    clock: &C,
) -> Result<(), RateLimitDenied> {
    let decision = limiter.reserve_at_ms(usage, clock.now_ms());
    if decision.allowed {
        Ok(())
    } else {
        Err(RateLimitDenied {
            wait_ms: decision.wait_ms,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitUsage {
    pub rows: u64,
    pub bytes: u64,
    pub source_requests: u64,
    pub destination_writes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitDecision {
    pub allowed: bool,
    pub wait_ms: u64,
}

#[derive(Debug, Clone)]
pub struct OperationRateLimiter {
    policy: RateLimitPolicy,
    state: Arc<Mutex<RateLimitState>>,
}

#[derive(Debug, Default)]
struct RateLimitState {
    bucket_start_ms: Option<u64>,
    usage: RateLimitUsage,
}

impl OperationRateLimiter {
    #[must_use]
    pub fn new(policy: RateLimitPolicy) -> Self {
        Self {
            policy,
            state: Arc::new(Mutex::new(RateLimitState::default())),
        }
    }

    #[must_use]
    pub fn reserve(&self, usage: RateLimitUsage) -> RateLimitDecision {
        self.reserve_at_ms(usage, system_now_ms())
    }

    #[must_use]
    pub fn reserve_at_minute(&self, usage: RateLimitUsage, minute_utc: u16) -> RateLimitDecision {
        self.reserve_at(usage, u64::from(minute_utc) * 60_000, minute_utc)
    }

    #[must_use]
    pub fn reserve_at_ms(&self, usage: RateLimitUsage, now_ms: u64) -> RateLimitDecision {
        let minute_utc = ((now_ms / 1_000) % 86_400 / 60) as u16;
        self.reserve_at(usage, now_ms, minute_utc)
    }

    fn reserve_at(&self, usage: RateLimitUsage, now_ms: u64, minute_utc: u16) -> RateLimitDecision {
        if let Some(wait_ms) =
            wait_for_maintenance_window(minute_utc, self.policy.maintenance_windows_utc.as_slice())
        {
            return RateLimitDecision {
                allowed: false,
                wait_ms,
            };
        }
        if has_zero_capacity(usage, &self.policy) {
            return RateLimitDecision {
                allowed: false,
                wait_ms: 0,
            };
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Use fixed epoch-aligned buckets so the same logical second always
        // has the same budget, regardless of which call first touches it.
        // This also makes injected clocks deterministic at exact boundaries.
        let bucket_start_ms = (now_ms / 1_000).saturating_mul(1_000);
        if state.bucket_start_ms != Some(bucket_start_ms) {
            state.bucket_start_ms = Some(bucket_start_ms);
            state.usage = RateLimitUsage::default();
        }
        let cumulative = state.usage.saturating_add(usage);
        let wait_ms = wait_for_limit(cumulative.rows, self.policy.max_rows_per_second)
            .max(wait_for_limit(
                cumulative.bytes,
                self.policy.max_bytes_per_second,
            ))
            .max(wait_for_limit(
                cumulative.source_requests,
                self.policy.max_source_requests_per_second,
            ))
            .max(wait_for_limit(
                cumulative.destination_writes,
                self.policy.max_destination_writes_per_second,
            ));
        if wait_ms == 0 {
            state.usage = cumulative;
        }
        RateLimitDecision {
            allowed: wait_ms == 0,
            wait_ms,
        }
    }

    #[must_use]
    pub fn retry_backoff_ms(&self, retry_count: u64) -> u64 {
        self.policy
            .retry_backoff_ms
            .saturating_mul(retry_count.max(1))
    }
}

impl RateLimitUsage {
    fn saturating_add(self, other: Self) -> Self {
        Self {
            rows: self.rows.saturating_add(other.rows),
            bytes: self.bytes.saturating_add(other.bytes),
            source_requests: self.source_requests.saturating_add(other.source_requests),
            destination_writes: self
                .destination_writes
                .saturating_add(other.destination_writes),
        }
    }
}

fn system_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn has_zero_capacity(usage: RateLimitUsage, policy: &RateLimitPolicy) -> bool {
    (usage.rows > 0 && policy.max_rows_per_second == Some(0))
        || (usage.bytes > 0 && policy.max_bytes_per_second == Some(0))
        || (usage.source_requests > 0 && policy.max_source_requests_per_second == Some(0))
        || (usage.destination_writes > 0 && policy.max_destination_writes_per_second == Some(0))
}

fn wait_for_maintenance_window(
    minute_utc: u16,
    windows: &[crate::MaintenanceWindow],
) -> Option<u64> {
    if windows.is_empty()
        || windows
            .iter()
            .any(|window| window_contains(*window, minute_utc))
    {
        return None;
    }
    windows
        .iter()
        .map(|window| minutes_until_window(*window, minute_utc))
        .min()
        .map(|minutes| u64::from(minutes) * 60_000)
}

fn window_contains(window: crate::MaintenanceWindow, minute_utc: u16) -> bool {
    let start = window.start_minute_utc.min(1_439);
    let end = window.end_minute_utc.min(1_440);
    if start == end {
        return true;
    }
    if start < end {
        minute_utc >= start && minute_utc < end
    } else {
        minute_utc >= start || minute_utc < end
    }
}

fn minutes_until_window(window: crate::MaintenanceWindow, minute_utc: u16) -> u16 {
    let start = window.start_minute_utc.min(1_439);
    if minute_utc <= start {
        start - minute_utc
    } else {
        1_440 - minute_utc + start
    }
}

fn wait_for_limit(amount: u64, limit_per_second: Option<u64>) -> u64 {
    let Some(limit) = limit_per_second else {
        return 0;
    };
    if amount <= limit {
        return 0;
    }
    amount.saturating_mul(1_000).div_ceil(limit)
}
