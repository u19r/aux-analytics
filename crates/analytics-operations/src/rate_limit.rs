use serde::{Deserialize, Serialize};

use crate::RateLimitPolicy;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRateLimiter {
    policy: RateLimitPolicy,
}

impl OperationRateLimiter {
    #[must_use]
    pub fn new(policy: RateLimitPolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn reserve(&self, usage: RateLimitUsage) -> RateLimitDecision {
        self.reserve_budget(usage)
    }

    #[must_use]
    pub fn reserve_at_minute(&self, usage: RateLimitUsage, minute_utc: u16) -> RateLimitDecision {
        if let Some(wait_ms) =
            wait_for_maintenance_window(minute_utc, self.policy.maintenance_windows_utc.as_slice())
        {
            return RateLimitDecision {
                allowed: false,
                wait_ms,
            };
        }
        self.reserve_budget(usage)
    }

    fn reserve_budget(&self, usage: RateLimitUsage) -> RateLimitDecision {
        let wait_ms = [
            wait_for_limit(usage.rows, self.policy.max_rows_per_second),
            wait_for_limit(usage.bytes, self.policy.max_bytes_per_second),
            wait_for_limit(
                usage.source_requests,
                self.policy.max_source_requests_per_second,
            ),
            wait_for_limit(
                usage.destination_writes,
                self.policy.max_destination_writes_per_second,
            ),
        ]
        .into_iter()
        .max()
        .unwrap_or(0);
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
    if limit == 0 || amount <= limit {
        return 0;
    }
    amount.saturating_mul(1_000).div_ceil(limit)
}
