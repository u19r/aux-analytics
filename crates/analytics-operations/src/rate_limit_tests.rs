use crate::{MaintenanceWindow, OperationRateLimiter, RateLimitPolicy, RateLimitUsage};

#[test]
fn given_usage_within_budget_when_reserved_then_no_wait_is_required() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        ..RateLimitPolicy::default()
    });

    let decision = limiter.reserve(RateLimitUsage {
        rows: 100,
        ..RateLimitUsage::default()
    });

    assert!(decision.allowed);
    assert_eq!(decision.wait_ms, 0);
}

#[test]
fn given_usage_over_rows_budget_when_reserved_then_wait_covers_required_window() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        ..RateLimitPolicy::default()
    });

    let decision = limiter.reserve(RateLimitUsage {
        rows: 250,
        ..RateLimitUsage::default()
    });

    assert!(!decision.allowed);
    assert_eq!(decision.wait_ms, 2_500);
}

#[test]
fn given_multiple_limited_dimensions_when_reserved_then_slowest_budget_wins() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        max_destination_writes_per_second: Some(10),
        ..RateLimitPolicy::default()
    });

    let decision = limiter.reserve(RateLimitUsage {
        rows: 200,
        destination_writes: 50,
        ..RateLimitUsage::default()
    });

    assert_eq!(decision.wait_ms, 5_000);
}

#[test]
fn given_lower_budget_when_new_limiter_is_created_then_budget_takes_effect_without_state_reset() {
    let original = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(500),
        ..RateLimitPolicy::default()
    });
    let lowered = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(50),
        ..RateLimitPolicy::default()
    });
    let usage = RateLimitUsage {
        rows: 500,
        ..RateLimitUsage::default()
    };

    assert!(original.reserve(usage).allowed);

    let lowered_decision = lowered.reserve(usage);

    assert!(!lowered_decision.allowed);
    assert_eq!(lowered_decision.wait_ms, 10_000);
}

#[test]
fn given_retry_count_when_backoff_requested_then_backoff_scales_linearly() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        retry_backoff_ms: 250,
        ..RateLimitPolicy::default()
    });

    assert_eq!(limiter.retry_backoff_ms(0), 250);
    assert_eq!(limiter.retry_backoff_ms(3), 750);
}

#[test]
fn given_current_time_inside_maintenance_window_when_reserved_then_budget_is_evaluated() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        maintenance_windows_utc: vec![MaintenanceWindow {
            start_minute_utc: 60,
            end_minute_utc: 120,
        }],
        ..RateLimitPolicy::default()
    });

    let decision = limiter.reserve_at_minute(
        RateLimitUsage {
            rows: 50,
            ..RateLimitUsage::default()
        },
        90,
    );

    assert!(decision.allowed);
    assert_eq!(decision.wait_ms, 0);
}

#[test]
fn given_current_time_outside_maintenance_window_when_reserved_then_wait_until_next_window() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        maintenance_windows_utc: vec![MaintenanceWindow {
            start_minute_utc: 60,
            end_minute_utc: 120,
        }],
        ..RateLimitPolicy::default()
    });

    let decision = limiter.reserve_at_minute(RateLimitUsage::default(), 30);

    assert!(!decision.allowed);
    assert_eq!(decision.wait_ms, 30 * 60_000);
}

#[test]
fn given_wrapping_maintenance_window_when_reserved_after_midnight_then_it_is_allowed() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        maintenance_windows_utc: vec![MaintenanceWindow {
            start_minute_utc: 1_380,
            end_minute_utc: 120,
        }],
        ..RateLimitPolicy::default()
    });

    let decision = limiter.reserve_at_minute(RateLimitUsage::default(), 30);

    assert!(decision.allowed);
}
