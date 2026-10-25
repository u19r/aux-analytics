use crate::{
    MaintenanceWindow, OperationRateLimiter, RateLimitClock, RateLimitPolicy, RateLimitUsage,
    enforce_rate_limit_with_limiter,
};

#[test]
fn given_row_usage_at_capacity_when_reserved_then_work_is_allowed_without_wait() {
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
fn given_zero_row_capacity_when_batch_has_rows_then_batch_is_denied() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(0),
        ..RateLimitPolicy::default()
    });

    let decision = limiter.reserve(RateLimitUsage {
        rows: 1,
        ..RateLimitUsage::default()
    });

    assert!(!decision.allowed);
    assert_eq!(decision.wait_ms, 0);
}

#[test]
fn given_all_limited_dimensions_when_reserved_then_slowest_budget_wins() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        max_bytes_per_second: Some(1_000),
        max_source_requests_per_second: Some(20),
        max_destination_writes_per_second: Some(10),
        ..RateLimitPolicy::default()
    });

    let decision = limiter.reserve(RateLimitUsage {
        rows: 200,
        bytes: 2_000,
        source_requests: 40,
        destination_writes: 50,
    });

    assert!(!decision.allowed);
    assert_eq!(decision.wait_ms, 5_000);
}

#[test]
fn given_same_row_usage_when_reserved_against_independent_budgets_then_each_limit_is_enforced() {
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
fn given_row_usage_within_budget_and_maintenance_window_when_reserved_then_work_is_allowed_without_wait()
 {
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

#[test]
fn given_repeated_reservations_in_one_bucket_when_budget_accumulates_then_later_work_waits() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        max_bytes_per_second: None,
        max_source_requests_per_second: None,
        max_destination_writes_per_second: None,
        ..RateLimitPolicy::default()
    });

    assert!(
        limiter
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 60,
                    ..RateLimitUsage::default()
                },
                0
            )
            .allowed
    );
    assert!(
        limiter
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 40,
                    ..RateLimitUsage::default()
                },
                500
            )
            .allowed
    );
    let denied = limiter.reserve_at_ms(
        RateLimitUsage {
            rows: 1,
            ..RateLimitUsage::default()
        },
        999,
    );

    assert_eq!(
        denied,
        crate::RateLimitDecision {
            allowed: false,
            wait_ms: 1_010
        }
    );
}

#[test]
fn given_denied_reservation_when_bucket_expires_then_usage_resets_without_replaying_old_work() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        max_bytes_per_second: None,
        max_source_requests_per_second: None,
        max_destination_writes_per_second: None,
        ..RateLimitPolicy::default()
    });

    assert!(
        limiter
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 100,
                    ..RateLimitUsage::default()
                },
                0
            )
            .allowed
    );
    assert!(
        !limiter
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 1,
                    ..RateLimitUsage::default()
                },
                500
            )
            .allowed
    );
    assert!(
        limiter
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 100,
                    ..RateLimitUsage::default()
                },
                1_000
            )
            .allowed
    );
}

#[test]
fn given_first_reservation_mid_second_when_epoch_bucket_changes_then_budget_resets_at_boundary() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        max_bytes_per_second: None,
        max_source_requests_per_second: None,
        max_destination_writes_per_second: None,
        ..RateLimitPolicy::default()
    });

    assert!(
        limiter
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 100,
                    ..RateLimitUsage::default()
                },
                500
            )
            .allowed
    );
    assert!(
        !limiter
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 1,
                    ..RateLimitUsage::default()
                },
                999
            )
            .allowed
    );
    assert!(
        limiter
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 100,
                    ..RateLimitUsage::default()
                },
                1_000
            )
            .allowed
    );
}

#[test]
fn given_cloned_limiter_when_two_workers_reserve_then_the_shared_bucket_allows_only_one_budget() {
    let first = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        max_bytes_per_second: None,
        max_source_requests_per_second: None,
        max_destination_writes_per_second: None,
        ..RateLimitPolicy::default()
    });
    let second = first.clone();

    assert!(
        first
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 80,
                    ..RateLimitUsage::default()
                },
                10
            )
            .allowed
    );
    assert!(
        !second
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 21,
                    ..RateLimitUsage::default()
                },
                20
            )
            .allowed
    );
    assert!(
        second
            .reserve_at_ms(
                RateLimitUsage {
                    rows: 21,
                    ..RateLimitUsage::default()
                },
                1_010
            )
            .allowed
    );
}

#[test]
fn given_operation_limiter_when_real_loop_helper_reuses_it_then_source_and_destination_share_budget()
 {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(2),
        max_bytes_per_second: None,
        max_source_requests_per_second: None,
        max_destination_writes_per_second: None,
        ..RateLimitPolicy::default()
    });
    let clock = FixedRateLimitClock { now_ms: 0 };
    let one_row = RateLimitUsage {
        rows: 1,
        ..RateLimitUsage::default()
    };

    assert!(enforce_rate_limit_with_limiter(&limiter, one_row, &clock).is_ok());
    assert!(enforce_rate_limit_with_limiter(&limiter, one_row, &clock).is_ok());
    assert_eq!(
        enforce_rate_limit_with_limiter(&limiter, one_row, &clock),
        Err(crate::RateLimitDenied { wait_ms: 1_500 })
    );
}

#[test]
fn given_shared_limiter_when_calls_cross_the_second_boundary_then_budget_resets() {
    let limiter = OperationRateLimiter::new(RateLimitPolicy {
        max_rows_per_second: Some(100),
        max_bytes_per_second: None,
        max_source_requests_per_second: None,
        max_destination_writes_per_second: None,
        ..RateLimitPolicy::default()
    });
    let full_budget = RateLimitUsage {
        rows: 100,
        ..RateLimitUsage::default()
    };

    assert!(
        enforce_rate_limit_with_limiter(&limiter, full_budget, &FixedRateLimitClock { now_ms: 0 },)
            .is_ok()
    );
    assert!(
        enforce_rate_limit_with_limiter(
            &limiter,
            RateLimitUsage {
                rows: 1,
                ..RateLimitUsage::default()
            },
            &FixedRateLimitClock { now_ms: 999 },
        )
        .is_err()
    );
    assert!(
        enforce_rate_limit_with_limiter(
            &limiter,
            full_budget,
            &FixedRateLimitClock { now_ms: 1_000 },
        )
        .is_ok()
    );
}

#[derive(Clone, Copy)]
struct FixedRateLimitClock {
    now_ms: u64,
}

impl RateLimitClock for FixedRateLimitClock {
    fn minute_utc(&self) -> u16 {
        ((self.now_ms / 1_000) % 86_400 / 60) as u16
    }

    fn now_ms(&self) -> u64 {
        self.now_ms
    }
}
