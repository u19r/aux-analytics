use std::time::{Duration, Instant};

use super::state::{AwsDiscoveryAttempt, AwsDiscoverySchedule, next_retry_delay};
use crate::constants::{DISCOVERY_RETRY_INITIAL_DELAY, DISCOVERY_RETRY_MAX_DELAY};

#[test]
fn given_initial_intermediate_and_capped_retry_delays_when_delay_is_advanced_then_it_doubles_without_exceeding_the_cap()
 {
    assert_eq!(
        next_retry_delay(DISCOVERY_RETRY_INITIAL_DELAY),
        std::time::Duration::from_secs(2)
    );
    assert_eq!(
        next_retry_delay(std::time::Duration::from_secs(16)),
        DISCOVERY_RETRY_MAX_DELAY
    );
    assert_eq!(
        next_retry_delay(DISCOVERY_RETRY_MAX_DELAY),
        DISCOVERY_RETRY_MAX_DELAY
    );
}

#[test]
fn given_discovery_success_failure_and_capacity_states_when_schedule_transitions_then_deadlines_and_retry_delays_match()
 {
    let now = Instant::now();
    let success_cases = [
        (
            "initial success with capacity and pending work",
            AwsDiscoveryAttempt::Initial,
            0,
            1,
            3,
            true,
            now,
            now,
            DISCOVERY_RETRY_INITIAL_DELAY,
        ),
        (
            "empty initial success",
            AwsDiscoveryAttempt::Initial,
            0,
            0,
            3,
            false,
            now,
            now + DISCOVERY_RETRY_INITIAL_DELAY,
            Duration::from_secs(2),
        ),
        (
            "retry success with capacity and pending work",
            AwsDiscoveryAttempt::Retry {
                delay: Duration::from_secs(2),
            },
            0,
            1,
            3,
            true,
            now,
            now,
            DISCOVERY_RETRY_INITIAL_DELAY,
        ),
        (
            "retry success at capacity",
            AwsDiscoveryAttempt::Retry {
                delay: Duration::from_secs(2),
            },
            1,
            3,
            3,
            false,
            now,
            now,
            Duration::ZERO,
        ),
        (
            "retry success without pending work",
            AwsDiscoveryAttempt::Retry {
                delay: Duration::from_secs(2),
            },
            0,
            1,
            3,
            false,
            now,
            now + Duration::from_secs(2),
            Duration::from_secs(4),
        ),
        (
            "retry success at delay cap",
            AwsDiscoveryAttempt::Retry {
                delay: DISCOVERY_RETRY_MAX_DELAY,
            },
            0,
            0,
            3,
            false,
            now,
            now + DISCOVERY_RETRY_MAX_DELAY,
            DISCOVERY_RETRY_MAX_DELAY,
        ),
    ];
    for (
        label,
        attempt,
        active_before,
        active_after,
        max_shards,
        has_pending_work,
        attempt_time,
        expected_next_attempt_at,
        expected_delay,
    ) in success_cases
    {
        let mut schedule = AwsDiscoverySchedule::Initial;
        schedule.complete(
            attempt,
            active_before,
            active_after,
            max_shards,
            has_pending_work,
            attempt_time,
        );
        if expected_delay.is_zero() {
            assert!(
                matches!(schedule, AwsDiscoverySchedule::Quiet),
                "{label}: expected quiet schedule, got {schedule:?}"
            );
        } else {
            assert_retry_state(schedule, expected_next_attempt_at, expected_delay, label);
        }
    }

    let failure_cases = [
        ("DescribeStream error", AwsDiscoveryAttempt::Initial, 1),
        (
            "first shard iterator error",
            AwsDiscoveryAttempt::Initial,
            1,
        ),
        (
            "later shard iterator error after partial progress",
            AwsDiscoveryAttempt::Initial,
            1,
        ),
        (
            "retry-attempt error",
            AwsDiscoveryAttempt::Retry {
                delay: Duration::from_secs(2),
            },
            2,
        ),
    ];
    for (label, attempt, delay_seconds) in failure_cases {
        let mut schedule = AwsDiscoverySchedule::Initial;
        schedule.fail(attempt, now);
        let delay = Duration::from_secs(delay_seconds);
        assert_retry_state(schedule, now + delay, delay * 2, label);
    }

    {
        let label = "terminal shard removal";
        let mut schedule = AwsDiscoverySchedule::Quiet;
        schedule.schedule_immediate_retry(now);
        assert_retry_state(schedule, now, DISCOVERY_RETRY_INITIAL_DELAY, label);
    }

    let mut schedule = AwsDiscoverySchedule::Quiet;
    schedule.fail_current(now);
    assert_retry_state(
        schedule,
        now + DISCOVERY_RETRY_INITIAL_DELAY,
        Duration::from_secs(2),
        "expired iterator rebuild failure",
    );

    for (label, timestamp, is_due) in [
        ("before retry deadline", now, false),
        (
            "at retry deadline",
            now + DISCOVERY_RETRY_INITIAL_DELAY,
            true,
        ),
    ] {
        let mut schedule = AwsDiscoverySchedule::Retry {
            next_attempt_at: now + DISCOVERY_RETRY_INITIAL_DELAY,
            delay: DISCOVERY_RETRY_INITIAL_DELAY,
        };
        assert_eq!(schedule.begin(0, timestamp).is_some(), is_due, "{label}");
        let mut pending_schedule = AwsDiscoverySchedule::Retry {
            next_attempt_at: now + DISCOVERY_RETRY_INITIAL_DELAY,
            delay: DISCOVERY_RETRY_INITIAL_DELAY,
        };
        assert_eq!(
            pending_schedule.begin_pending(timestamp).is_some(),
            is_due,
            "pending {label}"
        );
    }
}

fn assert_retry_state(
    schedule: AwsDiscoverySchedule,
    expected_next_attempt_at: Instant,
    expected_delay: Duration,
    label: &str,
) {
    match schedule {
        AwsDiscoverySchedule::Retry {
            next_attempt_at,
            delay,
        } => {
            assert_eq!(next_attempt_at, expected_next_attempt_at, "{label}");
            assert_eq!(delay, expected_delay, "{label}");
        }
        other => panic!("{label}: expected retry schedule, got {other:?}"),
    }
}
