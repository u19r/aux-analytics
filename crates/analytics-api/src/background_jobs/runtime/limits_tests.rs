use analytics_storage::SourceTablePlan;
use config::AnalyticsSourceConfig;

use super::limits::bound_source_page;
use crate::constants::MAX_SOURCE_RECORDS;

#[test]
fn given_fanout_when_source_page_is_bounded_then_every_destination_fits_one_job() {
    let source = AnalyticsSourceConfig::default();
    for fanout in [1, 2, 23, MAX_SOURCE_RECORDS] {
        let plans = [SourceTablePlan::aux_storage(
            "events".to_string(),
            (0..fanout)
                .map(|index| format!("projection_{index}"))
                .collect(),
        )];
        let bounded = bound_source_page(&source, &plans).expect("bounded source page");
        let routed = bounded.poll_max_records_per_response as usize * fanout;
        assert!(routed > 0 && routed <= MAX_SOURCE_RECORDS);
        assert!((bounded.poll_max_records_per_response as usize + 1) * fanout > MAX_SOURCE_RECORDS);
    }
    assert_eq!(source.poll_max_records_per_response, 8_192);
}

#[test]
fn given_unequal_fanout_and_a_smaller_requested_page_when_bounded_then_neither_budget_grows() {
    let source = AnalyticsSourceConfig {
        poll_max_records_per_response: 7,
        ..AnalyticsSourceConfig::default()
    };
    let plans = [
        SourceTablePlan::aux_storage("events".to_string(), vec!["events_projection".to_string()]),
        SourceTablePlan::aux_storage(
            "other_events".to_string(),
            (0..23).map(|index| format!("projection_{index}")).collect(),
        ),
    ];
    assert_eq!(
        bound_source_page(&source, &plans)
            .expect("bounded page")
            .poll_max_records_per_response,
        7
    );
}

#[test]
fn given_one_record_with_excessive_fanout_when_bounded_then_configuration_is_rejected() {
    let plan = SourceTablePlan::aux_storage(
        "events".to_string(),
        (0..=MAX_SOURCE_RECORDS)
            .map(|index| format!("projection_{index}"))
            .collect(),
    );
    assert!(bound_source_page(&AnalyticsSourceConfig::default(), &[plan]).is_err());
}

#[test]
fn given_source_timeouts_when_driver_budget_is_built_then_claims_have_the_full_execution_window() {
    use std::time::Duration;

    use bg_jobs::runtime::MonotonicDeadline;

    use super::limits::driver_budget;
    for timeout in [
        Duration::from_millis(1),
        Duration::from_secs(5),
        crate::constants::MAX_SOURCE_JOB_TIMEOUT,
    ] {
        let request = driver_budget(timeout)
            .expect("driver budget")
            .request_at(MonotonicDeadline::ZERO)
            .expect("drive request");
        assert_eq!(
            request
                .stop_claiming_at()
                .remaining_until(MonotonicDeadline::ZERO),
            timeout
        );
        assert_eq!(
            request
                .finish_by()
                .remaining_until(request.stop_claiming_at()),
            timeout
        );
    }
    assert!(driver_budget(Duration::MAX).is_err());
}
