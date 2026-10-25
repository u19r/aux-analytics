use super::plan::{PerformanceGate, PlanBudget, PlanMode, parse_gate};
use crate::constants::{
    MAX_DEEP_SAMPLES, MAX_DEEP_STEPS, MAX_PLAN_RUNS, MAX_PLAN_WORK, MAX_STANDARD_SAMPLES,
    MAX_STANDARD_STEPS,
};

fn gate(samples: usize, steps: usize) -> PerformanceGate {
    PerformanceGate {
        seed: "0x1".to_owned(),
        samples,
        steps,
        description: "test".to_owned(),
    }
}

#[test]
fn given_deep_gate_when_scaled_then_bounds_and_overflow_are_checked() {
    assert_eq!(
        PlanMode::Deep.scale(&gate(MAX_STANDARD_SAMPLES, MAX_STANDARD_STEPS)),
        Some((MAX_DEEP_SAMPLES, MAX_DEEP_STEPS))
    );
    assert_eq!(
        PlanMode::Deep.scale(&gate(usize::MAX, usize::MAX)),
        None,
        "deep scaling must not saturate overflowing budgets"
    );
}

#[test]
fn given_unbounded_budget_when_gate_is_parsed_then_execution_is_rejected() {
    for (samples, steps) in [(usize::MAX, 1), (1, usize::MAX)] {
        assert!(
            parse_gate(&format!(
                "fixed seed 0x1; {samples} samples; {steps} steps; focused"
            ))
            .is_none(),
            "usize::MAX must not become a runnable formal budget"
        );
    }
}

#[test]
fn given_exhausted_plan_budget_when_reserved_then_count_and_work_overflow_are_rejected() {
    let mut count_budget = PlanBudget::default();
    for _ in 0..MAX_PLAN_RUNS {
        count_budget.reserve(1, 1).expect("budget capacity");
    }
    assert_eq!(
        count_budget.reserve(1, 1),
        Err("aggregate run budget exceeded")
    );

    let mut work_budget = PlanBudget::default();
    work_budget
        .reserve(MAX_PLAN_WORK, 1)
        .expect("work capacity");
    assert_eq!(
        work_budget.reserve(1, 1),
        Err("aggregate sample-step budget exceeded")
    );
    assert_eq!(
        PlanBudget::default().reserve(usize::MAX, 2),
        Err("sample-step product overflows")
    );
}

#[test]
fn given_independent_modes_when_each_uses_full_budget_then_overflow_stays_mode_local() {
    let mut standard_budget = PlanBudget::default();
    standard_budget
        .reserve(MAX_PLAN_WORK, 1)
        .expect("standard mode work capacity");
    assert_eq!(
        standard_budget.reserve(1, 1),
        Err("aggregate sample-step budget exceeded")
    );

    let mut deep_budget = PlanBudget::default();
    deep_budget
        .reserve(MAX_PLAN_WORK, 1)
        .expect("deep mode work capacity");
    assert_eq!(
        deep_budget.reserve(1, 1),
        Err("aggregate sample-step budget exceeded")
    );
}
