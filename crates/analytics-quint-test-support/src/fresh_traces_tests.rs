use std::cell::Cell;

use quint_connect::{Driver, Result, Step};

use crate::{constants::FRESH_TRACE_SPEC, run_fresh_traces};

#[derive(Default)]
struct FreshOnlyDriver {
    initialized: bool,
}

impl Driver for FreshOnlyDriver {
    type State = ();

    fn step(&mut self, step: &Step) -> Result {
        if step.action_taken == "init" {
            if self.initialized {
                return Err(anyhow::anyhow!("driver was reused across traces"));
            }
            self.initialized = true;
        }
        Ok(())
    }
}

#[test]
fn given_explicit_trace_plan_when_replayed_then_each_trace_gets_a_fresh_driver() {
    let trace_plan = ["0x1", "0x2", "0x3"];
    let factory_invocations = Cell::new(0);
    let receipt = run_fresh_traces(
        "fresh_trace_factory_hostile",
        FRESH_TRACE_SPEC,
        4,
        trace_plan,
        || {
            factory_invocations.set(factory_invocations.get() + 1);
            FreshOnlyDriver::default()
        },
    )
    .expect("fresh trace replay");

    assert_eq!(receipt.expected_trace_count, trace_plan.len());
    assert_eq!(receipt.factory_invocations, trace_plan.len());
    assert_eq!(receipt.seeds, trace_plan);
    assert_eq!(factory_invocations.get(), trace_plan.len());
}

#[test]
fn given_empty_trace_plan_when_replayed_then_it_is_rejected() {
    let error = run_fresh_traces(
        "fresh_trace_empty_plan",
        FRESH_TRACE_SPEC,
        4,
        Vec::<&str>::new(),
        FreshOnlyDriver::default,
    )
    .expect_err("empty trace plan");

    assert!(error.to_string().contains("at least one seed"));
}

#[test]
fn given_empty_seed_when_replayed_then_it_is_rejected() {
    let error = run_fresh_traces(
        "fresh_trace_empty_seed",
        FRESH_TRACE_SPEC,
        4,
        ["   "],
        FreshOnlyDriver::default,
    )
    .expect_err("empty seed");

    assert!(error.to_string().contains("seeds must not be empty"));
}
