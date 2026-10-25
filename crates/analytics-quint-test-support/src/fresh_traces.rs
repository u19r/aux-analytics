#[derive(Debug, Eq, PartialEq)]
pub struct FreshTraceReceipt {
    pub expected_trace_count: usize,
    pub factory_invocations: usize,
    pub seeds: Vec<String>,
}

/// Replays deterministic one-trace runs with a fresh production-backed driver
/// for each trace.
///
/// `quint_connect::runner::run_test` owns the trace replay loop and reuses one
/// driver for every ITF file in a run. Keeping each generated run to one trace
/// makes the driver factory the explicit trace-local reset boundary while
/// retaining the requested aggregate sample count.
pub fn run_fresh_traces<D, F, I>(
    test_name: &str,
    spec: &str,
    max_steps: usize,
    seeds: I,
    make_driver: F,
) -> quint_connect::Result<FreshTraceReceipt>
where
    D: quint_connect::Driver,
    F: Fn() -> D,
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let seeds: Vec<String> = seeds
        .into_iter()
        .map(|seed| seed.as_ref().to_owned())
        .collect();
    let expected_trace_count = seeds.len();
    anyhow::ensure!(
        expected_trace_count > 0,
        "fresh trace plan must contain at least one seed"
    );
    anyhow::ensure!(
        seeds.iter().all(|seed| !seed.trim().is_empty()),
        "fresh trace plan seeds must not be empty"
    );

    let mut factory_invocations = 0;
    for (trace, seed) in seeds.iter().enumerate() {
        let config = quint_connect::runner::Config {
            test_name: format!("{test_name}[trace {trace}]"),
            gen_config: quint_connect::runner::RunConfig {
                spec: spec.to_owned(),
                main: None,
                init: None,
                step: None,
                max_samples: Some(1),
                max_steps: Some(max_steps),
                seed: seed.clone(),
            },
        };
        let driver = make_driver();
        factory_invocations += 1;
        quint_connect::runner::run_test(driver, config)?;
    }
    anyhow::ensure!(
        factory_invocations == expected_trace_count,
        "fresh trace factory invocation count did not match requested trace count"
    );
    Ok(FreshTraceReceipt {
        expected_trace_count,
        factory_invocations,
        seeds,
    })
}

pub fn map_result<T, V, E>(
    result: Result<V, E>,
    success: impl FnOnce(V) -> T,
    failure: impl FnOnce(E) -> T,
) -> T {
    match result {
        Ok(value) => success(value),
        Err(error) => failure(error),
    }
}
