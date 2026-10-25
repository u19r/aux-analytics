use super::evidence_parser;

#[cfg(test)]
pub(crate) fn parse_evidence(
    status: &str,
    adapter: &str,
    mutation: &str,
) -> Result<(), Vec<String>> {
    evidence_parser::parse(status, adapter, mutation).map(|_| ())
}
#[cfg(test)]
thread_local! {
    static VALIDATION_RUNS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_validation_runs() {
    VALIDATION_RUNS.with(|runs| runs.set(0));
}

#[cfg(test)]
pub(crate) fn validation_runs() -> usize {
    VALIDATION_RUNS.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn mutation_models() -> impl Iterator<Item = (&'static str, &'static str)> {
    crate::constants::MUTATIONS
        .iter()
        .map(|mutation| (mutation.id, mutation.model))
}

pub(super) fn record_validation_run() {
    VALIDATION_RUNS.with(|runs| runs.set(runs.get() + 1));
}
