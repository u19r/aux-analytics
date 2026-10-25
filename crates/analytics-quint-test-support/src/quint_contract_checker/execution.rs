//! Validate inventory once before checking, planning, or running mutations.

use std::path::Path;

use super::{CheckFailure, CheckReport, Checker, RunPlan, RunPlanOptions, mutation, plan};

pub fn check_repository(
    repo_root: &Path,
    current_phase: &str,
) -> Result<CheckReport, CheckFailure> {
    let checker = Checker::validate_repository(repo_root, current_phase)?;
    Ok(CheckReport {
        rules: checker.rule_ids.len(),
        readable_models: checker.models.len(),
    })
}

pub fn run_plan(
    repo_root: &Path,
    current_phase: &str,
    options: RunPlanOptions<'_>,
) -> Result<RunPlan, CheckFailure> {
    let checker = Checker::validate_repository(repo_root, current_phase)?;
    let records = plan::build(
        &checker,
        options.family,
        options.mode,
        options.skip_adapters,
    )?;
    let mutation_receipts = if options.skip_mutations {
        Vec::new()
    } else {
        mutation::execution::execute(repo_root, &checker.rules, options.family, None)
            .map_err(|error| CheckFailure {
                diagnostics: vec![error],
            })?
            .receipts
    };
    Ok(RunPlan {
        records,
        mutation_receipts,
    })
}

pub fn run_mutations(
    repo_root: &Path,
    current_phase: &str,
    family: &str,
    mutation_filter: Option<&str>,
) -> Result<mutation::execution::MutationReport, CheckFailure> {
    let checker = Checker::validate_repository(repo_root, current_phase)?;
    if family != "all" && !checker.rules.iter().any(|rule| rule.family == family) {
        return Err(CheckFailure {
            diagnostics: vec![format!("family {family} has no indexed inventory rows")],
        });
    }
    mutation::execution::execute(repo_root, &checker.rules, family, mutation_filter).map_err(
        |error| CheckFailure {
            diagnostics: vec![error],
        },
    )
}
