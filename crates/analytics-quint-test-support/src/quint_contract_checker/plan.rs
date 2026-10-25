use std::collections::{BTreeMap, BTreeSet};

use super::{CheckFailure, Checker, evidence::CoverageEvidence};
use crate::constants::{
    DEEP_SAMPLE_MULTIPLIER, DEEP_STEP_MULTIPLIER, EXPLICIT_DEFERRED_ADAPTER_SKIPS,
    MAX_DEEP_SAMPLES, MAX_DEEP_STEPS, MAX_PLAN_RUNS, MAX_PLAN_WORK, MAX_STANDARD_SAMPLES,
    MAX_STANDARD_STEPS,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct PerformanceGate {
    pub(super) seed: String,
    pub(super) samples: usize,
    pub(super) steps: usize,
    pub(super) description: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) enum Gate {
    Fixed(PerformanceGate),
    Pending(String),
}

pub(super) fn parse_gate(value: &str) -> Option<Gate> {
    parse_performance_gate(value).map(Gate::Fixed).or_else(|| {
        value
            .strip_prefix("pending ")
            .filter(|reason| !reason.trim().is_empty())
            .map(|_| Gate::Pending(value.to_owned()))
    })
}

fn parse_performance_gate(value: &str) -> Option<PerformanceGate> {
    let remainder = value.strip_prefix("fixed seed ")?;
    let (seed, remainder) = remainder.split_once("; ")?;
    let (samples, remainder) = remainder.split_once(" samples; ")?;
    let (steps, description) = remainder.split_once(" steps; ")?;
    if !is_hex_seed(seed) || description.is_empty() {
        return None;
    }
    let samples = samples.parse().ok()?;
    let steps = steps.parse().ok()?;
    (standard_budget_is_valid(samples, steps)).then_some(PerformanceGate {
        seed: seed.to_owned(),
        samples,
        steps,
        description: description.to_owned(),
    })
}

fn is_hex_seed(value: &str) -> bool {
    value
        .strip_prefix("0x")
        .is_some_and(|hex| !hex.is_empty() && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn standard_budget_is_valid(samples: usize, steps: usize) -> bool {
    samples > 0 && samples <= MAX_STANDARD_SAMPLES && steps > 0 && steps <= MAX_STANDARD_STEPS
}

pub(super) fn adapter_package(path: &str) -> Option<String> {
    let components = path.split('/').collect::<Vec<_>>();
    if components.len() < 3
        || path.contains('\\')
        || components
            .iter()
            .any(|component| component.is_empty() || matches!(*component, "." | ".."))
        || components.first().copied() != Some("crates")
    {
        return None;
    }
    let package = components[1];
    if package.is_empty()
        || !package
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return None;
    }
    Some(package.to_owned())
}

#[derive(Clone, Copy)]
pub(super) enum PlanMode {
    Standard,
    Deep,
}

impl PlanMode {
    pub(super) fn parse(mode: &str) -> Option<Vec<Self>> {
        match mode {
            "standard" => Some(vec![Self::Standard]),
            "deep" => Some(vec![Self::Deep]),
            "all" => Some(vec![Self::Standard, Self::Deep]),
            _ => None,
        }
    }

    pub(super) fn scale(self, gate: &PerformanceGate) -> Option<(usize, usize)> {
        match self {
            Self::Standard => standard_budget_is_valid(gate.samples, gate.steps)
                .then_some((gate.samples, gate.steps)),
            Self::Deep => {
                let samples = gate.samples.checked_mul(DEEP_SAMPLE_MULTIPLIER)?;
                let steps = gate.steps.checked_mul(DEEP_STEP_MULTIPLIER)?;
                (samples <= MAX_DEEP_SAMPLES && steps <= MAX_DEEP_STEPS).then_some((samples, steps))
            }
        }
    }
}

#[derive(Default)]
pub(super) struct PlanBudget {
    runs: usize,
    work: usize,
}

impl PlanBudget {
    pub(super) fn reserve(&mut self, samples: usize, steps: usize) -> Result<(), &'static str> {
        let work = samples
            .checked_mul(steps)
            .ok_or("sample-step product overflows")?;
        let runs = self.runs.checked_add(1).ok_or("run count overflows")?;
        let total_work = self
            .work
            .checked_add(work)
            .ok_or("aggregate sample-step work overflows")?;
        if runs > MAX_PLAN_RUNS {
            return Err("aggregate run budget exceeded");
        }
        if total_work > MAX_PLAN_WORK {
            return Err("aggregate sample-step budget exceeded");
        }
        self.runs = runs;
        self.work = total_work;
        Ok(())
    }
}

pub(super) fn build(
    checker: &Checker<'_>,
    family: &str,
    mode: &str,
    skip_adapters: bool,
) -> Result<Vec<String>, CheckFailure> {
    let modes = PlanMode::parse(mode).ok_or_else(|| CheckFailure {
        diagnostics: vec!["mode must be standard, deep, or all".to_owned()],
    })?;
    if family != "all" && !checker.rules.iter().any(|rule| rule.family == family) {
        return Err(CheckFailure {
            diagnostics: vec![format!("family {family} has no indexed inventory rows")],
        });
    }
    let selected: Vec<_> = checker
        .rules
        .iter()
        .filter(|rule| family == "all" || rule.family == family)
        .collect();
    let models: BTreeSet<_> = selected
        .iter()
        .map(|rule| rule.readable_model.as_str())
        .collect();
    let mut records = Vec::new();
    for model in models {
        records.push(format!("typecheck\t{model}"));
        records.push(format!("test\t{model}"));
    }
    for mode in modes {
        mode.append_records(&selected, &mut records)?;
    }
    if !skip_adapters {
        append_adapter_records(checker, &selected, &mut records)?;
    }
    Ok(records)
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct FormalRun<'a> {
    model: &'a str,
    invariant: &'a str,
    seed: &'a str,
    samples: usize,
    steps: usize,
}

impl PlanMode {
    fn group_runs<'a>(
        self,
        selected: &[&'a super::inventory::Rule],
    ) -> Result<BTreeMap<FormalRun<'a>, BTreeSet<&'a str>>, CheckFailure> {
        let mut groups = BTreeMap::<FormalRun<'a>, BTreeSet<&'a str>>::new();
        for rule in selected.iter().filter(|rule| rule.evidence.is_runnable()) {
            let Gate::Fixed(gate) = &rule.performance_gate else {
                return Err(CheckFailure {
                    diagnostics: vec![format!(
                        "run plan has no fixed performance gate for {}",
                        rule.rule_id
                    )],
                });
            };
            let Some((samples, steps)) = self.scale(gate) else {
                return Err(CheckFailure {
                    diagnostics: vec![format!(
                        "run plan exceeds {} mode per-run sample/step budget for {}",
                        self.name(),
                        rule.rule_id
                    )],
                });
            };
            let witnesses = groups
                .entry(FormalRun {
                    model: &rule.readable_model,
                    invariant: &rule.invariant,
                    seed: &gate.seed,
                    samples,
                    steps,
                })
                .or_default();
            if rule.witnesses != "-" {
                witnesses.extend(rule.witnesses.split_whitespace());
            }
        }
        Ok(groups)
    }

    fn append_records(
        self,
        selected: &[&super::inventory::Rule],
        records: &mut Vec<String>,
    ) -> Result<(), CheckFailure> {
        let mut budget = PlanBudget::default();
        for (run, witnesses) in self.group_runs(selected)? {
            self.reserve_run(&mut budget, &run)?;
            records.push(run.record([format!("--invariant={}", run.invariant)]));
            if !witnesses.is_empty() {
                self.reserve_run(&mut budget, &run)?;
                records.push(
                    run.record(
                        witnesses
                            .into_iter()
                            .map(|witness| format!("--witnesses={witness}")),
                    ),
                );
            }
        }
        Ok(())
    }

    fn reserve_run(self, budget: &mut PlanBudget, run: &FormalRun<'_>) -> Result<(), CheckFailure> {
        budget
            .reserve(run.samples, run.steps)
            .map_err(|reason| CheckFailure {
                diagnostics: vec![format!(
                    "run plan exceeds aggregate formal budget in {} mode: {reason}",
                    self.name()
                )],
            })
    }
}

impl FormalRun<'_> {
    fn record(&self, selectors: impl IntoIterator<Item = String>) -> String {
        let mut fields = vec!["run".to_owned(), self.model.to_owned()];
        fields.extend(selectors);
        fields.extend([
            format!("--max-samples={}", self.samples),
            format!("--max-steps={}", self.steps),
            format!("--seed={}", self.seed),
        ]);
        fields.join("\t")
    }
}

fn append_adapter_records(
    checker: &Checker<'_>,
    selected: &[&super::inventory::Rule],
    records: &mut Vec<String>,
) -> Result<(), CheckFailure> {
    let mut packages = BTreeMap::<String, BTreeSet<String>>::new();
    for rule in selected
        .iter()
        .filter(|rule| matches!(&rule.evidence, CoverageEvidence::Covered(_)))
    {
        for adapter in rule.evidence.adapter_paths() {
            let package = adapter_package(adapter).ok_or_else(|| CheckFailure {
                diagnostics: vec![format!(
                    "run plan cannot determine package for adapter {adapter}"
                )],
            })?;
            packages.entry(package).or_default();
        }
    }
    register_deferred_skips(checker, &mut packages);
    for (package, skips) in packages {
        let mut fields = vec!["adapter".to_owned(), package];
        for skip in skips {
            fields.extend(["--skip".to_owned(), skip]);
        }
        records.push(fields.join("\t"));
    }
    Ok(())
}

fn register_deferred_skips(
    checker: &Checker<'_>,
    packages: &mut BTreeMap<String, BTreeSet<String>>,
) {
    for rule in checker
        .rules
        .iter()
        .filter(|rule| !matches!(&rule.evidence, CoverageEvidence::Covered(_)))
    {
        for adapter in rule.evidence.adapter_paths() {
            let Some(package) = adapter_package(adapter) else {
                continue;
            };
            if let Some((_, test_name)) = EXPLICIT_DEFERRED_ADAPTER_SKIPS
                .iter()
                .find(|(path, _)| *path == adapter)
                && let Some(skips) = packages.get_mut(&package)
            {
                skips.insert((*test_name).to_owned());
            }
        }
    }
}

impl PlanMode {
    fn name(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Deep => "deep",
        }
    }
}
