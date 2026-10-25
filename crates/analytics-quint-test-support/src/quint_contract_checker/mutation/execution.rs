use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;

use super::{
    super::{
        inventory::Rule,
        plan::{Gate, PerformanceGate},
    },
    MutationDefinition, QuintCommand, SourceTransform, TemporaryModel, apply_transform,
    counterexample::{read_bounded_bytes, read_bounded_json, read_bounded_source, validate_itf},
    definition,
    replay::independently_verify_invariant,
    validate_typed_contract,
};
use crate::constants::{MAX_COUNTEREXAMPLE_BYTES, MAX_MODEL_BYTES};

#[derive(Debug, Default)]
pub struct MutationReport {
    pub receipts: Vec<String>,
    pub matched: usize,
}

pub(crate) fn execute(
    repo_root: &Path,
    rules: &[Rule],
    family: &str,
    mutation_filter: Option<&str>,
) -> Result<MutationReport, String> {
    let command = QuintCommand::discover_trusted()?;
    let mut report = MutationReport::default();
    for rule in rules {
        if family != "all" && rule.family != family {
            continue;
        }
        let Some(mutation) = definition(&rule.mutation_id) else {
            continue;
        };
        if mutation_filter.is_some_and(|filter| filter != mutation.id) {
            continue;
        }
        report.matched += 1;
        run_one_with_trusted_command(repo_root, rule, mutation, &command)?;
        report.receipts.push(format!(
            "quint mutation green: {} ({})",
            mutation.id, mutation.model
        ));
    }
    if report.matched == 0 {
        if let Some(filter) = mutation_filter {
            return Err(format!("no mutation {filter} matched"));
        }
        report.receipts.push(format!(
            "quint mutations skipped: no audited mutations in family {family}"
        ));
    } else {
        report.receipts.push(format!(
            "quint mutations green: {} audited mutations",
            report.matched
        ));
    }
    Ok(report)
}

#[cfg(test)]
pub(crate) fn run_one(
    repo_root: &Path,
    rule: &Rule,
    mutation: &MutationDefinition,
) -> Result<(), String> {
    let trusted_command = QuintCommand::discover_trusted()?;
    run_one_with_trusted_command(repo_root, rule, mutation, &trusted_command)
}

fn run_one_with_trusted_command(
    repo_root: &Path,
    rule: &Rule,
    mutation: &MutationDefinition,
    trusted_command: &QuintCommand,
) -> Result<(), String> {
    let source_path = repo_root.join(mutation.model);
    let source = read_bounded_source(&source_path, mutation.model)?;
    validate_typed_contract(rule, mutation)?;
    let transform = SourceTransform::from(mutation);
    let mutated = apply_transform(&source, transform, mutation.id)?;
    let temporary = TemporaryModel::try_new(mutation.model)?;
    fs::write(&temporary.path, &mutated)
        .map_err(|error| format!("{} could not write mutation: {error}", mutation.id))?;
    let proof = MutationProof::try_new(&temporary, mutation, mutated.as_bytes())?;
    proof.check_typecheck(trusted_command)?;
    let Gate::Fixed(gate) = &rule.performance_gate else {
        return Err(format!("{} has no fixed performance gate", mutation.id));
    };
    let itf = proof.capture_counterexample(trusted_command, rule, gate)?;
    proof.verify_counterexample(trusted_command, rule, gate, &itf)?;
    if read_bounded_bytes(&source_path, MAX_MODEL_BYTES)? != source.as_bytes() {
        return Err(format!("{} changed the repository source", mutation.id));
    }
    Ok(())
}

pub(crate) fn ensure_temporary_model_unchanged(
    model_path: &Path,
    expected_path: &Path,
    expected_bytes: &[u8],
    phase: &str,
) -> Result<(), String> {
    let actual_path = model_path.canonicalize().map_err(|error| {
        format!("{phase}: temporary mutation model could not be canonicalized: {error}")
    })?;
    if actual_path != expected_path {
        return Err(format!("{phase}: temporary mutation model path changed"));
    }
    let actual_bytes = read_bounded_bytes(model_path, MAX_MODEL_BYTES).map_err(|error| {
        format!("{phase}: temporary mutation model could not be reread: {error}")
    })?;
    if actual_bytes != expected_bytes {
        return Err(format!("{phase}: temporary mutation model changed"));
    }
    Ok(())
}

struct MutationProof<'a> {
    temporary: &'a TemporaryModel,
    mutation: &'a MutationDefinition,
    expected_path: PathBuf,
    expected_bytes: &'a [u8],
}

impl<'a> MutationProof<'a> {
    fn try_new(
        temporary: &'a TemporaryModel,
        mutation: &'a MutationDefinition,
        expected_bytes: &'a [u8],
    ) -> Result<Self, String> {
        let expected_path = temporary.path.canonicalize().map_err(|error| {
            format!(
                "{} temporary mutation model could not be canonicalized: {error}",
                mutation.id
            )
        })?;
        Ok(Self {
            temporary,
            mutation,
            expected_path,
            expected_bytes,
        })
    }

    fn check_typecheck(&self, command: &QuintCommand) -> Result<(), String> {
        let status = command
            .run_status(&["typecheck", self.temporary.path.to_string_lossy().as_ref()])
            .map_err(|error| {
                format!(
                    "{} mutated model did not typecheck: {error}",
                    self.mutation.id
                )
            })?;
        if !status.success() {
            return Err(format!(
                "{} mutated model did not typecheck",
                self.mutation.id
            ));
        }
        Ok(())
    }

    fn verify_unchanged(&self, phase: &str) -> Result<(), String> {
        ensure_temporary_model_unchanged(
            &self.temporary.path,
            &self.expected_path,
            self.expected_bytes,
            &format!("{} {phase}", self.mutation.id),
        )
    }

    fn capture_counterexample(
        &self,
        command: &QuintCommand,
        rule: &Rule,
        gate: &PerformanceGate,
    ) -> Result<Value, String> {
        let path = self.temporary.directory.join("counterexample.itf.json");
        let status = CounterexampleRun {
            model_path: &self.temporary.path,
            invariant: &rule.invariant,
            gate,
        }
        .run_counterexample(command, &path)
        .map_err(|error| format!("{} failed to run: {error}", self.mutation.id))?;
        self.verify_unchanged("after run")?;
        if status.success() {
            return Err(format!(
                "{} did not produce a counterexample",
                self.mutation.id
            ));
        }
        let itf = read_bounded_json(&path, MAX_COUNTEREXAMPLE_BYTES).map_err(|reason| {
            format!(
                "{} failed without a structured invariant counterexample trace: {reason}",
                self.mutation.id
            )
        })?;
        validate_itf(&itf, &self.temporary.path, gate.steps).map_err(|reason| {
            format!(
                "{} failed without a structured invariant counterexample trace: {reason}",
                self.mutation.id
            )
        })?;
        Ok(itf)
    }

    fn verify_counterexample(
        &self,
        command: &QuintCommand,
        rule: &Rule,
        gate: &PerformanceGate,
        itf: &Value,
    ) -> Result<(), String> {
        let path = self.temporary.directory.join("reachable.itf.json");
        CounterexampleRun {
            model_path: &self.temporary.path,
            invariant: &rule.invariant,
            gate,
        }
        .verify_reachable_trace(command, itf, &path)
        .map_err(|reason| {
            format!(
                "{} failed without a trusted reachable invariant trace: {reason}",
                self.mutation.id
            )
        })?;
        self.verify_unchanged("after reachability verification")?;
        independently_verify_invariant(command, &self.temporary.path, itf, &rule.invariant)
            .map_err(|reason| {
                format!(
                    "{} failed without an independently verified invariant counterexample: \
                     {reason}",
                    self.mutation.id
                )
            })?;
        self.verify_unchanged("after independent evaluation")
    }
}

pub(crate) struct CounterexampleRun<'a> {
    pub(crate) model_path: &'a Path,
    pub(crate) invariant: &'a str,
    pub(crate) gate: &'a PerformanceGate,
}

impl CounterexampleRun<'_> {
    fn run_counterexample(
        &self,
        command: &QuintCommand,
        itf_path: &Path,
    ) -> Result<std::process::ExitStatus, String> {
        let invariant = format!("--invariant={}", self.invariant);
        let samples = format!("--max-samples={}", self.gate.samples);
        let steps = format!("--max-steps={}", self.gate.steps);
        let seed = format!("--seed={}", self.gate.seed);
        let model_path = self.model_path.to_string_lossy().into_owned();
        let out_itf = format!("--out-itf={}", itf_path.display());
        let run_arguments = [
            "run",
            model_path.as_str(),
            invariant.as_str(),
            samples.as_str(),
            steps.as_str(),
            seed.as_str(),
            out_itf.as_str(),
        ];
        command.run_status(&run_arguments)
    }

    pub(crate) fn verify_reachable_trace(
        &self,
        command: &QuintCommand,
        expected: &Value,
        replayed_itf_path: &Path,
    ) -> Result<(), String> {
        let status = self.run_counterexample(command, replayed_itf_path)?;
        if status.success() {
            return Err("trusted Quint could not reproduce a violating reachable trace".to_owned());
        }
        let replayed = read_bounded_json(replayed_itf_path, MAX_COUNTEREXAMPLE_BYTES)
            .map_err(|error| format!("trusted reachable trace is unavailable: {error}"))?;
        validate_itf(&replayed, self.model_path, self.gate.steps)
            .map_err(|error| format!("trusted reachable trace is invalid: {error}"))?;
        if replayed.get("vars") != expected.get("vars")
            || replayed.get("states") != expected.get("states")
        {
            return Err(
                "counterexample trace does not match the trusted reachable trace".to_owned(),
            );
        }
        Ok(())
    }
}
