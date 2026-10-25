use std::{fs, io::Read, path::Path};

use super::{
    Checker, dates,
    dates::Expiry,
    evidence::{AdapterEvidence, CoverageEvidence, ModelOnlyEvidence},
    inventory::Rule,
    mutation,
    plan::Gate,
    symbols::{SourceSymbols, SymbolKind, SymbolRequirements, camel_to_snake, is_quint_identifier},
};
use crate::constants::MAX_MODEL_BYTES;

impl<'a> Checker<'a> {
    pub(super) fn check_model_references(&mut self, line: usize, rule: &Rule) {
        let Some(path) = self.confined_model(line, &rule.readable_model) else {
            return;
        };
        let Some(source) = self.read_model_source(line, &path, &rule.readable_model) else {
            return;
        };
        let symbols = SourceSymbols::parse(&source);
        if let Some(definition) = mutation::definition(&rule.mutation_id)
            && let Err(error) = mutation::validate_transform_role(&source, &symbols, definition)
        {
            self.error(
                line,
                format!("rule {} mutation validation failed: {error}", rule.rule_id),
            );
        }
        self.validate_owner(line, &rule.rust_owner);
        self.validate_model_declarations(line, &symbols, rule);
        if let Some((actions, states)) = rule.evidence.model_symbols() {
            self.validate_symbols(
                line,
                &symbols,
                &rule.readable_model,
                SymbolRequirements {
                    actions,
                    states,
                    kind: SymbolKind::Model,
                },
            );
        }
        if rule.mbt_model != "-" {
            let _ = self.confined_mbt_model(line, &rule.mbt_model);
        }
    }

    fn validate_model_declarations(
        &mut self,
        line: usize,
        symbols: &SourceSymbols<'_>,
        rule: &Rule,
    ) {
        if !is_quint_identifier(&rule.invariant) {
            self.error(
                line,
                format!(
                    "invariant {} must be a strict Quint identifier",
                    rule.invariant
                ),
            );
        } else if !symbols.has_model_value(&rule.invariant) {
            self.error(
                line,
                format!(
                    "invariant {} is not declared by {}",
                    rule.invariant, rule.readable_model
                ),
            );
        }
        for witness in rule
            .witnesses
            .split_whitespace()
            .filter(|value| *value != "-")
        {
            if !symbols.has_model_symbol("val", witness)
                && !symbols.has_model_symbol("pure val", witness)
            {
                self.error(
                    line,
                    format!(
                        "witness {witness} is not declared by {}",
                        rule.readable_model
                    ),
                );
            }
        }
    }

    pub(super) fn validate_rule(&mut self, line: usize, rule: &Rule) {
        match &rule.evidence {
            CoverageEvidence::Covered(adapter) => {
                if !rule
                    .witnesses
                    .split_whitespace()
                    .any(|witness| witness != "-")
                {
                    self.error(
                        line,
                        "covered status must declare at least one model witness",
                    );
                }
                self.validate_adapter_paths(line, adapter, true);
                self.validate_stable_fields(line, rule);
                self.validate_complete_gate(line, rule);
            }
            CoverageEvidence::ModelOnly(evidence) => self.validate_model_only(line, evidence, rule),
            CoverageEvidence::NotApplicable(_) => {
                self.validate_stable_fields(line, rule);
                self.validate_complete_gate(line, rule);
            }
        }
    }

    fn validate_model_only(&mut self, line: usize, evidence: &ModelOnlyEvidence, rule: &Rule) {
        match evidence {
            ModelOnlyEvidence::Current {
                adapter, expiry, ..
            } => {
                self.validate_expiry(line, expiry);
                self.validate_adapter_paths(line, adapter, true);
                self.validate_stable_fields(line, rule);
                self.validate_complete_gate(line, rule);
            }
            ModelOnlyEvidence::Deferred {
                phase,
                reason,
                adapter,
                expiry,
                ..
            } => {
                // The parser owns phase/reason construction; validation only
                // needs to retain those typed fields while checking the
                // remaining boundary.
                let _ = (phase, reason);
                self.validate_expiry(line, expiry);
                if let Some(adapter) = adapter {
                    self.validate_adapter_paths(line, adapter, false);
                }
            }
            ModelOnlyEvidence::Unavailable { expiry, .. } => {
                self.validate_expiry(line, expiry);
                self.validate_stable_fields(line, rule);
                self.validate_complete_gate(line, rule);
            }
        }
    }

    fn validate_owner(&mut self, line: usize, owner_field: &str) {
        let Some(canonical_repo) = self.repo_root.canonicalize().ok() else {
            self.error(line, "cannot resolve repository root for rust_owner");
            return;
        };
        let crates_root = canonical_repo.join("crates");
        let canonical_root = crates_root.canonicalize().ok();
        for owner in owner_field.split(';').map(str::trim) {
            let valid_name = !owner.is_empty()
                && owner != "-"
                && !owner.contains('/')
                && !owner.contains('\\')
                && owner != "."
                && owner != ".."
                && owner
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
            let owner_path = crates_root.join(owner);
            let valid = valid_name
                && owner_path.is_dir()
                && canonical_root.as_ref().is_some_and(|root| {
                    owner_path
                        .canonicalize()
                        .is_ok_and(|path| path.starts_with(root) && path.is_dir())
                });
            if !valid {
                self.error(
                    line,
                    format!("references missing in-repository rust_owner crate {owner}"),
                );
            }
        }
    }

    fn validate_expiry(&mut self, line: usize, expiry: &Expiry) {
        match expiry {
            Expiry::Date { value, day } if *day <= dates::unix_today() => self.error(
                line,
                format!("model-only status expiry {value} is today or in the past"),
            ),
            Expiry::Phase { value, index }
                if self.current_phase_index.is_none()
                    || *index <= self.current_phase_index.unwrap_or(usize::MAX) =>
            {
                self.error(
                    line,
                    format!(
                        "expiry milestone {value} is current or past relative to {}",
                        self.current_phase
                    ),
                );
            }
            _ => {}
        }
    }

    fn validate_complete_gate(&mut self, line: usize, rule: &Rule) {
        if !matches!(&rule.performance_gate, Gate::Fixed(_)) {
            self.error(line, "has no fixed sample/step performance gate");
        }
        if !rule.hostile_trace.contains("HT-") {
            self.error(line, "has no named hostile trace");
        }
        if mutation::definition(&rule.mutation_id).is_none() {
            self.error(
                line,
                format!("mutation {} is not in the typed registry", rule.mutation_id),
            );
        }
    }

    fn validate_stable_fields(&mut self, line: usize, rule: &Rule) {
        let mut fields = vec![
            rule.family.as_str(),
            rule.rule_summary.as_str(),
            rule.readable_model.as_str(),
            rule.invariant.as_str(),
            rule.witnesses.as_str(),
            rule.hostile_trace.as_str(),
            rule.mutation_id.as_str(),
            rule.mbt_model.as_str(),
            rule.rust_owner.as_str(),
            rule.bdd_tags.as_str(),
        ];
        if let Gate::Fixed(gate) = &rule.performance_gate {
            fields.push(gate.description.as_str());
        }
        rule.evidence.append_stable_fields(&mut fields);
        for field in fields {
            if has_deferred_marker(field) {
                self.error(
                    line,
                    format!(
                        "{} evidence contains a deferred/placeholder marker",
                        rule.evidence.status()
                    ),
                );
                break;
            }
        }
    }

    fn validate_adapter_paths(
        &mut self,
        line: usize,
        adapter: &AdapterEvidence,
        check_symbols: bool,
    ) {
        for display in &adapter.paths {
            let Some(path) = self.confined_adapter(line, display) else {
                continue;
            };
            if check_symbols {
                let source = match fs::read_to_string(&path) {
                    Ok(source) => source,
                    Err(_) => {
                        self.error(line, format!("could not read {display}"));
                        continue;
                    }
                };
                let symbols = SourceSymbols::parse(&source);
                self.validate_symbols(
                    line,
                    &symbols,
                    display,
                    SymbolRequirements {
                        actions: &adapter.actions,
                        states: &adapter.states,
                        kind: SymbolKind::Adapter,
                    },
                );
            }
        }
    }

    fn read_model_source(&mut self, line: usize, path: &Path, display: &str) -> Option<String> {
        let bytes = self.read_model_bytes(line, path, display)?;
        match String::from_utf8(bytes) {
            Ok(source) => Some(source),
            Err(_) => {
                self.error(line, format!("readable model {display} is not valid UTF-8"));
                None
            }
        }
    }

    fn read_model_bytes(&mut self, line: usize, path: &Path, display: &str) -> Option<Vec<u8>> {
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(_) => {
                self.error(line, format!("could not read readable model {display}"));
                return None;
            }
        };
        if !metadata.is_file() {
            self.error(
                line,
                format!("readable model is not a regular file {display}"),
            );
            return None;
        }
        if metadata.len() > MAX_MODEL_BYTES {
            self.error(
                line,
                format!("readable model {display} exceeds {MAX_MODEL_BYTES}-byte limit"),
            );
            return None;
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        let read_result = fs::File::open(path)
            .and_then(|file| file.take(MAX_MODEL_BYTES + 1).read_to_end(&mut bytes));
        if read_result.is_err() {
            self.error(line, format!("could not read readable model {display}"));
            return None;
        }
        if bytes.len() as u64 > MAX_MODEL_BYTES {
            self.error(
                line,
                format!("readable model {display} exceeds {MAX_MODEL_BYTES}-byte limit"),
            );
            return None;
        }
        Some(bytes)
    }

    fn validate_symbols(
        &mut self,
        line: usize,
        symbols: &SourceSymbols<'_>,
        display: &str,
        requirements: SymbolRequirements<'_>,
    ) {
        let is_adapter = matches!(requirements.kind, SymbolKind::Adapter);
        for action in requirements.actions {
            let present = if is_adapter {
                symbols.has_identifier(action)
            } else {
                symbols.has_model_symbol("action", action)
            };
            if !present {
                self.error(
                    line,
                    format!(
                        "{} {display} has no action symbol {action}",
                        if is_adapter { "adapter" } else { "model" }
                    ),
                );
            }
        }
        for state in requirements.states {
            let present = if is_adapter {
                symbols.has_identifier(state) || symbols.has_identifier(&camel_to_snake(state))
            } else {
                symbols.has_model_symbol("var", state)
            };
            if !present {
                self.error(
                    line,
                    format!(
                        "{} {display} has no state symbol {state}",
                        if is_adapter { "adapter" } else { "model" }
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
pub(super) fn performance_read_model_source(
    repo_root: &Path,
    path: &Path,
    display: &str,
) -> Option<String> {
    let mut checker = Checker::new(repo_root, "F-A1");
    checker.read_model_source(0, path, display)
}

fn has_deferred_marker(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    ["pending", "placeholder", "deferred"].iter().any(|marker| {
        lower.match_indices(marker).any(|(index, _)| {
            let before = index == 0 || !lower.as_bytes()[index - 1].is_ascii_alphanumeric();
            let end = index + marker.len();
            let after = lower
                .as_bytes()
                .get(end)
                .is_none_or(|byte| !byte.is_ascii_alphanumeric());
            before && after
        })
    })
}
