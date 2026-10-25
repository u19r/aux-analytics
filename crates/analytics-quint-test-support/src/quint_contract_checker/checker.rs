//! Inventory, model, mutation, and BDD coverage validation state.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use super::{
    CheckFailure, bdd, dates, evidence_parser,
    inventory::{RawInventoryRow, Rule},
    mutation, plan,
};
use crate::constants::EXPECTED_HEADER;

pub(super) struct Checker<'a> {
    pub(super) repo_root: &'a Path,
    pub(super) current_phase: &'a str,
    pub(super) current_phase_index: Option<usize>,
    pub(super) diagnostics: Vec<String>,
    pub(super) rule_ids: BTreeSet<String>,
    pub(super) models: BTreeSet<String>,
    pub(super) mutation_ids: BTreeMap<String, usize>,
    pub(super) rules: Vec<Rule>,
    pub(super) bdd_catalog: Option<bdd::BddCatalog>,
    pub(super) mapped_bdd_tags: BTreeSet<String>,
}

impl<'a> Checker<'a> {
    pub(super) fn validate_repository(
        repo_root: &'a Path,
        current_phase: &'a str,
    ) -> Result<Self, CheckFailure> {
        #[cfg(test)]
        super::test_support::record_validation_run();

        let mut checker = Checker::new(repo_root, current_phase);
        checker.check_inventory();
        checker.check_models_indexed();
        if checker.diagnostics.is_empty() {
            Ok(checker)
        } else {
            Err(CheckFailure {
                diagnostics: checker.diagnostics,
            })
        }
    }

    pub(super) fn new(repo_root: &'a Path, current_phase: &'a str) -> Self {
        let current_phase_index = dates::phase_index(current_phase);
        let diagnostics = if current_phase_index.is_none() {
            vec![format!(
                "unknown QUINT_CURRENT_PHASE {current_phase}; expected an ordered audit phase"
            )]
        } else {
            Vec::new()
        };
        Self {
            repo_root,
            current_phase,
            current_phase_index,
            diagnostics,
            rule_ids: BTreeSet::new(),
            models: BTreeSet::new(),
            mutation_ids: BTreeMap::new(),
            rules: Vec::new(),
            bdd_catalog: None,
            mapped_bdd_tags: BTreeSet::new(),
        }
    }

    pub(super) fn error(&mut self, line: usize, message: impl Into<String>) {
        self.diagnostics
            .push(format!("line {line} {}", message.into()));
    }

    fn check_inventory(&mut self) {
        let inventory = self.repo_root.join("specs/model-inventory.tsv");
        let source = match fs::read_to_string(&inventory) {
            Ok(source) => source,
            Err(_) => {
                self.diagnostics
                    .push("missing specs/model-inventory.tsv".to_owned());
                return;
            }
        };
        self.bdd_catalog = self.read_bdd_catalog();
        let mut lines = source.lines();
        if lines.next().unwrap_or_default() != EXPECTED_HEADER {
            self.diagnostics
                .push("inventory header is not the canonical 14-column header".to_owned());
        }
        for (offset, raw_line) in lines.enumerate() {
            let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
            if line.is_empty() {
                continue;
            }
            let fields: Vec<_> = line.split('\t').collect();
            let line_number = offset + 2;
            if fields.len() != 14 {
                self.error(line_number, format!("has {} columns", fields.len()));
                continue;
            }
            let raw = RawInventoryRow::from_fields(&fields);
            if raw.rule_id.is_empty() {
                self.error(line_number, "has empty rule_id");
                continue;
            }
            self.check_row(line_number, raw);
        }
        self.check_bdd_coverage();
        self.check_mutation_registry();
    }

    fn read_bdd_catalog(&mut self) -> Option<bdd::BddCatalog> {
        let feature = self
            .repo_root
            .join("crates/cucumber-tests/features/analytics.feature");
        let source = match fs::read_to_string(feature) {
            Ok(source) => source,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
            Err(error) => {
                self.diagnostics
                    .push(format!("could not read analytics.feature: {error}"));
                return None;
            }
        };
        match bdd::parse_feature(&source) {
            Ok(catalog) => Some(catalog),
            Err(errors) => {
                self.diagnostics.extend(errors);
                None
            }
        }
    }

    fn check_row(&mut self, line: usize, raw: RawInventoryRow) {
        self.check_bdd_tags(line, &raw.bdd_tags);
        if !self.rule_ids.insert(raw.rule_id.clone()) {
            self.error(line, format!("duplicates rule {}", raw.rule_id));
        }
        if mutation::is_mutation_id(&raw.mutation_id) {
            let count = self
                .mutation_ids
                .entry(raw.mutation_id.clone())
                .or_insert(0);
            *count += 1;
            if *count > 1 {
                self.error(line, format!("duplicates mutation {}", raw.mutation_id));
            }
        }
        self.models.insert(raw.readable_model.clone());
        self.validate_required_fields(line, &raw);
        let evidence = evidence_parser::parse(&raw.status, &raw.rust_adapter, &raw.mutation_id);
        if let Err(errors) = &evidence {
            for error in errors {
                self.error(line, error.clone());
            }
        }
        let gate = plan::parse_gate(&raw.performance_gate);
        if gate.is_none() {
            self.error(line, "has no recognized performance gate");
        }
        let (Some(gate), Ok(evidence)) = (gate, evidence) else {
            return;
        };
        let rule = Rule::from_raw(raw, gate, evidence);
        self.check_model_references(line, &rule);
        self.validate_rule(line, &rule);
        self.rules.push(rule);
    }

    fn validate_required_fields(&mut self, line: usize, raw: &RawInventoryRow) {
        for (name, value) in raw.all_fields() {
            if value.is_empty()
                || (value == "-"
                    && matches!(
                        name,
                        "rule_id"
                            | "rule_summary"
                            | "readable_model"
                            | "invariant"
                            | "hostile_trace"
                            | "mutation_id"
                            | "rust_owner"
                            | "performance_gate"
                            | "status"
                    ))
            {
                self.error(line, format!("has empty {name}"));
            }
        }
    }

    fn check_bdd_tags(&mut self, line: usize, raw: &str) {
        let tags = match bdd::parse_tags(raw) {
            Ok(tags) => tags,
            Err(errors) => {
                for error in errors {
                    self.error(line, error);
                }
                return;
            }
        };
        if tags.is_empty() {
            return;
        }
        let Some(catalog) = self.bdd_catalog.as_ref() else {
            self.error(
                line,
                "BDD tags cannot be validated without analytics.feature",
            );
            return;
        };
        self.mapped_bdd_tags.extend(tags.iter().cloned());
        let diagnostics = tags
            .into_iter()
            .flat_map(|tag| catalog.validate_tag(&tag))
            .collect::<Vec<_>>();
        for diagnostic in diagnostics {
            self.error(line, diagnostic);
        }
    }

    fn check_bdd_coverage(&mut self) {
        let Some(catalog) = self.bdd_catalog.as_ref() else {
            return;
        };
        let missing = catalog
            .required_scenario_tags()
            .filter(|tag| !self.mapped_bdd_tags.contains(*tag))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for tag in missing {
            self.diagnostics
                .push(format!("BDD tag {tag} has no inventory mapping"));
        }
    }

    fn check_mutation_registry(&mut self) {
        if self.mutation_ids.is_empty() {
            return;
        }
        for (mutation, count) in &self.mutation_ids {
            if *count != 1 || mutation::definition(mutation).is_none() {
                self.diagnostics.push(format!(
                    "mutation {mutation} must have exactly one inventory row and typed definition"
                ));
            }
        }
        for definition in crate::constants::MUTATIONS {
            if self
                .mutation_ids
                .get(definition.id)
                .copied()
                .unwrap_or_default()
                != 1
            {
                self.diagnostics.push(format!(
                    "typed mutation definition {} must have exactly one inventory row",
                    definition.id
                ));
            }
        }
        for rule in &self.rules {
            if let Some(definition) = mutation::definition(&rule.mutation_id)
                && definition.model != rule.readable_model
            {
                self.diagnostics.push(format!(
                    "mutation {} maps to {}, inventory says {}",
                    rule.mutation_id, definition.model, rule.readable_model
                ));
            }
        }
    }

    fn check_models_indexed(&mut self) {
        let directory = self.repo_root.join("specs");
        let mut models = Vec::new();
        let Ok(entries) = fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|extension| extension == "qnt")
                && let Ok(relative) = path.strip_prefix(self.repo_root)
            {
                models.push(relative.to_string_lossy().into_owned());
            }
        }
        models.sort();
        for model in models {
            if !self.models.contains(&model) {
                self.diagnostics
                    .push(format!("model {model} is not indexed"));
            }
        }
    }
}
