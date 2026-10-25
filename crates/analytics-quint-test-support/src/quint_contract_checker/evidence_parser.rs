use super::{
    dates::Expiry,
    evidence::{AdapterEvidence, CoverageEvidence, ModelOnlyEvidence, NotApplicableEvidence},
    evidence_tokens::{
        named_proof_present, parse_expiry, pending_token, split_paths, valid_nested_proof,
    },
};

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
enum EvidenceKind {
    #[default]
    Ordinary,
    Current,
    Deferred,
    Unavailable,
    NotApplicable,
}

#[derive(Debug, Default)]
struct EvidenceBuilder {
    kind: EvidenceKind,
    deferred_phase: Option<String>,
    reason: Option<String>,
    adapter_paths: Vec<String>,
    actions: Option<Vec<String>>,
    states: Option<Vec<String>>,
    proof: Option<String>,
    expiry: Option<Expiry>,
}

impl EvidenceBuilder {
    fn parse(raw: &str, mutation: &str) -> Result<Self, Vec<String>> {
        let mut builder = Self::default();
        let mut errors = Vec::new();
        if raw.is_empty() {
            errors.push("rust_adapter metadata is empty".to_owned());
            return Err(errors);
        }
        for (index, field) in raw.split(';').map(str::trim).enumerate() {
            if field.is_empty() {
                errors.push("rust_adapter metadata has an empty field".to_owned());
                continue;
            }
            if index == 0 && builder.parse_leading(field, &mut errors) {
                continue;
            }
            if field.contains('=') && builder.parse_marker(field, &mut errors) {
                continue;
            }
            if let Some(path) = field.strip_prefix("crates/") {
                builder.adapter_paths.push(format!("crates/{path}"));
                continue;
            }
            if builder.parse_expiry_field(field, &mut errors)
                || builder.parse_pending_field(field, &mut errors)
            {
                continue;
            }
            if matches!(
                builder.kind,
                EvidenceKind::Current | EvidenceKind::NotApplicable
            ) {
                errors.push(format!("unknown metadata field {field}"));
            } else {
                builder.add_reason(field);
            }
        }
        builder.apply_mutation_phase(mutation, &mut errors);
        if errors.is_empty() {
            Ok(builder)
        } else {
            Err(errors)
        }
    }

    fn parse_expiry_field(&mut self, field: &str, errors: &mut Vec<String>) -> bool {
        if !field.starts_with("expiry") {
            return false;
        }
        if self.expiry.is_some() {
            errors.push("rust_adapter metadata duplicates expiry".to_owned());
        } else if let Some(expiry) = parse_expiry(field) {
            self.expiry = Some(expiry);
        } else {
            errors.push(format!("malformed expiry condition {field}"));
        }
        true
    }

    fn parse_pending_field(&mut self, field: &str, errors: &mut Vec<String>) -> bool {
        let Some((phase, malformed)) = pending_token(field) else {
            return false;
        };
        if malformed {
            errors.push("malformed current/deferred reason token".to_owned());
        } else if phase == "U-A1" {
            errors.push("current U-A1 reason token must lead metadata".to_owned());
        } else {
            self.set_deferred(phase, errors);
        }
        true
    }

    fn parse_leading(&mut self, field: &str, errors: &mut Vec<String>) -> bool {
        let mut words = field.split_whitespace();
        match (words.next(), words.next()) {
            (Some("pending"), Some(phase)) if matches!(phase, "U-A1" | "F-A2" | "F-A3") => {
                self.set_pending_reason(phase, words, errors);
                true
            }
            (Some("pending"), Some(phase))
                if phase.starts_with("U-A") || phase.starts_with("F-A") =>
            {
                errors.push(format!("malformed pending reason token {phase}"));
                true
            }
            (Some("pending"), _) => {
                errors.push("unknown leading pending reason".to_owned());
                true
            }
            (Some("not-applicable"), None) => {
                self.kind = EvidenceKind::NotApplicable;
                self.add_reason("not-applicable");
                true
            }
            (Some(first), _) if first.starts_with("crates/") => {
                self.adapter_paths.push(field.to_owned());
                true
            }
            (Some("NONE"), _) => {
                self.kind = EvidenceKind::Unavailable;
                let reason = field.strip_prefix("NONE").unwrap_or_default().trim();
                if !reason.is_empty() {
                    self.add_reason(reason);
                }
                true
            }
            _ => false,
        }
    }

    fn set_pending_reason(
        &mut self,
        phase: &str,
        words: std::str::SplitWhitespace<'_>,
        errors: &mut Vec<String>,
    ) {
        self.kind = if phase == "U-A1" {
            EvidenceKind::Current
        } else {
            EvidenceKind::Deferred
        };
        if phase != "U-A1" {
            self.deferred_phase = Some(phase.to_owned());
        }
        let reason = words.collect::<Vec<_>>().join(" ");
        if reason.is_empty() {
            errors.push(format!("{phase} reason is empty"));
        } else {
            self.add_reason(&reason);
        }
    }

    fn parse_marker(&mut self, field: &str, errors: &mut Vec<String>) -> bool {
        let Some((key, value)) = field.split_once('=') else {
            return false;
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() {
            errors.push(format!("malformed metadata marker {field}"));
            return true;
        }
        if value.contains('=') && (key != "proof" || !valid_nested_proof(value)) {
            errors.push(format!("malformed metadata marker {field}"));
            return true;
        }
        match key {
            "adapter" => {
                if !self.adapter_paths.is_empty() {
                    errors.push("duplicate adapter marker".to_owned());
                } else {
                    let paths = split_paths(value);
                    if value.is_empty() || paths.is_empty() {
                        errors.push("adapter marker has no path".to_owned());
                    } else if paths.iter().any(String::is_empty) {
                        errors.push("adapter marker has an empty comma element".to_owned());
                    }
                    self.adapter_paths.extend(paths);
                }
            }
            "actions" => Self::set_list("actions", value, &mut self.actions, errors),
            "state" => Self::set_list("state", value, &mut self.states, errors),
            "proof" => {
                if self.proof.is_some() {
                    errors.push("duplicate proof marker".to_owned());
                } else {
                    self.proof = Some(value.to_owned());
                }
            }
            _ => errors.push(format!("unknown metadata marker {key}")),
        }
        true
    }

    fn set_list(
        marker: &str,
        value: &str,
        target: &mut Option<Vec<String>>,
        errors: &mut Vec<String>,
    ) {
        if target.is_some() {
            errors.push(format!("duplicate {marker} marker"));
            return;
        }
        let parts = value.split(',').map(str::trim).collect::<Vec<_>>();
        if value.is_empty() || parts.iter().any(|value| value.is_empty()) {
            errors.push(format!("{marker} has an empty comma element"));
        }
        *target = Some(parts.into_iter().map(str::to_owned).collect());
    }

    fn add_reason(&mut self, reason: &str) {
        if reason.is_empty() {
            return;
        }
        match &mut self.reason {
            Some(current) => {
                current.push_str("; ");
                current.push_str(reason);
            }
            None => self.reason = Some(reason.to_owned()),
        }
    }

    fn set_deferred(&mut self, phase: String, errors: &mut Vec<String>) {
        match self.kind {
            EvidenceKind::Current => {
                errors.push(format!("current U-A1 metadata contains deferred {phase}"));
            }
            EvidenceKind::Ordinary => {
                self.kind = EvidenceKind::Deferred;
                self.deferred_phase = Some(phase.clone());
            }
            EvidenceKind::Deferred if self.deferred_phase.as_deref() != Some(phase.as_str()) => {
                errors.push("mixed deferred phases".to_owned());
            }
            _ => {}
        }
        self.add_reason(&format!("pending {phase}"));
    }

    fn apply_mutation_phase(&mut self, mutation: &str, errors: &mut Vec<String>) {
        let Some(phase) = mutation.strip_prefix("PENDING-") else {
            return;
        };
        if !matches!(phase, "F-A2" | "F-A3") {
            return;
        }
        match self.kind {
            EvidenceKind::Ordinary => {
                self.kind = EvidenceKind::Deferred;
                self.deferred_phase = Some(phase.to_owned());
                self.add_reason(&format!("pending {phase}"));
            }
            EvidenceKind::Deferred if self.deferred_phase.as_deref() != Some(phase) => {
                errors.push("mixed deferred mutation and metadata phases".to_owned());
            }
            _ => {}
        }
    }

    fn adapter(
        &mut self,
        errors: &mut Vec<String>,
        require_proof: bool,
    ) -> Option<AdapterEvidence> {
        if self.adapter_paths.is_empty() {
            errors.push("missing adapter evidence".to_owned());
            errors.push("adapter evidence has no path".to_owned());
        }
        let actions = self.actions.take().unwrap_or_default();
        if actions.is_empty() {
            errors.push("adapter evidence has no actions".to_owned());
        }
        let states = self.states.take().unwrap_or_default();
        if states.is_empty() {
            errors.push("adapter evidence has no state".to_owned());
        }
        if require_proof && self.proof.as_deref().is_none_or(str::is_empty) {
            errors.push("covered evidence has no proof".to_owned());
        }
        (!self.adapter_paths.is_empty() && !actions.is_empty() && !states.is_empty()).then(|| {
            AdapterEvidence {
                paths: std::mem::take(&mut self.adapter_paths),
                actions,
                states,
                proof: self.proof.take(),
            }
        })
    }

    fn finish(self, status: &str, errors: Vec<String>) -> Result<CoverageEvidence, Vec<String>> {
        match status {
            "covered" => self.finish_covered(errors),
            "model-only" => self.finish_model_only(errors),
            "not-applicable" => self.finish_not_applicable(errors),
            other => Err(vec![format!("unknown status {other}")]),
        }
    }

    fn finish_covered(mut self, mut errors: Vec<String>) -> Result<CoverageEvidence, Vec<String>> {
        if self.kind != EvidenceKind::Ordinary {
            errors.push("covered status has a pending/exception evidence kind".to_owned());
        }
        if self.expiry.is_some() {
            errors.push("covered status has an expiry".to_owned());
        }
        let adapter = self.adapter(&mut errors, true);
        adapter
            .filter(|_| errors.is_empty())
            .map(CoverageEvidence::Covered)
            .ok_or(errors)
    }

    fn finish_model_only(
        mut self,
        mut errors: Vec<String>,
    ) -> Result<CoverageEvidence, Vec<String>> {
        let Some(expiry) = self.expiry.take() else {
            errors.push("model-only status has no expiry condition".to_owned());
            return Err(errors);
        };
        let evidence = self.build_model_only(expiry, &mut errors);
        if self.reason.as_deref().is_none_or(str::is_empty) {
            errors.push("model-only status has no reason".to_owned());
        }
        evidence
            .filter(|_| errors.is_empty())
            .map(CoverageEvidence::ModelOnly)
            .ok_or(errors)
    }

    fn build_model_only(
        &mut self,
        expiry: Expiry,
        errors: &mut Vec<String>,
    ) -> Option<ModelOnlyEvidence> {
        match self.kind {
            EvidenceKind::Current => {
                let adapter = self.adapter(errors, false)?;
                Some(ModelOnlyEvidence::Current {
                    reason: self.reason.clone().unwrap_or_default(),
                    adapter,
                    expiry,
                })
            }
            EvidenceKind::Deferred => {
                let Some(phase) = self.deferred_phase.take() else {
                    errors.push("deferred model-only status has no phase".to_owned());
                    return None;
                };
                let adapter = self.deferred_adapter(errors);
                Some(ModelOnlyEvidence::Deferred {
                    phase,
                    reason: self.reason.clone().unwrap_or_default(),
                    adapter,
                    expiry,
                })
            }
            EvidenceKind::Unavailable => Some(self.build_unavailable(expiry, errors)),
            EvidenceKind::Ordinary | EvidenceKind::NotApplicable => {
                errors.push(
                    "model-only status requires typed pending or unavailable evidence".to_owned(),
                );
                None
            }
        }
    }

    fn build_unavailable(&mut self, expiry: Expiry, errors: &mut Vec<String>) -> ModelOnlyEvidence {
        if !self.adapter_paths.is_empty() {
            errors.push("unavailable model-only status claims an adapter".to_owned());
        }
        let actions = self.actions.take().unwrap_or_default();
        let states = self.states.take().unwrap_or_default();
        if actions.is_empty() {
            errors.push("unavailable model-only status has no actions evidence".to_owned());
        }
        if states.is_empty() {
            errors.push("unavailable model-only status has no state evidence".to_owned());
        }
        ModelOnlyEvidence::Unavailable {
            reason: self.reason.clone().unwrap_or_default(),
            actions,
            states,
            expiry,
        }
    }

    fn finish_not_applicable(
        self,
        mut errors: Vec<String>,
    ) -> Result<CoverageEvidence, Vec<String>> {
        if self.kind != EvidenceKind::NotApplicable {
            errors.push("not-applicable status lacks not-applicable evidence".to_owned());
        }
        let Some(proof) = self.proof else {
            errors.push("not-applicable status lacks proof".to_owned());
            return Err(errors);
        };
        if !named_proof_present(&proof) {
            errors.push("not-applicable proof is not named".to_owned());
        }
        if !errors.is_empty() {
            return Err(errors);
        }
        Ok(CoverageEvidence::NotApplicable(NotApplicableEvidence {
            reason: self.reason.unwrap_or_default(),
            proof,
        }))
    }

    fn deferred_adapter(&mut self, errors: &mut Vec<String>) -> Option<AdapterEvidence> {
        let contains_mapping =
            self.actions.is_some() || self.states.is_some() || self.proof.is_some();
        if !contains_mapping {
            return None;
        }
        self.adapter(errors, false)
    }
}

pub(super) fn parse(
    status: &str,
    raw_adapter: &str,
    mutation: &str,
) -> Result<CoverageEvidence, Vec<String>> {
    let builder = EvidenceBuilder::parse(raw_adapter, mutation)?;
    builder.finish(status, Vec::new())
}
