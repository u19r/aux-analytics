use super::{evidence::CoverageEvidence, plan::Gate};

#[derive(Debug, Clone)]
pub(super) struct RawInventoryRow {
    pub(super) rule_id: String,
    pub(super) family: String,
    pub(super) rule_summary: String,
    pub(super) readable_model: String,
    pub(super) invariant: String,
    pub(super) witnesses: String,
    pub(super) hostile_trace: String,
    pub(super) mutation_id: String,
    pub(super) mbt_model: String,
    pub(super) rust_owner: String,
    pub(super) rust_adapter: String,
    pub(super) bdd_tags: String,
    pub(super) performance_gate: String,
    pub(super) status: String,
}

impl RawInventoryRow {
    pub(super) fn from_fields(fields: &[&str]) -> Self {
        let mut values = fields.iter().map(|value| (*value).to_owned());
        Self {
            rule_id: values.next().unwrap_or_default(),
            family: values.next().unwrap_or_default(),
            rule_summary: values.next().unwrap_or_default(),
            readable_model: values.next().unwrap_or_default(),
            invariant: values.next().unwrap_or_default(),
            witnesses: values.next().unwrap_or_default(),
            hostile_trace: values.next().unwrap_or_default(),
            mutation_id: values.next().unwrap_or_default(),
            mbt_model: values.next().unwrap_or_default(),
            rust_owner: values.next().unwrap_or_default(),
            rust_adapter: values.next().unwrap_or_default(),
            bdd_tags: values.next().unwrap_or_default(),
            performance_gate: values.next().unwrap_or_default(),
            status: values.next().unwrap_or_default(),
        }
    }

    pub(super) fn all_fields(&self) -> [(&str, &str); 14] {
        [
            ("rule_id", &self.rule_id),
            ("family", &self.family),
            ("rule_summary", &self.rule_summary),
            ("readable_model", &self.readable_model),
            ("invariant", &self.invariant),
            ("witnesses", &self.witnesses),
            ("hostile_trace", &self.hostile_trace),
            ("mutation_id", &self.mutation_id),
            ("mbt_model", &self.mbt_model),
            ("rust_owner", &self.rust_owner),
            ("rust_adapter", &self.rust_adapter),
            ("bdd_tags", &self.bdd_tags),
            ("performance_gate", &self.performance_gate),
            ("status", &self.status),
        ]
    }
}

#[derive(Debug, Clone)]
pub(super) struct Rule {
    pub(super) rule_id: String,
    pub(super) family: String,
    pub(super) rule_summary: String,
    pub(super) readable_model: String,
    pub(super) invariant: String,
    pub(super) witnesses: String,
    pub(super) hostile_trace: String,
    pub(super) mutation_id: String,
    pub(super) mbt_model: String,
    pub(super) rust_owner: String,
    pub(super) bdd_tags: String,
    pub(super) performance_gate: Gate,
    pub(super) evidence: CoverageEvidence,
}

impl Rule {
    pub(super) fn from_raw(
        raw: RawInventoryRow,
        performance_gate: Gate,
        evidence: CoverageEvidence,
    ) -> Self {
        Self {
            rule_id: raw.rule_id,
            family: raw.family,
            rule_summary: raw.rule_summary,
            readable_model: raw.readable_model,
            invariant: raw.invariant,
            witnesses: raw.witnesses,
            hostile_trace: raw.hostile_trace,
            mutation_id: raw.mutation_id,
            mbt_model: raw.mbt_model,
            rust_owner: raw.rust_owner,
            bdd_tags: raw.bdd_tags,
            performance_gate,
            evidence,
        }
    }
}
