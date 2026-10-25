use super::dates::Expiry;

#[derive(Debug, Clone)]
pub(super) enum CoverageEvidence {
    Covered(AdapterEvidence),
    ModelOnly(ModelOnlyEvidence),
    NotApplicable(NotApplicableEvidence),
}

impl CoverageEvidence {
    pub(super) fn append_stable_fields<'a>(&'a self, fields: &mut Vec<&'a str>) {
        match self {
            Self::Covered(adapter) => adapter.append_stable_fields(fields),
            Self::ModelOnly(ModelOnlyEvidence::Current {
                reason, adapter, ..
            }) => {
                fields.push(reason);
                adapter.append_stable_fields(fields);
            }
            Self::NotApplicable(evidence) => {
                fields.extend([evidence.reason.as_str(), evidence.proof.as_str()])
            }
            Self::ModelOnly(ModelOnlyEvidence::Unavailable {
                reason,
                actions,
                states,
                ..
            }) => {
                fields.push(reason);
                fields.extend(actions.iter().chain(states).map(String::as_str));
            }
            Self::ModelOnly(ModelOnlyEvidence::Deferred { .. }) => {}
        }
    }

    pub(super) fn status(&self) -> &'static str {
        match self {
            Self::Covered(_) => "covered",
            Self::ModelOnly(_) => "model-only",
            Self::NotApplicable(_) => "not-applicable",
        }
    }

    pub(super) fn is_runnable(&self) -> bool {
        matches!(self, Self::Covered(_) | Self::NotApplicable(_))
    }

    pub(super) fn adapter_paths(&self) -> &[String] {
        match self {
            Self::Covered(adapter)
            | Self::ModelOnly(ModelOnlyEvidence::Current { adapter, .. }) => &adapter.paths,
            Self::ModelOnly(ModelOnlyEvidence::Deferred {
                adapter: Some(adapter),
                ..
            }) => &adapter.paths,
            Self::ModelOnly(ModelOnlyEvidence::Deferred { adapter: None, .. })
            | Self::ModelOnly(ModelOnlyEvidence::Unavailable { .. })
            | Self::NotApplicable(_) => &[],
        }
    }

    pub(super) fn model_symbols(&self) -> Option<(&[String], &[String])> {
        match self {
            Self::Covered(adapter)
            | Self::ModelOnly(ModelOnlyEvidence::Current { adapter, .. }) => {
                Some((&adapter.actions, &adapter.states))
            }
            Self::ModelOnly(ModelOnlyEvidence::Deferred {
                adapter: Some(adapter),
                ..
            }) => Some((&adapter.actions, &adapter.states)),
            Self::ModelOnly(ModelOnlyEvidence::Unavailable {
                actions, states, ..
            }) => Some((actions, states)),
            Self::ModelOnly(ModelOnlyEvidence::Deferred { adapter: None, .. })
            | Self::NotApplicable(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct AdapterEvidence {
    pub(super) paths: Vec<String>,
    pub(super) actions: Vec<String>,
    pub(super) states: Vec<String>,
    pub(super) proof: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) enum ModelOnlyEvidence {
    Current {
        reason: String,
        adapter: AdapterEvidence,
        expiry: Expiry,
    },
    Deferred {
        phase: String,
        reason: String,
        adapter: Option<AdapterEvidence>,
        expiry: Expiry,
    },
    Unavailable {
        reason: String,
        actions: Vec<String>,
        states: Vec<String>,
        expiry: Expiry,
    },
}

#[derive(Debug, Clone)]
pub(super) struct NotApplicableEvidence {
    pub(super) reason: String,
    pub(super) proof: String,
}

impl AdapterEvidence {
    fn append_stable_fields<'a>(&'a self, fields: &mut Vec<&'a str>) {
        fields.extend(self.actions.iter().chain(&self.states).map(String::as_str));
        fields.extend(self.proof.as_deref());
    }
}
