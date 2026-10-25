//! Registered mutation transforms and their typed contract validation.

use super::super::{
    inventory::Rule,
    symbols::{DeclarationRole, SourceSymbols},
};
use crate::constants::MUTATIONS;

pub(in crate::quint_contract_checker) fn is_mutation_id(value: &str) -> bool {
    value.starts_with("M-")
        && value.len() > 2
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MutationDefinition {
    pub(crate) id: &'static str,
    pub(crate) model: &'static str,
    pub(crate) needle: &'static str,
    pub(crate) replacement: &'static str,
    pub(crate) contract: Option<MutationContract>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(in crate::quint_contract_checker) struct SourceTransform {
    pub(in crate::quint_contract_checker) needle: &'static str,
    pub(in crate::quint_contract_checker) replacement: &'static str,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct MutationContract {
    pub(crate) invariant: &'static str,
    pub(crate) witness: &'static str,
}

impl From<&MutationDefinition> for SourceTransform {
    fn from(mutation: &MutationDefinition) -> Self {
        Self {
            needle: mutation.needle,
            replacement: mutation.replacement,
        }
    }
}

pub(in crate::quint_contract_checker) fn validate_typed_contract(
    rule: &Rule,
    mutation: &MutationDefinition,
) -> Result<(), String> {
    let Some(contract) = mutation.contract else {
        return Ok(());
    };
    if rule.mutation_id != mutation.id {
        return Err(format!(
            "{} is not bound to inventory mutation {}",
            mutation.id, rule.mutation_id
        ));
    }
    if rule.readable_model != mutation.model {
        return Err(format!(
            "{} targets inventory model {}, typed contract requires {}",
            mutation.id, rule.readable_model, mutation.model
        ));
    }
    if rule.invariant != contract.invariant {
        return Err(format!(
            "{} targets invariant {}, inventory requests {}",
            mutation.id, contract.invariant, rule.invariant
        ));
    }
    if !rule
        .witnesses
        .split_whitespace()
        .any(|witness| witness == contract.witness)
    {
        return Err(format!(
            "{} witness {} is not indexed for {}",
            mutation.id, contract.witness, rule.readable_model
        ));
    }
    Ok(())
}

pub(in crate::quint_contract_checker) fn apply_transform(
    source: &str,
    transform: SourceTransform,
    mutation_id: &str,
) -> Result<String, String> {
    if transform.needle.is_empty() {
        return Err(format!("{mutation_id} has no source transform"));
    }
    let matches = source.match_indices(transform.needle).count();
    if matches != 1 {
        return Err(format!(
            "{mutation_id} expected one guard target, found {matches}"
        ));
    }
    Ok(source.replacen(transform.needle, transform.replacement, 1))
}

/// Check the declaration containing a registered transform target.
///
/// A mutation must exercise behavior, not change an invariant or a pure oracle
/// that is used to calculate the expected result. Missing targets are left to
/// `apply_transform`, which reports the exact source-match failure.
pub(in crate::quint_contract_checker) fn validate_transform_role(
    source: &str,
    symbols: &SourceSymbols<'_>,
    mutation: &MutationDefinition,
) -> Result<(), String> {
    let transform = SourceTransform::from(mutation);
    if transform.needle.is_empty() {
        return Ok(());
    }
    let mut matches = source.match_indices(transform.needle);
    let Some((offset, _)) = matches.next() else {
        return Ok(());
    };
    if matches.next().is_some() {
        return Err(format!(
            "{} has more than one transform target; declaration role is ambiguous",
            mutation.id
        ));
    }
    let Some(declaration) = symbols.find_declaration_at(offset) else {
        return Err(format!(
            "{} transform target at byte {offset} is not inside a recognized module declaration",
            mutation.id
        ));
    };
    if declaration.role == DeclarationRole::Oracle {
        return Err(format!(
            "{} transform targets invariant/oracle declaration {} at byte {}; registered \
             mutations must target behavior/action declarations",
            mutation.id, declaration.name, declaration.start
        ));
    }
    Ok(())
}

pub(in crate::quint_contract_checker) fn definition(
    id: &str,
) -> Option<&'static MutationDefinition> {
    MUTATIONS.iter().find(|mutation| mutation.id == id)
}

#[cfg(test)]
pub(in crate::quint_contract_checker) fn validate_registered_transform(
    source: &str,
    mutation: &MutationDefinition,
) -> Result<(), String> {
    validate_transform_role(source, &SourceSymbols::parse(source), mutation)
}
