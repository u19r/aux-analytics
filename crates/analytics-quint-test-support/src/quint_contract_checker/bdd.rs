use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct BddCatalog {
    declarations: BTreeMap<String, BddDeclaration>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct BddDeclaration {
    line: usize,
    count: usize,
    scenario: bool,
}

impl BddCatalog {
    pub(super) fn validate_tag(&self, tag: &str) -> Vec<String> {
        let Some((count, is_scenario)) = self.declaration(tag) else {
            return vec![format!(
                "BDD tag {tag} is not declared in analytics.feature"
            )];
        };
        let mut diagnostics = Vec::new();
        if count != 1 {
            diagnostics.push(format!(
                "BDD tag {tag} is declared {count} times in analytics.feature"
            ));
        }
        if !is_scenario {
            diagnostics.push(format!(
                "BDD tag {tag} is not attached to a Scenario in analytics.feature"
            ));
        }
        diagnostics
    }

    pub(super) fn declaration(&self, tag: &str) -> Option<(usize, bool)> {
        self.declarations
            .get(tag)
            .map(|declaration| (declaration.count, declaration.scenario))
    }

    pub(super) fn required_scenario_tags(&self) -> impl Iterator<Item = &str> {
        self.declarations.iter().filter_map(|(tag, declaration)| {
            (declaration.scenario && is_required_scenario_tag(tag)).then_some(tag.as_str())
        })
    }
}

pub(super) fn parse_tags(raw: &str) -> Result<Vec<String>, Vec<String>> {
    if raw.trim() == "-" {
        return Ok(Vec::new());
    }

    let mut tags = Vec::new();
    let mut seen = BTreeSet::new();
    let mut errors = Vec::new();
    for value in raw.split(',').map(str::trim) {
        if value.is_empty() {
            errors.push("BDD tags have an empty comma element".to_owned());
            continue;
        }
        if !is_valid_tag(value) {
            errors.push(format!("malformed BDD tag {value}"));
            continue;
        }
        if !seen.insert(value.to_owned()) {
            errors.push(format!("BDD tags duplicate {value}"));
            continue;
        }
        tags.push(value.to_owned());
    }
    if tags.is_empty() && errors.is_empty() {
        errors.push("BDD tags are empty; use '-' when no scenario applies".to_owned());
    }
    if errors.is_empty() {
        Ok(tags)
    } else {
        Err(errors)
    }
}

pub(super) fn parse_feature(source: &str) -> Result<BddCatalog, Vec<String>> {
    let mut declarations = BTreeMap::new();
    let mut pending = Vec::new();
    let mut errors = Vec::new();

    for (offset, raw_line) in source.lines().enumerate() {
        let line = offset + 1;
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed.starts_with('@') {
            for tag in trimmed.split_whitespace() {
                if !is_valid_tag(tag) {
                    errors.push(format!("feature line {line} has malformed BDD tag {tag}"));
                    continue;
                }
                pending.push((tag.to_owned(), line));
            }
            continue;
        }

        let target_is_scenario =
            trimmed.starts_with("Scenario:") || trimmed.starts_with("Scenario Outline:");
        let is_feature = trimmed.starts_with("Feature:");
        if target_is_scenario || is_feature {
            register_feature_tags(&mut declarations, &mut pending, target_is_scenario);
            continue;
        }

        reject_unattached_tags(&mut pending, &mut errors);
    }
    reject_unattached_tags(&mut pending, &mut errors);

    if errors.is_empty() {
        Ok(BddCatalog { declarations })
    } else {
        Err(errors)
    }
}

fn reject_unattached_tags(pending: &mut Vec<(String, usize)>, errors: &mut Vec<String>) {
    for (tag, line) in pending.drain(..) {
        errors.push(format!(
            "BDD tag {tag} on feature line {line} is not attached to a Feature or Scenario"
        ));
    }
}

fn register_feature_tags(
    declarations: &mut BTreeMap<String, BddDeclaration>,
    pending: &mut Vec<(String, usize)>,
    target_is_scenario: bool,
) {
    for (tag, line) in pending.drain(..) {
        let declaration = declarations.entry(tag).or_insert(BddDeclaration {
            line,
            count: 0,
            scenario: false,
        });
        declaration.count += 1;
        declaration.scenario |= target_is_scenario;
    }
}

fn is_valid_tag(tag: &str) -> bool {
    let Some(value) = tag.strip_prefix('@') else {
        return false;
    };
    if value.is_empty() {
        return false;
    }
    let mut previous_was_separator = false;
    for (index, byte) in value.bytes().enumerate() {
        let valid = byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-';
        if !valid || (index == 0 && !byte.is_ascii_alphabetic()) {
            return false;
        }
        if byte == b'_' || byte == b'-' {
            if previous_was_separator {
                return false;
            }
            previous_was_separator = true;
        } else {
            previous_was_separator = false;
        }
    }
    !previous_was_separator
}

fn is_required_scenario_tag(tag: &str) -> bool {
    let Some(value) = tag.strip_prefix("@F") else {
        return false;
    };
    let Some((family, scenario)) = value.split_once("_S") else {
        return false;
    };
    matches!(family, "101" | "102" | "103")
        && scenario.len() == 3
        && scenario.bytes().all(|byte| byte.is_ascii_digit())
}
