use std::fmt::Write;

use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    StorageItem, StorageValue,
    constants::{MAX_REGEX_LENGTH, MAX_REGEX_RULES},
};

#[derive(Debug, Error)]
pub enum PrivacyPolicyError {
    #[error("privacy policy version cannot be empty")]
    EmptyVersion,
    #[error("privacy policy regex rule is too long")]
    RegexTooLong,
    #[error("privacy policy has too many regex rules")]
    TooManyRegexRules,
    #[error("privacy policy regex is invalid: {0}")]
    InvalidRegex(#[from] regex::Error),
}

pub type PrivacyPolicyResult<T> = Result<T, PrivacyPolicyError>;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PrivacyPolicy {
    pub version: String,
    #[serde(default)]
    pub denied_key_names: Vec<String>,
    #[serde(default)]
    pub denied_key_prefixes: Vec<String>,
    #[serde(default)]
    pub denied_key_suffixes: Vec<String>,
    #[serde(default)]
    pub denied_exact_paths: Vec<String>,
    #[serde(default)]
    pub denied_path_regexes: Vec<String>,
    #[serde(default)]
    pub denied_value_regexes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivacyFilterReport {
    pub item: StorageItem,
    pub dropped_fields: u64,
    pub policy_version: String,
}

impl PrivacyPolicy {
    pub fn try_new(version: impl Into<String>) -> PrivacyPolicyResult<Self> {
        let policy = Self {
            version: version.into(),
            ..Self::default()
        };
        policy.validate()?;
        Ok(policy)
    }

    #[must_use]
    pub fn with_denied_key_name(mut self, value: impl Into<String>) -> Self {
        self.denied_key_names.push(value.into());
        self
    }

    #[must_use]
    pub fn with_denied_value_regex(mut self, value: impl Into<String>) -> Self {
        self.denied_value_regexes.push(value.into());
        self
    }

    pub fn validate(&self) -> PrivacyPolicyResult<()> {
        self.validated().map(|_| ())
    }

    /// Compiles and validates one immutable policy use.
    pub fn validated(&self) -> PrivacyPolicyResult<PrivacyPolicyMatcher<'_>> {
        if self.version.trim().is_empty() {
            return Err(PrivacyPolicyError::EmptyVersion);
        }
        if self.denied_path_regexes.len() + self.denied_value_regexes.len() > MAX_REGEX_RULES {
            return Err(PrivacyPolicyError::TooManyRegexRules);
        }
        Ok(PrivacyPolicyMatcher {
            policy: self,
            denied_path_regexes: compile_patterns(&self.denied_path_regexes)?,
            denied_value_regexes: compile_patterns(&self.denied_value_regexes)?,
        })
    }

    pub fn filter_item(&self, item: StorageItem) -> PrivacyPolicyResult<PrivacyFilterReport> {
        Ok(self.validated()?.filter_item(item))
    }
}

/// An immutable, per-use privacy-policy matcher with regexes compiled once.
#[derive(Debug)]
pub struct PrivacyPolicyMatcher<'a> {
    policy: &'a PrivacyPolicy,
    denied_path_regexes: Vec<Regex>,
    denied_value_regexes: Vec<Regex>,
}

impl PrivacyPolicyMatcher<'_> {
    #[must_use]
    pub fn filter_item(&self, item: StorageItem) -> PrivacyFilterReport {
        let mut dropped_fields = 0;
        let item = self.filter_map(item, &mut String::new(), &mut dropped_fields);
        PrivacyFilterReport {
            item,
            dropped_fields,
            policy_version: self.policy.version.clone(),
        }
    }

    fn filter_map(&self, item: StorageItem, path: &mut String, dropped: &mut u64) -> StorageItem {
        let mut filtered = StorageItem::with_capacity(item.len());
        for (key, value) in item {
            let parent_length = path.len();
            if parent_length > 0 {
                path.push('.');
            }
            path.push_str(&key);
            if self.denies_key(&key) {
                *dropped += 1;
            } else if let Some(value) = self.filter_value(value, path, dropped) {
                filtered.insert(key, value);
            }
            path.truncate(parent_length);
        }
        filtered
    }

    fn filter_value(
        &self,
        value: StorageValue,
        path: &mut String,
        dropped: &mut u64,
    ) -> Option<StorageValue> {
        if self.denies_path(path) {
            *dropped += 1;
            return None;
        }
        if let StorageValue::S(text) | StorageValue::N(text) | StorageValue::B(text) = &value
            && self.denies_value(text)
        {
            *dropped += 1;
            return None;
        }
        Some(match value {
            StorageValue::SS(values) => StorageValue::SS(self.filter_values(values, dropped)),
            StorageValue::NS(values) => StorageValue::NS(self.filter_values(values, dropped)),
            StorageValue::BS(values) => StorageValue::BS(self.filter_values(values, dropped)),
            StorageValue::L(values) => StorageValue::L(
                values
                    .into_iter()
                    .enumerate()
                    .filter_map(|(index, value)| {
                        let parent_length = path.len();
                        write!(path, "[{index}]").expect("writing a path to a String cannot fail");
                        let filtered = self.filter_value(value, path, dropped);
                        path.truncate(parent_length);
                        filtered
                    })
                    .collect(),
            ),
            StorageValue::M(values) => StorageValue::M(self.filter_map(values, path, dropped)),
            value => value,
        })
    }

    fn filter_values(&self, mut values: Vec<String>, dropped: &mut u64) -> Vec<String> {
        values.retain(|value| {
            if self.denies_value(value) {
                *dropped += 1;
                return false;
            }
            true
        });
        values
    }

    #[must_use]
    pub fn denies_key(&self, key: &str) -> bool {
        self.policy.denied_key_names.iter().any(|rule| rule == key)
            || self
                .policy
                .denied_key_prefixes
                .iter()
                .any(|rule| key.starts_with(rule))
            || self
                .policy
                .denied_key_suffixes
                .iter()
                .any(|rule| key.ends_with(rule))
    }

    #[must_use]
    pub fn denies_path(&self, path: &str) -> bool {
        self.policy
            .denied_exact_paths
            .iter()
            .any(|rule| rule == path)
            || self
                .denied_path_regexes
                .iter()
                .any(|pattern| pattern.is_match(path))
    }

    #[must_use]
    pub fn denies_value(&self, value: &str) -> bool {
        self.denied_value_regexes
            .iter()
            .any(|pattern| pattern.is_match(value))
    }
}

fn compile_patterns(patterns: &[String]) -> PrivacyPolicyResult<Vec<Regex>> {
    patterns
        .iter()
        .map(|pattern| {
            if pattern.len() > MAX_REGEX_LENGTH {
                return Err(PrivacyPolicyError::RegexTooLong);
            }
            Ok(Regex::new(pattern)?)
        })
        .collect()
}
