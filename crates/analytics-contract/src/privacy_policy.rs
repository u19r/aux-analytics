use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{StorageItem, StorageValue};

const MAX_REGEX_RULES: usize = 32;
const MAX_REGEX_LENGTH: usize = 512;

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    pub fn new(version: impl Into<String>) -> PrivacyPolicyResult<Self> {
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
        if self.version.trim().is_empty() {
            return Err(PrivacyPolicyError::EmptyVersion);
        }
        let regex_count = self.denied_path_regexes.len() + self.denied_value_regexes.len();
        if regex_count > MAX_REGEX_RULES {
            return Err(PrivacyPolicyError::TooManyRegexRules);
        }
        for pattern in self
            .denied_path_regexes
            .iter()
            .chain(self.denied_value_regexes.iter())
        {
            if pattern.len() > MAX_REGEX_LENGTH {
                return Err(PrivacyPolicyError::RegexTooLong);
            }
            Regex::new(pattern)?;
        }
        Ok(())
    }

    pub fn denies_key(&self, key: &str) -> PrivacyPolicyResult<bool> {
        self.validate()?;
        Ok(self.denied_key_names.iter().any(|rule| rule == key)
            || self
                .denied_key_prefixes
                .iter()
                .any(|rule| key.starts_with(rule))
            || self
                .denied_key_suffixes
                .iter()
                .any(|rule| key.ends_with(rule)))
    }

    pub fn denies_path(&self, path: &str) -> PrivacyPolicyResult<bool> {
        self.validate()?;
        if self.denied_exact_paths.iter().any(|rule| rule == path) {
            return Ok(true);
        }
        for pattern in &self.denied_path_regexes {
            if Regex::new(pattern)?.is_match(path) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn denies_value(&self, value: &str) -> PrivacyPolicyResult<bool> {
        self.validate()?;
        for pattern in &self.denied_value_regexes {
            if Regex::new(pattern)?.is_match(value) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn filter_item(&self, item: &StorageItem) -> PrivacyPolicyResult<PrivacyFilterReport> {
        self.validate()?;
        let mut dropped_fields = 0_u64;
        let item = self.filter_map(item, "", &mut dropped_fields)?;
        Ok(PrivacyFilterReport {
            item,
            dropped_fields,
            policy_version: self.version.clone(),
        })
    }

    fn filter_map(
        &self,
        item: &StorageItem,
        parent_path: &str,
        dropped_fields: &mut u64,
    ) -> PrivacyPolicyResult<StorageItem> {
        let mut filtered = StorageItem::new();
        for (key, value) in item {
            let path = join_path(parent_path, key);
            if self.denies_key(key)? || self.denies_path(path.as_str())? {
                *dropped_fields += 1;
                continue;
            }
            if let Some(value) = self.filter_value(value, path.as_str(), dropped_fields)? {
                filtered.insert(key.clone(), value);
            }
        }
        Ok(filtered)
    }

    fn filter_value(
        &self,
        value: &StorageValue,
        path: &str,
        dropped_fields: &mut u64,
    ) -> PrivacyPolicyResult<Option<StorageValue>> {
        match value {
            StorageValue::S(value) => {
                if self.denies_value(value)? {
                    *dropped_fields += 1;
                    Ok(None)
                } else {
                    Ok(Some(StorageValue::S(value.clone())))
                }
            }
            StorageValue::N(value) => {
                if self.denies_value(value)? {
                    *dropped_fields += 1;
                    Ok(None)
                } else {
                    Ok(Some(StorageValue::N(value.clone())))
                }
            }
            StorageValue::B(value) => {
                if self.denies_value(value)? {
                    *dropped_fields += 1;
                    Ok(None)
                } else {
                    Ok(Some(StorageValue::B(value.clone())))
                }
            }
            StorageValue::SS(values) => {
                filter_string_list(values, dropped_fields, |value| self.denies_value(value))
            }
            StorageValue::NS(values) => {
                filter_number_list(values, dropped_fields, |value| self.denies_value(value))
            }
            StorageValue::BS(values) => {
                filter_binary_list(values, dropped_fields, |value| self.denies_value(value))
            }
            StorageValue::BOOL(value) => Ok(Some(StorageValue::BOOL(*value))),
            StorageValue::NULL(value) => Ok(Some(StorageValue::NULL(*value))),
            StorageValue::L(values) => {
                let mut filtered = Vec::new();
                for (index, value) in values.iter().enumerate() {
                    let path = format!("{path}[{index}]");
                    if let Some(value) = self.filter_value(value, path.as_str(), dropped_fields)? {
                        filtered.push(value);
                    }
                }
                Ok(Some(StorageValue::L(filtered)))
            }
            StorageValue::M(values) => Ok(Some(StorageValue::M(self.filter_map(
                values,
                path,
                dropped_fields,
            )?))),
        }
    }
}

fn join_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_string()
    } else {
        format!("{parent}.{key}")
    }
}

fn filter_string_list(
    values: &[String],
    dropped_fields: &mut u64,
    denies: impl Fn(&str) -> PrivacyPolicyResult<bool>,
) -> PrivacyPolicyResult<Option<StorageValue>> {
    Ok(Some(StorageValue::SS(filter_values(
        values,
        dropped_fields,
        denies,
    )?)))
}

fn filter_number_list(
    values: &[String],
    dropped_fields: &mut u64,
    denies: impl Fn(&str) -> PrivacyPolicyResult<bool>,
) -> PrivacyPolicyResult<Option<StorageValue>> {
    Ok(Some(StorageValue::NS(filter_values(
        values,
        dropped_fields,
        denies,
    )?)))
}

fn filter_binary_list(
    values: &[String],
    dropped_fields: &mut u64,
    denies: impl Fn(&str) -> PrivacyPolicyResult<bool>,
) -> PrivacyPolicyResult<Option<StorageValue>> {
    Ok(Some(StorageValue::BS(filter_values(
        values,
        dropped_fields,
        denies,
    )?)))
}

fn filter_values(
    values: &[String],
    dropped_fields: &mut u64,
    denies: impl Fn(&str) -> PrivacyPolicyResult<bool>,
) -> PrivacyPolicyResult<Vec<String>> {
    let mut filtered = Vec::new();
    for value in values {
        if denies(value)? {
            *dropped_fields += 1;
        } else {
            filtered.push(value.clone());
        }
    }
    Ok(filtered)
}
