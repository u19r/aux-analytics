use std::collections::HashMap;

use crate::{PrivacyPolicy, PrivacyPolicyError, StorageItem, StorageValue};

#[test]
fn given_policy_with_denied_keys_when_checked_then_matching_keys_are_denied() {
    let policy = PrivacyPolicy::new("privacy-v1")
        .unwrap()
        .with_denied_key_name("email");

    assert!(policy.denies_key("email").unwrap());
    assert!(!policy.denies_key("display_name").unwrap());
}

#[test]
fn given_policy_with_value_regex_when_checked_then_matching_values_are_denied() {
    let policy = PrivacyPolicy::new("privacy-v1")
        .unwrap()
        .with_denied_value_regex("(?i)secret");

    assert!(policy.denies_value("my SECRET token").unwrap());
    assert!(!policy.denies_value("public").unwrap());
}

#[test]
fn given_invalid_regex_when_policy_validates_then_it_is_rejected() {
    let policy = PrivacyPolicy {
        version: "privacy-v1".to_string(),
        denied_value_regexes: vec!["[".to_string()],
        ..PrivacyPolicy::default()
    };

    assert!(matches!(
        policy.validate().unwrap_err(),
        PrivacyPolicyError::InvalidRegex(_)
    ));
}

#[test]
fn given_too_many_regex_rules_when_policy_validates_then_it_is_rejected() {
    let policy = PrivacyPolicy {
        version: "privacy-v1".to_string(),
        denied_value_regexes: vec!["secret".to_string(); 33],
        ..PrivacyPolicy::default()
    };

    assert!(matches!(
        policy.validate().unwrap_err(),
        PrivacyPolicyError::TooManyRegexRules
    ));
}

#[test]
fn given_policy_with_denied_nested_path_when_item_filtered_then_field_is_removed() {
    let policy = PrivacyPolicy {
        version: "privacy-v1".to_string(),
        denied_exact_paths: vec!["profile.email".to_string()],
        ..PrivacyPolicy::default()
    };
    let item = storage_item([(
        "profile",
        StorageValue::M(storage_item([
            ("email", StorageValue::S("ada@example.com".to_string())),
            ("name", StorageValue::S("Ada".to_string())),
        ])),
    )]);

    let report = policy.filter_item(&item).unwrap();

    assert_eq!(report.dropped_fields, 1);
    assert_eq!(report.policy_version, "privacy-v1");
    let Some(StorageValue::M(profile)) = report.item.get("profile") else {
        panic!("profile map missing");
    };
    assert!(!profile.contains_key("email"));
    assert!(profile.contains_key("name"));
}

#[test]
fn given_policy_with_denied_value_when_item_filtered_then_field_is_removed() {
    let policy = PrivacyPolicy::new("privacy-v1")
        .unwrap()
        .with_denied_value_regex("secret");
    let item = storage_item([
        ("token", StorageValue::S("secret-value".to_string())),
        ("name", StorageValue::S("Ada".to_string())),
    ]);

    let report = policy.filter_item(&item).unwrap();

    assert_eq!(report.dropped_fields, 1);
    assert!(!report.item.contains_key("token"));
    assert!(report.item.contains_key("name"));
}

#[test]
fn given_policy_with_denied_set_value_when_item_filtered_then_drop_count_is_reported() {
    let policy = PrivacyPolicy::new("privacy-v1")
        .unwrap()
        .with_denied_value_regex("secret");
    let item = storage_item([(
        "tags",
        StorageValue::SS(vec!["public".to_string(), "secret".to_string()]),
    )]);

    let report = policy.filter_item(&item).unwrap();

    assert_eq!(report.dropped_fields, 1);
    assert_eq!(
        report.item.get("tags"),
        Some(&StorageValue::SS(vec!["public".to_string()]))
    );
}

fn storage_item<const N: usize>(values: [(&str, StorageValue); N]) -> StorageItem {
    HashMap::from(values.map(|(key, value)| (key.to_string(), value)))
}
