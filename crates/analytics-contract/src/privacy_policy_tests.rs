use serde_json::json;

use crate::{PrivacyPolicy, PrivacyPolicyError, StorageItem, StorageValue};

#[test]
fn given_policy_with_denied_keys_when_checked_then_matching_keys_are_denied() {
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .unwrap()
        .with_denied_key_name("email");

    assert!(policy.validated().unwrap().denies_key("email"));
    assert!(!policy.validated().unwrap().denies_key("display_name"));
}

#[test]
fn given_policy_with_value_regex_when_checked_then_matching_values_are_denied() {
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .unwrap()
        .with_denied_value_regex("(?i)secret");

    assert!(policy.validated().unwrap().denies_value("my SECRET token"));
    assert!(!policy.validated().unwrap().denies_value("public"));
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

    let report = policy.filter_item(item).unwrap();

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
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .unwrap()
        .with_denied_value_regex("secret");
    let item = storage_item([
        ("token", StorageValue::S("secret-value".to_string())),
        ("name", StorageValue::S("Ada".to_string())),
    ]);

    let report = policy.filter_item(item).unwrap();

    assert_eq!(report.dropped_fields, 1);
    assert!(!report.item.contains_key("token"));
    assert!(report.item.contains_key("name"));
}

#[test]
fn given_valid_policy_when_compiled_then_filtering_matches_one_off_use() {
    let mut policy = PrivacyPolicy::try_new("privacy-v1")
        .unwrap()
        .with_denied_key_name("email")
        .with_denied_value_regex("(?i)secret");
    policy.denied_exact_paths.push("profile.token".to_string());
    let item = storage_item([
        ("email", StorageValue::S("private@example.test".to_string())),
        (
            "profile",
            StorageValue::M(storage_item([
                ("token", StorageValue::S("secret".to_string())),
                ("name", StorageValue::S("public".to_string())),
            ])),
        ),
    ]);
    let matcher = policy.validated().expect("validated matcher");

    assert_eq!(
        policy.filter_item(item.clone()).expect("one-off filter"),
        matcher.filter_item(item)
    );
}

#[test]
fn given_policy_with_denied_set_value_when_item_filtered_then_drop_count_is_reported() {
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .unwrap()
        .with_denied_value_regex("secret");
    let item = storage_item([(
        "tags",
        StorageValue::SS(vec!["public".to_string(), "secret".to_string()]),
    )]);

    let report = policy.filter_item(item).unwrap();

    assert_eq!(report.dropped_fields, 1);
    assert_eq!(
        report.item.get("tags"),
        Some(&StorageValue::SS(vec!["public".to_string()]))
    );
}

#[test]
fn given_privacy_policy_with_unknown_field_when_deserialized_then_it_is_rejected() {
    let policy = json!({
        "version": "privacy-v1",
        "legacy_mode": true
    });

    assert!(serde_json::from_value::<PrivacyPolicy>(policy).is_err());
}

#[test]
fn given_privacy_policy_schema_when_generated_then_unknown_fields_are_rejected() {
    let schema = serde_json::to_value(schemars::schema_for!(PrivacyPolicy)).expect("schema");
    let definition = schema
        .get("$defs")
        .and_then(|definitions| definitions.get("PrivacyPolicy"))
        .unwrap_or(&schema);

    assert_eq!(definition["additionalProperties"], json!(false));
}

fn storage_item<const N: usize>(values: [(&str, StorageValue); N]) -> StorageItem {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

#[test]
fn given_different_policy_versions_when_matchers_filter_then_reports_use_their_own_rules_and_version()
 {
    let first = PrivacyPolicy::try_new("first")
        .unwrap()
        .with_denied_key_name("email");
    let second = PrivacyPolicy::try_new("second")
        .unwrap()
        .with_denied_value_regex("secret");
    let item = storage_item([
        ("email", StorageValue::S("public".to_string())),
        ("token", StorageValue::S("secret".to_string())),
    ]);
    let first_report = first.validated().unwrap().filter_item(item.clone());
    let second_report = second.validated().unwrap().filter_item(item);
    assert_eq!(first_report.policy_version, "first");
    assert_eq!(second_report.policy_version, "second");
    assert_eq!(first_report.dropped_fields, 1);
    assert_eq!(second_report.dropped_fields, 1);
    assert!(!first_report.item.contains_key("email"));
    assert!(first_report.item.contains_key("token"));
    assert!(second_report.item.contains_key("email"));
    assert!(!second_report.item.contains_key("token"));
}

#[test]
fn given_denied_list_and_nested_paths_when_owned_item_is_filtered_then_original_indexes_and_sibling_paths_are_preserved()
 {
    let policy = PrivacyPolicy {
        version: "privacy".to_owned(),
        denied_exact_paths: vec![
            "events[0]".to_owned(),
            "events[1].token".to_owned(),
            "tail.secret".to_owned(),
        ],
        ..PrivacyPolicy::default()
    };
    let item = storage_item([
        (
            "events",
            StorageValue::L(vec![
                StorageValue::M(storage_item([(
                    "name",
                    StorageValue::S("removed".to_owned()),
                )])),
                StorageValue::M(storage_item([
                    ("token", StorageValue::S("removed".to_owned())),
                    ("name", StorageValue::S("retained".to_owned())),
                ])),
            ]),
        ),
        (
            "tail",
            StorageValue::M(storage_item([
                ("secret", StorageValue::S("removed".to_owned())),
                ("name", StorageValue::S("retained".to_owned())),
            ])),
        ),
        ("token", StorageValue::S("retained".to_owned())),
    ]);
    let report = policy.filter_item(item).expect("filter");
    assert_eq!(report.dropped_fields, 3);
    assert_eq!(
        report.item,
        storage_item([
            (
                "events",
                StorageValue::L(vec![StorageValue::M(storage_item([(
                    "name",
                    StorageValue::S("retained".to_owned())
                )]))])
            ),
            (
                "tail",
                StorageValue::M(storage_item([(
                    "name",
                    StorageValue::S("retained".to_owned())
                )]))
            ),
            ("token", StorageValue::S("retained".to_owned())),
        ])
    );
}

#[test]
#[ignore = "paired consuming privacy allocation probe; prepare inputs outside guards"]
fn given_owned_nested_items_when_privacy_filter_consumes_them_then_output_matches_with_fewer_copies()
 {
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .unwrap()
        .with_denied_key_name("secret");
    let matcher = policy.validated().unwrap();
    let fixture = StorageItem::from_iter([
        ("secret".to_owned(), StorageValue::S("remove".to_owned())),
        (
            "events".to_owned(),
            StorageValue::L(
                (0..64)
                    .map(|_| {
                        StorageValue::M(StorageItem::from_iter([
                            ("secret".to_owned(), StorageValue::S("remove".to_owned())),
                            ("label".to_owned(), StorageValue::S("public".repeat(32))),
                        ]))
                    })
                    .collect(),
            ),
        ),
    ]);
    for pair in 0..8 {
        let mut results = Vec::new();
        for consumes_input in [pair % 2 == 0, pair % 2 != 0] {
            let inputs = vec![fixture.clone(); 128];
            let guard = alloc_counter::AllocationGuard::start(
                module_path!(),
                "consuming privacy filter",
                file!(),
                line!(),
                None,
            );
            let reports = if consumes_input {
                inputs
                    .into_iter()
                    .map(|item| matcher.filter_item(item))
                    .collect::<Vec<_>>()
            } else {
                inputs
                    .iter()
                    .map(|item| matcher.filter_item(item.clone()))
                    .collect::<Vec<_>>()
            };
            std::hint::black_box(&reports);
            results.push((consumes_input, reports, guard.finish()));
        }
        let baseline = results.iter().find(|(consumes, _, _)| !consumes).unwrap();
        let candidate = results.iter().find(|(consumes, _, _)| *consumes).unwrap();
        assert_eq!(baseline.1, candidate.1);
        assert!(candidate.1.iter().all(|report| report.dropped_fields == 65));
        assert!(candidate.2.allocation_count < baseline.2.allocation_count);
        assert!(candidate.2.allocated_bytes < baseline.2.allocated_bytes);
        println!(
            "consuming-privacy | pair={pair} | rows=128 | baseline_allocations={} | \
             candidate_allocations={} | baseline_bytes={} | candidate_bytes={}",
            baseline.2.allocation_count,
            candidate.2.allocation_count,
            baseline.2.allocated_bytes,
            candidate.2.allocated_bytes,
        );
    }
}
