use std::collections::HashMap;

use analytics_contract::{
    AnalyticsManifest, StorageItem, StorageStreamRecord, StorageValue, TableRegistration,
};
use serde_json::json;

use crate::source_routes::CompiledSourceRoutes;

#[test]
fn given_exact_and_prefix_routes_when_source_is_matched_then_only_intended_tables_are_returned() {
    let manifest = AnalyticsManifest::new(vec![
        registration("source_users", None, "users"),
        registration("tenant_template", Some("tenant_"), "events"),
        registration("source_users", None, "user_audit"),
    ]);
    let routes = CompiledSourceRoutes::from_manifest(&manifest).expect("compile routes");
    let record = record(None, Some(item("user")));

    assert_eq!(
        routes.analytics_tables("source_users", &record),
        vec!["users", "user_audit"]
    );
    assert_eq!(
        routes.analytics_tables("tenant_0123", &record),
        vec!["events"]
    );
    assert!(routes.analytics_tables("unregistered", &record).is_empty());
}

#[test]
fn given_many_registrations_when_one_entity_matches_then_record_is_not_sprayed() {
    let manifest = AnalyticsManifest::new(vec![
        conditional_registration("n", Some("n"), "users", "user"),
        conditional_registration("n", Some("n"), "memberships", "membership"),
        conditional_registration("n", Some("n"), "groups", "group"),
    ]);
    let routes = CompiledSourceRoutes::from_manifest(&manifest).expect("compile routes");

    assert_eq!(
        routes.analytics_tables("n00042", &record(None, Some(item("membership")))),
        vec!["memberships"]
    );
    assert!(
        routes
            .analytics_tables("n00042", &record(None, Some(item("unknown"))))
            .is_empty()
    );
}

#[test]
fn given_two_independently_matching_registrations_when_routed_then_both_are_selected() {
    let manifest = AnalyticsManifest::new(vec![
        conditional_registration("n", Some("n"), "users", "user"),
        conditional_registration("n", Some("n"), "user_audit", "user"),
    ]);
    let routes = CompiledSourceRoutes::from_manifest(&manifest).expect("compile routes");

    assert_eq!(
        routes.analytics_tables("n00042", &record(None, Some(item("user")))),
        vec!["users", "user_audit"]
    );
}

#[test]
fn given_entity_transition_when_routed_then_old_and_new_destinations_are_selected() {
    let manifest = AnalyticsManifest::new(vec![
        conditional_registration("n", Some("n"), "users", "user"),
        conditional_registration("n", Some("n"), "memberships", "membership"),
    ]);
    let routes = CompiledSourceRoutes::from_manifest(&manifest).expect("compile routes");

    assert_eq!(
        routes.analytics_tables(
            "n00042",
            &record(Some(item("user")), Some(item("membership")))
        ),
        vec!["users", "memberships"]
    );
    assert_eq!(
        routes.analytics_tables("n00042", &record(Some(item("user")), None)),
        vec!["users"]
    );
}

#[test]
fn given_invalid_registration_condition_when_compiled_then_startup_fails() {
    let mut invalid = conditional_registration("n", Some("n"), "users", "user");
    invalid.condition_expression = Some("entity_type =".to_string());

    let error = CompiledSourceRoutes::from_manifest(&AnalyticsManifest::new(vec![invalid]))
        .expect_err("invalid condition must fail");

    assert!(error.to_string().contains("condition is invalid"));
}

fn registration(
    source_table_name: &str,
    source_table_name_prefix: Option<&str>,
    analytics_table_name: &str,
) -> TableRegistration {
    serde_json::from_value(json!({
        "source_table_name": source_table_name,
        "source_table_name_prefix": source_table_name_prefix,
        "analytics_table_name": analytics_table_name
    }))
    .expect("registration")
}

fn conditional_registration(
    source_table_name: &str,
    source_table_name_prefix: Option<&str>,
    analytics_table_name: &str,
    entity_type: &str,
) -> TableRegistration {
    serde_json::from_value(json!({
        "source_table_name": source_table_name,
        "source_table_name_prefix": source_table_name_prefix,
        "analytics_table_name": analytics_table_name,
        "condition_expression": "#entity_type = :entity_type",
        "expression_attribute_names": {"#entity_type": "entity_type"},
        "expression_attribute_values": {":entity_type": {"S": entity_type}}
    }))
    .expect("conditional registration")
}

fn item(entity_type: &str) -> StorageItem {
    HashMap::from([("et".to_string(), StorageValue::S(entity_type.to_string()))])
}

fn record(old_image: Option<StorageItem>, new_image: Option<StorageItem>) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: HashMap::new(),
        sequence_number: "sequence".to_string(),
        old_image,
        new_image,
    }
}
