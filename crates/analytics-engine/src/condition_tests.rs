use std::collections::{BTreeMap, HashMap};

use analytics_contract::{StorageValue, TableRegistration};
use analytics_fixtures::generic_items_manifest;

use crate::{cache::EngineCaches, condition::item_matches_registration};

#[test]
fn condition_expression_matches_alias_equality() {
    let mut table = test_table();
    table.condition_expression = Some("#entity_type = :entity_type".to_string());
    table.expression_attribute_names = Some(HashMap::from([(
        "#entity_type".to_string(),
        "entity_type".to_string(),
    )]));
    table.expression_attribute_values = Some(BTreeMap::from([(
        ":entity_type".to_string(),
        StorageValue::S("AUTHZ_ROLE".to_string()),
    )]));
    let matching = HashMap::from([(
        "entity_type".to_string(),
        StorageValue::S("AUTHZ_ROLE".to_string()),
    )]);
    let other = HashMap::from([(
        "entity_type".to_string(),
        StorageValue::S("AUTHZ_PERMISSION".to_string()),
    )]);

    let mut caches = EngineCaches::new();
    assert!(item_matches_registration(&table, &matching, &mut caches).expect("matching condition"));
    assert!(
        !item_matches_registration(&table, &other, &mut caches).expect("non-matching condition")
    );
}

#[test]
fn condition_expression_supports_dynamodb_comparison_semantics() {
    let mut table = test_table();
    table.condition_expression = Some(
        "attribute_exists(org_id) AND org_id > :empty_string AND age >= :minimum_age".to_string(),
    );
    table.expression_attribute_values = Some(BTreeMap::from([
        (":empty_string".to_string(), StorageValue::S(String::new())),
        (
            ":minimum_age".to_string(),
            StorageValue::N("18".to_string()),
        ),
    ]));
    let item = HashMap::from([
        ("org_id".to_string(), StorageValue::S("org_1".to_string())),
        ("age".to_string(), StorageValue::N("21".to_string())),
    ]);

    assert!(item_matches_registration(&table, &item, &mut EngineCaches::new()).expect("condition"));
}

fn test_table() -> TableRegistration {
    generic_items_manifest()
        .tables
        .into_iter()
        .next()
        .expect("fixture table")
}
