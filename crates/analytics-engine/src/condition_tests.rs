use analytics_contract::{StorageValue, TableRegistration};
use analytics_fixtures::generic_items_manifest;
use storage_types::VecMap;

use crate::{cache::EngineCaches, condition::item_matches_registration};

#[test]
fn given_attribute_alias_and_equality_condition_when_items_are_matched_then_only_the_expected_value_matches()
 {
    let mut table = test_table();
    table.condition_expression = Some("#entity_type = :entity_type".to_string());
    table.expression_attribute_names = Some(VecMap::from_iter([(
        "#entity_type".to_string(),
        "entity_type".to_string(),
    )]));
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":entity_type".to_string(),
        StorageValue::S("AUTHZ_ROLE".to_string()),
    )]));
    let matching = VecMap::from_iter([(
        "entity_type".to_string(),
        StorageValue::S("AUTHZ_ROLE".to_string()),
    )]);
    let other = VecMap::from_iter([(
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
fn given_entity_type_alias_when_compact_attribute_is_matched_then_condition_accepts_the_expected_value()
 {
    let mut table = test_table();
    table.condition_expression = Some("#entity_type = :entity_type".to_string());
    table.expression_attribute_names = Some(VecMap::from_iter([(
        "#entity_type".to_string(),
        "entity_type".to_string(),
    )]));
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":entity_type".to_string(),
        StorageValue::S("METRIC_WAL_BATCH".to_string()),
    )]));
    let compact = VecMap::from_iter([(
        "et".to_string(),
        StorageValue::S("METRIC_WAL_BATCH".to_string()),
    )]);

    assert!(
        item_matches_registration(&table, &compact, &mut EngineCaches::new())
            .expect("compact entity type condition")
    );
}

#[test]
fn given_string_and_numeric_comparisons_when_item_is_matched_then_dynamodb_condition_semantics_are_preserved()
 {
    let mut table = test_table();
    table.condition_expression = Some(
        "attribute_exists(org_id) AND org_id > :empty_string AND age >= :minimum_age".to_string(),
    );
    table.expression_attribute_values = Some(VecMap::from_iter([
        (":empty_string".to_string(), StorageValue::S(String::new())),
        (
            ":minimum_age".to_string(),
            StorageValue::N("18".to_string()),
        ),
    ]));
    let item = VecMap::from_iter([
        ("org_id".to_string(), StorageValue::S("org_1".to_string())),
        ("age".to_string(), StorageValue::N("21".to_string())),
    ]);

    assert!(item_matches_registration(&table, &item, &mut EngineCaches::new()).expect("condition"));
}

#[test]
fn given_cached_condition_when_expected_value_changes_then_previous_match_is_rejected() {
    let mut table = test_table();
    table.condition_expression = Some("status = :status".to_string());
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":status".to_string(),
        StorageValue::S("ready".to_string()),
    )]));
    let item = VecMap::from_iter([("status".to_string(), StorageValue::S("ready".to_string()))]);
    let mut caches = EngineCaches::new();

    assert!(item_matches_registration(&table, &item, &mut caches).expect("ready condition"));

    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":status".to_string(),
        StorageValue::S("paused".to_string()),
    )]));
    assert!(!item_matches_registration(&table, &item, &mut caches).expect("paused condition"));
}

#[test]
fn given_cached_condition_when_expression_alias_changes_then_cache_reparses() {
    let mut table = test_table();
    table.condition_expression = Some("#field = :value".to_string());
    table.expression_attribute_names = Some(VecMap::from_iter([(
        "#field".to_string(),
        "status".to_string(),
    )]));
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":value".to_string(),
        StorageValue::S("ready".to_string()),
    )]));
    let item = VecMap::from_iter([("status".to_string(), StorageValue::S("ready".to_string()))]);
    let mut caches = EngineCaches::new();

    assert!(item_matches_registration(&table, &item, &mut caches).expect("status condition"));

    table.expression_attribute_names = Some(VecMap::from_iter([(
        "#field".to_string(),
        "state".to_string(),
    )]));
    assert!(!item_matches_registration(&table, &item, &mut caches).expect("state condition"));
}

#[test]
fn given_nested_map_equality_condition_when_item_is_matched_then_nested_value_is_resolved() {
    let mut table = test_table();
    table.condition_expression = Some("profile.email = :email".to_string());
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":email".to_string(),
        StorageValue::S("ada@example.com".to_string()),
    )]));
    let item = VecMap::from_iter([(
        "profile".to_string(),
        StorageValue::M(VecMap::from_iter([(
            "email".to_string(),
            StorageValue::S("ada@example.com".to_string()),
        )])),
    )]);

    assert!(
        item_matches_registration(&table, &item, &mut EngineCaches::new())
            .expect("nested condition")
    );
}

#[test]
fn given_nested_and_missing_condition_paths_when_items_are_matched_then_selected_roots_match_full_image()
 {
    let mut table = test_table();
    table.condition_expression = Some(
        "NOT attribute_exists(missing) AND (profile.items[1] = :value OR entity_type = :value)"
            .to_string(),
    );
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":value".to_string(),
        StorageValue::S("ready".to_string()),
    )]));
    let mut item = VecMap::from_iter([
        ("et".to_string(), StorageValue::S("other".to_string())),
        (
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([(
                "items".to_string(),
                StorageValue::L(vec![
                    StorageValue::S("other".to_string()),
                    StorageValue::S("ready".to_string()),
                ]),
            )])),
        ),
    ]);
    let cached = crate::condition::performance_cached_condition(&table).expect("condition parses");
    let mut caches = EngineCaches::new();
    for expected in [true, false] {
        assert_eq!(
            item_matches_registration(&table, &item, &mut caches).expect("condition evaluates"),
            expected,
        );
        assert_eq!(
            crate::condition::performance_evaluate_cached_condition(&cached, &item),
            expected,
        );
        item.insert("missing".to_string(), StorageValue::NULL(true));
    }
}

#[test]
fn given_explicit_entity_type_when_compact_alias_disagrees_then_explicit_value_wins() {
    let mut table = test_table();
    table.condition_expression = Some("entity_type = :value".to_string());
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":value".to_string(),
        StorageValue::S("ready".to_string()),
    )]));
    let item = VecMap::from_iter([
        ("et".to_string(), StorageValue::S("ready".to_string())),
        (
            "entity_type".to_string(),
            StorageValue::S("other".to_string()),
        ),
    ]);
    assert!(
        !item_matches_registration(&table, &item, &mut EngineCaches::new())
            .expect("explicit condition")
    );
}

#[test]
fn given_large_unused_attribute_when_condition_runs_then_materialization_allocations_stay_constant()
{
    let mut table = test_table();
    table.condition_expression = Some("attribute_exists(status) AND status = :value".to_string());
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":value".to_string(),
        StorageValue::S("ready".to_string()),
    )]));
    let small = VecMap::from_iter([("status".to_string(), StorageValue::S("ready".to_string()))]);
    let mut large = small.clone();
    large.insert(
        "unused".to_string(),
        StorageValue::L(vec![StorageValue::S("x".repeat(1024)); 1024]),
    );
    let mut caches = EngineCaches::new();
    assert!(item_matches_registration(&table, &small, &mut caches).expect("warm condition"));
    let mut reports = Vec::new();
    for item in [&small, &large] {
        let guard = alloc_counter::AllocationGuard::start(
            module_path!(),
            "condition selected roots",
            file!(),
            line!(),
            None,
        );
        let matched = item_matches_registration(&table, item, &mut caches).expect("condition");
        let report = guard.finish();
        assert!(matched);
        reports.push(report);
    }
    assert_eq!(reports[0].allocation_count, reports[1].allocation_count);
    assert_eq!(reports[0].allocated_bytes, reports[1].allocated_bytes);
}

fn test_table() -> TableRegistration {
    generic_items_manifest()
        .tables
        .into_iter()
        .next()
        .expect("fixture table")
}
