use analytics_contract::{StorageItem, StorageValue, TenantSelector};
use analytics_fixtures::generic_items_manifest;

use super::TenantResolver;
use crate::AnalyticsEngineError;

#[test]
fn given_table_name_selector_when_resolving_other_source_then_actual_source_is_returned() {
    let mut table = generic_items_manifest().tables.remove(0);
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::TableName;
    assert_eq!(
        TenantResolver::default()
            .resolve(&table, "actual-source", &StorageItem::new())
            .expect("tenant"),
        Some("actual-source".to_string())
    );
}

#[test]
fn given_fixed_tenant_when_selector_attribute_is_absent_then_registered_tenant_wins() {
    let mut table = generic_items_manifest().tables.remove(0);
    table.tenant_id = Some("registered".to_string());
    table.tenant_selector = TenantSelector::Attribute {
        attribute_name: "missing".to_string(),
    };
    assert_eq!(
        TenantResolver::default()
            .resolve(&table, "actual", &StorageItem::new())
            .expect("tenant"),
        Some("registered".to_string())
    );
}

#[test]
fn given_regex_selector_when_resolving_multiple_images_then_each_capture_is_returned() {
    let mut table = generic_items_manifest().tables.remove(0);
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::AttributeRegex {
        attribute_name: "key".to_string(),
        regex: "^(?P<tenant>[^#]+)#".to_string(),
        capture: "tenant".to_string(),
    };
    let resolver = TenantResolver::default();
    for tenant in ["first", "second"] {
        let item =
            StorageItem::from_iter([("key".to_string(), StorageValue::S(format!("{tenant}#row")))]);
        assert_eq!(
            resolver.resolve(&table, "actual", &item).expect("tenant"),
            Some(tenant.to_string())
        );
    }
    let item = StorageItem::from_iter([(
        "key".to_string(),
        StorageValue::S("without-delimiter".to_string()),
    )]);
    assert!(matches!(
        resolver.resolve(&table, "actual", &item),
        Err(AnalyticsEngineError::RegexNoMatch { .. })
    ));
}

#[test]
fn given_prefix_selector_when_attribute_is_missing_then_resolution_fails_closed() {
    let mut table = generic_items_manifest().tables.remove(0);
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::TableNameOrPartitionKeyPrefix {
        attribute_name: "key".to_string(),
    };
    assert!(matches!(
        TenantResolver::default().resolve(&table, "actual", &StorageItem::new()),
        Err(AnalyticsEngineError::MissingTenant)
    ));
    let item =
        StorageItem::from_iter([("key".to_string(), StorageValue::S("ns_123#row".to_string()))]);
    assert_eq!(
        TenantResolver::default()
            .resolve(&table, "actual", &item)
            .expect("tenant"),
        Some("t_123".to_string())
    );
}

#[test]
fn given_no_tenant_selector_when_resolving_image_then_tenant_remains_absent() {
    let mut table = generic_items_manifest().tables.remove(0);
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::None;
    assert_eq!(
        TenantResolver::default()
            .resolve(&table, "actual", &StorageItem::new())
            .expect("tenant"),
        None
    );
}

#[test]
fn given_shared_resolver_when_sources_resolve_concurrently_then_source_identity_stays_isolated() {
    let mut table = generic_items_manifest().tables.remove(0);
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::TableName;
    let resolver = TenantResolver::default();
    std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            resolver
                .resolve(&table, "first", &StorageItem::new())
                .expect("first tenant")
        });
        let second = scope.spawn(|| {
            resolver
                .resolve(&table, "second", &StorageItem::new())
                .expect("second tenant")
        });
        assert_eq!(
            first.join().expect("first thread"),
            Some("first".to_string())
        );
        assert_eq!(
            second.join().expect("second thread"),
            Some("second".to_string())
        );
    });
}
