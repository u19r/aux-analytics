use std::collections::HashMap;

use analytics_contract::{StorageItem, StorageValue, TableRegistration};
use analytics_fixtures::generic_items_manifest;
use storage_types::VecMap;

#[test]
#[ignore = "manual paired condition cache and attribute selection probe"]
fn given_cached_conditions_when_baseline_and_candidate_are_measured_then_outputs_match_and_costs_are_recorded()
 {
    let workload = condition_workload();
    let key = workload.table.analytics_table_name.clone();

    // Parse and insert both entries before measurement. The probes therefore
    // measure cache hits, condition evaluation, and full-image versus
    // selected-root conversion.
    let mut baseline_cache = BorrowedLruCache::new(crate::constants::CACHE_CAPACITY);
    baseline_cache.insert(
        key.clone(),
        crate::condition::performance_cached_condition(&workload.table).expect("condition parses"),
    );
    let _ = baseline_condition_probe(&workload.table, &workload.items[..1], &mut baseline_cache);
    let mut candidate_caches = crate::cache::EngineCaches::new();
    crate::condition::item_matches_registration(
        &workload.table,
        &workload.items[0],
        &mut candidate_caches,
    )
    .expect("condition evaluates");

    let result = super::measure_production_candidate(
        "condition cache hit evaluation",
        "condition.rs::shared_cache_with_selected_attributes",
        || baseline_condition_probe(&workload.table, &workload.items, &mut baseline_cache),
        || production_condition_probe(&workload.table, &workload.items, &mut candidate_caches),
    );
    super::print_production_candidate_results(
        "condition-cache-hit-evaluation-candidate",
        std::slice::from_ref(&result),
    );
    assert_eq!(result.name, "condition cache hit evaluation");
    assert_eq!(
        result.owner,
        "condition.rs::shared_cache_with_selected_attributes"
    );
}

struct ConditionWorkload {
    table: TableRegistration,
    items: Vec<StorageItem>,
}

fn condition_workload() -> ConditionWorkload {
    let mut table = generic_items_manifest()
        .tables
        .into_iter()
        .next()
        .expect("fixture table");
    table.condition_expression = Some("#tenant = :tenant".to_string());
    table.expression_attribute_names = Some(VecMap::from_iter([(
        "#tenant".to_string(),
        "tenant_id".to_string(),
    )]));
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":tenant".to_string(),
        StorageValue::S("tenant-a".to_string()),
    )]));
    ConditionWorkload {
        table,
        items: (0..super::REALISTIC_RECORD_COUNT)
            .map(|index| {
                let record = super::realistic_record(index);
                record.record.new_image.expect("fixture image")
            })
            .collect(),
    }
}

#[derive(Debug)]
struct BorrowedLruCache<V> {
    capacity: usize,
    tick: u64,
    entries: HashMap<String, BorrowedCacheEntry<V>>,
}

#[derive(Debug)]
struct BorrowedCacheEntry<V> {
    value: V,
    last_used: u64,
}

impl<V> BorrowedLruCache<V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            tick: 0,
            entries: HashMap::with_capacity(capacity.min(1024)),
        }
    }

    fn get_ref(&mut self, key: &str) -> Option<&V> {
        let entry = self.entries.get_mut(key)?;
        self.tick = self.tick.wrapping_add(1);
        entry.last_used = self.tick;
        Some(&entry.value)
    }

    fn insert(&mut self, key: String, value: V) {
        if self.capacity == 0 {
            return;
        }
        self.tick = self.tick.wrapping_add(1);
        self.entries.insert(
            key,
            BorrowedCacheEntry {
                value,
                last_used: self.tick,
            },
        );
        if self.entries.len() > self.capacity {
            self.evict_least_recently_used();
        }
    }

    fn evict_least_recently_used(&mut self) {
        let Some(key) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone())
        else {
            return;
        };
        self.entries.remove(&key);
    }
}

fn baseline_condition_probe(
    table: &TableRegistration,
    items: &[StorageItem],
    cache: &mut BorrowedLruCache<crate::condition::CachedCondition>,
) -> super::ProductionProbe {
    let mut digest = 1469598103934665603_u64;
    let mut matched = 0_u64;
    for item in items {
        let cached = cache
            .get_ref(table.analytics_table_name.as_str())
            .expect("warmed baseline condition cache");
        assert!(crate::condition::performance_cached_condition_matches(
            cached, table
        ));
        let result = crate::condition::performance_evaluate_cached_condition(cached, item);
        digest = super::digest_bytes_from(digest, &[u8::from(result)]);
        matched += u64::from(result);
    }
    super::ProductionProbe {
        output_digest: digest,
        serialized_bytes: matched,
    }
}

fn production_condition_probe(
    table: &TableRegistration,
    items: &[StorageItem],
    caches: &mut crate::cache::EngineCaches,
) -> super::ProductionProbe {
    let mut digest = 1469598103934665603_u64;
    let mut matched = 0_u64;
    for item in items {
        let result = crate::condition::item_matches_registration(table, item, caches)
            .expect("condition evaluates");
        digest = super::digest_bytes_from(digest, &[u8::from(result)]);
        matched += u64::from(result);
    }
    super::ProductionProbe {
        output_digest: digest,
        serialized_bytes: matched,
    }
}
