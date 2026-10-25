use std::{collections::BTreeMap, hint::black_box};

use analytics_contract::{AnalyticsManifest, StorageItem, StorageStreamRecord, StorageValue};
use analytics_fixtures::{user_item, users_manifest};
use storage_types::VecMap;

use crate::{AnalyticsEngine, AnalyticsEngineError, StreamRecordBatchItem};

#[test]
fn given_unselected_invalid_attribute_when_ingested_then_projection_ignores_it() {
    let mut manifest = users_manifest();
    manifest.tables[0].document_column = None;
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    engine.ensure_manifest(&manifest).unwrap();
    let mut item = user_item("1", "first@example.com", "org-a");
    item.insert("unselected".to_owned(), StorageValue::NULL(false));
    engine
        .ingest_stream_record_batch(&manifest, vec![batch_item("1", item)])
        .unwrap();
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT email, org_id FROM users")
            .unwrap(),
        vec![serde_json::json!({"email":"first@example.com", "org_id":"org-a"})]
    );
}

#[test]
fn given_selected_invalid_attribute_when_batch_ingested_then_no_rows_survive() {
    let mut manifest = users_manifest();
    manifest.tables[0].document_column = None;
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    engine.ensure_manifest(&manifest).unwrap();
    let mut invalid = user_item("2", "second@example.com", "org-b");
    invalid.insert(
        "profile".to_owned(),
        StorageValue::M(VecMap::from_iter([(
            "email".to_owned(),
            StorageValue::NULL(false),
        )])),
    );
    let error = engine
        .ingest_stream_record_batch(
            &manifest,
            vec![
                batch_item("1", user_item("1", "first@example.com", "org-a")),
                batch_item("2", invalid),
            ],
        )
        .unwrap_err();
    assert!(
        matches!(error, AnalyticsEngineError::AttributeConversion(ref name) if name == "email")
    );
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT count(*) AS count FROM users")
            .unwrap(),
        vec![serde_json::json!({"count":0})]
    );
}

fn batch_item(key: &str, item: StorageItem) -> StreamRecordBatchItem {
    StreamRecordBatchItem {
        analytics_table_name: "users".to_owned(),
        source_table_name: "tenant_01".to_owned(),
        shard_id: None,
        system_position: None,
        record_key: key.as_bytes().to_vec(),
        record: StorageStreamRecord {
            sequence_number: key.to_owned(),
            keys: StorageItem::new(),
            old_image: None,
            new_image: Some(item),
        },
    }
}

#[test]
#[ignore = "focused selected-map allocation comparison; run with --ignored --nocapture"]
fn given_wide_selected_values_when_json_lowered_then_intermediate_map_allocations_disappear() {
    let mut manifest: AnalyticsManifest = users_manifest();
    let table = &mut manifest.tables[0];
    table.document_column = None;
    table.projection_columns = None;
    table.projection_attribute_names = Some(vec!["labels".to_owned()]);
    let item = VecMap::from_iter([(
        "labels".to_owned(),
        StorageValue::L((0..512).map(|_| StorageValue::S("a".repeat(128))).collect()),
    )]);
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    let convert = |value: &StorageValue| value.to_json_value().unwrap();
    let baseline_guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "selected_map",
        file!(),
        line!(),
        Some("owned map"),
    );
    let mut selected = StorageItem::new();
    engine
        .for_each_projected_attribute(table, &item, |name, value| {
            selected.insert(name.to_owned(), value.clone());
            Ok(())
        })
        .unwrap();
    let baseline = selected
        .iter()
        .map(|(name, value)| (name.clone(), convert(value)))
        .collect::<BTreeMap<_, _>>();
    black_box(&baseline);
    let baseline_report = baseline_guard.finish();
    let candidate_guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "selected_map",
        file!(),
        line!(),
        Some("direct values"),
    );
    let mut candidate = BTreeMap::new();
    engine
        .for_each_projected_attribute(table, &item, |name, value| {
            candidate.insert(name.to_owned(), convert(value));
            Ok(())
        })
        .unwrap();
    black_box(&candidate);
    let candidate_report = candidate_guard.finish();
    assert_eq!(candidate, baseline);
    println!(
        "selected map: baseline={} allocations/{} bytes; candidate={} allocations/{} bytes",
        baseline_report.allocation_count,
        baseline_report.allocated_bytes,
        candidate_report.allocation_count,
        candidate_report.allocated_bytes
    );
    assert!(candidate_report.allocation_count < baseline_report.allocation_count);
    assert!(candidate_report.allocated_bytes < baseline_report.allocated_bytes);
}
