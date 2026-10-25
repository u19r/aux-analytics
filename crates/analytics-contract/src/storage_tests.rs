use std::{
    collections::{BTreeMap, HashMap},
    hint::black_box,
    time::Instant,
};

use serde_json::json;
use storage_types::VecMap;

use crate::{
    StorageItem, StorageStreamRecord, StorageValue, StorageValueJsonError,
    constants::CANONICAL_PROBE_ITERATIONS,
};

#[test]
fn given_storage_number_when_converted_to_json_then_numeric_values_stay_numeric() {
    assert_eq!(
        StorageValue::N("42".to_string()).to_json_value(),
        Ok(json!(42))
    );
    assert_eq!(
        StorageValue::N("not-a-number".to_string()).to_json_value(),
        Ok(json!("not-a-number"))
    );
}

#[test]
fn given_storage_sets_when_converted_to_json_then_array_values_are_preserved() {
    assert_eq!(
        StorageValue::NS(vec!["1".to_string(), "two".to_string()]).to_json_value(),
        Ok(json!([1, "two"]))
    );
    assert_eq!(
        StorageValue::SS(vec!["a".to_string(), "b".to_string()]).to_json_value(),
        Ok(json!(["a", "b"]))
    );
}

#[test]
fn given_false_null_marker_when_converted_to_json_then_value_is_rejected() {
    assert_eq!(
        StorageValue::NULL(false).to_json_value(),
        Err(StorageValueJsonError::UnsupportedNullFalse)
    );
}

#[test]
fn given_storage_map_when_converted_to_json_then_keys_are_emitted_in_stable_order() {
    let value = StorageValue::M(VecMap::from_iter([
        ("b".to_string(), StorageValue::S("second".to_string())),
        ("a".to_string(), StorageValue::S("first".to_string())),
    ]));

    assert_eq!(
        value.to_json_value(),
        Ok(json!({"a": "first", "b": "second"}))
    );
    assert_eq!(
        value.to_json_value().unwrap().to_string(),
        r#"{"a":"first","b":"second"}"#
    );
}

#[test]
fn given_storage_items_with_different_insertion_order_when_compared_then_they_are_equal() {
    let first: StorageItem = [
        ("first".to_string(), StorageValue::S("one".to_string())),
        ("second".to_string(), StorageValue::N("2".to_string())),
    ]
    .into_iter()
    .collect();
    let second: StorageItem = [
        ("second".to_string(), StorageValue::N("2".to_string())),
        ("first".to_string(), StorageValue::S("one".to_string())),
    ]
    .into_iter()
    .collect();

    assert_eq!(first, second);
}

#[test]
fn given_all_storage_value_variants_when_round_tripped_then_dynamo_json_is_preserved() {
    let item = compact_item([
        ("s", StorageValue::S("text".to_string())),
        ("n", StorageValue::N("42.00".to_string())),
        ("b", StorageValue::B("Ymlu".to_string())),
        (
            "ss",
            StorageValue::SS(vec!["a".to_string(), "b".to_string()]),
        ),
        (
            "ns",
            StorageValue::NS(vec!["1".to_string(), "2".to_string()]),
        ),
        ("bs", StorageValue::BS(vec!["YQ==".to_string()])),
        ("bool", StorageValue::BOOL(true)),
        ("null", StorageValue::NULL(false)),
        (
            "list",
            StorageValue::L(vec![StorageValue::S("nested".to_string())]),
        ),
        (
            "map",
            StorageValue::M(compact_item([(
                "inner",
                StorageValue::S("value".to_string()),
            )])),
        ),
    ]);

    let encoded = serde_json::to_value(&item).expect("storage item serializes");
    let decoded =
        serde_json::from_value::<StorageItem>(encoded.clone()).expect("storage item deserializes");

    assert_eq!(decoded, item);
    assert_eq!(encoded["map"]["M"]["inner"]["S"], json!("value"));
    assert_eq!(encoded["null"]["NULL"], json!(false));
}

#[test]
fn given_nested_stream_record_when_canonical_json_is_written_then_wire_bytes_are_stable() {
    let record = StorageStreamRecord {
        keys: VecMap::from_iter([
            ("z".to_string(), StorageValue::S("last".to_string())),
            (
                "a".to_string(),
                StorageValue::M(VecMap::from_iter([(
                    "inner".to_string(),
                    StorageValue::BOOL(true),
                )])),
            ),
        ]),
        sequence_number: "sequence-1".to_string(),
        old_image: None,
        new_image: Some(VecMap::from_iter([(
            "list".to_string(),
            StorageValue::L(vec![
                StorageValue::N("1.2300".to_string()),
                StorageValue::NULL(true),
            ]),
        )])),
    };
    let mut actual = Vec::new();

    record.write_canonical_json(&mut actual);

    assert_eq!(
        actual,
        br#"{"Keys":{"a":{"M":{"inner":{"BOOL":true}}},"z":{"S":"last"}},"NewImage":{"list":{"L":[{"N":"1.2300"},{"NULL":true}]}},"SequenceNumber":"sequence-1"}"#
    );
    let mut baseline = Vec::new();
    legacy_write_canonical_json(&record, &mut baseline);
    assert_eq!(actual, baseline);
}

#[test]
#[ignore = "release-only storage canonical allocation and CPU probe"]
fn given_storage_records_when_canonicalized_then_vecmap_reduces_hashmap_work() {
    let records = canonical_probe_records();
    for _ in 0..2 {
        let baseline =
            measure_canonical_path(&records, "hashmap-baseline", legacy_write_canonical_json);
        let candidate =
            measure_canonical_path(&records, "vecmap-candidate", compact_write_canonical_json);
        assert_eq!(baseline.checksum, candidate.checksum);
        assert_eq!(baseline.bytes, candidate.bytes);
    }

    let baseline =
        measure_canonical_path(&records, "hashmap-baseline", legacy_write_canonical_json);
    let candidate =
        measure_canonical_path(&records, "vecmap-candidate", compact_write_canonical_json);
    assert_eq!(baseline.checksum, candidate.checksum);
    assert_eq!(baseline.bytes, candidate.bytes);
    println!(
        "storage-canonical | records={} | iterations={} | baseline={baseline:?} | \
         candidate={candidate:?}",
        records.len(),
        CANONICAL_PROBE_ITERATIONS
    );
    assert!(candidate.allocations < baseline.allocations);
    assert!(candidate.allocated_bytes < baseline.allocated_bytes);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalProbeSample {
    checksum: u64,
    bytes: usize,
    elapsed_ns: u128,
    allocations: u64,
    allocated_bytes: u64,
}

fn canonical_probe_records() -> Vec<StorageStreamRecord> {
    (0..100)
        .map(|index| StorageStreamRecord {
            keys: compact_item([
                ("pk", StorageValue::S(format!("ITEM#{index}"))),
                ("escaped\"key", StorageValue::S("line\nvalue".to_string())),
            ]),
            sequence_number: format!("sequence-{index}"),
            old_image: (index % 3 == 0).then(|| {
                compact_item([(
                    "profile",
                    StorageValue::M(compact_item([
                        ("email", StorageValue::S("old@example.test".to_string())),
                        ("active", StorageValue::BOOL(false)),
                    ])),
                )])
            }),
            new_image: Some(compact_item([
                (
                    "profile",
                    StorageValue::M(compact_item([
                        ("email", StorageValue::S("new@example.test".to_string())),
                        ("active", StorageValue::BOOL(true)),
                    ])),
                ),
                (
                    "values",
                    StorageValue::L(vec![
                        StorageValue::N("1.2300".to_string()),
                        StorageValue::NULL(true),
                    ]),
                ),
            ])),
        })
        .collect()
}

fn compact_item<const N: usize>(values: [(&str, StorageValue); N]) -> StorageItem {
    values
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn legacy_write_canonical_json(record: &StorageStreamRecord, output: &mut Vec<u8>) {
    let record = record.clone();
    output.push(b'{');
    legacy_write_field(output, b"Keys");
    legacy_write_storage_item(&record.keys, output);
    if let Some(new_image) = record.new_image.as_ref() {
        output.extend_from_slice(b",\"NewImage\":");
        legacy_write_storage_item(new_image, output);
    }
    if let Some(old_image) = record.old_image.as_ref() {
        output.extend_from_slice(b",\"OldImage\":");
        legacy_write_storage_item(old_image, output);
    }
    output.extend_from_slice(b",\"SequenceNumber\":");
    serde_json::to_writer(&mut *output, &record.sequence_number).expect("sequence serializes");
    output.push(b'}');
}

fn compact_write_canonical_json(record: &StorageStreamRecord, output: &mut Vec<u8>) {
    record.clone().write_canonical_json(output);
}

fn legacy_write_storage_item(item: &StorageItem, output: &mut Vec<u8>) {
    let map = item
        .iter()
        .map(|(key, value)| (key.clone(), value))
        .collect::<HashMap<_, _>>();
    let mut entries = map.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(key, _)| *key);
    output.push(b'{');
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        serde_json::to_writer(&mut *output, key).expect("storage key serializes");
        output.push(b':');
        legacy_write_storage_value(value, output);
    }
    output.push(b'}');
}

fn legacy_write_storage_value(value: &StorageValue, output: &mut Vec<u8>) {
    match value {
        StorageValue::S(value) => legacy_write_string(output, b"S", value),
        StorageValue::N(value) => legacy_write_string(output, b"N", value),
        StorageValue::B(value) => legacy_write_string(output, b"B", value),
        StorageValue::SS(values) => legacy_write_strings(output, b"SS", values),
        StorageValue::NS(values) => legacy_write_strings(output, b"NS", values),
        StorageValue::BS(values) => legacy_write_strings(output, b"BS", values),
        StorageValue::BOOL(value) => legacy_write_bool(output, b"BOOL", *value),
        StorageValue::NULL(value) => legacy_write_bool(output, b"NULL", *value),
        StorageValue::L(values) => {
            output.extend_from_slice(b"{\"L\":[");
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                legacy_write_storage_value(value, output);
            }
            output.extend_from_slice(b"]}");
        }
        StorageValue::M(values) => {
            output.extend_from_slice(b"{\"M\":");
            legacy_write_storage_item(values, output);
            output.push(b'}');
        }
    }
}

fn legacy_write_string(output: &mut Vec<u8>, tag: &[u8], value: &str) {
    output.push(b'{');
    legacy_write_field(output, tag);
    serde_json::to_writer(&mut *output, value).expect("storage value serializes");
    output.push(b'}');
}

fn legacy_write_strings(output: &mut Vec<u8>, tag: &[u8], values: &[String]) {
    output.push(b'{');
    legacy_write_field(output, tag);
    serde_json::to_writer(&mut *output, values).expect("storage values serialize");
    output.push(b'}');
}

fn legacy_write_bool(output: &mut Vec<u8>, tag: &[u8], value: bool) {
    output.push(b'{');
    legacy_write_field(output, tag);
    output.extend_from_slice(if value { b"true" } else { b"false" });
    output.push(b'}');
}

fn legacy_write_field(output: &mut Vec<u8>, key: &[u8]) {
    output.push(b'"');
    output.extend_from_slice(key);
    output.extend_from_slice(b"\":");
}

fn measure_canonical_path(
    records: &[StorageStreamRecord],
    label: &'static str,
    write_record: fn(&StorageStreamRecord, &mut Vec<u8>),
) -> CanonicalProbeSample {
    let started = Instant::now();
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "storage-canonical",
        file!(),
        line!(),
        Some(label),
    );
    let mut output = Vec::new();
    let mut checksum = 1_469_598_103_934_665_603_u64;
    let mut bytes = 0;
    for _ in 0..CANONICAL_PROBE_ITERATIONS {
        for record in records {
            output.clear();
            write_record(record, &mut output);
            bytes += output.len();
            for byte in &output {
                checksum ^= u64::from(*byte);
                checksum = checksum.wrapping_mul(1_099_511_628_211);
            }
            black_box(output.as_slice());
        }
    }
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("storage canonical allocation report");
    CanonicalProbeSample {
        checksum,
        bytes,
        elapsed_ns: started.elapsed().as_nanos(),
        allocations: report.allocation_count,
        allocated_bytes: report.allocated_bytes,
    }
}

#[test]
#[ignore = "manual conversion allocation and CPU comparison"]
fn given_storage_maps_when_converted_to_json_values_then_values_match_and_allocations_drop() {
    let value = StorageValue::M(VecMap::from_iter([
        ("pk".to_string(), StorageValue::S("item-0001".to_string())),
        (
            "tenant_id".to_string(),
            StorageValue::S("tenant-01".to_string()),
        ),
        ("payload".to_string(), StorageValue::S("x".repeat(1_200))),
        ("active".to_string(), StorageValue::BOOL(true)),
        (
            "sequence".to_string(),
            StorageValue::N("123456789".to_string()),
        ),
        (
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([
                ("first".to_string(), StorageValue::S("Ada".to_string())),
                ("last".to_string(), StorageValue::S("Lovelace".to_string())),
            ])),
        ),
    ]));
    let iterations = 1_000;
    let expected = sorted_map_json(&value);
    let (baseline_count, baseline_bytes, baseline_elapsed) =
        measure_conversion("sorted_baseline", iterations, || sorted_map_json(&value));
    let (production_count, production_bytes, production_elapsed) =
        measure_conversion("production", iterations, || value.to_json_value().unwrap());
    assert_eq!(value.to_json_value().unwrap(), expected);
    assert!(production_count < baseline_count);
    assert!(production_bytes < baseline_bytes);
    println!(
        "storage-map-conversion | iterations={iterations} | baseline_allocs={baseline_count} | \
         production_allocs={production_count} | baseline_bytes={baseline_bytes} | \
         production_bytes={production_bytes} | baseline_ns={} | production_ns={}",
        baseline_elapsed.as_nanos(),
        production_elapsed.as_nanos()
    );
}

fn measure_conversion(
    label: &'static str,
    iterations: usize,
    mut convert: impl FnMut() -> serde_json::Value,
) -> (u64, u64, std::time::Duration) {
    let started = Instant::now();
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "storage_map_json_conversion_performance",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..iterations {
        black_box(convert());
    }
    let report = guard.finish();
    (
        report.allocation_count,
        report.allocated_bytes,
        started.elapsed(),
    )
}

fn sorted_map_json(value: &StorageValue) -> serde_json::Value {
    let StorageValue::M(values) = value else {
        return value.to_json_value().expect("fixture value");
    };
    let mut object = serde_json::Map::with_capacity(values.len());
    for (name, attribute) in values.iter().collect::<BTreeMap<_, _>>() {
        object.insert(name.clone(), sorted_map_json(attribute));
    }
    serde_json::Value::Object(object)
}

#[test]
fn given_stream_record_with_unknown_field_when_deserialized_then_it_is_rejected() {
    let record = json!({
        "Keys": {},
        "SequenceNumber": "1",
        "NewImage": {},
        "legacy_mode": true
    });

    assert!(serde_json::from_value::<StorageStreamRecord>(record).is_err());
}

#[test]
fn given_stream_record_attribute_map_with_arbitrary_key_when_deserialized_then_it_is_preserved() {
    let record = json!({
        "Keys": {},
        "SequenceNumber": "1",
        "NewImage": {
            "legacy_mode": {"S": "attribute-value"}
        }
    });

    let record: StorageStreamRecord = serde_json::from_value(record).expect("stream record");
    assert!(
        record
            .new_image
            .expect("new image")
            .contains_key("legacy_mode")
    );
}

#[test]
fn given_stream_record_schema_when_generated_then_unknown_structural_fields_are_rejected() {
    let schema = serde_json::to_value(schemars::schema_for!(StorageStreamRecord)).expect("schema");
    let definition = schema
        .get("$defs")
        .and_then(|definitions| definitions.get("StorageStreamRecord"))
        .unwrap_or(&schema);

    assert_eq!(definition["additionalProperties"], json!(false));
    assert_eq!(definition["properties"]["Keys"]["type"], json!("object"));
    assert_eq!(
        definition["properties"]["NewImage"]["type"],
        json!(["object", "null"])
    );
    assert_eq!(
        schema["$defs"]["StorageValue"]["oneOf"][9]["properties"]["M"]["type"],
        json!("object")
    );
}
