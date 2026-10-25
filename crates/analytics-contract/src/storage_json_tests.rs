use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use storage_types::VecMap;

use super::{StorageValue, StorageValueJsonError};

#[test]
fn given_all_plain_json_variants_when_written_then_existing_bytes_are_preserved() {
    let value = make_document();
    let expected = r#"{"binary":"YQ==","binary_set":["YQ==","Yg=="],"boolean":true,"list":["line\n\"quoted\"",null,{"a":1.0,"z":"invalid"}],"number":100.0,"number_set":[1,"invalid",2.5],"string":"text","string_set":["z","a"]}"#;
    assert_eq!(value.to_json_value().unwrap().to_string(), expected);
    assert_eq!(value.to_json_text().unwrap(), expected);
    let mut buffer = b"prefix:".to_vec();
    value.append_json(&mut buffer).unwrap();
    assert_eq!(&buffer[7..], expected.as_bytes());
}

#[test]
fn given_nested_false_null_when_written_then_every_json_target_rejects_it() {
    let value = StorageValue::M(VecMap::from_iter([(
        "nested".to_string(),
        StorageValue::L(vec![StorageValue::NULL(false)]),
    )]));
    assert_eq!(
        value.to_json_value(),
        Err(StorageValueJsonError::UnsupportedNullFalse)
    );
    assert_eq!(
        value.to_json_text(),
        Err(StorageValueJsonError::UnsupportedNullFalse)
    );
    assert_eq!(
        value.append_json(&mut Vec::new()),
        Err(StorageValueJsonError::UnsupportedNullFalse)
    );
}

#[test]
fn given_large_document_when_written_directly_then_value_tree_allocations_are_removed() {
    let value = make_large_document();
    let (baseline, old) = measure_document(&value, false);
    let (direct, new) = measure_document(&value, true);
    assert_eq!(baseline, direct);
    assert!(new.0 < old.0, "direct {new:?}, tree {old:?}");
    assert!(new.1 < old.1, "direct {new:?}, tree {old:?}");
}

#[test]
#[ignore = "manual paired plain JSON document conversion probe"]
fn given_large_document_when_profiled_then_direct_json_matches_tree_bytes() {
    let value = make_large_document();
    let expected = value.to_json_value().unwrap().to_string();
    assert_eq!(value.to_json_text().unwrap(), expected);
    for direct in [false, true] {
        let guard = AllocationGuard::start(module_path!(), "plain-json", file!(), line!(), None);
        let start = Instant::now();
        for _ in 0..500 {
            black_box(if direct {
                value.to_json_text().unwrap()
            } else {
                value.to_json_value().unwrap().to_string()
            });
        }
        let elapsed = start.elapsed();
        let stats = guard.finish();
        eprintln!(
            "plain-json direct={direct} loops=500 elapsed={elapsed:?} allocations={} bytes={}",
            stats.allocation_count, stats.allocated_bytes
        );
    }
}

fn measure_document(value: &StorageValue, direct: bool) -> (String, (u64, u64)) {
    let guard = AllocationGuard::start(module_path!(), "plain-json", file!(), line!(), None);
    let text = if direct {
        value.to_json_text().unwrap()
    } else {
        value.to_json_value().unwrap().to_string()
    };
    let stats = guard.finish();
    (text, (stats.allocation_count, stats.allocated_bytes))
}

fn make_document() -> StorageValue {
    StorageValue::M(VecMap::from_iter([
        (
            "string_set".into(),
            StorageValue::SS(vec!["z".into(), "a".into()]),
        ),
        ("string".into(), StorageValue::S("text".into())),
        (
            "number_set".into(),
            StorageValue::NS(vec!["1".into(), "invalid".into(), "2.50".into()]),
        ),
        ("number".into(), StorageValue::N("1E2".into())),
        (
            "list".into(),
            StorageValue::L(vec![
                StorageValue::S("line\n\"quoted\"".into()),
                StorageValue::NULL(true),
                StorageValue::M(VecMap::from_iter([
                    ("z".into(), StorageValue::N("invalid".into())),
                    ("a".into(), StorageValue::N("1.0".into())),
                ])),
            ]),
        ),
        ("boolean".into(), StorageValue::BOOL(true)),
        (
            "binary_set".into(),
            StorageValue::BS(vec!["YQ==".into(), "Yg==".into()]),
        ),
        ("binary".into(), StorageValue::B("YQ==".into())),
    ]))
}

fn make_large_document() -> StorageValue {
    StorageValue::L((0..64).map(|_| make_document()).collect())
}
