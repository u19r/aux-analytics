use std::collections::HashMap;

use serde_json::json;

use crate::{StorageValue, StorageValueJsonError};

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
    let value = StorageValue::M(HashMap::from([
        ("b".to_string(), StorageValue::S("second".to_string())),
        ("a".to_string(), StorageValue::S("first".to_string())),
    ]));

    assert_eq!(
        value.to_json_value(),
        Ok(json!({"a": "first", "b": "second"}))
    );
}
