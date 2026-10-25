use std::cmp::Ordering;

use storage_types::{SystemStreamPosition, SystemStreamScope};

use super::row_conversion::source_position_json_to_check_position;
use crate::CheckSourcePosition;

#[test]
fn given_system_numeric_offset_json_when_projected_then_decimal_order_is_preserved() {
    let position = r#"{
        "type": "system_stream",
        "source_scope": {"Version": 1, "ScopeId": "scope", "Generation": 1},
        "position": {"ShardId": "shard", "LineageId": "lineage", "Offset": "10"}
    }"#;

    assert_eq!(
        source_position_json_to_check_position(position).unwrap(),
        CheckSourcePosition::system_stream(
            SystemStreamScope::try_new("scope", 1).unwrap(),
            SystemStreamPosition::try_new("shard", "lineage", "10").unwrap(),
        )
    );
}

#[test]
fn given_system_versionstamp_offset_json_when_projected_then_full_stamp_order_is_preserved() {
    let position = r#"{
        "type": "system_stream",
        "source_scope": {"Version": 1, "ScopeId": "scope", "Generation": 1},
        "position": {
            "ShardId": "shard",
            "LineageId": "lineage",
            "Offset": "00000000000000000010-vs00000001"
        }
    }"#;

    let same_version_later = position.replace("00000001", "00000002");
    let earlier = source_position_json_to_check_position(position).unwrap();
    let later = source_position_json_to_check_position(same_version_later.as_str()).unwrap();
    assert_eq!(earlier.partial_cmp_position(&later), Some(Ordering::Less));
    assert_ne!(earlier.hash_seed(), later.hash_seed());
}

#[test]
fn given_system_versionstamps_when_version_increases_then_order_is_preserved() {
    let earlier = r#"{
        "type": "system_stream",
        "source_scope": {"Version": 1, "ScopeId": "scope", "Generation": 1},
        "position": {
            "ShardId": "shard",
            "LineageId": "lineage",
            "Offset": "00000000000000000009-vsffffffff"
        }
    }"#;
    let later = earlier.replace("00000000000000000009", "00000000000000000010");

    assert_eq!(
        source_position_json_to_check_position(earlier)
            .unwrap()
            .partial_cmp_position(&source_position_json_to_check_position(later.as_str()).unwrap()),
        Some(Ordering::Less)
    );
}

#[test]
fn given_unorderable_system_offset_json_when_projected_then_it_is_rejected() {
    let position = r#"{
        "type": "system_stream",
        "source_scope": {"Version": 1, "ScopeId": "scope", "Generation": 1},
        "position": {"ShardId": "shard", "LineageId": "lineage", "Offset": "offset-9"}
    }"#;

    assert!(source_position_json_to_check_position(position).is_err());
}
