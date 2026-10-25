use analytics_contract::SourcePosition;
use storage_types::{SystemStreamPosition, SystemStreamScope};

use crate::CheckSourcePosition;

fn scope() -> SystemStreamScope {
    SystemStreamScope::try_new("scope", 1).unwrap()
}

fn position(offset: &str) -> SystemStreamPosition {
    SystemStreamPosition::try_new("shard", "lineage", offset).unwrap()
}

#[test]
fn given_absent_positions_when_round_tripped_then_null_is_preserved_and_numeric_legacy_is_rejected()
{
    assert_eq!(
        serde_json::to_value(CheckSourcePosition::default()).unwrap(),
        serde_json::Value::Null
    );
    assert_eq!(
        serde_json::from_str::<CheckSourcePosition>("null").unwrap(),
        CheckSourcePosition::default()
    );
    assert!(serde_json::from_str::<CheckSourcePosition>("10").is_err());
}

#[test]
fn given_system_positions_when_round_tripped_then_versioned_typed_shape_is_preserved() {
    let position = CheckSourcePosition::system_stream(scope(), position("10"));
    let json = serde_json::to_value(&position).unwrap();
    assert_eq!(json["type"], "system_stream");
    assert_eq!(
        serde_json::from_value::<CheckSourcePosition>(json).unwrap(),
        position
    );
}

#[test]
fn given_system_versionstamp_suffixes_when_compared_then_positions_are_distinct_and_ordered() {
    let earlier =
        CheckSourcePosition::system_stream(scope(), position("00000000000000000010-vs00000001"));
    let later =
        CheckSourcePosition::system_stream(scope(), position("00000000000000000010-vs00000002"));
    assert_eq!(
        earlier.partial_cmp_position(&later),
        Some(std::cmp::Ordering::Less)
    );
    assert_ne!(earlier.hash_seed(), later.hash_seed());
}

#[test]
fn given_ambiguous_lane_strings_when_hashed_then_framing_distinguishes_positions() {
    let left = CheckSourcePosition::system_stream(
        scope(),
        SystemStreamPosition::try_new("bc", "d", "10").unwrap(),
    );
    let right = CheckSourcePosition::system_stream(
        scope(),
        SystemStreamPosition::try_new("b", "cd", "10").unwrap(),
    );
    assert_ne!(left.hash_seed(), right.hash_seed());
}

#[test]
fn given_backup_and_system_positions_when_compared_then_order_is_unknown() {
    assert_eq!(
        crate::check_test_support::backup_position(11)
            .partial_cmp_position(&CheckSourcePosition::system_stream(scope(), position("10")),),
        None
    );
}

#[test]
fn given_malformed_system_position_when_deserialized_then_position_is_rejected() {
    let value = serde_json::json!({
        "type": "system_stream",
        "source_scope": {"Version": 1, "ScopeId": "scope", "Generation": 1},
        "position": {
            "ShardId": "shard",
            "LineageId": "lineage",
            "Offset": "offset-9"
        }
    });
    assert!(serde_json::from_value::<CheckSourcePosition>(value).is_err());
}

#[test]
fn given_system_position_json_when_mapped_then_scope_is_preserved() {
    let source = SourcePosition::SystemStream {
        source_scope: scope(),
        position: position("00000000000000000010-vs00000002"),
    };
    let encoded = serde_json::to_string(&source).unwrap();
    let parsed = CheckSourcePosition::parse_source_position(&encoded).unwrap();
    assert_eq!(
        parsed,
        CheckSourcePosition::system_stream(scope(), position("00000000000000000010-vs00000002"))
    );
}
