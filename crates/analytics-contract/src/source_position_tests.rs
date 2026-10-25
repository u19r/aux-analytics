use serde_json::json;
use storage_types::{SystemStreamPosition, SystemStreamScope};

use crate::{MergeDecision, RowIdentityValue, SourceMutation, SourcePosition, merge_decision};

fn stream_position(sequence_number: &str) -> SourcePosition {
    SourcePosition::DynamoDbStreamSequence {
        stream_arn: "stream".to_string(),
        shard_id: "shard-1".to_string(),
        sequence_number: sequence_number.to_string(),
    }
}

fn system_scope(generation: u64) -> SystemStreamScope {
    SystemStreamScope::try_new("scope", generation).expect("system scope")
}

fn system_position(offset: &str) -> SourcePosition {
    system_position_in_lane("shard-1", "lineage-1", offset, 1)
}

fn system_position_in_lane(
    shard_id: &str,
    lineage_id: &str,
    offset: &str,
    generation: u64,
) -> SourcePosition {
    SourcePosition::SystemStream {
        source_scope: system_scope(generation),
        position: SystemStreamPosition::try_new(shard_id, lineage_id, offset)
            .expect("system position"),
    }
}

#[test]
fn given_no_existing_row_when_update_arrives_then_insert_is_selected() {
    let decision =
        merge_decision(None, &stream_position("10"), SourceMutation::Update, false).unwrap();

    assert_eq!(decision, MergeDecision::Insert);
}

#[test]
fn given_newer_existing_row_when_stale_snapshot_arrives_then_it_is_ignored() {
    let decision = merge_decision(
        Some(&stream_position("20")),
        &stream_position("10"),
        SourceMutation::Update,
        false,
    )
    .unwrap();

    assert_eq!(decision, MergeDecision::IgnoreStale);
}

#[test]
fn given_same_source_position_when_event_replays_then_it_is_duplicate() {
    let decision = merge_decision(
        Some(&stream_position("10")),
        &stream_position("10"),
        SourceMutation::Update,
        false,
    )
    .unwrap();

    assert_eq!(decision, MergeDecision::IgnoreDuplicate);
}

#[test]
fn given_incomparable_positions_when_event_arrives_then_validation_is_required() {
    let existing = SourcePosition::RawBackupObjectOffset {
        object_id: "object".to_string(),
        offset: 1,
    };

    let decision = merge_decision(
        Some(&existing),
        &stream_position("10"),
        SourceMutation::Update,
        false,
    )
    .unwrap();

    assert_eq!(decision, MergeDecision::RequiresValidation);
}

#[test]
fn given_append_only_table_when_newer_event_arrives_then_append_is_selected() {
    let decision = merge_decision(
        Some(&stream_position("10")),
        &stream_position("20"),
        SourceMutation::Update,
        true,
    )
    .unwrap();

    assert_eq!(decision, MergeDecision::Append);
}

#[test]
fn given_system_numeric_offsets_when_ordered_then_decimal_order_is_used() {
    let existing = system_position("9");

    let newer = merge_decision(
        Some(&existing),
        &system_position("10"),
        SourceMutation::Update,
        false,
    )
    .unwrap();
    let stale = merge_decision(
        Some(&system_position("10")),
        &existing,
        SourceMutation::Update,
        false,
    )
    .unwrap();

    assert_eq!(newer, MergeDecision::Replace);
    assert_eq!(stale, MergeDecision::IgnoreStale);
}

#[test]
fn given_equal_system_position_when_event_replays_then_it_is_duplicate() {
    let position = system_position("10");

    let decision =
        merge_decision(Some(&position), &position, SourceMutation::Update, false).unwrap();

    assert_eq!(decision, MergeDecision::IgnoreDuplicate);
}

#[test]
fn given_system_versionstamps_when_ordered_then_full_stamp_order_is_used() {
    let earlier = system_position("00000000000000000009-vsffffffff");
    let later = system_position("00000000000000000010-vs00000000");

    let decision = merge_decision(Some(&earlier), &later, SourceMutation::Update, false).unwrap();

    assert_eq!(decision, MergeDecision::Replace);
}

#[test]
fn given_system_positions_from_different_lanes_when_compared_then_validation_is_required() {
    let existing = system_position("9");
    let incoming = SourcePosition::SystemStream {
        source_scope: system_scope(1),
        position: SystemStreamPosition::try_new("shard-1", "lineage-2", "10")
            .expect("system position"),
    };

    let decision =
        merge_decision(Some(&existing), &incoming, SourceMutation::Update, false).unwrap();

    assert_eq!(decision, MergeDecision::RequiresValidation);
}

#[test]
fn given_system_positions_from_different_shards_when_compared_then_validation_is_required() {
    let existing = system_position("9");
    let incoming = system_position_in_lane("shard-2", "lineage-1", "10", 1);

    let decision =
        merge_decision(Some(&existing), &incoming, SourceMutation::Update, false).unwrap();

    assert_eq!(decision, MergeDecision::RequiresValidation);
}

#[test]
fn given_system_positions_from_different_generations_when_compared_then_validation_is_required() {
    let existing = system_position("9");
    let incoming = SourcePosition::SystemStream {
        source_scope: system_scope(2),
        position: SystemStreamPosition::try_new("shard-1", "lineage-1", "10")
            .expect("system position"),
    };

    let decision =
        merge_decision(Some(&existing), &incoming, SourceMutation::Update, false).unwrap();

    assert_eq!(decision, MergeDecision::RequiresValidation);
}

#[test]
fn given_unorderable_system_offset_when_inserted_then_it_is_rejected() {
    let position = SourcePosition::SystemStream {
        source_scope: system_scope(1),
        position: SystemStreamPosition::try_new("shard-1", "lineage-1", "offset-9")
            .expect("system position"),
    };

    let error = merge_decision(None, &position, SourceMutation::Insert, false).unwrap_err();

    assert_eq!(error.to_string(), "SystemStream offset cannot be ordered");
}

#[test]
fn given_malformed_persisted_system_position_when_decoded_then_it_is_rejected() {
    let error = serde_json::from_value::<SourcePosition>(json!({
        "type": "system_stream",
        "source_scope": {
            "Version": 1,
            "ScopeId": "scope",
            "Generation": 1,
        },
        "position": {
            "ShardId": "shard-1",
            "LineageId": "lineage-1",
            "Offset": "offset-9",
        },
    }))
    .expect_err("invalid SystemStream offsets must fail while decoding");

    assert!(
        error
            .to_string()
            .contains("SystemStream offset cannot be ordered")
    );
}

#[test]
fn given_system_position_when_serialized_then_storage_fields_keep_their_wire_names() {
    let position = system_position("10");
    let value = serde_json::to_value(&position).expect("source position JSON");

    assert_eq!(
        value,
        json!({
            "type": "system_stream",
            "source_scope": {
                "Version": 1,
                "ScopeId": "scope",
                "Generation": 1,
            },
            "position": {
                "ShardId": "shard-1",
                "LineageId": "lineage-1",
                "Offset": "10",
            },
        })
    );

    let decoded: SourcePosition = serde_json::from_value(value).expect("source position JSON");
    assert_eq!(decoded, position);
}

#[test]
fn given_blank_row_identity_when_constructed_then_it_is_rejected() {
    let error = RowIdentityValue::try_new(" ").unwrap_err();

    assert!(error.to_string().contains("cannot be empty"));
}

#[test]
fn given_source_position_variant_with_unknown_field_when_deserialized_then_it_is_rejected() {
    let positions = [
        SourcePosition::DynamoDbStreamSequence {
            stream_arn: "stream".to_string(),
            shard_id: "shard-1".to_string(),
            sequence_number: "1".to_string(),
        },
        SourcePosition::AuxStorageStreamCursor {
            stream_name: "stream".to_string(),
            cursor: "cursor-1".to_string(),
        },
        system_position("1"),
        SourcePosition::DynamoDbExportSnapshot {
            export_arn: "export".to_string(),
            exported_at_ms: 1,
        },
        SourcePosition::DynamoDbScanChunk {
            table_name: "users".to_string(),
            scan_generation: 1,
            segment: 0,
            item_index: 1,
        },
        SourcePosition::RawBackupObjectOffset {
            object_id: "object".to_string(),
            offset: 1,
        },
    ];

    for position in positions {
        let mut value = serde_json::to_value(position).expect("source position JSON");
        value["legacy_mode"] = json!(true);
        assert!(
            serde_json::from_value::<SourcePosition>(value).is_err(),
            "source position accepted an unknown field"
        );
    }
}

#[test]
fn given_source_position_schema_when_generated_then_each_variant_rejects_unknown_fields() {
    let schema = serde_json::to_value(schemars::schema_for!(SourcePosition)).expect("schema");
    let definition = schema
        .get("$defs")
        .and_then(|definitions| definitions.get("SourcePosition"))
        .unwrap_or(&schema);
    let variants = definition["oneOf"].as_array().expect("source variants");

    assert_eq!(variants.len(), 6);
    for variant in variants {
        assert_eq!(variant["additionalProperties"], json!(false));
    }
}

#[test]
fn given_empty_tokens_for_each_opaque_source_when_deserialized_then_validation_fails_at_the_field_boundary()
 {
    let positions = [
        (stream_position("1"), "sequence_number"),
        (
            SourcePosition::AuxStorageStreamCursor {
                stream_name: "stream".to_string(),
                cursor: "1".to_string(),
            },
            "cursor",
        ),
        (
            SourcePosition::DynamoDbExportSnapshot {
                export_arn: "export".to_string(),
                exported_at_ms: 1,
            },
            "export_arn",
        ),
        (
            SourcePosition::DynamoDbScanChunk {
                table_name: "events".to_string(),
                scan_generation: 1,
                segment: 0,
                item_index: 0,
            },
            "table_name",
        ),
        (
            SourcePosition::RawBackupObjectOffset {
                object_id: "object".to_string(),
                offset: 0,
            },
            "object_id",
        ),
    ];
    for (position, field) in positions {
        let mut position = serde_json::to_value(position).unwrap();
        position[field] = json!(" ");
        let error = serde_json::from_value::<SourcePosition>(position).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("source position token cannot be empty")
        );
    }
}

#[test]
fn given_arbitrary_width_decimal_sequences_when_compared_then_numeric_order_is_used() {
    for (left, right) in [
        ("9", "10"),
        (
            "99999999999999999999999999999999999999999999999999",
            "100000000000000000000000000000000000000000000000000",
        ),
        (
            "100000000000000000000000000000000000000000000000003",
            "100000000000000000000000000000000000000000000000004",
        ),
    ] {
        assert_eq!(
            stream_position(left).partial_cmp_position(&stream_position(right)),
            Some(std::cmp::Ordering::Less)
        );
    }
    assert_eq!(
        stream_position("00010").partial_cmp_position(&stream_position("10")),
        Some(std::cmp::Ordering::Equal)
    );
    assert_eq!(
        stream_position("opaque").partial_cmp_position(&stream_position("10")),
        None
    );
}
