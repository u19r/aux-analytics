use crate::{MergeDecision, RowIdentityValue, SourceMutation, SourcePosition, merge_decision};

fn stream_position(sequence_number: &str) -> SourcePosition {
    SourcePosition::DynamoDbStreamSequence {
        stream_arn: "stream".to_string(),
        shard_id: "shard-1".to_string(),
        sequence_number: sequence_number.to_string(),
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
fn given_blank_row_identity_when_constructed_then_it_is_rejected() {
    let error = RowIdentityValue::new(" ").unwrap_err();

    assert!(error.to_string().contains("cannot be empty"));
}
