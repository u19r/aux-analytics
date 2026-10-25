use analytics_contract::SourcePosition;
use analytics_operations::{
    CheckRowSource, CheckSourcePosition, CurrentRowPageRequest, CurrentRowStreamOrder,
    ProductionCurrentRowPageReader, ProjectionBackedCheckRows,
};
use storage_types::{
    AttributeValue, ExclusiveStartKey, SystemStreamPosition, SystemStreamScope, WireItem,
    WireItemKeyAttributes,
};

use crate::{
    AuxStorageCurrentRowPageReader,
    current_row::test_support::{RecordingClient, item, projection, scan_response},
};

#[test]
fn given_overlapping_projection_fields_when_item_consumed_then_metadata_and_values_are_preserved() {
    let client = RecordingClient::new(vec![scan_response(
        vec![item("u1", "a@example.test", "org-a", "9", true)],
        None,
    )]);
    let reader = AuxStorageCurrentRowPageReader::new_partition_sorted(&client, "org_id");
    let mut projection = projection();
    projection.hash_columns = vec![
        "user_id".to_string(),
        "email".to_string(),
        "__source_position".to_string(),
        "contains_private_data".to_string(),
        "user_id".to_string(),
    ];
    let page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection,
            cursor: None,
            page_size: 1,
        })
        .unwrap();
    let row = &page.rows[0];
    assert_eq!(row.key, "u1");
    assert_eq!(
        row.source_position,
        crate::current_row::test_support::backup_position(9)
    );
    assert!(row.contains_private_data);
    assert_eq!(page.partition.as_deref(), Some("org-a"));
    for (column, expected) in [
        ("user_id", "u1"),
        ("email", "a@example.test"),
        (
            "__source_position",
            r#"{"type":"raw_backup_object_offset","object_id":"check-fixture","offset":9}"#,
        ),
        ("contains_private_data", "true"),
    ] {
        assert_eq!(row.values.get(column), Some(&Some(expected.to_string())));
    }
    assert_eq!(row.values.len(), 4);
    assert_eq!(
        client.requests.borrow()[0].projection_expression,
        "#c0, #c1, #c2, #c3, #c4"
    );
}

#[test]
fn given_aux_storage_current_row_reader_when_page_requested_then_scan_shape_is_projection_backed() {
    let client = RecordingClient::new(vec![scan_response(
        vec![item("u1", "a@example.test", "org-a", "9", false)],
        Some("next-page"),
    )]);
    let reader = AuxStorageCurrentRowPageReader::new(&client);

    assert_eq!(reader.stream_order(), CurrentRowStreamOrder::Unordered);

    let page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: Some("start-page".to_string()),
            page_size: 25,
        })
        .unwrap();

    assert_eq!(page.rows[0].key, "u1");
    assert_eq!(
        page.rows[0].source_position,
        crate::current_row::test_support::backup_position(9)
    );
    assert_eq!(
        page.next_cursor,
        Some(r#"{"user_id":{"S":"next-page"}}"#.to_string())
    );
    let request = &client.requests.borrow()[0];
    assert_eq!(request.table_name, "users");
    assert_eq!(request.limit, 25);
    assert!(matches!(
        request.exclusive_start_key.as_ref(),
        Some(ExclusiveStartKey::Token(cursor)) if cursor == "start-page"
    ));
    assert_eq!(request.projection_expression, "#c0, #c1, #c2, #c3, #c4");
}

#[test]
fn given_aux_storage_system_source_position_when_projected_then_full_typed_position_is_retained() {
    let source_position = serde_json::to_string(&SourcePosition::SystemStream {
        source_scope: SystemStreamScope::try_new("scope", 1).unwrap(),
        position: SystemStreamPosition::try_new(
            "shard",
            "lineage",
            "00000000000000000010-vs00000002",
        )
        .unwrap(),
    })
    .unwrap();
    let client = RecordingClient::new(vec![scan_response(
        vec![item(
            "u1",
            "a@example.test",
            "org-a",
            source_position.as_str(),
            false,
        )],
        None,
    )]);
    let reader = AuxStorageCurrentRowPageReader::new(&client);

    let page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .unwrap();

    assert_eq!(
        page.rows[0].source_position,
        CheckSourcePosition::system_stream(
            SystemStreamScope::try_new("scope", 1).unwrap(),
            SystemStreamPosition::try_new("shard", "lineage", "00000000000000000010-vs00000002",)
                .unwrap(),
        )
    );
}

#[test]
fn given_aux_storage_current_row_reader_when_private_string_flag_returned_then_boolean_is_parsed() {
    let mut item = item("u1", "a@example.test", "org-a", "9", false);
    item.insert(
        "contains_private_data",
        AttributeValue::S("true".to_string()),
    );
    let reader = AuxStorageCurrentRowPageReader::new(RecordingClient::new(vec![scan_response(
        vec![item],
        None,
    )]));

    let page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .unwrap();

    assert!(page.rows[0].contains_private_data);
}

#[test]
fn given_aux_storage_scalar_variants_when_projected_then_values_preserve_scalar_text() {
    let mut item = item("u1", "a@example.test", "org-a", "9", false);
    item.insert("email", AttributeValue::B("binary-value".to_string()));
    item.insert("org_id", AttributeValue::NULL(true));
    let reader = AuxStorageCurrentRowPageReader::new(RecordingClient::new(vec![scan_response(
        vec![item],
        None,
    )]));

    let mut page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .unwrap();
    let row = page.rows.remove(0);

    assert_eq!(
        row.values.get("email").and_then(Option::as_deref),
        Some("binary-value")
    );
    assert_eq!(row.values.get("org_id"), Some(&None));
}

#[test]
fn given_aux_storage_invalid_source_position_when_projected_then_it_fails_closed() {
    let reader = AuxStorageCurrentRowPageReader::new(RecordingClient::new(vec![scan_response(
        vec![item("u1", "a@example.test", "org-a", "not-a-number", false)],
        None,
    )]));

    let error = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .unwrap_err();

    assert!(format!("{error}").contains("current-row source position is invalid"));
}

#[test]
fn given_aux_storage_invalid_private_data_when_projected_then_it_fails_closed() {
    let mut item = item("u1", "a@example.test", "org-a", "9", false);
    item.insert("contains_private_data", AttributeValue::N("1".to_string()));
    let reader = AuxStorageCurrentRowPageReader::new(RecordingClient::new(vec![scan_response(
        vec![item],
        None,
    )]));

    let error = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .unwrap_err();

    assert!(format!("{error}").contains("private-data column must be BOOL"));
}

#[test]
fn given_aux_storage_current_row_reader_when_key_cursor_supplied_then_request_uses_key_boundary() {
    let client = RecordingClient::new(vec![scan_response(
        vec![item("u1", "a@example.test", "org-a", "9", false)],
        None,
    )]);
    let reader = AuxStorageCurrentRowPageReader::new(&client);

    reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: Some(r#"{"user_id":{"S":"start-page"}}"#.to_string()),
            page_size: 25,
        })
        .unwrap();

    let request = &client.requests.borrow()[0];
    let Some(ExclusiveStartKey::Key(key)) = request.exclusive_start_key.as_ref() else {
        panic!("expected aux-storage key cursor");
    };
    assert_eq!(
        key.get("user_id"),
        Some(&AttributeValue::S("start-page".to_string()))
    );
}

#[test]
fn given_sorted_aux_storage_reader_when_used_for_production_check_rows_then_stream_is_allowed() {
    let client = RecordingClient::new(vec![scan_response(
        vec![item("u1", "a@example.test", "org-a", "9", false)],
        None,
    )]);
    let reader = AuxStorageCurrentRowPageReader::new_globally_sorted(client);
    let source = ProjectionBackedCheckRows::full_table_aux_storage(projection(), reader, 10)
        .expect("sorted aux-storage reader should be accepted");

    let mut stream = source.row_stream(&["users".to_string()]).unwrap();
    let row = stream.next_row().unwrap().unwrap();

    assert_eq!(row.key, "u1");
    assert!(stream.next_row().unwrap().is_none());
}

#[test]
fn given_partition_sorted_aux_storage_reader_when_page_requested_then_partition_is_emitted() {
    let client = RecordingClient::new(vec![scan_response(
        vec![item("u1", "a@example.test", "org-a", "9", false)],
        None,
    )]);
    let reader = AuxStorageCurrentRowPageReader::new_partition_sorted(&client, "org_id");

    let page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 10,
        })
        .unwrap();

    assert_eq!(
        reader.stream_order(),
        CurrentRowStreamOrder::PartitionSortedByKey
    );
    assert_eq!(page.partition, Some("org-a".to_string()));
    assert_eq!(
        client.requests.borrow()[0].projection_expression,
        "#c0, #c1, #c2, #c3, #c4"
    );
}

#[test]
fn given_partition_sorted_aux_storage_reader_when_page_has_multiple_partitions_then_it_fails_closed()
 {
    let reader = AuxStorageCurrentRowPageReader::new_partition_sorted(
        RecordingClient::new(vec![scan_response(
            vec![
                item("u1", "a@example.test", "org-a", "9", false),
                item("u2", "b@example.test", "org-b", "10", false),
            ],
            None,
        )]),
        "org_id",
    );

    let error = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 10,
        })
        .unwrap_err();

    assert!(format!("{error}").contains("multiple org_id values"));
}

#[test]
fn given_default_aux_storage_reader_when_used_for_production_check_rows_then_it_fails_closed() {
    let client = RecordingClient::new(vec![scan_response(
        vec![item("u1", "a@example.test", "org-a", "9", false)],
        None,
    )]);
    let reader = AuxStorageCurrentRowPageReader::new(client);
    let source = ProjectionBackedCheckRows::full_table_aux_storage(projection(), reader, 10)
        .expect("default aux-storage reader can be constructed");

    let Err(error) = source.row_stream(&["users".to_string()]) else {
        panic!("default aux-storage reader should fail closed");
    };

    assert!(
        format!("{error}").contains("globally sorted or partition-sorted rows"),
        "unexpected error: {error}"
    );
    assert!(
        format!("{error}").contains("declared Unordered"),
        "unexpected error: {error}"
    );
}

#[test]
fn given_aux_storage_current_row_reader_when_required_column_missing_then_it_fails_closed() {
    let mut item = storage_types::AttributeMap::new();
    item.insert("user_id", AttributeValue::S("u1".to_string()));
    let reader = AuxStorageCurrentRowPageReader::new(RecordingClient::new(vec![scan_response(
        vec![item],
        None,
    )]));

    let error = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .unwrap_err();

    assert!(format!("{error}").contains("email"));
}

#[test]
fn given_aux_storage_current_row_reader_when_wire_item_is_malformed_then_it_fails_closed() {
    let reader = AuxStorageCurrentRowPageReader::new(RecordingClient::new(vec![
        storage_types::ScanResponse {
            count: 1,
            scanned_count: 1,
            items: Some(vec![storage_types::WireItem::dynamo_json(
                br#"{"user_id":{"S":"u1"}"#.to_vec(),
            )]),
            last_evaluated_key: None,
            consumed_capacity: None,
        },
    ]));

    let error = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .expect_err("malformed wire items must fail closed");

    assert!(format!("{error}").contains("invalid item"));
}

#[test]
fn given_aux_storage_current_row_reader_when_unprojected_wire_attribute_is_malformed_then_it_fails_closed()
 {
    let reader = AuxStorageCurrentRowPageReader::new(RecordingClient::new(vec![
        storage_types::ScanResponse {
            count: 1,
            scanned_count: 1,
            items: Some(vec![WireItem::dynamo_json(
                br#"{
                    "user_id":{"S":"u1"},
                    "email":{"S":"a@example.test"},
                    "org_id":{"S":"org-a"},
                    "__source_position":{"S":"{\"type\":\"raw_backup_object_offset\",\"object_id\":\"check-fixture\",\"offset\":9}"},
                    "contains_private_data":{"BOOL":false},
                    "ignored":{"invalid":"shape"}
                }"#
                .to_vec(),
            )]),
            last_evaluated_key: None,
            consumed_capacity: None,
        },
    ]));

    let error = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .expect_err("malformed unprojected attributes must fail closed");

    assert!(format!("{error}").contains("invalid item"));
}

#[test]
fn given_aux_storage_current_row_reader_when_local_split_keys_overlap_then_secondary_key_wins() {
    let primary_key = WireItemKeyAttributes::new(
        "user_id".to_string(),
        AttributeValue::S("primary".to_string()),
        None,
        None,
    );
    let secondary_key = WireItemKeyAttributes::new(
        "user_id".to_string(),
        AttributeValue::S("secondary".to_string()),
        None,
        None,
    );
    let non_key_attributes = br#"{
        "user_id":{"S":"blob"},
        "email":{"S":"a@example.test"},
        "org_id":{"S":"org-a"},
        "__source_position":{"S":"{\"type\":\"raw_backup_object_offset\",\"object_id\":\"check-fixture\",\"offset\":9}"},
        "contains_private_data":{"BOOL":false}
    }"#;
    let reader = AuxStorageCurrentRowPageReader::new(RecordingClient::new(vec![
        storage_types::ScanResponse {
            count: 1,
            scanned_count: 1,
            items: Some(vec![WireItem::local_split(
                primary_key,
                Some(secondary_key),
                Some(non_key_attributes.to_vec()),
            )]),
            last_evaluated_key: None,
            consumed_capacity: None,
        },
    ]));

    let page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .expect("canonical LocalSplit conversion should succeed");

    assert_eq!(page.rows[0].key, "secondary");
}
