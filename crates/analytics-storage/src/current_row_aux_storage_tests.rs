use analytics_operations::{
    CheckRowSource, CurrentRowPageRequest, CurrentRowStreamOrder, ProductionCurrentRowPageReader,
    ProjectionBackedCheckRows,
};
use storage_types::{AttributeValue, ExclusiveStartKey};

use crate::{
    AuxStorageCurrentRowPageReader,
    current_row_test_support::{RecordingClient, item, projection, scan_response},
};

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
    assert_eq!(page.rows[0].source_position, 9);
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
    assert_eq!(
        request.projection_expression,
        "__source_position, contains_private_data, email, org_id, user_id"
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
        "__source_position, contains_private_data, email, org_id, user_id"
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
