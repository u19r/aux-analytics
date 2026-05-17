use std::collections::BTreeMap;

use analytics_operations::{
    CheckRowSource, CurrentRowPageRequest, CurrentRowStreamOrder, ProductionCurrentRowPageReader,
    ProjectionBackedCheckRows,
};
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;

use crate::{
    DynamoDbCurrentRowPageReader,
    current_row_test_support::{
        RecordingDynamoDbClient, dynamodb_item, dynamodb_key, dynamodb_response, projection,
    },
};

#[test]
fn given_dynamodb_current_row_reader_when_page_requested_then_scan_shape_and_cursor_are_used() {
    let client = RecordingDynamoDbClient::new(vec![dynamodb_response(
        vec![dynamodb_item("u1", "a@example.test", "org-a", "9", false)],
        Some(dynamodb_key("u1")),
    )]);
    let reader = DynamoDbCurrentRowPageReader::new(&client);
    let cursor = serde_json::json!({
        "user_id": {"type": "s", "value": "start"}
    })
    .to_string();

    assert_eq!(reader.stream_order(), CurrentRowStreamOrder::Unordered);

    let page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: Some(cursor),
            page_size: 10,
        })
        .unwrap();

    assert_eq!(page.rows[0].key, "u1");
    assert_eq!(page.rows[0].source_position, 9);
    assert_eq!(
        page.next_cursor,
        Some(serde_json::json!({"user_id":{"type":"s","value":"u1"}}).to_string())
    );
    let request = &client.requests.borrow()[0];
    assert_eq!(request.table_name, "users");
    assert_eq!(request.limit, 10);
    assert!(matches!(
        request
            .exclusive_start_key
            .as_ref()
            .and_then(|key| key.get("user_id")),
        Some(DynamoDbAttributeValue::S(value)) if value == "start"
    ));
}

#[test]
fn given_dynamodb_current_row_reader_when_required_column_missing_then_it_fails_closed() {
    let reader =
        DynamoDbCurrentRowPageReader::new(RecordingDynamoDbClient::new(vec![dynamodb_response(
            vec![BTreeMap::from([(
                "user_id".to_string(),
                DynamoDbAttributeValue::S("u1".to_string()),
            )])],
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
fn given_sorted_dynamodb_reader_when_used_for_production_check_rows_then_stream_is_allowed() {
    let client = RecordingDynamoDbClient::new(vec![dynamodb_response(
        vec![dynamodb_item("u1", "a@example.test", "org-a", "9", false)],
        None,
    )]);
    let reader = DynamoDbCurrentRowPageReader::new_globally_sorted(client);
    let source = ProjectionBackedCheckRows::full_table_dynamodb(projection(), reader, 10)
        .expect("sorted DynamoDB reader should be accepted");

    let mut stream = source.row_stream(&["users".to_string()]).unwrap();
    let row = stream.next_row().unwrap().unwrap();

    assert_eq!(row.key, "u1");
    assert!(stream.next_row().unwrap().is_none());
}

#[test]
fn given_partition_sorted_dynamodb_reader_when_page_requested_then_partition_is_emitted() {
    let client = RecordingDynamoDbClient::new(vec![dynamodb_response(
        vec![dynamodb_item("u1", "a@example.test", "org-a", "9", false)],
        None,
    )]);
    let reader = DynamoDbCurrentRowPageReader::new_partition_sorted(&client, "org_id");

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
fn given_partition_sorted_dynamodb_reader_when_page_has_multiple_partitions_then_it_fails_closed() {
    let reader = DynamoDbCurrentRowPageReader::new_partition_sorted(
        RecordingDynamoDbClient::new(vec![dynamodb_response(
            vec![
                dynamodb_item("u1", "a@example.test", "org-a", "9", false),
                dynamodb_item("u2", "b@example.test", "org-b", "10", false),
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
fn given_default_dynamodb_reader_when_used_for_production_check_rows_then_it_fails_closed() {
    let client = RecordingDynamoDbClient::new(vec![dynamodb_response(
        vec![dynamodb_item("u1", "a@example.test", "org-a", "9", false)],
        None,
    )]);
    let reader = DynamoDbCurrentRowPageReader::new(client);
    let source = ProjectionBackedCheckRows::full_table_dynamodb(projection(), reader, 10)
        .expect("default DynamoDB reader can be constructed");

    let Err(error) = source.row_stream(&["users".to_string()]) else {
        panic!("default DynamoDB reader should fail closed");
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
