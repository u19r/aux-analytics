use analytics_operations::{
    CheckRowSource, CurrentRowPageRequest, CurrentRowStreamOrder, ProductionCurrentRowPageReader,
    ProjectionBackedCheckRows,
};
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use storage_types::VecMap;

use crate::{
    AuxStorageCurrentRowPageReader, DynamoDbCurrentRowPageReader,
    current_row::test_support::{
        RecordingClient, RecordingDynamoDbClient, dynamodb_item, dynamodb_key, dynamodb_response,
        item, projection, scan_response,
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
    assert_eq!(
        page.rows[0].source_position,
        crate::current_row::test_support::backup_position(9)
    );
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
fn given_dynamodb_reader_when_multi_key_response_returns_then_cursor_is_canonicalized() {
    let client = RecordingDynamoDbClient::new(vec![dynamodb_response(
        vec![dynamodb_item("u1", "a@example.test", "org-a", "9", false)],
        Some(VecMap::from_iter([
            (
                "z_partition".to_string(),
                DynamoDbAttributeValue::S("z".to_string()),
            ),
            (
                "a_partition".to_string(),
                DynamoDbAttributeValue::S("a".to_string()),
            ),
        ])),
    )]);
    let reader = DynamoDbCurrentRowPageReader::new(&client);

    let page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 10,
        })
        .unwrap();

    assert_eq!(
        page.next_cursor.as_deref(),
        Some(r#"{"a_partition":{"type":"s","value":"a"},"z_partition":{"type":"s","value":"z"}}"#)
    );
}

#[test]
fn given_dynamodb_scalar_variants_when_projected_then_values_match_aux_storage() {
    let item = VecMap::from_iter([
        (
            "user_id".to_string(),
            DynamoDbAttributeValue::S("u1".to_string()),
        ),
        (
            "email".to_string(),
            DynamoDbAttributeValue::B(b"binary-value".to_vec().into()),
        ),
        ("org_id".to_string(), DynamoDbAttributeValue::Null(true)),
        (
            "__source_position".to_string(),
            DynamoDbAttributeValue::S(
                serde_json::to_string(&crate::current_row::test_support::backup_position(9))
                    .unwrap(),
            ),
        ),
        (
            "contains_private_data".to_string(),
            DynamoDbAttributeValue::Bool(true),
        ),
    ]);
    let reader =
        DynamoDbCurrentRowPageReader::new(RecordingDynamoDbClient::new(vec![dynamodb_response(
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
    assert_eq!(
        row.source_position,
        crate::current_row::test_support::backup_position(9)
    );
    assert!(row.contains_private_data);
}

#[test]
fn given_aux_storage_and_dynamodb_rows_when_projected_then_results_have_parity() {
    let aux_reader =
        AuxStorageCurrentRowPageReader::new(RecordingClient::new(vec![scan_response(
            vec![item("u1", "a@example.test", "org-a", "9", true)],
            None,
        )]));
    let dynamodb_reader =
        DynamoDbCurrentRowPageReader::new(RecordingDynamoDbClient::new(vec![dynamodb_response(
            vec![dynamodb_item("u1", "a@example.test", "org-a", "9", true)],
            None,
        )]));
    let request = CurrentRowPageRequest {
        projection: projection(),
        cursor: None,
        page_size: 1,
    };

    let aux_page = aux_reader.current_row_page(&request).unwrap();
    let dynamodb_page = dynamodb_reader.current_row_page(&request).unwrap();

    assert_eq!(aux_page.rows, dynamodb_page.rows);
    assert_eq!(aux_page.partition, dynamodb_page.partition);
}

#[test]
fn given_dynamodb_unsupported_cursor_attribute_when_page_requested_then_it_fails_closed() {
    let reader =
        DynamoDbCurrentRowPageReader::new(RecordingDynamoDbClient::new(vec![dynamodb_response(
            vec![dynamodb_item("u1", "a@example.test", "org-a", "9", false)],
            Some(VecMap::from_iter([(
                "user_id".to_string(),
                DynamoDbAttributeValue::L(Vec::new()),
            )])),
        )]));

    let error = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: None,
            page_size: 1,
        })
        .unwrap_err();

    assert!(format!("{error}").contains("cannot encode attribute variant"));
}

#[test]
fn given_dynamodb_malformed_cursor_when_page_requested_then_it_fails_closed() {
    let reader =
        DynamoDbCurrentRowPageReader::new(RecordingDynamoDbClient::new(vec![dynamodb_response(
            vec![dynamodb_item("u1", "a@example.test", "org-a", "9", false)],
            None,
        )]));

    let error = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection(),
            cursor: Some("{malformed".to_string()),
            page_size: 1,
        })
        .unwrap_err();

    assert!(matches!(error, analytics_operations::CheckError::Json(_)));
}

#[test]
fn given_dynamodb_current_row_reader_when_required_column_missing_then_it_fails_closed() {
    let reader =
        DynamoDbCurrentRowPageReader::new(RecordingDynamoDbClient::new(vec![dynamodb_response(
            vec![VecMap::from_iter([(
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
        "#c0, #c1, #c2, #c3, #c4"
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

#[test]
fn given_binary_composite_cursor_when_next_page_requested_then_original_key_bytes_reach_provider() {
    let key = VecMap::from_iter([
        (
            "binary".to_string(),
            DynamoDbAttributeValue::B(vec![0xff, 0, 0xc3, 0xa9, 10, 0x80].into()),
        ),
        (
            "partition".to_string(),
            DynamoDbAttributeValue::S("partition".to_string()),
        ),
        (
            "sequence".to_string(),
            DynamoDbAttributeValue::N("123".to_string()),
        ),
    ]);
    let client = RecordingDynamoDbClient::new(vec![
        dynamodb_response(Vec::new(), Some(key.clone())),
        dynamodb_response(Vec::new(), None),
    ]);
    let reader = DynamoDbCurrentRowPageReader::new(&client);
    let mut request = CurrentRowPageRequest {
        projection: projection(),
        cursor: None,
        page_size: 1,
    };
    request.cursor = reader.current_row_page(&request).unwrap().next_cursor;
    assert!(request.cursor.is_some());
    reader.current_row_page(&request).unwrap();
    assert_eq!(
        client.requests.borrow()[1].exclusive_start_key.as_ref(),
        Some(&key)
    );
}

#[test]
fn given_invalid_binary_cursor_when_page_requested_then_provider_is_not_called() {
    let client = RecordingDynamoDbClient::new(Vec::new());
    let reader = DynamoDbCurrentRowPageReader::new(&client);
    for value in [
        serde_json::json!([-1]),
        serde_json::json!([256]),
        serde_json::json!("lossy text"),
    ] {
        let request = CurrentRowPageRequest {
            projection: projection(),
            cursor: Some(serde_json::json!({"key": {"type": "b", "value": value}}).to_string()),
            page_size: 1,
        };
        assert!(reader.current_row_page(&request).is_err());
    }
    assert!(client.requests.borrow().is_empty());
}
