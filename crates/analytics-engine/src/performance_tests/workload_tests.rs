use super::*;

#[test]
fn given_realistic_fixture_when_built_then_shape_and_payload_budget_are_exact() {
    let workload = realistic_workload(1);
    let record = &workload.records[0];
    let item = record.record.new_image.as_ref().expect("fixture image");
    let payload_bytes = ["payload_a", "payload_b", "payload_c"]
        .into_iter()
        .map(|name| match item.get(name).expect("fixture payload") {
            StorageValue::S(value) => value.len(),
            _ => panic!("fixture payload is a string"),
        })
        .sum::<usize>();
    assert_eq!(item.len(), REALISTIC_ATTRIBUTE_COUNT);
    assert_eq!(payload_bytes, REALISTIC_PAYLOAD_BYTES);
    assert_eq!(record.record_key.len(), 100);
    assert_eq!(
        record
            .record
            .keys
            .get("pk")
            .expect("fixture partition key")
            .to_json_value()
            .unwrap()
            .as_str()
            .unwrap()
            .len(),
        100
    );
    assert_eq!(
        workload.manifest.tables[0]
            .projection_columns
            .as_ref()
            .unwrap()
            .len(),
        9
    );
    assert_eq!(realistic_query(500).select.len(), 10);
}

#[test]
fn given_document_fixture_when_materialized_then_document_matches_reference() {
    let workload = realistic_workload(1);
    let table = realistic_document_row_table(&workload);
    let record = &workload.records[0];
    let item = record.record.new_image.as_ref().expect("fixture image");
    let engine = realistic_engine(&workload);
    let actual = engine
        .row_from_item(
            &table,
            RowSource {
                stream_key_digest: None,
                source_table_name: &record.source_table_name,
                record_key: &record.record_key,
                record_keys: &record.record.keys,
            },
            item,
            1_900_000_000,
            None,
        )
        .expect("document fixture row materializes");
    let mut expected = row_reference(item, record);
    expected.insert("item".to_string(), realistic_storage_item_to_json(item));
    assert_eq!(actual, expected);
    assert!(actual.contains_key("item"));
}

#[test]
fn given_no_document_fixture_when_materialized_then_row_matches_reference_without_item() {
    let workload = realistic_workload(1);
    let table = realistic_row_table(&workload);
    let record = &workload.records[0];
    let item = record.record.new_image.as_ref().expect("fixture image");
    let engine = realistic_engine(&workload);
    let actual = engine
        .row_from_item(
            &table,
            RowSource {
                stream_key_digest: None,
                source_table_name: &record.source_table_name,
                record_key: &record.record_key,
                record_keys: &record.record.keys,
            },
            item,
            1_900_000_000,
            None,
        )
        .expect("no-document fixture row materializes");

    assert_eq!(actual, row_reference(item, record));
    assert!(!actual.contains_key("item"));
}
