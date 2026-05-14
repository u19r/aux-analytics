use std::collections::HashMap;

use analytics_contract::StorageValue;
use aws_sdk_dynamodbstreams::{
    primitives::Blob,
    types::{AttributeValue as AwsStreamAttributeValue, OperationType, Record, StreamRecord},
};

use crate::aws_stream::aws_stream_record_to_contract;

#[test]
fn given_aws_stream_record_when_converted_then_storage_wire_shape_is_preserved() {
    let record = Record::builder()
        .event_name(OperationType::Modify)
        .dynamodb(
            StreamRecord::builder()
                .sequence_number("42")
                .set_keys(Some(HashMap::from([(
                    "pk".to_string(),
                    AwsStreamAttributeValue::S("user#1".to_string()),
                )])))
                .set_new_image(Some(HashMap::from([
                    (
                        "email".to_string(),
                        AwsStreamAttributeValue::S("a@example.com".to_string()),
                    ),
                    (
                        "age".to_string(),
                        AwsStreamAttributeValue::N("37".to_string()),
                    ),
                    ("active".to_string(), AwsStreamAttributeValue::Bool(true)),
                    (
                        "payload".to_string(),
                        AwsStreamAttributeValue::B(Blob::new("abc")),
                    ),
                    (
                        "tags".to_string(),
                        AwsStreamAttributeValue::Ss(vec!["admin".to_string(), "beta".to_string()]),
                    ),
                    (
                        "scores".to_string(),
                        AwsStreamAttributeValue::Ns(vec!["1".to_string(), "2".to_string()]),
                    ),
                    (
                        "blobs".to_string(),
                        AwsStreamAttributeValue::Bs(vec![Blob::new("a"), Blob::new("b")]),
                    ),
                    (
                        "nested".to_string(),
                        AwsStreamAttributeValue::M(HashMap::from([(
                            "deleted".to_string(),
                            AwsStreamAttributeValue::Null(true),
                        )])),
                    ),
                    (
                        "list".to_string(),
                        AwsStreamAttributeValue::L(vec![AwsStreamAttributeValue::S(
                            "one".to_string(),
                        )]),
                    ),
                ])))
                .build(),
        )
        .build();

    let converted = aws_stream_record_to_contract(&record).unwrap();

    assert_eq!(converted.sequence_number, "42");
    assert_eq!(converted.keys["pk"], StorageValue::S("user#1".to_string()));
    let new_image = converted.new_image.unwrap();
    assert_eq!(
        new_image["email"],
        StorageValue::S("a@example.com".to_string())
    );
    assert_eq!(new_image["age"], StorageValue::N("37".to_string()));
    assert_eq!(new_image["active"], StorageValue::BOOL(true));
    assert_eq!(new_image["payload"], StorageValue::B("YWJj".to_string()));
    assert_eq!(
        new_image["tags"],
        StorageValue::SS(vec!["admin".to_string(), "beta".to_string()])
    );
    assert_eq!(
        new_image["scores"],
        StorageValue::NS(vec!["1".to_string(), "2".to_string()])
    );
    assert_eq!(
        new_image["blobs"],
        StorageValue::BS(vec!["YQ==".to_string(), "Yg==".to_string()])
    );
    assert_eq!(
        new_image["nested"],
        StorageValue::M(HashMap::from([(
            "deleted".to_string(),
            StorageValue::NULL(true)
        )]))
    );
    assert_eq!(
        new_image["list"],
        StorageValue::L(vec![StorageValue::S("one".to_string())])
    );
}

#[test]
fn given_empty_aws_stream_image_when_converted_then_optional_image_is_omitted() {
    let record = Record::builder()
        .dynamodb(
            StreamRecord::builder()
                .sequence_number("42")
                .set_keys(Some(HashMap::from([(
                    "pk".to_string(),
                    AwsStreamAttributeValue::S("user#1".to_string()),
                )])))
                .set_old_image(Some(HashMap::new()))
                .set_new_image(Some(HashMap::new()))
                .build(),
        )
        .build();

    let converted = aws_stream_record_to_contract(&record).unwrap();

    assert!(converted.old_image.is_none());
    assert!(converted.new_image.is_none());
}

#[test]
fn given_aws_stream_record_without_keys_when_converted_then_error_names_missing_keys() {
    let record = Record::builder()
        .dynamodb(StreamRecord::builder().sequence_number("42").build())
        .build();

    let error = aws_stream_record_to_contract(&record).unwrap_err();

    assert_eq!(error.to_string(), "aws stream record did not include keys");
}

#[test]
fn given_aws_stream_record_without_dynamodb_payload_when_converted_then_sequence_context_is_required()
 {
    let record = Record::builder().event_name(OperationType::Insert).build();

    let error = aws_stream_record_to_contract(&record).unwrap_err();

    assert_eq!(
        error.to_string(),
        "aws stream record did not include a sequence number"
    );
}

#[test]
fn given_aws_stream_record_without_sequence_number_when_converted_then_resume_position_is_required()
{
    let record = Record::builder()
        .dynamodb(
            StreamRecord::builder()
                .set_keys(Some(HashMap::from([(
                    "pk".to_string(),
                    AwsStreamAttributeValue::S("user#1".to_string()),
                )])))
                .build(),
        )
        .build();

    let error = aws_stream_record_to_contract(&record).unwrap_err();

    assert_eq!(
        error.to_string(),
        "aws stream record did not include a sequence number"
    );
}
