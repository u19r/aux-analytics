use reqwest::StatusCode;

use crate::error::{AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind};

#[test]
fn given_missing_aux_storage_endpoint_when_formatted_then_message_names_required_config() {
    let error = AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingAuxStorageEndpoint);

    assert_eq!(
        error.to_string(),
        "analytics.source.endpoint_url is required for aux_storage polling"
    );
}

#[test]
fn given_source_table_debug_when_formatted_then_table_name_is_included() {
    let error = AnalyticsStorageError::with_debug(
        AnalyticsStorageErrorKind::MissingStreamIdentifier,
        AnalyticsStorageErrorDebug::SourceTableName("source_users".to_string()),
    );

    assert_eq!(
        error.to_string(),
        "analytics source table needs stream_identifier for storage_stream polling: \
         table_name=source_users"
    );
}

#[test]
fn given_http_status_debug_when_formatted_then_status_and_body_are_included() {
    let error = AnalyticsStorageError::with_debug(
        AnalyticsStorageErrorKind::HttpStatus,
        AnalyticsStorageErrorDebug::HttpStatus {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: "temporarily unavailable".to_string(),
        },
    );

    assert_eq!(
        error.to_string(),
        "analytics source http request returned an error status: status=503 Service Unavailable, \
         body=temporarily unavailable"
    );
}

#[test]
fn given_storage_boundary_failures_when_formatted_then_operator_messages_name_required_context() {
    let cases = [
        (
            AnalyticsStorageErrorKind::UnregisteredSourceTable,
            "analytics source table is not registered in the manifest",
        ),
        (
            AnalyticsStorageErrorKind::InvalidUrl,
            "invalid analytics source endpoint url",
        ),
        (
            AnalyticsStorageErrorKind::InvalidHeader,
            "invalid analytics source header",
        ),
        (
            AnalyticsStorageErrorKind::Http,
            "analytics source http request failed",
        ),
        (
            AnalyticsStorageErrorKind::Json,
            "analytics source json error",
        ),
        (
            AnalyticsStorageErrorKind::AwsSdk,
            "aws stream polling failed",
        ),
        (
            AnalyticsStorageErrorKind::MissingSequenceNumber,
            "aws stream record did not include a sequence number",
        ),
        (
            AnalyticsStorageErrorKind::MissingKeys,
            "aws stream record did not include keys",
        ),
        (
            AnalyticsStorageErrorKind::UnsupportedAwsAttributeValue,
            "aws stream attribute value variant is unsupported",
        ),
    ];

    for (kind, message) in cases {
        let error = AnalyticsStorageError::new(kind);

        assert_eq!(error.to_string(), message);
    }
}

#[test]
fn given_aws_sdk_debug_when_formatted_then_sdk_message_is_visible_to_operator() {
    let error = AnalyticsStorageError::with_debug(
        AnalyticsStorageErrorKind::AwsSdk,
        AnalyticsStorageErrorDebug::AwsSdk("expired iterator".to_string()),
    );

    assert_eq!(
        error.to_string(),
        "aws stream polling failed: expired iterator"
    );
}
