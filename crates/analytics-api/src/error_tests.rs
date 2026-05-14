use crate::error::{ApiError, ApiErrorDebug, ApiErrorKind};

#[test]
fn given_config_error_debug_when_formatted_then_message_is_visible_to_operator() {
    let error = ApiError::with_debug(
        ApiErrorKind::Config,
        ApiErrorDebug::Message("missing analytics.source".to_string()),
    );

    assert_eq!(
        error.to_string(),
        "analytics configuration failed: missing analytics.source"
    );
}

#[test]
fn given_storage_error_kind_when_formatted_then_message_names_storage_operation() {
    let error = ApiError::with_debug(ApiErrorKind::Storage, ApiErrorDebug::None);

    assert_eq!(error.to_string(), "analytics storage operation failed");
}

#[test]
fn given_api_boundary_failures_when_formatted_then_operator_messages_name_the_failed_subsystem() {
    let cases = [
        (ApiErrorKind::Io, "api I/O failed"),
        (ApiErrorKind::Json, "api JSON processing failed"),
        (
            ApiErrorKind::ManifestValidation,
            "analytics manifest validation failed",
        ),
        (ApiErrorKind::Engine, "analytics engine operation failed"),
        (ApiErrorKind::AddressParse, "http bind address is invalid"),
        (ApiErrorKind::TracingInit, "tracing initialization failed"),
        (ApiErrorKind::Metrics, "metrics initialization failed"),
    ];

    for (kind, message) in cases {
        let error = ApiError::with_debug(kind, ApiErrorDebug::None);

        assert_eq!(error.to_string(), message);
    }
}

#[test]
fn given_manifest_load_error_when_converted_then_file_context_is_visible_to_operator() {
    let source = analytics_contract::read_manifest(
        "/tmp/aux-analytics-missing-manifest-for-api-error-test.json",
    )
    .unwrap_err();

    let error = ApiError::from(source);

    assert!(
        error
            .to_string()
            .starts_with("analytics manifest validation failed:")
    );
}
