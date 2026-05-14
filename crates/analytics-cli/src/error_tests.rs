use crate::error::{CliError, CliErrorDebug, CliErrorKind};

#[test]
fn given_config_error_debug_when_formatted_then_message_is_visible_to_cli_user() {
    let error = CliError::with_debug(
        CliErrorKind::Config,
        CliErrorDebug::Message("missing manifest path".to_string()),
    );

    assert_eq!(
        error.to_string(),
        "analytics configuration failed: missing manifest path"
    );
}

#[test]
fn given_structured_query_validation_error_kind_when_formatted_then_query_context_is_named() {
    let error = CliError::with_debug(CliErrorKind::StructuredQueryValidation, CliErrorDebug::None);

    assert_eq!(error.to_string(), "structured query validation failed");
}

#[test]
fn given_cli_boundary_failures_when_formatted_then_user_messages_name_the_failed_subsystem() {
    let cases = [
        (CliErrorKind::Io, "cli I/O failed"),
        (CliErrorKind::Json, "cli JSON processing failed"),
        (
            CliErrorKind::ManifestValidation,
            "analytics manifest validation failed",
        ),
        (CliErrorKind::Engine, "analytics engine operation failed"),
    ];

    for (kind, message) in cases {
        let error = CliError::with_debug(kind, CliErrorDebug::None);

        assert_eq!(error.to_string(), message);
    }
}

#[test]
fn given_manifest_load_error_when_converted_then_cli_error_includes_manifest_context() {
    let source = analytics_contract::read_manifest(
        "/tmp/aux-analytics-missing-manifest-for-cli-error-test.json",
    )
    .unwrap_err();

    let error = CliError::from(source);

    assert!(
        error
            .to_string()
            .starts_with("analytics manifest validation failed:")
    );
}
