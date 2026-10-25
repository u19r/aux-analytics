use super::runtime::sanitize_error_message;

#[test]
fn given_database_url_and_password_tokens_when_error_sanitized_then_credentials_are_redacted() {
    let message =
        "failed to connect to db://user:secret_token@example.test:1234/catalog with secret_token";

    let sanitized = sanitize_error_message(message);

    assert!(!sanitized.contains("db://"));
    assert!(!sanitized.contains("secret_token"));
    assert_eq!(
        sanitized,
        "failed to connect to <redacted-url> with <redacted-password>"
    );
}
