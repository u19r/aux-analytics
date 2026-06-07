use serde_json::json;

use crate::{
    DEFAULT_QUERY_MAX_READ_CONNECTIONS,
    loader::{
        apply_override, expand_env_placeholders, load_optional_with_overrides, merge_json,
        parse_override_value,
    },
};

#[test]
fn given_partial_file_config_when_loaded_then_defaults_are_recursively_preserved() {
    let mut base = json!({
        "http": {
            "bind_addr": "127.0.0.1:3000",
            "cors": {
                "allow_origins": ["https://app.example"]
            }
        },
        "features": {
            "metrics": {
                "enabled": true
            }
        }
    });

    merge_json(
        &mut base,
        json!({
            "http": {
                "cors": {
                    "allow_origins": ["https://admin.example"]
                }
            }
        }),
    );

    assert_eq!(base["http"]["bind_addr"], json!("127.0.0.1:3000"));
    assert_eq!(
        base["http"]["cors"]["allow_origins"],
        json!(["https://admin.example"])
    );
    assert_eq!(base["features"]["metrics"]["enabled"], json!(true));
}

#[test]
fn given_override_path_when_applied_then_nested_config_value_is_replaced() {
    let mut root = json!({
        "analytics": {
            "http": {
                "ingest_endpoint_enabled": true
            }
        }
    });

    apply_override(&mut root, "analytics.http.ingest_endpoint_enabled", "false").unwrap();

    assert_eq!(
        root["analytics"]["http"]["ingest_endpoint_enabled"],
        json!(false)
    );
}

#[test]
fn given_no_query_config_when_loaded_then_read_connection_default_is_used() {
    let config = load_optional_with_overrides(None, &[]).expect("config");

    assert_eq!(
        config.root.analytics.query.max_read_connections,
        DEFAULT_QUERY_MAX_READ_CONNECTIONS
    );
}

#[test]
fn given_query_read_connection_override_when_loaded_then_value_is_used() {
    let config = load_optional_with_overrides(
        None,
        &[(
            "analytics.query.max_read_connections".to_string(),
            "128".to_string(),
        )],
    )
    .expect("config");

    assert_eq!(config.root.analytics.query.max_read_connections, 128);
}

#[test]
fn given_invalid_override_path_when_applied_then_config_rejects_it() {
    let mut root = json!({
        "analytics": {
            "http": {}
        }
    });

    let error = apply_override(&mut root, "analytics..enabled", "true").unwrap_err();

    assert!(error.to_string().contains("invalid override path"));
}

#[test]
fn given_override_value_when_it_is_json_then_type_is_preserved() {
    assert_eq!(parse_override_value("42"), json!(42));
    assert_eq!(parse_override_value("false"), json!(false));
    assert_eq!(parse_override_value("plain"), json!("plain"));
}

#[test]
fn given_runtime_secret_placeholder_when_env_exists_then_value_is_expanded() {
    let home = std::env::var("HOME").expect("HOME should be set in test environment");

    assert_eq!(
        expand_env_placeholders("host=db password=${HOME}"),
        format!("host=db password={home}")
    );
}

#[test]
fn given_missing_config_file_when_loaded_then_error_names_config_path() {
    let path = std::path::Path::new("/tmp/aux-analytics-missing-config-for-loader-test.json");

    let error = load_optional_with_overrides(Some(path), &[]).unwrap_err();

    assert!(error.to_string().contains("failed to read config"));
    assert!(
        error
            .to_string()
            .contains("aux-analytics-missing-config-for-loader-test.json")
    );
}

#[test]
fn given_override_path_descends_through_scalar_when_applied_then_config_rejects_it() {
    let mut root = json!({
        "analytics": "not-an-object"
    });

    let error = apply_override(&mut root, "analytics.http.enabled", "true").unwrap_err();

    assert!(error.to_string().contains("invalid override path"));
}

#[test]
fn given_empty_override_path_when_applied_then_config_rejects_it() {
    let mut root = json!({});

    let error = apply_override(&mut root, "", "true").unwrap_err();

    assert!(error.to_string().contains("invalid override path"));
}
