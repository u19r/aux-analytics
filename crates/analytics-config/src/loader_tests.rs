use serde_json::json;

use crate::loader::{
    apply_override, load_optional_with_overrides, merge_json, parse_override_value,
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
