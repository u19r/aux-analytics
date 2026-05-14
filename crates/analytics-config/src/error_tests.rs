use std::path::PathBuf;

use crate::{ConfigError, ConfigErrorDebug, ConfigErrorKind};

#[test]
fn given_missing_manifest_path_when_formatted_then_message_names_manifest_requirement() {
    let error = ConfigError::new(ConfigErrorKind::MissingManifestPath);

    assert_eq!(
        error.to_string(),
        "analytics manifest path is required: pass --manifest or set analytics.manifest_path"
    );
}

#[test]
fn given_invalid_source_table_stream_type_when_formatted_then_table_name_is_included() {
    let error = ConfigError::with_debug(
        ConfigErrorKind::InvalidSourceTableStreamType,
        ConfigErrorDebug::SourceTableName("source_users".to_string()),
    );

    assert_eq!(
        error.to_string(),
        "analytics source table needs a stream_type or analytics.source.stream_type: \
         table_name=source_users"
    );
}

#[test]
fn given_config_path_debug_when_formatted_then_override_path_is_included() {
    let error = ConfigError::invalid_override_path("analytics..source");

    assert_eq!(
        error.to_string(),
        "invalid override path: config_path=analytics..source"
    );
}

#[test]
fn given_io_error_when_formatted_then_path_is_included() {
    let error = ConfigError::io(
        PathBuf::from("analytics.json"),
        std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
    );

    assert_eq!(
        error.to_string(),
        "failed to read config: path=analytics.json"
    );
}
