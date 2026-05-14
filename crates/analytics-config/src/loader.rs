use std::{path::Path, sync::Arc};

use serde_json::Value;

use crate::{ConfigError, RootConfig};

#[derive(Debug, Clone)]
pub struct Config {
    pub root: RootConfig,
}

pub fn load_optional_with_overrides(
    config_path: Option<&Path>,
    overrides: &[(String, String)],
) -> Result<Arc<Config>, ConfigError> {
    let mut value = serde_json::to_value(RootConfig::default())
        .map_err(|source| ConfigError::json(None, source))?;
    if let Some(path) = config_path {
        let raw = std::fs::read_to_string(path)
            .map_err(|source| ConfigError::io(path.to_path_buf(), source))?;
        let file_value: Value = serde_json::from_str(&raw)
            .map_err(|source| ConfigError::json(Some(path.to_path_buf()), source))?;
        merge_json(&mut value, file_value);
    }
    for (path, raw) in overrides {
        apply_override(&mut value, path, raw)?;
    }
    let root = serde_json::from_value(value).map_err(|source| ConfigError::json(None, source))?;
    Ok(Arc::new(Config { root }))
}

pub(crate) fn merge_json(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                merge_json(base.entry(key).or_insert(Value::Null), value);
            }
        }
        (base, overlay) => *base = overlay,
    }
}

pub(crate) fn apply_override(root: &mut Value, path: &str, raw: &str) -> Result<(), ConfigError> {
    let parsed = parse_override_value(raw);
    let mut cursor = root;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        if part.is_empty() {
            return Err(ConfigError::invalid_override_path(path));
        }
        if parts.peek().is_none() {
            let Some(object) = cursor.as_object_mut() else {
                return Err(ConfigError::invalid_override_path(path));
            };
            object.insert(part.to_string(), parsed);
            return Ok(());
        }
        let Some(object) = cursor.as_object_mut() else {
            return Err(ConfigError::invalid_override_path(path));
        };
        cursor = object
            .entry(part.to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::default()));
    }
    Err(ConfigError::invalid_override_path(path))
}

pub(crate) fn parse_override_value(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}
