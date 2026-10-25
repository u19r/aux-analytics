use std::{fs, io::Read, path::Path};

use serde_json::Value;

use super::super::symbols::is_quint_identifier;
use crate::constants::MAX_MODEL_BYTES;

pub(crate) fn read_bounded_source(path: &Path, display: &str) -> Result<String, String> {
    let bytes = read_bounded_bytes(path, MAX_MODEL_BYTES)
        .map_err(|error| format!("could not read {display}: {error}"))?;
    String::from_utf8(bytes).map_err(|_| format!("{display} is not valid UTF-8"))
}

pub(crate) fn read_bounded_bytes(path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err("path is not a regular file".to_owned());
    }
    if metadata.len() > limit {
        return Err(format!("{limit}-byte limit exceeded"));
    }
    let mut bytes = Vec::with_capacity(metadata.len().min(limit) as usize);
    fs::File::open(path)
        .map_err(|error| error.to_string())?
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > limit {
        return Err(format!("{limit}-byte limit exceeded"));
    }
    Ok(bytes)
}

#[cfg(test)]
pub(crate) fn performance_read_bounded_bytes(path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    read_bounded_bytes(path, limit)
}

pub(crate) fn read_bounded_json(path: &Path, limit: u64) -> Result<Value, String> {
    let bytes = read_bounded_bytes(path, limit)?;
    serde_json::from_slice(&bytes).map_err(|error| format!("JSON is invalid: {error}"))
}

pub(crate) fn validate_itf(
    document: &Value,
    expected_source: &Path,
    max_steps: usize,
) -> Result<(), String> {
    validate_trace_source(document, expected_source)?;
    let vars = document
        .get("vars")
        .and_then(Value::as_array)
        .filter(|vars| !vars.is_empty())
        .ok_or("ITF trace has no variables")?;
    let states = document
        .get("states")
        .and_then(Value::as_array)
        .filter(|states| !states.is_empty())
        .ok_or("ITF trace has no states")?;
    let max_states = max_steps
        .checked_add(1)
        .ok_or("ITF trace step bound overflows")?;
    if states.len() > max_states {
        return Err("ITF trace exceeds the requested step bound".to_owned());
    }
    if states.iter().any(|state| !state.is_object()) {
        return Err("ITF trace states are not structured objects".to_owned());
    }
    let variable_names = parse_trace_variables(vars)?;
    if states.iter().any(|state| {
        let Some(state) = state.as_object() else {
            return true;
        };
        variable_names
            .iter()
            .any(|variable| !state.contains_key(*variable))
            || state
                .keys()
                .any(|key| key != "#meta" && !variable_names.iter().any(|variable| variable == key))
    }) {
        return Err("ITF trace states do not exactly match declared variables".to_owned());
    }
    Ok(())
}

fn validate_trace_source(document: &Value, expected_source: &Path) -> Result<(), String> {
    let metadata = document
        .get("#meta")
        .and_then(Value::as_object)
        .ok_or("ITF metadata is missing")?;
    if metadata.get("format").and_then(Value::as_str) != Some("ITF") {
        return Err("ITF metadata has an unexpected format".to_owned());
    }
    if metadata.get("status").and_then(Value::as_str) != Some("violation") {
        return Err("ITF metadata does not report an invariant violation".to_owned());
    }
    let source = metadata
        .get("source")
        .and_then(Value::as_str)
        .ok_or("ITF metadata has no source")?;
    let actual_source = Path::new(source)
        .canonicalize()
        .map_err(|_| "ITF source cannot be resolved".to_owned())?;
    let expected_source = expected_source
        .canonicalize()
        .map_err(|_| "mutated source cannot be resolved".to_owned())?;
    if actual_source != expected_source {
        return Err("ITF source does not match the mutated model".to_owned());
    }
    Ok(())
}

fn parse_trace_variables(vars: &[Value]) -> Result<Vec<&str>, String> {
    let variable_names = vars
        .iter()
        .map(|variable| {
            let variable = variable
                .as_str()
                .ok_or("ITF trace variables are not structured names")?;
            if !is_quint_identifier(variable) {
                return Err("ITF trace variable is not a safe Quint identifier");
            }
            Ok(variable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut unique_variable_names = variable_names.to_vec();
    unique_variable_names.sort_unstable();
    if unique_variable_names
        .windows(2)
        .any(|pair| pair[0] == pair[1])
    {
        return Err("ITF trace variables contain duplicates".to_owned());
    }
    Ok(variable_names)
}
