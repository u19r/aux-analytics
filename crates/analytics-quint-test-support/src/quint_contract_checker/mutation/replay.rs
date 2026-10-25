use std::path::Path;

use serde_json::Value;

use super::{
    super::symbols::{CodeToken, SourceSymbols},
    QuintCommand,
    counterexample::read_bounded_bytes,
};
use crate::constants::{
    MAX_INDEPENDENT_EVALUATION_OUTPUT_BYTES, MAX_INDEPENDENT_REPLAY_ARGUMENT_BYTES, MAX_MODEL_BYTES,
};

pub(crate) fn independently_verify_invariant(
    command: &QuintCommand,
    model_path: &Path,
    document: &Value,
    invariant: &str,
) -> Result<(), String> {
    let vars = document
        .get("vars")
        .and_then(Value::as_array)
        .ok_or("ITF trace variables are missing")?;
    let states = document
        .get("states")
        .and_then(Value::as_array)
        .ok_or("ITF trace states are missing")?;
    let source = read_bounded_bytes(model_path, MAX_MODEL_BYTES).map_err(|error| {
        format!("mutated source could not be read for independent evaluation: {error}")
    })?;
    let source = String::from_utf8(source)
        .map_err(|_| "mutated source is not valid UTF-8 for independent evaluation".to_owned())?;
    let module = parse_module_name(&source)?;
    let require = format!("{}::{}", model_path.display(), module);
    let mut arguments = Vec::new();
    let mut argument_bytes = 0usize;
    push_replay_argument(&mut arguments, &mut argument_bytes, "-q".to_owned())?;
    push_replay_argument(
        &mut arguments,
        &mut argument_bytes,
        "--backend=typescript".to_owned(),
    )?;
    push_replay_argument(&mut arguments, &mut argument_bytes, "-r".to_owned())?;
    push_replay_argument(&mut arguments, &mut argument_bytes, require)?;
    ReplayStates {
        vars,
        states,
        invariant,
    }
    .append_arguments(&mut arguments, &mut argument_bytes)?;
    let output = command.run_output(&arguments)?;
    validate_evaluation_output(output, states.len())
}

struct ReplayStates<'a> {
    vars: &'a [Value],
    states: &'a [Value],
    invariant: &'a str,
}

impl ReplayStates<'_> {
    fn append_arguments(
        &self,
        arguments: &mut Vec<String>,
        argument_bytes: &mut usize,
    ) -> Result<(), String> {
        for state in self.states {
            let state = state
                .as_object()
                .ok_or("ITF trace state is not a structured object")?;
            let mut assignments = Vec::new();
            for variable in self.vars {
                let variable = variable
                    .as_str()
                    .ok_or("ITF trace variable is not a string")?;
                let value = state
                    .get(variable)
                    .ok_or_else(|| format!("ITF trace state is missing variable {variable}"))?;
                let literal = serialize_quint_literal(value)?;
                assignments.push(format!("{variable}' = {literal}"));
            }
            let assignment = format!("all {{ {}, }}", assignments.join(", "));
            push_replay_argument(arguments, argument_bytes, assignment)?;
            push_replay_argument(arguments, argument_bytes, self.invariant.to_owned())?;
        }
        Ok(())
    }
}

fn validate_evaluation_output(
    output: std::process::Output,
    expected_assignments: usize,
) -> Result<(), String> {
    if !output.status.success() {
        return Err(format!(
            "independent Quint evaluator exited with {}",
            output.status
        ));
    }
    if output.stdout.len() as u64 > MAX_INDEPENDENT_EVALUATION_OUTPUT_BYTES
        || output.stderr.len() as u64 > MAX_INDEPENDENT_EVALUATION_OUTPUT_BYTES
    {
        return Err("independent Quint evaluator output exceeds its bound".to_owned());
    }
    if !output.stderr.is_empty() {
        return Err("independent Quint evaluator reported an error".to_owned());
    }
    let output = String::from_utf8(output.stdout)
        .map_err(|_| "independent Quint evaluator output is not valid UTF-8".to_owned())?;
    validate_evaluation_lines(&output, expected_assignments)
}

fn validate_evaluation_lines(output: &str, expected_assignments: usize) -> Result<(), String> {
    let lines = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let expected_results = expected_assignments
        .checked_mul(2)
        .ok_or("independent evaluator result count overflows")?;
    if lines.len() != expected_results {
        return Err(format!(
            "independent evaluator returned {} results, expected {expected_results}",
            lines.len()
        ));
    }
    for line in &lines[..expected_assignments] {
        if *line != "true" {
            return Err("independent evaluator could not assign an ITF state".to_owned());
        }
    }
    let violations = lines[expected_assignments..]
        .iter()
        .map(|line| match *line {
            "true" => Ok(false),
            "false" => Ok(true),
            _ => Err("independent evaluator did not return Boolean invariant results".to_owned()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if violations.iter().any(|violation| *violation) {
        Ok(())
    } else {
        Err("the requested invariant holds for every ITF state".to_owned())
    }
}

fn push_replay_argument(
    arguments: &mut Vec<String>,
    total_bytes: &mut usize,
    argument: String,
) -> Result<(), String> {
    let next = total_bytes
        .checked_add(argument.len())
        .and_then(|bytes| bytes.checked_add(1))
        .ok_or("independent evaluator argument size overflows")?;
    if next > MAX_INDEPENDENT_REPLAY_ARGUMENT_BYTES {
        return Err(format!(
            "independent evaluator arguments exceed {MAX_INDEPENDENT_REPLAY_ARGUMENT_BYTES}-byte \
             limit"
        ));
    }
    *total_bytes = next;
    arguments.push(argument);
    Ok(())
}

fn parse_module_name(source: &str) -> Result<String, String> {
    SourceSymbols::parse(source)
        .tokens
        .windows(2)
        .find_map(|window| match window {
            [
                CodeToken::Identifier("module", _),
                CodeToken::Identifier(name, _),
            ] => Some((*name).to_owned()),
            _ => None,
        })
        .ok_or_else(|| "mutated source has no module declaration".to_owned())
}

fn serialize_quint_literal(value: &Value) -> Result<String, String> {
    match value {
        Value::Null => Err("ITF contains a null value".to_owned()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => {
            if value.as_i64().is_some() || value.as_u64().is_some() {
                Ok(value.to_string())
            } else {
                Err("ITF contains a non-integral number".to_owned())
            }
        }
        Value::String(value) => serde_json::to_string(value)
            .map_err(|error| format!("ITF string could not be encoded: {error}")),
        Value::Array(values) => serialize_quint_constructor("List", values),
        Value::Object(object) => serialize_quint_object(object),
    }
}

fn serialize_quint_object(object: &serde_json::Map<String, Value>) -> Result<String, String> {
    for (field, constructor, kind) in [("#tup", "Tup", "tuple"), ("#set", "Set", "set")] {
        if let Some(value) = object.get(field) {
            validate_single_object_field(object, kind)?;
            return serialize_quint_array_constructor(constructor, value);
        }
    }
    if let Some(value) = object.get("#bigint") {
        validate_single_object_field(object, "bigint")?;
        return serialize_bigint(value);
    }
    if let Some(value) = object.get("#map") {
        validate_single_object_field(object, "map")?;
        return serialize_map_entries(value);
    }
    if object.contains_key("#unserializable") {
        return Err("ITF contains an unserializable value".to_owned());
    }
    if let Some(tag) = object.get("tag") {
        if object.len() > 2 || (object.len() == 2 && !object.contains_key("value")) {
            return Err("ITF variant has unexpected fields".to_owned());
        }
        let tag = tag.as_str().ok_or("ITF variant tag is not a string")?;
        if tag == "UNIT" && object.len() == 1 {
            return Ok("Tup()".to_owned());
        }
        let unit = Value::Array(Vec::new());
        let value = object.get("value").unwrap_or(&unit);
        return Ok(format!(
            "variant({}, {})",
            serde_json::to_string(tag).map_err(|error| error.to_string())?,
            serialize_quint_literal(value)?
        ));
    }
    let mut fields = Vec::with_capacity(object.len() * 2);
    for (field, value) in object {
        fields.push(serde_json::to_string(field).map_err(|error| error.to_string())?);
        fields.push(serialize_quint_literal(value)?);
    }
    Ok(format!("Rec({})", fields.join(", ")))
}

fn serialize_quint_constructor(name: &str, values: &[Value]) -> Result<String, String> {
    values
        .iter()
        .map(serialize_quint_literal)
        .collect::<Result<Vec<_>, _>>()
        .map(|values| format!("{name}({})", values.join(", ")))
}

fn serialize_quint_array_constructor(name: &str, value: &Value) -> Result<String, String> {
    let values = value
        .as_array()
        .ok_or_else(|| format!("ITF {name} is not an array"))?;
    serialize_quint_constructor(name, values)
}

fn validate_single_object_field(
    object: &serde_json::Map<String, Value>,
    kind: &str,
) -> Result<(), String> {
    if object.len() != 1 {
        return Err(format!("ITF {kind} has unexpected fields"));
    }
    Ok(())
}

fn serialize_bigint(value: &Value) -> Result<String, String> {
    let value = value.as_str().ok_or("ITF bigint is not a decimal string")?;
    if value.is_empty()
        || !value
            .as_bytes()
            .iter()
            .enumerate()
            .all(|(index, byte)| byte.is_ascii_digit() || (index == 0 && *byte == b'-'))
    {
        return Err("ITF bigint is not a decimal integer".to_owned());
    }
    Ok(value.to_owned())
}

fn serialize_map_entries(value: &Value) -> Result<String, String> {
    let entries = value
        .as_array()
        .ok_or("ITF map entries are not an array")?
        .iter()
        .map(|entry| {
            let pair = entry
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or("ITF map entry is not a key/value pair")?;
            Ok(format!(
                "{} -> {}",
                serialize_quint_literal(&pair[0])?,
                serialize_quint_literal(&pair[1])?
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(format!("Map({})", entries.join(", ")))
}
