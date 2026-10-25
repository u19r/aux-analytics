//! Convert and intern native literal parameters without changing their order.

use super::*;

impl QueryCompiler<'_> {
    pub(super) fn literal_sql(&self, value: &serde_json::Value) -> AnalyticsEngineResult<String> {
        if !self.bind_literals {
            return literal_sql(value);
        }
        let parameter = Self::convert_literal_parameter(value)?;
        let index = self.intern_literal_parameter(parameter);
        let mut placeholder = format!("${}", index + 1);
        if value.is_array() || value.is_object() {
            placeholder.push_str("::JSON");
        }
        Ok(placeholder)
    }

    fn convert_literal_parameter(
        value: &serde_json::Value,
    ) -> AnalyticsEngineResult<duckdb::types::Value> {
        Ok(match value {
            serde_json::Value::Null => duckdb::types::Value::Null,
            serde_json::Value::Bool(value) => duckdb::types::Value::Boolean(*value),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    duckdb::types::Value::BigInt(value)
                } else if let Some(value) = value.as_u64() {
                    duckdb::types::Value::UBigInt(value)
                } else {
                    duckdb::types::Value::Double(value.as_f64().ok_or_else(|| {
                        AnalyticsEngineError::InvalidStructuredQuery(
                            "query literal is not a finite number".to_string(),
                        )
                    })?)
                }
            }
            serde_json::Value::String(value) => duckdb::types::Value::Text(value.clone()),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                duckdb::types::Value::Text(serde_json::to_string(value)?)
            }
        })
    }

    fn intern_literal_parameter(&self, parameter: duckdb::types::Value) -> usize {
        let fingerprint = parameter_fingerprint(&parameter);
        let existing_index = self
            .parameter_indexes
            .borrow()
            .get(&fingerprint)
            .and_then(|index| {
                let parameters = self.parameters.borrow();
                if parameters[*index] == parameter {
                    Some(*index)
                } else {
                    // A hash collision must not change parameter equality.
                    parameters
                        .iter()
                        .position(|existing| existing == &parameter)
                }
            });
        if let Some(index) = existing_index {
            return index;
        }
        let mut parameters = self.parameters.borrow_mut();
        let index = parameters.len();
        parameters.push(parameter);
        drop(parameters);
        self.parameter_indexes
            .borrow_mut()
            .entry(fingerprint)
            .or_insert(index);
        index
    }
}
