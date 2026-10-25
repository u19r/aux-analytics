use super::*;

pub(crate) fn alias_sql(expression: &str, alias: &str) -> String {
    format!("{expression} AS {}", sql::quote_identifier(alias))
}

pub(crate) fn document_json_sql(document_sql: &str, path: Option<&str>) -> String {
    match path {
        Some(path) => format!(
            "json_extract({document_sql}, '{}')",
            escape_sql_string(document_json_path(path).as_str())
        ),
        None => document_sql.to_string(),
    }
}

pub(crate) fn document_string_sql(document_sql: &str, path: Option<&str>) -> String {
    format!(
        "json_extract_string({document_sql}, '{}')",
        escape_sql_string(
            path.map(document_json_path)
                .unwrap_or_else(|| "$".to_string())
                .as_str()
        )
    )
}

pub(crate) fn document_json_path(path: &str) -> String {
    if path.starts_with('[') {
        format!("${path}")
    } else {
        format!("$.{path}")
    }
}

pub(crate) fn document_comparison_sql(
    document_sql: &str,
    path: Option<&str>,
    operator: QueryComparisonOperator,
    value: &serde_json::Value,
) -> AnalyticsEngineResult<String> {
    let operator = comparison_operator_sql(operator);
    match value {
        serde_json::Value::Null => {
            let expression = document_json_sql(document_sql, path);
            match operator {
                "=" => Ok(format!("{expression} IS NULL")),
                "<>" => Ok(format!("{expression} IS NOT NULL")),
                _ => Ok("FALSE".to_string()),
            }
        }
        serde_json::Value::Bool(_) => Ok(format!(
            "try_cast({} AS BOOLEAN) {operator} {}",
            document_string_sql(document_sql, path),
            literal_sql(value)?
        )),
        serde_json::Value::Number(_) => Ok(format!(
            "try_cast({} AS DOUBLE) {operator} {}",
            document_string_sql(document_sql, path),
            literal_sql(value)?
        )),
        serde_json::Value::String(_) => Ok(format!(
            "{} {operator} {}",
            document_string_sql(document_sql, path),
            literal_sql(value)?
        )),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(AnalyticsEngineError::InvalidStructuredQuery(
                "document comparison literal must be scalar".to_string(),
            ))
        }
    }
}

pub(crate) fn document_leaf_predicate_sql(
    document_sql: &str,
    predicate: &QueryDocumentPredicate,
) -> AnalyticsEngineResult<String> {
    match predicate {
        QueryDocumentPredicate::Constant { value } => {
            Ok(if *value { "TRUE" } else { "FALSE" }.to_string())
        }
        QueryDocumentPredicate::Exists { path } => Ok(format!(
            "{} IS NOT NULL",
            document_json_sql(document_sql, path.as_deref())
        )),
        QueryDocumentPredicate::NonEmpty { path } => Ok(format!(
            "coalesce({}, '') <> ''",
            document_string_sql(document_sql, path.as_deref())
        )),
        QueryDocumentPredicate::Compare {
            path,
            operator,
            value,
        } => document_comparison_sql(document_sql, path.as_deref(), *operator, value),
        QueryDocumentPredicate::String { .. }
        | QueryDocumentPredicate::TimestampString { .. }
        | QueryDocumentPredicate::AffixedString { .. } => {
            document_text_predicate_sql(document_sql, predicate)
        }
        QueryDocumentPredicate::All { .. }
        | QueryDocumentPredicate::Any { .. }
        | QueryDocumentPredicate::Not { .. }
        | QueryDocumentPredicate::ArrayAny { .. } => unreachable!("composite predicate"),
    }
}

pub(crate) fn document_text_predicate_sql(
    document_sql: &str,
    predicate: &QueryDocumentPredicate,
) -> AnalyticsEngineResult<String> {
    match predicate {
        QueryDocumentPredicate::String {
            path,
            operator,
            value,
            case_sensitive,
        } => document_string_predicate_sql(
            document_sql,
            path.as_deref(),
            StringPredicateInput::new(*operator, value, *case_sensitive),
        ),
        QueryDocumentPredicate::TimestampString {
            path,
            operator,
            value,
        } => document_timestamp_string_predicate_sql(
            document_sql,
            path.as_deref(),
            StringPredicateInput::new(*operator, value, true),
        ),
        QueryDocumentPredicate::AffixedString {
            path,
            operator,
            value,
            prefix,
            suffix,
            case_sensitive,
        } => document_affixed_string_predicate_sql(
            document_sql,
            path.as_deref(),
            prefix,
            suffix,
            StringPredicateInput::new(*operator, value, *case_sensitive),
        ),
        _ => unreachable!("non-text predicate"),
    }
}

#[derive(Clone, Copy)]
pub(crate) struct StringPredicateInput<'a> {
    operator: QueryStringOperator,
    value: &'a str,
    case_sensitive: bool,
}

impl<'a> StringPredicateInput<'a> {
    const fn new(operator: QueryStringOperator, value: &'a str, case_sensitive: bool) -> Self {
        Self {
            operator,
            value,
            case_sensitive,
        }
    }
}

pub(crate) fn document_string_predicate_sql(
    document_sql: &str,
    path: Option<&str>,
    input: StringPredicateInput<'_>,
) -> AnalyticsEngineResult<String> {
    string_predicate_sql(document_string_sql(document_sql, path), input)
}

pub(crate) fn document_timestamp_string_predicate_sql(
    document_sql: &str,
    path: Option<&str>,
    input: StringPredicateInput<'_>,
) -> AnalyticsEngineResult<String> {
    let millis = format!(
        "try_cast({} AS BIGINT)",
        document_string_sql(document_sql, path)
    );
    let timestamp = format!("make_timestamp_ms({millis})");
    let expression = format!(
        "CASE WHEN {millis} % 1000 = 0 THEN strftime({timestamp}, '%Y-%m-%dT%H:%M:%SZ') ELSE \
         strftime({timestamp}, '%Y-%m-%dT%H:%M:%S.') || lpad(cast({millis} % 1000 AS VARCHAR), 3, \
         '0') || 'Z' END"
    );
    string_predicate_sql(expression, input)
}

pub(crate) fn document_affixed_string_predicate_sql(
    document_sql: &str,
    path: Option<&str>,
    prefix: &str,
    suffix: &str,
    input: StringPredicateInput<'_>,
) -> AnalyticsEngineResult<String> {
    let raw = document_string_sql(document_sql, path);
    let prefix = literal_sql(&serde_json::Value::String(prefix.to_string()))?;
    let suffix = literal_sql(&serde_json::Value::String(suffix.to_string()))?;
    string_predicate_sql(format!("({prefix} || {raw} || {suffix})"), input)
}

pub(crate) fn string_predicate_sql(
    mut expression: String,
    input: StringPredicateInput<'_>,
) -> AnalyticsEngineResult<String> {
    let mut value = literal_sql(&serde_json::Value::String(input.value.to_string()))?;
    if !input.case_sensitive {
        expression = format!("lower({expression})");
        value = format!("lower({value})");
    }
    Ok(match input.operator {
        QueryStringOperator::Eq => format!("{expression} = {value}"),
        QueryStringOperator::NotEq => format!("{expression} <> {value}"),
        QueryStringOperator::Contains => format!("contains({expression}, {value})"),
        QueryStringOperator::StartsWith => format!("starts_with({expression}, {value})"),
        QueryStringOperator::EndsWith => format!("ends_with({expression}, {value})"),
        QueryStringOperator::Gt => format!("{expression} > {value}"),
        QueryStringOperator::Gte => format!("{expression} >= {value}"),
        QueryStringOperator::Lt => format!("{expression} < {value}"),
        QueryStringOperator::Lte => format!("{expression} <= {value}"),
    })
}

pub(crate) fn comparison_operator_sql(operator: QueryComparisonOperator) -> &'static str {
    match operator {
        QueryComparisonOperator::Eq => "=",
        QueryComparisonOperator::NotEq => "<>",
        QueryComparisonOperator::Gt => ">",
        QueryComparisonOperator::Gte => ">=",
        QueryComparisonOperator::Lt => "<",
        QueryComparisonOperator::Lte => "<=",
    }
}

pub(crate) fn literal_sql(value: &serde_json::Value) -> AnalyticsEngineResult<String> {
    match value {
        serde_json::Value::Null => Ok("NULL".to_string()),
        serde_json::Value::Bool(value) => Ok(if *value { "TRUE" } else { "FALSE" }.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::String(value) => Ok(format!("'{}'", escape_sql_string(value))),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let encoded = serde_json::to_string(value)?;
            Ok(format!("'{}'::JSON", escape_sql_string(encoded.as_str())))
        }
    }
}

pub(crate) fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
