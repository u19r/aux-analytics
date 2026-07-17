use std::time::Instant;

use analytics_contract::{QueryExpression, QuerySelect, StructuredQuery};
use serde_json::Value;
use thiserror::Error;

use crate::types::{
    QueryBatchResult, QueryExecutionMetadata, QueryPlanShape, QueryResponse, QueryResultColumn,
    QueryResultValueType, QuerySourceWatermark,
};

#[derive(Debug, Error)]
#[error("{0}")]
pub struct QueryResponseBuildError(String);

impl QueryResponseBuildError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

pub fn build_query_response(
    rows: Vec<Value>,
    query: &StructuredQuery,
    started: Instant,
) -> Result<QueryResponse, QueryResponseBuildError> {
    Ok(QueryResponse {
        columns: result_columns(rows.as_slice()),
        execution: execution_metadata(query, rows.len(), started)?,
        source_watermark: source_watermark(rows.as_slice()),
        rows,
    })
}

pub fn build_query_batch_result(
    name: String,
    rows: Vec<Value>,
    query: &StructuredQuery,
    started: Instant,
) -> Result<QueryBatchResult, QueryResponseBuildError> {
    Ok(QueryBatchResult {
        name,
        columns: result_columns(rows.as_slice()),
        execution: execution_metadata(query, rows.len(), started)?,
        source_watermark: source_watermark(rows.as_slice()),
        rows,
    })
}

fn execution_metadata(
    query: &StructuredQuery,
    row_count: usize,
    started: Instant,
) -> Result<QueryExecutionMetadata, QueryResponseBuildError> {
    Ok(QueryExecutionMetadata {
        query_hash: query
            .query_hash()
            .map_err(|err| QueryResponseBuildError::new(err.to_string()))?,
        row_count: row_count as u64,
        truncated: query.limit.is_some_and(|limit| row_count >= limit as usize),
        tables: participating_tables(query),
        elapsed_ms: started.elapsed().as_millis() as u64,
        plan_shape: plan_shape(query),
    })
}

fn plan_shape(query: &StructuredQuery) -> QueryPlanShape {
    QueryPlanShape {
        logical_source_count: count_u32(query.joins.len().saturating_add(1)),
        join_count: count_u32(query.joins.len()),
        filter_count: count_u32(query.filters.len()),
        group_expression_count: count_u32(query.group_by.len()),
        order_expression_count: count_u32(query.order_by.len()),
        aggregate_count: count_u32(
            query
                .select
                .iter()
                .filter(|select| {
                    matches!(
                        select,
                        QuerySelect::Count { .. }
                            | QuerySelect::Sum { .. }
                            | QuerySelect::Min { .. }
                            | QuerySelect::Max { .. }
                            | QuerySelect::Avg { .. }
                            | QuerySelect::CountDistinct { .. }
                    )
                })
                .count(),
        ),
        conditional_expression_count: count_u32(
            query
                .select
                .iter()
                .filter_map(select_expression)
                .chain(query.filters.iter().filter_map(predicate_expression))
                .chain(query.group_by.iter())
                .chain(query.order_by.iter().map(|order| &order.expression))
                .filter(|expression| matches!(expression, QueryExpression::Conditional { .. }))
                .count(),
        ),
    }
}

fn select_expression(select: &QuerySelect) -> Option<&QueryExpression> {
    match select {
        QuerySelect::Expression { expression, .. }
        | QuerySelect::Sum { expression, .. }
        | QuerySelect::Min { expression, .. }
        | QuerySelect::Max { expression, .. }
        | QuerySelect::Avg { expression, .. }
        | QuerySelect::CountDistinct { expression, .. } => Some(expression),
        QuerySelect::Column { .. }
        | QuerySelect::DocumentPath { .. }
        | QuerySelect::Count { .. } => None,
    }
}

fn predicate_expression(
    predicate: &analytics_contract::QueryPredicate,
) -> Option<&QueryExpression> {
    match predicate {
        analytics_contract::QueryPredicate::Eq { expression, .. }
        | analytics_contract::QueryPredicate::NotEq { expression, .. }
        | analytics_contract::QueryPredicate::Gt { expression, .. }
        | analytics_contract::QueryPredicate::Gte { expression, .. }
        | analytics_contract::QueryPredicate::Lt { expression, .. }
        | analytics_contract::QueryPredicate::Lte { expression, .. }
        | analytics_contract::QueryPredicate::IsNull { expression }
        | analytics_contract::QueryPredicate::IsNotNull { expression } => Some(expression),
        analytics_contract::QueryPredicate::DocumentMatches { .. } => None,
    }
}

fn count_u32(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

fn participating_tables(query: &StructuredQuery) -> Vec<String> {
    let mut tables = Vec::with_capacity(query.joins.len() + 1);
    tables.push(query.analytics_table_name.clone());
    tables.extend(
        query
            .joins
            .iter()
            .map(|join| join.analytics_table_name.clone()),
    );
    tables
}

fn result_columns(rows: &[Value]) -> Vec<QueryResultColumn> {
    let Some(row) = rows.first().and_then(Value::as_object) else {
        return Vec::new();
    };
    row.iter()
        .map(|(name, value)| QueryResultColumn {
            name: name.clone(),
            value_type: result_value_type(name, value),
            nullable: rows
                .iter()
                .any(|row| row.get(name).is_none_or(serde_json::Value::is_null)),
        })
        .collect()
}

fn result_value_type(name: &str, value: &Value) -> QueryResultValueType {
    match value {
        Value::Bool(_) => QueryResultValueType::Boolean,
        Value::String(value) if looks_like_decimal(value) => QueryResultValueType::Decimal,
        Value::String(_) => QueryResultValueType::String,
        Value::Number(number)
            if (number.is_i64() || number.is_u64()) && name.ends_with("_at_ms") =>
        {
            QueryResultValueType::TimestampMs
        }
        Value::Number(number) if number.is_i64() || number.is_u64() => QueryResultValueType::I64,
        Value::Number(_) => QueryResultValueType::F64,
        Value::Null | Value::Array(_) | Value::Object(_) => QueryResultValueType::Json,
    }
}

fn source_watermark(rows: &[Value]) -> QuerySourceWatermark {
    QuerySourceWatermark {
        max_occurred_at_ms: max_i64_column(rows, "occurred_at_ms"),
        max_ingested_at_ms: max_i64_column(rows, "ingested_at_ms"),
    }
}

fn max_i64_column(rows: &[Value], column_name: &str) -> Option<i64> {
    rows.iter()
        .filter_map(|row| row.get(column_name).and_then(Value::as_i64))
        .max()
}

fn looks_like_decimal(value: &str) -> bool {
    value.contains('.')
        && !value.trim().is_empty()
        && value
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, '.' | '-'))
}

#[cfg(test)]
mod tests {
    use analytics_contract::{
        QueryColumnComparison, QueryComparisonOperator, QueryConditionalBranch, QueryExpression,
        QuerySelect, StructuredQuery,
    };
    use serde_json::json;

    use super::build_query_response;

    #[test]
    fn given_conditional_aggregate_when_response_is_built_then_plan_shape_is_bounded_and_sanitized()
    {
        let classification = QueryExpression::Conditional {
            branches: vec![QueryConditionalBranch {
                all: vec![QueryColumnComparison {
                    table_alias: Some("m".to_string()),
                    column_name: "ingested_at_ms".to_string(),
                    operator: QueryComparisonOperator::Gt,
                    value: json!("sensitive-cutoff"),
                }],
                then_value: json!("sensitive-class"),
            }],
            else_value: json!("other-class"),
        };
        let query = StructuredQuery {
            analytics_table_name: "metric_points_v1".to_string(),
            table_alias: Some("m".to_string()),
            joins: Vec::new(),
            select: vec![
                QuerySelect::Expression {
                    expression: classification.clone(),
                    alias: "usage_class".to_string(),
                },
                QuerySelect::Sum {
                    expression: QueryExpression::Column {
                        table_alias: Some("m".to_string()),
                        column_name: "value_i64".to_string(),
                    },
                    alias: "quantity".to_string(),
                },
            ],
            filters: Vec::new(),
            group_by: vec![classification],
            order_by: Vec::new(),
            limit: Some(2),
            offset: None,
        };

        let response = build_query_response(
            vec![json!({"usage_class": "other-class", "quantity": 10})],
            &query,
            std::time::Instant::now(),
        )
        .expect("response metadata");

        assert_eq!(response.execution.plan_shape.logical_source_count, 1);
        assert_eq!(response.execution.plan_shape.aggregate_count, 1);
        assert_eq!(
            response.execution.plan_shape.conditional_expression_count,
            2
        );
        let encoded = serde_json::to_string(&response.execution.plan_shape).expect("serialize");
        assert!(!encoded.contains("sensitive-cutoff"));
        assert!(!encoded.contains("sensitive-class"));
    }
}
