use std::time::Instant;

use analytics_engine::PreparedQueryMetadata;
use serde_json::Value;
use thiserror::Error;

use crate::types::{
    QueryBatchResult, QueryExecutionMetadata, QueryPlanShape, QueryResponse, QueryResultColumn,
    QueryResultValueType, QuerySourceWatermark,
};

#[derive(Debug, Error)]
#[error("{0}")]
pub struct QueryResponseBuildError(String);

pub fn build_query_response(
    rows: Vec<Value>,
    metadata: &PreparedQueryMetadata,
    started: Instant,
) -> Result<QueryResponse, QueryResponseBuildError> {
    Ok(QueryResponse {
        columns: result_columns(rows.as_slice()),
        execution: execution_metadata(metadata, rows.len(), started),
        source_watermark: source_watermark(rows.as_slice()),
        rows,
    })
}

pub fn build_query_batch_result(
    name: String,
    rows: Vec<Value>,
    metadata: &PreparedQueryMetadata,
    started: Instant,
) -> Result<QueryBatchResult, QueryResponseBuildError> {
    Ok(QueryBatchResult {
        name,
        columns: result_columns(rows.as_slice()),
        execution: execution_metadata(metadata, rows.len(), started),
        source_watermark: source_watermark(rows.as_slice()),
        rows,
    })
}

fn execution_metadata(
    metadata: &PreparedQueryMetadata,
    row_count: usize,
    started: Instant,
) -> QueryExecutionMetadata {
    QueryExecutionMetadata {
        query_hash: metadata.query_hash.clone(),
        row_count: row_count as u64,
        truncated: metadata
            .limit
            .is_some_and(|limit| row_count >= limit as usize),
        tables: metadata.tables.clone(),
        elapsed_ms: started.elapsed().as_millis() as u64,
        plan_shape: QueryPlanShape {
            logical_source_count: metadata.logical_source_count,
            join_count: metadata.join_count,
            filter_count: metadata.filter_count,
            group_expression_count: metadata.group_expression_count,
            order_expression_count: metadata.order_expression_count,
            aggregate_count: metadata.aggregate_count,
            conditional_expression_count: metadata.conditional_expression_count,
        },
    }
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
    use analytics_engine::PreparedQueryMetadata;
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

        let metadata = PreparedQueryMetadata::from_query(&query).expect("query metadata");
        let response = build_query_response(
            vec![json!({"usage_class": "other-class", "quantity": 10})],
            &metadata,
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
