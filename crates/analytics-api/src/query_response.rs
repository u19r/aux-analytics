use std::time::Instant;

use analytics_contract::StructuredQuery;
use serde_json::Value;
use thiserror::Error;

use crate::types::{
    QueryBatchResult, QueryExecutionMetadata, QueryResponse, QueryResultColumn,
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
    })
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
