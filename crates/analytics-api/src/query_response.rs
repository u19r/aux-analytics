use std::time::Instant;

use analytics_engine::{PreparedQueryMetadata, PreparedQueryResultValueType};
use serde_json::Value;

use crate::http_contract::{
    QueryBatchResult, QueryExecutionMetadata, QueryPlanShape, QueryResponse, QueryResultColumn,
    QueryResultValueType, QuerySourceWatermark,
};

impl QueryResponse {
    pub fn new(rows: Vec<Value>, metadata: &PreparedQueryMetadata, started: Instant) -> Self {
        Self {
            columns: result_columns(rows.as_slice(), metadata),
            execution: execution_metadata(metadata, rows.len(), started),
            source_watermark: source_watermark(rows.as_slice()),
            rows,
        }
    }
}

impl QueryBatchResult {
    pub fn new(
        name: String,
        rows: Vec<Value>,
        metadata: &PreparedQueryMetadata,
        started: Instant,
    ) -> Self {
        Self {
            name,
            columns: result_columns(rows.as_slice(), metadata),
            execution: execution_metadata(metadata, rows.len(), started),
            source_watermark: source_watermark(rows.as_slice()),
            rows,
        }
    }
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

fn result_columns(rows: &[Value], metadata: &PreparedQueryMetadata) -> Vec<QueryResultColumn> {
    let Some(row) = rows.first().and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut columns = Vec::with_capacity(row.len());
    for (name, value) in row {
        columns.push(QueryResultColumn {
            name: name.clone(),
            value_type: result_value_type(name, value, metadata),
            nullable: rows
                .iter()
                .any(|row| row.get(name).is_none_or(serde_json::Value::is_null)),
        });
    }
    columns
}

fn result_value_type(
    name: &str,
    value: &Value,
    metadata: &PreparedQueryMetadata,
) -> QueryResultValueType {
    if let Some(value_type) = metadata
        .result_columns
        .iter()
        .find(|column| column.name == name)
        .map(|column| column.value_type)
    {
        return match value_type {
            PreparedQueryResultValueType::I64 => QueryResultValueType::I64,
            PreparedQueryResultValueType::U64 => QueryResultValueType::U64,
            PreparedQueryResultValueType::Decimal => QueryResultValueType::Decimal,
            PreparedQueryResultValueType::F64 => QueryResultValueType::F64,
        };
    }
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
    let mut max_occurred_at_ms: Option<i64> = None;
    let mut max_ingested_at_ms: Option<i64> = None;
    for row in rows {
        if let Some(value) = row.get("occurred_at_ms").and_then(Value::as_i64) {
            max_occurred_at_ms =
                Some(max_occurred_at_ms.map_or(value, |current| current.max(value)));
        }
        if let Some(value) = row.get("ingested_at_ms").and_then(Value::as_i64) {
            max_ingested_at_ms =
                Some(max_ingested_at_ms.map_or(value, |current| current.max(value)));
        }
    }
    QuerySourceWatermark {
        max_occurred_at_ms,
        max_ingested_at_ms,
    }
}

fn looks_like_decimal(value: &str) -> bool {
    value.contains('.')
        && !value.trim().is_empty()
        && value
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, '.' | '-'))
}
