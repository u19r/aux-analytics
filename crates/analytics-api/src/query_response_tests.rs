use analytics_contract::{
    AnalyticsManifest, QueryColumnComparison, QueryComparisonOperator, QueryConditionalBranch,
    QueryExpression, QuerySelect, StructuredQuery, TableRegistration,
};
use analytics_engine::{PreparedQueryMetadata, prepare_unscoped_structured_query};
use serde_json::json;

use crate::QueryResponse;

#[test]
fn given_conditional_aggregate_when_response_is_built_then_plan_shape_is_bounded_and_sanitized() {
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
    let response = QueryResponse::new(
        vec![json!({"usage_class": "other-class", "quantity": 10})],
        &metadata,
        std::time::Instant::now(),
    );

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

#[test]
fn given_unsigned_source_with_arbitrary_alias_when_response_is_built_then_u64_is_preserved() {
    let table: TableRegistration = serde_json::from_value(json!({
        "source_table_name": "source_points",
        "analytics_table_name": "points",
        "document_column": null,
        "columns": [{
            "column_name": "unsigned_value",
            "column_type": {"kind": "primitive", "primitive": "unsigned_big_int"}
        }]
    }))
    .expect("table registration");
    let query = StructuredQuery {
        analytics_table_name: "points".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::Sum {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "unsigned_value".to_string(),
            },
            alias: "arbitrary_total".to_string(),
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };
    let prepared = prepare_unscoped_structured_query(&AnalyticsManifest::new(vec![table]), &query)
        .expect("prepared query");

    let response = QueryResponse::new(
        vec![json!({"arbitrary_total": 10})],
        prepared.metadata(),
        std::time::Instant::now(),
    );

    assert_eq!(response.columns.len(), 1);
    assert!(matches!(
        response.columns[0].value_type,
        crate::http_contract::QueryResultValueType::U64
    ));
}

#[test]
fn given_mixed_source_watermark_rows_when_response_is_built_then_maxima_ignore_missing_values() {
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::Column {
            table_alias: None,
            column_name: "occurred_at_ms".to_string(),
            alias: None,
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };
    let metadata = PreparedQueryMetadata::from_query(&query).expect("query metadata");
    let response = QueryResponse::new(
        vec![
            json!({"occurred_at_ms": 12, "ingested_at_ms": 8}),
            json!({"occurred_at_ms": null, "ingested_at_ms": 20}),
            json!({"occurred_at_ms": 7}),
        ],
        &metadata,
        std::time::Instant::now(),
    );
    let watermark = response.source_watermark;

    assert_eq!(watermark.max_occurred_at_ms, Some(12));
    assert_eq!(watermark.max_ingested_at_ms, Some(20));
}
