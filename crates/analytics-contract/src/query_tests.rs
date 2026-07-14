use serde_json::json;

use crate::{
    QueryColumnComparison, QueryComparisonOperator, QueryConditionalBranch, QueryExpression,
    QueryJoin, QueryJoinKind, QueryJoinPredicate, QueryOrder, QueryPredicate, QuerySelect,
    SortOrder, StructuredQuery, StructuredQueryValidationError,
};

#[test]
fn given_existing_single_table_json_when_deserialized_then_shape_remains_valid() {
    let query: StructuredQuery = serde_json::from_value(json!({
        "analytics_table_name": "users",
        "select": [{"kind": "column", "column_name": "email", "alias": null}],
        "filters": [{
            "kind": "eq",
            "expression": {"kind": "column", "column_name": "org_id"},
            "value": "org-a"
        }],
        "group_by": [],
        "order_by": [{
            "expression": {"kind": "column", "column_name": "email"},
            "direction": "asc"
        }],
        "limit": 100
    }))
    .expect("existing single-table structured query json should decode");

    query.validate_shape().expect("single-table query is valid");
    assert_eq!(query.table_alias, None);
    assert!(query.joins.is_empty());
}

#[test]
fn given_joined_query_when_serialized_then_json_is_deterministic() {
    let query = joined_query();

    assert_eq!(
        serde_json::to_value(&query).expect("query serializes"),
        json!({
            "analytics_table_name": "metric_points_v1",
            "table_alias": "m",
            "joins": [{
                "kind": "inner",
                "analytics_table_name": "billing_rate_class_map_v1",
                "table_alias": "r",
                "on": [{
                    "left": {"kind": "column", "table_alias": "m", "column_name": "dim_2"},
                    "right": {"kind": "column", "table_alias": "r", "column_name": "model_name"}
                }]
            }],
            "select": [
                {"kind": "column", "table_alias": "r", "column_name": "rate_class", "alias": "rate_class"},
                {
                    "kind": "sum",
                    "expression": {"kind": "column", "table_alias": "m", "column_name": "value_i64"},
                    "alias": "quantity"
                }
            ],
            "filters": [{
                "kind": "eq",
                "expression": {"kind": "column", "table_alias": "m", "column_name": "series_name"},
                "value": "ai_token_units"
            }],
            "group_by": [{"kind": "column", "table_alias": "r", "column_name": "rate_class"}],
            "order_by": [{
                "expression": {"kind": "column", "table_alias": "r", "column_name": "rate_class"},
                "direction": "asc"
            }],
            "limit": 1000
        })
    );
}

#[test]
fn given_equivalent_query_json_when_hashed_then_hash_is_stable() {
    let query = joined_query();
    let decoded: StructuredQuery = serde_json::from_value(json!({
        "limit": 1000,
        "order_by": [{
            "direction": "asc",
            "expression": {"column_name": "rate_class", "kind": "column", "table_alias": "r"}
        }],
        "group_by": [{"column_name": "rate_class", "kind": "column", "table_alias": "r"}],
        "filters": [{
            "value": "ai_token_units",
            "expression": {"column_name": "series_name", "kind": "column", "table_alias": "m"},
            "kind": "eq"
        }],
        "select": [
            {"alias": "rate_class", "column_name": "rate_class", "kind": "column", "table_alias": "r"},
            {
                "alias": "quantity",
                "expression": {"column_name": "value_i64", "kind": "column", "table_alias": "m"},
                "kind": "sum"
            }
        ],
        "joins": [{
            "on": [{
                "right": {"column_name": "model_name", "kind": "column", "table_alias": "r"},
                "left": {"column_name": "dim_2", "kind": "column", "table_alias": "m"}
            }],
            "table_alias": "r",
            "analytics_table_name": "billing_rate_class_map_v1",
            "kind": "inner"
        }],
        "table_alias": "m",
        "analytics_table_name": "metric_points_v1"
    }))
    .expect("equivalent query json should decode");

    assert_eq!(
        query.canonical_json().expect("query canonicalizes"),
        decoded
            .canonical_json()
            .expect("decoded query canonicalizes")
    );
    let query_hash = query.query_hash().expect("query hashes");
    assert_eq!(
        query_hash,
        decoded.query_hash().expect("decoded query hashes")
    );
    assert!(query_hash.starts_with("sha256:"));
}

#[test]
fn given_invalid_join_shapes_when_validating_then_they_fail_before_compilation() {
    let mut too_many = joined_query();
    too_many.joins.push(QueryJoin {
        kind: QueryJoinKind::Left,
        analytics_table_name: "another_reference_v1".to_string(),
        table_alias: "a".to_string(),
        on: vec![QueryJoinPredicate {
            left: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_3".to_string(),
            },
            right: QueryExpression::Column {
                table_alias: Some("a".to_string()),
                column_name: "code".to_string(),
            },
        }],
    });
    too_many.joins.push(QueryJoin {
        kind: QueryJoinKind::Left,
        analytics_table_name: "third_reference_v1".to_string(),
        table_alias: "t".to_string(),
        on: vec![QueryJoinPredicate {
            left: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_1".to_string(),
            },
            right: QueryExpression::Column {
                table_alias: Some("t".to_string()),
                column_name: "code".to_string(),
            },
        }],
    });
    assert_eq!(
        too_many.validate_shape(),
        Err(StructuredQueryValidationError::TooManyJoins {
            actual: 3,
            maximum: 2,
        })
    );

    let mut missing_on = joined_query();
    missing_on.joins[0].on.clear();
    assert_eq!(
        missing_on.validate_shape(),
        Err(StructuredQueryValidationError::MissingJoinPredicate(
            "r".to_string()
        ))
    );

    let mut duplicate_output_alias = joined_query();
    duplicate_output_alias.select.push(QuerySelect::Count {
        alias: "quantity".to_string(),
    });
    assert_eq!(
        duplicate_output_alias.validate_shape(),
        Err(StructuredQueryValidationError::DuplicateAlias(
            "quantity".to_string()
        ))
    );

    let mut invalid_alias = joined_query();
    invalid_alias.table_alias = Some("not-valid".to_string());
    assert_eq!(
        invalid_alias.validate_shape(),
        Err(StructuredQueryValidationError::InvalidIdentifier {
            field: "table_alias",
            value: "not-valid".to_string(),
        })
    );
}

#[test]
fn given_conditional_classification_when_serialized_then_contract_is_explicit_and_valid() {
    let expression = late_arrival_expression(json!("late'arrival"));
    let query = StructuredQuery {
        analytics_table_name: "metric_points_v1".to_string(),
        table_alias: Some("m".to_string()),
        joins: Vec::new(),
        select: vec![QuerySelect::Expression {
            expression: expression.clone(),
            alias: "usage_class".to_string(),
        }],
        filters: Vec::new(),
        group_by: vec![expression],
        order_by: Vec::new(),
        limit: None,
    };

    query.validate_shape().expect("conditional query is valid");
    assert_eq!(
        serde_json::to_value(&query).expect("query serializes"),
        json!({
            "analytics_table_name": "metric_points_v1",
            "table_alias": "m",
            "select": [{
                "kind": "expression",
                "expression": {
                    "kind": "conditional",
                    "branches": [{
                        "all": [
                            {
                                "table_alias": "m",
                                "column_name": "occurred_at_ms",
                                "operator": "gte",
                                "value": 1000
                            },
                            {
                                "table_alias": "m",
                                "column_name": "occurred_at_ms",
                                "operator": "lt",
                                "value": 2000
                            },
                            {
                                "table_alias": "m",
                                "column_name": "ingested_at_ms",
                                "operator": "gt",
                                "value": 3000
                            }
                        ],
                        "then_value": "late'arrival"
                    }],
                    "else_value": "standard"
                },
                "alias": "usage_class"
            }],
            "group_by": [{
                "kind": "conditional",
                "branches": [{
                    "all": [
                        {
                            "table_alias": "m",
                            "column_name": "occurred_at_ms",
                            "operator": "gte",
                            "value": 1000
                        },
                        {
                            "table_alias": "m",
                            "column_name": "occurred_at_ms",
                            "operator": "lt",
                            "value": 2000
                        },
                        {
                            "table_alias": "m",
                            "column_name": "ingested_at_ms",
                            "operator": "gt",
                            "value": 3000
                        }
                    ],
                    "then_value": "late'arrival"
                }],
                "else_value": "standard"
            }]
        })
    );
}

#[test]
fn given_invalid_conditional_shapes_when_validating_then_they_are_rejected() {
    let mut empty_expression = late_arrival_expression(json!("late"));
    let QueryExpression::Conditional { branches, .. } = &mut empty_expression else {
        panic!("fixture should be conditional");
    };
    branches.clear();
    assert_eq!(
        expression_query(empty_expression).validate_shape(),
        Err(StructuredQueryValidationError::EmptyConditionalExpression)
    );

    let mut empty_branch = late_arrival_expression(json!("late"));
    let QueryExpression::Conditional { branches, .. } = &mut empty_branch else {
        panic!("fixture should be conditional");
    };
    branches[0].all.clear();
    assert_eq!(
        expression_query(empty_branch).validate_shape(),
        Err(StructuredQueryValidationError::EmptyConditionalBranch)
    );

    let incompatible = late_arrival_expression(json!(true));
    assert_eq!(
        expression_query(incompatible).validate_shape(),
        Err(StructuredQueryValidationError::IncompatibleConditionalLiteral)
    );

    let unsupported = QueryExpression::Conditional {
        branches: vec![QueryConditionalBranch {
            all: vec![comparison(
                "occurred_at_ms",
                QueryComparisonOperator::Gte,
                json!(1000),
            )],
            then_value: json!({"class": "late"}),
        }],
        else_value: json!({"class": "standard"}),
    };
    assert_eq!(
        expression_query(unsupported).validate_shape(),
        Err(StructuredQueryValidationError::UnsupportedConditionalLiteral)
    );
}

#[test]
fn given_structured_query_schema_when_generated_then_conditional_bounds_are_described() {
    let schema = serde_json::to_value(schemars::schema_for!(StructuredQuery))
        .expect("structured query schema serializes");
    let encoded = serde_json::to_string(&schema).expect("schema encodes");

    assert!(encoded.contains("conditional"));
    assert!(encoded.contains("QueryConditionalBranch"));
    assert!(encoded.contains("QueryColumnComparison"));
    assert!(encoded.contains("maxItems"));
    assert!(encoded.contains("then_value"));
    assert!(encoded.contains("else_value"));
}

fn expression_query(expression: QueryExpression) -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "metric_points_v1".to_string(),
        table_alias: Some("m".to_string()),
        joins: Vec::new(),
        select: vec![QuerySelect::Expression {
            expression,
            alias: "usage_class".to_string(),
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
    }
}

fn late_arrival_expression(then_value: serde_json::Value) -> QueryExpression {
    QueryExpression::Conditional {
        branches: vec![QueryConditionalBranch {
            all: vec![
                comparison("occurred_at_ms", QueryComparisonOperator::Gte, json!(1000)),
                comparison("occurred_at_ms", QueryComparisonOperator::Lt, json!(2000)),
                comparison("ingested_at_ms", QueryComparisonOperator::Gt, json!(3000)),
            ],
            then_value,
        }],
        else_value: json!("standard"),
    }
}

fn comparison(
    column_name: &str,
    operator: QueryComparisonOperator,
    value: serde_json::Value,
) -> QueryColumnComparison {
    QueryColumnComparison {
        table_alias: Some("m".to_string()),
        column_name: column_name.to_string(),
        operator,
        value,
    }
}

fn joined_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "metric_points_v1".to_string(),
        table_alias: Some("m".to_string()),
        joins: vec![QueryJoin {
            kind: QueryJoinKind::Inner,
            analytics_table_name: "billing_rate_class_map_v1".to_string(),
            table_alias: "r".to_string(),
            on: vec![QueryJoinPredicate {
                left: QueryExpression::Column {
                    table_alias: Some("m".to_string()),
                    column_name: "dim_2".to_string(),
                },
                right: QueryExpression::Column {
                    table_alias: Some("r".to_string()),
                    column_name: "model_name".to_string(),
                },
            }],
        }],
        select: vec![
            QuerySelect::Column {
                table_alias: Some("r".to_string()),
                column_name: "rate_class".to_string(),
                alias: Some("rate_class".to_string()),
            },
            QuerySelect::Sum {
                expression: QueryExpression::Column {
                    table_alias: Some("m".to_string()),
                    column_name: "value_i64".to_string(),
                },
                alias: "quantity".to_string(),
            },
        ],
        filters: vec![QueryPredicate::Eq {
            expression: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "series_name".to_string(),
            },
            value: json!("ai_token_units"),
        }],
        group_by: vec![QueryExpression::Column {
            table_alias: Some("r".to_string()),
            column_name: "rate_class".to_string(),
        }],
        order_by: vec![QueryOrder {
            expression: QueryExpression::Column {
                table_alias: Some("r".to_string()),
                column_name: "rate_class".to_string(),
            },
            direction: Some(SortOrder::Asc),
        }],
        limit: Some(1000),
    }
}
