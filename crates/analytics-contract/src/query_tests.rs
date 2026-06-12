use serde_json::json;

use crate::{
    QueryExpression, QueryJoin, QueryJoinKind, QueryJoinPredicate, QueryOrder, QueryPredicate,
    QuerySelect, SortOrder, StructuredQuery, StructuredQueryValidationError,
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
