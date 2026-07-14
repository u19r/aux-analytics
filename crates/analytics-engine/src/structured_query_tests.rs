use analytics_contract::{
    AnalyticsManifest, JoinPolicy, QueryColumnComparison, QueryComparisonOperator,
    QueryConditionalBranch, QueryExpression, QueryJoin, QueryJoinKind, QueryJoinPredicate,
    QueryOrder, QueryPredicate, QuerySelect, RowIdentity, SortOrder, StructuredQuery,
    TableRegistration, TableScope, TenantSelector,
};
use serde_json::json;

use crate::structured_query::{
    structured_query_sql, structured_query_sql_for_manifest, tenant_scoped_structured_query_sql,
    tenant_scoped_structured_query_sql_for_manifest,
};

fn table() -> TableRegistration {
    TableRegistration {
        source_table_name: "source_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::None,
        row_identity: RowIdentity::RecordKey,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: Some(vec!["email".to_string(), "org_id".to_string()]),
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }
}

#[test]
fn given_structured_query_when_compiled_then_literals_and_identifiers_are_escaped() {
    let sql = structured_query_sql(
        &table(),
        &StructuredQuery {
            analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            select: vec![QuerySelect::Column {
                table_alias: None,
                column_name: "email".to_string(),
                alias: Some("user\"email".to_string()),
            }],
            filters: vec![QueryPredicate::Eq {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "org_id".to_string(),
                },
                value: json!("org's"),
            }],
            group_by: Vec::new(),
            order_by: vec![QueryOrder {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "email".to_string(),
                },
                direction: Some(SortOrder::Desc),
            }],
            limit: Some(10),
        },
    )
    .unwrap();

    assert_eq!(
        sql,
        "SELECT \"email\" AS \"user\"\"email\" FROM \"users\" WHERE \"org_id\" = 'org''s' ORDER \
         BY \"email\" DESC LIMIT 10"
    );
}

#[test]
fn given_tenant_scoped_structured_query_when_compiled_then_tenant_filter_is_injected() {
    let sql = tenant_scoped_structured_query_sql(
        &table(),
        &StructuredQuery {
            analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            select: vec![QuerySelect::Column {
                table_alias: None,
                column_name: "email".to_string(),
                alias: None,
            }],
            filters: vec![QueryPredicate::Eq {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "org_id".to_string(),
                },
                value: json!("org-a"),
            }],
            group_by: Vec::new(),
            order_by: vec![QueryOrder {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "email".to_string(),
                },
                direction: Some(SortOrder::Asc),
            }],
            limit: Some(10),
        },
        "tenant_01",
    )
    .unwrap();

    assert_eq!(
        sql,
        "SELECT \"email\" AS \"email\" FROM \"users\" WHERE \"org_id\" = 'org-a' AND \
         \"tenant_id\" = 'tenant_01' ORDER BY \"email\" ASC LIMIT 10"
    );
}

#[test]
fn given_empty_target_tenant_when_tenant_scoped_query_is_compiled_then_query_is_rejected() {
    let error = tenant_scoped_structured_query_sql(
        &table(),
        &StructuredQuery {
            analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            select: vec![QuerySelect::Column {
                table_alias: None,
                column_name: "email".to_string(),
                alias: None,
            }],
            filters: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        },
        "",
    )
    .unwrap_err();

    assert!(error.to_string().contains("target tenant id is required"));
}

#[test]
fn given_document_path_query_when_compiled_then_only_registered_document_column_is_allowed() {
    let sql = structured_query_sql(
        &table(),
        &StructuredQuery {
            analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            select: vec![QuerySelect::DocumentPath {
                table_alias: None,
                document_column: "item".to_string(),
                path: "profile.email".to_string(),
                alias: "email".to_string(),
            }],
            filters: vec![QueryPredicate::IsNotNull {
                expression: QueryExpression::DocumentPath {
                    table_alias: None,
                    document_column: "item".to_string(),
                    path: "profile.email".to_string(),
                },
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        },
    )
    .unwrap();

    assert_eq!(
        sql,
        "SELECT json_extract_string(\"item\", '$.profile.email') AS \"email\" FROM \"users\" \
         WHERE json_extract_string(\"item\", '$.profile.email') IS NOT NULL"
    );
}

#[test]
fn given_unregistered_column_when_query_is_compiled_then_query_is_rejected() {
    let error = structured_query_sql(
        &table(),
        &StructuredQuery {
            analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            select: vec![QuerySelect::Column {
                table_alias: None,
                column_name: "password".to_string(),
                alias: None,
            }],
            filters: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        },
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("column password is not registered")
    );
}

#[test]
fn given_conditional_grouping_when_compiled_then_registered_columns_and_literals_are_safe() {
    let expression = conditional_expression("late'; DROP TABLE users; --");
    let sql = structured_query_sql(
        &conditional_table(),
        &StructuredQuery {
            analytics_table_name: "metric_points_v1".to_string(),
            table_alias: Some("m".to_string()),
            joins: Vec::new(),
            select: vec![
                QuerySelect::Expression {
                    expression: expression.clone(),
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
            group_by: vec![expression],
            order_by: Vec::new(),
            limit: None,
        },
    )
    .expect("conditional query compiles");

    assert_eq!(
        sql,
        "SELECT CASE WHEN (\"m\".\"occurred_at_ms\" >= 1000 AND \"m\".\"occurred_at_ms\" < 2000 \
         AND \"m\".\"ingested_at_ms\" > 3000) THEN 'late''; DROP TABLE users; --' ELSE 'standard' \
         END AS \"usage_class\", sum(\"m\".\"value_i64\") AS \"quantity\" FROM \
         \"metric_points_v1\" AS \"m\" GROUP BY CASE WHEN (\"m\".\"occurred_at_ms\" >= 1000 AND \
         \"m\".\"occurred_at_ms\" < 2000 AND \"m\".\"ingested_at_ms\" > 3000) THEN 'late''; DROP \
         TABLE users; --' ELSE 'standard' END"
    );
}

#[test]
fn given_conditional_with_unregistered_column_when_compiled_then_query_is_rejected() {
    let expression = QueryExpression::Conditional {
        branches: vec![QueryConditionalBranch {
            all: vec![QueryColumnComparison {
                table_alias: Some("m".to_string()),
                column_name: "secret_timestamp".to_string(),
                operator: QueryComparisonOperator::Gt,
                value: json!(3000),
            }],
            then_value: json!("late"),
        }],
        else_value: json!("standard"),
    };
    let error = structured_query_sql(
        &conditional_table(),
        &StructuredQuery {
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
        },
    )
    .expect_err("unregistered conditional column should fail");

    assert!(
        error
            .to_string()
            .contains("column secret_timestamp is not registered")
    );
}

#[test]
fn given_joined_structured_query_when_compiled_then_alias_qualified_sql_is_generated() {
    let sql = structured_query_sql_for_manifest(&joined_manifest(), &joined_query()).unwrap();

    assert_eq!(
        sql,
        "SELECT \"r\".\"rate_class\" AS \"rate_class\", sum(\"m\".\"value_i64\") AS \"quantity\" \
         FROM \"metric_points_v1\" AS \"m\" INNER JOIN \"billing_rate_class_map_v1\" AS \"r\" ON \
         \"m\".\"dim_2\" = \"r\".\"model_name\" WHERE \"m\".\"series_name\" = 'ai_token_units' \
         GROUP BY \"r\".\"rate_class\" LIMIT 1000"
    );
    assert!(!sql.contains("json_extract"));
    assert!(!sql.to_ascii_lowercase().contains("materialized"));
}

#[test]
fn given_tenant_joined_query_when_compiled_then_every_tenant_scoped_alias_is_filtered() {
    let sql = tenant_scoped_structured_query_sql_for_manifest(
        &joined_manifest(),
        &joined_query(),
        "tenant_01",
    )
    .unwrap();

    assert!(
        sql.contains("\"m\".\"tenant_id\" = 'tenant_01'"),
        "primary table tenant predicate should be alias qualified: {sql}"
    );
    assert!(
        sql.contains("\"r\".\"tenant_id\" = 'tenant_01'"),
        "joined tenant-scoped table tenant predicate should be alias qualified: {sql}"
    );
}

#[test]
fn given_joined_query_with_ambiguous_column_when_compiled_then_query_is_rejected() {
    let mut query = joined_query();
    query.filters.push(QueryPredicate::Eq {
        expression: QueryExpression::Column {
            table_alias: None,
            column_name: "tenant_id".to_string(),
        },
        value: json!("tenant_01"),
    });

    let error = structured_query_sql_for_manifest(&joined_manifest(), &query).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("column tenant_id is ambiguous; table_alias is required")
    );
}

#[test]
fn given_join_policy_disallows_join_when_compiled_then_query_is_rejected() {
    let mut manifest = joined_manifest();
    manifest.tables[1].join_policy.allowed_as_join = false;

    let error = structured_query_sql_for_manifest(&manifest, &joined_query()).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("joined table billing_rate_class_map_v1 is not allowed by join policy")
    );
}

#[test]
fn given_aggregate_and_column_without_group_by_when_compiled_then_query_is_rejected() {
    let mut query = joined_query();
    query.group_by.clear();

    let error = structured_query_sql_for_manifest(&joined_manifest(), &query).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("non-aggregate selects must appear in group_by")
    );
}

#[test]
fn given_unbounded_global_reference_join_when_compiled_then_query_is_rejected() {
    let mut manifest = joined_manifest();
    manifest.tables[1].tenant_selector = TenantSelector::None;
    manifest.tables[1].table_scope = TableScope::GlobalReference {
        reference_class: "rates".to_string(),
    };
    manifest.tables[1].join_policy = JoinPolicy {
        allowed_as_primary: false,
        allowed_as_join: false,
        max_join_rows_hint: Some(1000),
    };

    let error = structured_query_sql_for_manifest(&manifest, &joined_query()).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("joined table billing_rate_class_map_v1 is not allowed as reference data")
    );
}

fn joined_manifest() -> AnalyticsManifest {
    analytics_fixtures::metric_points_manifest()
}

fn conditional_table() -> TableRegistration {
    let mut table = table();
    table.analytics_table_name = "metric_points_v1".to_string();
    table.projection_attribute_names = Some(vec![
        "occurred_at_ms".to_string(),
        "ingested_at_ms".to_string(),
        "value_i64".to_string(),
    ]);
    table
}

fn conditional_expression(late_value: &str) -> QueryExpression {
    QueryExpression::Conditional {
        branches: vec![QueryConditionalBranch {
            all: vec![
                QueryColumnComparison {
                    table_alias: Some("m".to_string()),
                    column_name: "occurred_at_ms".to_string(),
                    operator: QueryComparisonOperator::Gte,
                    value: json!(1000),
                },
                QueryColumnComparison {
                    table_alias: Some("m".to_string()),
                    column_name: "occurred_at_ms".to_string(),
                    operator: QueryComparisonOperator::Lt,
                    value: json!(2000),
                },
                QueryColumnComparison {
                    table_alias: Some("m".to_string()),
                    column_name: "ingested_at_ms".to_string(),
                    operator: QueryComparisonOperator::Gt,
                    value: json!(3000),
                },
            ],
            then_value: json!(late_value),
        }],
        else_value: json!("standard"),
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
        order_by: Vec::new(),
        limit: Some(1000),
    }
}
