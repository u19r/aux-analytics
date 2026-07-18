use analytics_contract::{
    AnalyticsManifest, JoinPolicy, QueryColumnComparison, QueryComparisonOperator,
    QueryConditionalBranch, QueryDocumentPredicate, QueryExpression, QueryJoin, QueryJoinKind,
    QueryJoinPredicate, QueryOrder, QueryPredicate, QuerySelect, QueryStringOperator, RowIdentity,
    SortOrder, StructuredQuery, TableRegistration, TableScope, TenantSelector,
};
use serde_json::json;

use crate::structured_query::{
    prepare_tenant_structured_query, structured_query_sql, structured_query_sql_for_manifest,
    tenant_scoped_structured_query_sql, tenant_scoped_structured_query_sql_for_manifest,
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
            offset: None,
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
fn given_dense_prefix_search_when_compiled_then_duckdb_applies_range_order_and_limit() {
    let mut users = table();
    users.projection_attribute_names =
        Some(vec!["id".to_string(), "analytics_search_name".to_string()]);
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![
            QuerySelect::Column {
                table_alias: None,
                column_name: "id".to_string(),
                alias: None,
            },
            QuerySelect::Column {
                table_alias: None,
                column_name: "analytics_search_name".to_string(),
                alias: None,
            },
        ],
        filters: vec![
            QueryPredicate::Gte {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "analytics_search_name".to_string(),
                },
                value: json!("ann"),
            },
            QueryPredicate::Lt {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "analytics_search_name".to_string(),
                },
                value: json!("ano"),
            },
        ],
        group_by: Vec::new(),
        order_by: vec![
            QueryOrder {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "analytics_search_name".to_string(),
                },
                direction: Some(SortOrder::Asc),
            },
            QueryOrder {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "id".to_string(),
                },
                direction: Some(SortOrder::Asc),
            },
        ],
        limit: Some(25),
        offset: None,
    };
    let sql = structured_query_sql(&users, &query).unwrap();
    assert!(sql.contains(
        "WHERE \"analytics_search_name\" >= 'ann' AND \"analytics_search_name\" < 'ano'"
    ));
    assert!(sql.ends_with("ORDER BY \"analytics_search_name\" ASC, \"id\" ASC LIMIT 25"));

    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE users (id VARCHAR, analytics_search_name VARCHAR); INSERT INTO users \
             SELECT 'user-' || lpad(i::VARCHAR, 4, '0'), 'ann' || lpad(i::VARCHAR, 4, '0') FROM \
             range(1500) AS rows(i); INSERT INTO users VALUES ('before', 'amy'), ('after', 'ano');",
        )
        .unwrap();
    let mut statement = connection.prepare(&sql).unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 25);
    assert_eq!(
        rows.first().unwrap(),
        &("user-0000".to_string(), "ann0000".to_string())
    );
    assert_eq!(
        rows.last().unwrap(),
        &("user-0024".to_string(), "ann0024".to_string())
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
            offset: None,
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
fn given_tenant_query_when_prepared_then_values_are_bound_and_metadata_is_reused() {
    let query = StructuredQuery {
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
        order_by: Vec::new(),
        limit: Some(10),
        offset: None,
    };

    let prepared = prepare_tenant_structured_query(
        &AnalyticsManifest::new(vec![table()]),
        &query,
        "tenant_01",
    )
    .expect("prepared query");

    assert_eq!(
        prepared.sql,
        "SELECT \"email\" AS \"email\" FROM \"users\" WHERE \"org_id\" = $1 AND \"tenant_id\" = \
         $2 LIMIT 10"
    );
    assert_eq!(
        prepared.parameters,
        vec![
            duckdb::types::Value::Text("org-a".to_string()),
            duckdb::types::Value::Text("tenant_01".to_string()),
        ]
    );
    assert_eq!(prepared.metadata().tables, vec!["users"]);
    assert_eq!(prepared.metadata().limit, Some(10));
    assert!(prepared.metadata().query_hash.starts_with("sha256:"));
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
            offset: None,
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
            offset: None,
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
fn given_nested_document_filter_when_executed_then_array_members_and_offset_are_applied() {
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::DocumentPath {
            table_alias: None,
            document_column: "item".to_string(),
            path: "user_name".to_string(),
            alias: "user_name".to_string(),
        }],
        filters: vec![QueryPredicate::DocumentMatches {
            table_alias: None,
            document_column: "item".to_string(),
            predicate: QueryDocumentPredicate::All {
                predicates: vec![
                    QueryDocumentPredicate::ArrayAny {
                        path: Some("emails".to_string()),
                        predicate: Box::new(QueryDocumentPredicate::All {
                            predicates: vec![
                                QueryDocumentPredicate::String {
                                    path: Some("type".to_string()),
                                    operator: QueryStringOperator::Eq,
                                    value: "WORK".to_string(),
                                    case_sensitive: false,
                                },
                                QueryDocumentPredicate::String {
                                    path: Some("value".to_string()),
                                    operator: QueryStringOperator::EndsWith,
                                    value: "@company.com".to_string(),
                                    case_sensitive: false,
                                },
                            ],
                        }),
                    },
                    QueryDocumentPredicate::TimestampString {
                        path: Some("created_at".to_string()),
                        operator: QueryStringOperator::StartsWith,
                        value: "2025-02-01T00:00:00".to_string(),
                    },
                    QueryDocumentPredicate::AffixedString {
                        path: Some("_v".to_string()),
                        operator: QueryStringOperator::StartsWith,
                        value: "W/\"1".to_string(),
                        prefix: "W/\"".to_string(),
                        suffix: "\"".to_string(),
                        case_sensitive: false,
                    },
                ],
            },
        }],
        group_by: Vec::new(),
        order_by: vec![QueryOrder {
            expression: QueryExpression::Lower {
                expression: Box::new(QueryExpression::DocumentPath {
                    table_alias: None,
                    document_column: "item".to_string(),
                    path: "user_name".to_string(),
                }),
            },
            direction: Some(SortOrder::Asc),
        }],
        limit: Some(1),
        offset: Some(1),
    };
    query.validate_shape().expect("nested query is bounded");
    let sql = structured_query_sql(&table(), &query).expect("nested query compiles");
    assert!(sql.contains("EXISTS (SELECT 1 FROM json_each("));
    assert!(sql.ends_with("LIMIT 1 OFFSET 1"));

    let connection = duckdb::Connection::open_in_memory().expect("open DuckDB");
    connection
        .execute_batch(
            r#"
            CREATE TABLE users (item JSON, email VARCHAR, org_id VARCHAR);
            INSERT INTO users VALUES
              ('{"user_name":"Ada","_v":1,"created_at":1738368000000,"emails":[{"type":"work","value":"ada@company.com"}]}', NULL, NULL),
              ('{"user_name":"bob","_v":12,"created_at":1738368000123,"emails":[{"type":"WORK","value":"bob@company.com"}]}', NULL, NULL),
              ('{"user_name":"Cara","_v":2,"created_at":1738454400000,"emails":[{"type":"home","value":"cara@company.com"}]}', NULL, NULL);
            "#,
        )
        .expect("seed users");
    let selected = connection
        .query_row(&sql, [], |row| row.get::<_, String>(0))
        .expect("execute nested query");
    assert_eq!(selected, "bob");
}

#[test]
fn given_group_member_document_filter_when_executed_then_only_matching_groups_are_returned() {
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::DocumentPath {
            table_alias: None,
            document_column: "item".to_string(),
            path: "id".to_string(),
            alias: "id".to_string(),
        }],
        filters: vec![QueryPredicate::DocumentMatches {
            table_alias: None,
            document_column: "item".to_string(),
            predicate: QueryDocumentPredicate::ArrayAny {
                path: Some("members".to_string()),
                predicate: Box::new(QueryDocumentPredicate::String {
                    path: Some("user_id".to_string()),
                    operator: QueryStringOperator::Eq,
                    value: "u_target's".to_string(),
                    case_sensitive: false,
                }),
            },
        }],
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(10),
        offset: None,
    };
    let sql = structured_query_sql(&table(), &query).expect("member query compiles");
    assert!(sql.contains("u_target''s"));

    let connection = duckdb::Connection::open_in_memory().expect("open DuckDB");
    connection
        .execute_batch(
            r#"
            CREATE TABLE users (item JSON, email VARCHAR, org_id VARCHAR);
            INSERT INTO users VALUES
              ('{"id":"g_match","members":[{"user_id":"u_target''s"}]}', NULL, NULL),
              ('{"id":"g_other","members":[{"user_id":"u_other"}]}', NULL, NULL);
            "#,
        )
        .expect("seed groups");
    let selected = connection
        .query_row(&sql, [], |row| row.get::<_, String>(0))
        .expect("execute member query");
    assert_eq!(selected, "g_match");
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
            offset: None,
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
            offset: None,
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
            offset: None,
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
        offset: None,
    }
}
