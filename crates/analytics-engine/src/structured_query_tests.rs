use analytics_contract::{
    QueryExpression, QueryOrder, QueryPredicate, QuerySelect, RowIdentity, SortOrder,
    StructuredQuery, TableRegistration, TenantSelector,
};
use serde_json::json;

use crate::structured_query::{structured_query_sql, tenant_scoped_structured_query_sql};

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
    }
}

#[test]
fn given_structured_query_when_compiled_then_literals_and_identifiers_are_escaped() {
    let sql = structured_query_sql(
        &table(),
        &StructuredQuery {
            analytics_table_name: "users".to_string(),
            select: vec![QuerySelect::Column {
                column_name: "email".to_string(),
                alias: Some("user\"email".to_string()),
            }],
            filters: vec![QueryPredicate::Eq {
                expression: QueryExpression::Column {
                    column_name: "org_id".to_string(),
                },
                value: json!("org's"),
            }],
            group_by: Vec::new(),
            order_by: vec![QueryOrder {
                expression: QueryExpression::Column {
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
            select: vec![QuerySelect::Column {
                column_name: "email".to_string(),
                alias: None,
            }],
            filters: vec![QueryPredicate::Eq {
                expression: QueryExpression::Column {
                    column_name: "org_id".to_string(),
                },
                value: json!("org-a"),
            }],
            group_by: Vec::new(),
            order_by: vec![QueryOrder {
                expression: QueryExpression::Column {
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
            select: vec![QuerySelect::Column {
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
            select: vec![QuerySelect::DocumentPath {
                document_column: "item".to_string(),
                path: "profile.email".to_string(),
                alias: "email".to_string(),
            }],
            filters: vec![QueryPredicate::IsNotNull {
                expression: QueryExpression::DocumentPath {
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
            select: vec![QuerySelect::Column {
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
