use std::time::Duration;

use analytics_contract::{AnalyticsColumn, AnalyticsColumnType, PrimitiveColumnType};
use analytics_fixtures::users_manifest;

use crate::batch_writer::{
    BatchSqlExecutor, batch_columns, create_stage_statement, ducklake_table_phase_is_slow,
};

#[test]
fn given_ducklake_table_phase_duration_when_classified_then_one_second_is_slow() {
    assert!(!ducklake_table_phase_is_slow(Duration::from_millis(999)));
    assert!(ducklake_table_phase_is_slow(Duration::from_secs(1)));
}

#[test]
fn given_staged_changes_when_rows_are_replaced_then_one_set_oriented_statement_is_used() {
    let statement = BatchSqlExecutor::build_replacement_statement(
        "__stage",
        "events",
        &[
            "table_name".to_string(),
            "tenant_id".to_string(),
            "__id".to_string(),
            "value".to_string(),
        ],
        false,
    );

    assert_eq!(statement.matches("MERGE INTO").count(), 1);
    assert!(statement.contains("WHERE \"__analytics_batch_upsert\""));
    assert!(statement.contains("WHEN MATCHED THEN UPDATE"));
    assert!(statement.contains("WHEN NOT MATCHED THEN"));
    assert!(!statement.contains("DELETE FROM"));
    assert!(!statement.contains("INSERT INTO"));
}

#[test]
fn given_staged_deletes_when_rows_are_replaced_then_delete_is_separate_from_merge() {
    let statement = BatchSqlExecutor::build_replacement_statement(
        "__stage",
        "events",
        &[
            "table_name".to_string(),
            "tenant_id".to_string(),
            "__id".to_string(),
        ],
        true,
    );

    assert_eq!(statement.matches("MERGE INTO").count(), 1);
    assert_eq!(statement.matches("DELETE FROM").count(), 1);
    assert!(!statement.contains("THEN DELETE"));
    assert!(statement.contains("WHERE NOT \"__analytics_batch_upsert\""));
}

#[test]
fn given_registered_columns_when_stage_is_created_then_target_table_is_not_read() {
    let table = users_manifest()
        .tables
        .into_iter()
        .next()
        .expect("users fixture contains a table");
    let statement = create_stage_statement(
        "__stage",
        &table,
        &[
            "table_name".to_string(),
            "tenant_id".to_string(),
            "__id".to_string(),
            "email".to_string(),
        ],
    );

    assert_eq!(
        statement,
        "CREATE TEMP TABLE \"__stage\" (\"table_name\" VARCHAR, \"tenant_id\" VARCHAR, \"__id\" \
         VARCHAR, \"email\" VARCHAR, \"__analytics_batch_ordinal\" BIGINT, \
         \"__analytics_batch_upsert\" BOOLEAN)"
    );
    assert!(!statement.contains("FROM \"users\""));
}

#[test]
fn given_reserved_source_position_outputs_when_batch_columns_are_built_then_prefix_is_unique() {
    let mut document = users_manifest()
        .tables
        .into_iter()
        .next()
        .expect("users fixture contains a table");
    document.document_column = Some("__source_position".to_string());
    assert_eq!(
        batch_columns(&document, document.retention.is_some()),
        vec![
            "table_name",
            "tenant_id",
            "__id",
            "__source_position",
            "user_id",
            "email",
            "org_id",
        ]
    );

    let mut explicit = document.clone();
    explicit.document_column = None;
    explicit.projection_columns = None;
    explicit.columns = vec![
        AnalyticsColumn {
            column_name: "__source_position".to_string(),
            column_type: AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::Json,
            },
        },
        AnalyticsColumn {
            column_name: "email".to_string(),
            column_type: AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::VarChar,
            },
        },
    ];
    assert_eq!(
        batch_columns(&explicit, explicit.retention.is_some()),
        vec![
            "table_name",
            "tenant_id",
            "__id",
            "__source_position",
            "email",
        ]
    );
}
