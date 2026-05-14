use analytics_contract::{
    AnalyticsColumn, AnalyticsColumnType, ClusteringKey, PrimitiveColumnType, SortOrder,
};
use config::CatalogType;

use crate::sql::{
    alter_table_partitioned_by, alter_table_sorted_by, attach_ducklake, create_table,
    quote_identifier,
};

#[test]
fn given_identifier_with_quote_when_quoted_then_sql_identifier_is_escaped() {
    assert_eq!(quote_identifier("bad\"name"), "\"bad\"\"name\"");
}

#[test]
fn given_reserved_columns_when_table_sql_is_created_then_reserved_columns_are_not_duplicated() {
    let sql = create_table(
        "users",
        &[
            AnalyticsColumn {
                column_name: "email".to_string(),
                column_type: AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::VarChar,
                },
            },
            AnalyticsColumn {
                column_name: "tenant_id".to_string(),
                column_type: AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::VarChar,
                },
            },
            AnalyticsColumn {
                column_name: "__id".to_string(),
                column_type: AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::VarChar,
                },
            },
        ],
    );

    assert_eq!(
        sql,
        "CREATE TABLE IF NOT EXISTS \"users\" (tenant_id VARCHAR, __id VARCHAR, table_name \
         VARCHAR, \"email\" VARCHAR);"
    );
}

#[test]
fn given_empty_partition_columns_when_partition_sql_is_requested_then_no_statement_is_emitted() {
    assert!(alter_table_partitioned_by("users", &[]).is_none());
}

#[test]
fn given_clustering_keys_when_sorted_sql_is_created_then_default_order_is_ascending() {
    let sql = alter_table_sorted_by(
        "users",
        &[
            ClusteringKey {
                column_name: "org_id".to_string(),
                order: None,
            },
            ClusteringKey {
                column_name: "created_at".to_string(),
                order: Some(SortOrder::Desc),
            },
        ],
    )
    .unwrap();

    assert_eq!(
        sql,
        "ALTER TABLE \"users\" SET SORTED BY (\"org_id\" ASC, \"created_at\" DESC);"
    );
}

#[test]
fn given_ducklake_paths_with_quotes_when_attached_then_literals_are_escaped() {
    let sql = attach_ducklake(CatalogType::Sqlite, "catalog's.db", "s3://bucket/data's");

    assert_eq!(
        sql,
        "ATTACH 'ducklake:sqlite:catalog''s.db' AS dlake (DATA_PATH 's3://bucket/data''s');"
    );
}
