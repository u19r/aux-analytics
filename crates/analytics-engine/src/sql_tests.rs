use analytics_contract::{
    AnalyticsColumn, AnalyticsColumnType, ClusteringKey, PrimitiveColumnType, SortOrder,
};
use config::{
    AnalyticsObjectStorageConfig, CatalogType, DuckLakeCatalogSettings, RemoteCredentialsConfig,
    RemoteStaticCredentialsConfig,
};

use crate::sql::{
    SOURCE_POSITION_COLUMN, alter_table_partitioned_by, alter_table_sorted_by, attach_ducklake,
    attach_existing_ducklake, create_table, manifest_column_statements, object_storage_secret_sql,
    postgres_catalog_pool_statements, quote_identifier,
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
            AnalyticsColumn {
                column_name: SOURCE_POSITION_COLUMN.to_string(),
                column_type: AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::Json,
                },
            },
        ],
    );

    assert_eq!(
        sql,
        "CREATE TABLE IF NOT EXISTS \"users\" (tenant_id VARCHAR, __id VARCHAR, table_name \
         VARCHAR, \"__source_position\" JSON, \"email\" VARCHAR);"
    );
}

#[test]
fn given_manifest_columns_when_column_sql_is_created_then_reserved_columns_are_not_added() {
    let sql = manifest_column_statements(
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
                column_name: SOURCE_POSITION_COLUMN.to_string(),
                column_type: AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::Json,
                },
            },
        ],
    );

    assert_eq!(
        sql,
        vec!["ALTER TABLE \"users\" ADD COLUMN IF NOT EXISTS \"email\" VARCHAR;"]
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
        "ATTACH 'ducklake:sqlite:catalog''s.db' AS dlake (DATA_PATH 's3://bucket/data''s', \
         DATA_INLINING_ROW_LIMIT 0);"
    );
}

#[test]
fn given_existing_ducklake_catalog_when_attached_then_creation_is_disabled() {
    let sql = attach_existing_ducklake(CatalogType::Postgres, "dbname=duck'lake");

    assert_eq!(
        sql,
        "ATTACH 'ducklake:postgres:dbname=duck''lake' AS dlake (CREATE_IF_NOT_EXISTS false, \
         DATA_INLINING_ROW_LIMIT 0);"
    );
    assert!(!sql.contains("DATA_PATH"));
}

#[test]
fn given_planetscale_safe_catalog_settings_when_configured_then_pool_is_limited_before_attach() {
    let statements = postgres_catalog_pool_statements(&DuckLakeCatalogSettings {
        duckdb_threads: Some(1),
        postgres_pool_max_connections: Some(1),
        postgres_pool_idle_timeout_ms: Some(1_000),
        postgres_pool_wait_timeout_ms: Some(5_000),
        postgres_pool_acquire_mode: Some("wait".to_string()),
        postgres_pool_enable_thread_local_cache: Some(false),
        ..DuckLakeCatalogSettings::default()
    });

    assert_eq!(
        statements,
        [
            "SET threads = 1;",
            "SET pg_pool_max_connections = 1;",
            "SET pg_pool_idle_timeout_millis = 1000;",
            "SET pg_pool_wait_timeout_millis = 5000;",
            "SET pg_pool_acquire_mode = 'wait';",
            "SET pg_pool_enable_thread_local_cache = false;",
        ]
    );
}

#[test]
fn given_catalog_pool_acquire_mode_with_quote_when_configured_then_literal_is_escaped() {
    let statements = postgres_catalog_pool_statements(&DuckLakeCatalogSettings {
        postgres_pool_acquire_mode: Some("wa'it".to_string()),
        ..DuckLakeCatalogSettings::default()
    });

    assert_eq!(statements, ["SET pg_pool_acquire_mode = 'wa''it';"]);
}

#[test]
fn s3_object_store_secret_sql_uses_static_credentials() {
    let sql = object_storage_secret_sql(&AnalyticsObjectStorageConfig {
        bucket: Some("analytics-bucket".to_string()),
        region: Some("us-east-1".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "AKIA'KEY".to_string(),
                secret_key: "secret'value".to_string(),
                session_token: Some("session'token".to_string()),
            }),
            instance_keys: None,
        }),
        ..AnalyticsObjectStorageConfig::default()
    })
    .expect("secret sql");

    assert_eq!(
        sql,
        "CREATE OR REPLACE SECRET aux_analytics_object_store (TYPE S3, REGION 'us-east-1', KEY_ID \
         'AKIA''KEY', SECRET 'secret''value', SESSION_TOKEN 'session''token');"
    );
    assert!(!sql.contains("credential_chain"));
}

#[test]
fn s3_object_store_secret_sql_ignores_unresolved_instance_keys() {
    let sql = object_storage_secret_sql(&AnalyticsObjectStorageConfig {
        bucket: Some("analytics-bucket".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            instance_keys: Some(true),
            ..RemoteCredentialsConfig::default()
        }),
        ..AnalyticsObjectStorageConfig::default()
    });

    assert!(sql.is_none());
}
