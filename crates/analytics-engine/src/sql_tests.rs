use analytics_contract::{
    AnalyticsColumn, AnalyticsColumnType, ClusteringKey, PrimitiveColumnType, SortOrder,
};
use config::{
    AnalyticsObjectStorageConfig, CatalogType, RemoteCredentialsConfig,
    RemoteStaticCredentialsConfig,
};
use duckdb::Connection;
use tempfile::tempdir;

use crate::{
    constants::SOURCE_POSITION_COLUMN,
    sql::{
        ObjectStorageSecret, alter_table_partitioned_by, alter_table_sorted_by, attach_ducklake,
        create_table, load_extension_for_test, manifest_column_statements, quote_identifier,
        source_position_column_statement,
    },
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
fn given_missing_local_extension_when_loaded_then_original_error_is_returned_without_install_path()
{
    let extension_directory = tempdir().expect("temporary extension directory");
    let connection = Connection::open_in_memory().expect("open DuckDB");
    connection
        .execute(
            format!(
                "SET extension_directory = '{}';",
                extension_directory.path().to_string_lossy()
            )
            .as_str(),
            [],
        )
        .expect("configure extension directory");
    connection
        .execute_batch(
            "SET custom_extension_repository = 'file:///tmp/analytics-no-such-extension-repository';",
        )
        .expect("configure nonexistent extension repository");

    let error = load_extension_for_test(&connection, "mysql_scanner")
        .expect_err("missing local extension must fail closed");
    let message = error.to_string();
    assert!(
        message.contains("Install it first"),
        "expected the LOAD error without an INSTALL fallback, got: {message}"
    );
    let autoinstall: bool = connection
        .query_row(
            "SELECT current_setting('autoinstall_known_extensions')",
            [],
            |row| row.get(0),
        )
        .expect("read automatic-install setting");
    let autoload: bool = connection
        .query_row(
            "SELECT current_setting('autoload_known_extensions')",
            [],
            |row| row.get(0),
        )
        .expect("read automatic-load setting");
    assert!(!autoinstall);
    assert!(!autoload);
    assert!(
        extension_directory
            .path()
            .read_dir()
            .expect("read extension directory")
            .next()
            .is_none(),
        "missing extension must not create an installed extension path"
    );
}

#[test]
fn given_source_position_column_when_alter_sql_is_created_then_fixed_column_is_quoted() {
    assert_eq!(
        source_position_column_statement("users"),
        "ALTER TABLE \"users\" ADD COLUMN IF NOT EXISTS \"__source_position\" JSON;"
    );
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
fn given_ducklake_paths_with_quotes_when_attach_sql_is_built_then_literals_are_escaped() {
    let sql = attach_ducklake(CatalogType::Sqlite, "catalog's.db", "s3://bucket/data's");

    assert_eq!(
        sql,
        "ATTACH 'ducklake:sqlite:catalog''s.db' AS dlake (DATA_PATH 's3://bucket/data''s', \
         META_JOURNAL_MODE 'WAL', ENCRYPTED, DATA_INLINING_ROW_LIMIT 100, AUTOMATIC_MIGRATION \
         true);"
    );
}

#[test]
fn given_aux_catalog_when_attach_sql_is_built_then_statement_sets_meta_type() {
    let sql = attach_ducklake(
        CatalogType::AuxCatalog,
        "/var/lib/aux-analytics/metadata.duckdb",
        "s3://bucket/data",
    );

    assert_eq!(
        sql,
        "ATTACH 'ducklake:/var/lib/aux-analytics/metadata.duckdb' AS dlake (DATA_PATH \
         's3://bucket/data', META_TYPE 'aux_catalog', ENCRYPTED, DATA_INLINING_ROW_LIMIT 100, \
         AUTOMATIC_MIGRATION true);"
    );
}

#[test]
fn given_sqlite_catalog_when_small_changes_reopen_then_inline_values_and_mutations_are_preserved() {
    let directory = tempdir().unwrap();
    let backend = config::StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: directory
            .path()
            .join("catalog.sqlite")
            .to_string_lossy()
            .into_owned(),
        data_path: directory.path().join("data").to_string_lossy().into_owned(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    let connection = Connection::open_in_memory().unwrap();
    crate::sql::configure_connection(&connection, &backend, None).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE events(id INTEGER, quantity DECIMAL(38,9), tags VARCHAR[], payload \
             STRUCT(name VARCHAR, count INTEGER)); BEGIN",
        )
        .unwrap();
    for id in 0..32 {
        connection
            .execute(
                "INSERT INTO events VALUES (?,1.25,['a','b'],{'name':'sample','count':2})",
                [id],
            )
            .unwrap();
    }
    connection
        .execute_batch(
            "COMMIT; UPDATE events SET quantity=2.5 WHERE id < 8; DELETE FROM events WHERE id >= \
             24",
        )
        .unwrap();
    let files: i64 = connection
        .query_row(
            "SELECT count(*) FROM ducklake_list_files('dlake', 'events')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        files <= 1,
        "32 small inserts must be inlined or flushed as one file, got {files}"
    );
    connection
        .execute_batch(
            "UPDATE events SET quantity=2.5 WHERE id < 8; DELETE FROM events WHERE id >= 24",
        )
        .unwrap();
    drop(connection);
    let reopened = Connection::open_in_memory().unwrap();
    crate::sql::configure_connection(&reopened, &backend, None).unwrap();
    let result: (i64, String, i64, String) = reopened
        .query_row(
            "SELECT count(*), CAST(sum(quantity) AS VARCHAR), min(payload.count), min(tags[2]) \
             FROM events",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(result, (24, "40.000000000".into(), 2, "b".into()));
}

#[test]
fn given_quoted_static_credentials_when_secret_sql_is_built_then_all_values_are_escaped() {
    let sql = ObjectStorageSecret::from_config(&AnalyticsObjectStorageConfig {
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
    .and_then(|secret| secret.build_statement(None))
    .expect("secret sql");

    assert_eq!(
        sql,
        "CREATE OR REPLACE SECRET aux_analytics_object_store (TYPE S3, REGION 'us-east-1', KEY_ID \
         'AKIA''KEY', SECRET 'secret''value', SESSION_TOKEN 'session''token');"
    );
    assert!(!sql.contains("credential_chain"));
}

#[test]
fn given_http_object_store_endpoint_when_secret_sql_is_built_then_tls_is_disabled_and_path_style_selected()
 {
    let sql = ObjectStorageSecret::from_config(&AnalyticsObjectStorageConfig {
        bucket: Some("analytics-bucket".to_string()),
        endpoint_url: Some("http://minio:9000".to_string()),
        ..AnalyticsObjectStorageConfig::default()
    })
    .and_then(|secret| secret.build_statement(None))
    .expect("secret sql");

    assert_eq!(
        sql,
        "CREATE OR REPLACE SECRET aux_analytics_object_store (TYPE S3, ENDPOINT 'minio:9000', \
         USE_SSL false, URL_STYLE 'path');"
    );
}

#[test]
fn given_unresolved_instance_credentials_when_secret_sql_is_built_then_no_statement_is_returned() {
    let sql = ObjectStorageSecret::from_config(&AnalyticsObjectStorageConfig {
        bucket: Some("analytics-bucket".to_string()),
        credentials: Some(RemoteCredentialsConfig {
            instance_keys: Some(true),
            ..RemoteCredentialsConfig::default()
        }),
        ..AnalyticsObjectStorageConfig::default()
    })
    .and_then(|secret| secret.build_statement(None));

    assert!(sql.is_none());
}

#[test]
fn given_resolved_credentials_when_secret_sql_is_built_then_override_is_escaped_and_endpoint_settings_are_preserved()
 {
    let object_storage = AnalyticsObjectStorageConfig {
        bucket: Some("bucket".to_owned()),
        region: Some("region".to_owned()),
        endpoint_url: Some("https://store.example.test".to_owned()),
        credentials: Some(RemoteCredentialsConfig {
            r#static: Some(RemoteStaticCredentialsConfig {
                access_key: "previous-key".to_owned(),
                secret_key: "previous-secret".to_owned(),
                session_token: None,
            }),
            instance_keys: None,
        }),
        ..AnalyticsObjectStorageConfig::default()
    };
    let resolved = aws_credentials::AwsStaticCredentials {
        access_key_id: "resolved'key".to_owned(),
        secret_access_key: "resolved'secret".to_owned(),
        session_token: Some("resolved'token".to_owned()),
    };
    let sql = ObjectStorageSecret::from_config(&object_storage)
        .and_then(|secret| secret.build_statement(Some(&resolved)))
        .expect("secret SQL");
    assert_eq!(
        sql,
        "CREATE OR REPLACE SECRET aux_analytics_object_store (TYPE S3, REGION 'region', ENDPOINT \
         'store.example.test', USE_SSL true, URL_STYLE 'path', KEY_ID 'resolved''key', SECRET \
         'resolved''secret', SESSION_TOKEN 'resolved''token');"
    );
    assert_eq!(
        object_storage
            .credentials
            .as_ref()
            .expect("configuration retained")
            .r#static
            .as_ref()
            .expect("static settings retained")
            .access_key,
        "previous-key"
    );
}
