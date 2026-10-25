use analytics_contract::{AnalyticsColumn, AnalyticsColumnType, PrimitiveColumnType};
use analytics_fixtures::users_manifest;

use super::AnalyticsEngine;

#[test]
fn given_initialized_manifest_when_runtime_schema_is_read_then_internal_tables_exist() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    engine.ensure_manifest(&users_manifest()).unwrap();
    engine
        .connection()
        .execute_batch(
            "SELECT * FROM analytics_checkpoints LIMIT 0;
             SELECT * FROM analytics_system_stream_checkpoints LIMIT 0;
             SELECT * FROM analytics_source_batch_failures LIMIT 0;",
        )
        .expect("manifest initialization prepares the runtime schema");
}

#[test]
fn given_missing_runtime_schema_when_checkpoint_is_read_then_operation_does_not_recreate_tables() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    engine.ensure_manifest(&users_manifest()).unwrap();
    engine
        .connection()
        .execute_batch("DROP TABLE analytics_checkpoints")
        .unwrap();

    assert!(engine.load_source_checkpoints().is_err());
}

#[test]
fn given_multiple_tables_when_manifest_grows_then_each_table_converges_and_retry_is_idempotent() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    let mut manifest = users_manifest();
    let mut orders = manifest.tables[0].clone();
    orders.analytics_table_name = "orders".into();
    manifest.tables.push(orders);
    engine.ensure_manifest(&manifest).unwrap();
    for table in &mut manifest.tables {
        let mut column = table.projection_columns.as_ref().unwrap()[0].clone();
        column.column_name = "additional_value".into();
        column.attribute_path = "additional_value".into();
        table.projection_columns.as_mut().unwrap().push(column);
    }
    for _ in 0..2 {
        engine.ensure_manifest(&manifest).unwrap();
        for name in ["users", "orders"] {
            engine
                .query_unscoped_sql_json(&format!("SELECT additional_value FROM {name}"))
                .unwrap();
        }
    }
}

#[test]
fn given_later_table_ddl_failure_when_manifest_retries_then_prior_table_is_committed_and_connection_recovers()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    engine
        .connection()
        .execute_batch("CREATE VIEW orders AS SELECT 1 AS id")
        .unwrap();
    let mut manifest = users_manifest();
    let mut orders = manifest.tables[0].clone();
    orders.analytics_table_name = "orders".into();
    manifest.tables.push(orders);
    assert!(engine.ensure_manifest(&manifest).is_err());
    engine
        .query_unscoped_sql_json("SELECT * FROM users")
        .unwrap();
    engine
        .connection()
        .execute_batch("DROP VIEW orders")
        .unwrap();
    engine.ensure_manifest(&manifest).unwrap();
    engine
        .query_unscoped_sql_json("SELECT * FROM orders")
        .unwrap();
}

#[test]
fn given_invalid_later_condition_when_manifest_validates_then_no_table_is_created() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    let mut manifest = users_manifest();
    let mut orders = manifest.tables[0].clone();
    orders.analytics_table_name = "orders".into();
    orders.condition_expression = Some("(".into());
    manifest.tables.push(orders);
    assert!(engine.ensure_manifest(&manifest).is_err());
    assert_eq!(
        engine
            .query_unscoped_sql_json(
                "SELECT count(*) AS count FROM duckdb_tables() WHERE NOT internal"
            )
            .unwrap(),
        [serde_json::json!({"count": 0})]
    );
}

#[test]
fn given_multiple_registered_tables_when_schema_evolves_and_restarts_then_each_table_converges() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut manifest = analytics_fixtures::users_manifest();
    let mut second = manifest.tables[0].clone();
    second.analytics_table_name = "other_users".into();
    manifest.tables.push(second);
    engine.ensure_manifest(&manifest).expect("initial schema");
    for table in &mut manifest.tables {
        table.columns.push(AnalyticsColumn {
            column_name: "schema_revision".into(),
            column_type: AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::BigInt,
            },
        });
    }
    engine.ensure_manifest(&manifest).expect("evolved schema");
    engine.ensure_manifest(&manifest).expect("restart schema");
    for table in &manifest.tables {
        let statement = format!("SELECT schema_revision FROM {}", table.analytics_table_name);
        engine
            .connection()
            .prepare(&statement)
            .expect("column exists");
    }
}

#[test]
fn given_invalid_later_registration_when_schema_is_ensured_then_no_table_is_created() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut manifest = analytics_fixtures::users_manifest();
    let mut second = manifest.tables[0].clone();
    second.analytics_table_name = "other_users".into();
    second.condition_expression = Some("unknown_function(".into());
    manifest.tables.push(second);
    assert!(engine.ensure_manifest(&manifest).is_err());
    let tables: i64 = engine
        .connection()
        .query_row(
            "SELECT count(*) FROM information_schema.tables",
            [],
            |row| row.get(0),
        )
        .expect("table count");
    assert_eq!(tables, 0, "validation must precede all schema effects");
}
