use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, QuerySelect,
    RowIdentity, StorageStreamRecord, StorageValue, StructuredQuery, TableRegistration,
    TenantSelector,
};
use analytics_engine::AnalyticsEngine;
use clap::Parser;

use crate::{app::run, cli::Cli};

#[tokio::test]
async fn given_schema_command_when_run_then_manifest_schema_json_is_returned() {
    let cli = Cli::parse_from(["aux-analytics", "schema"]);

    let output = run(cli)
        .await
        .expect("schema command should run")
        .expect("output");

    let schema: serde_json::Value = serde_json::from_str(&output).expect("schema json");
    assert_eq!(schema["title"], "AnalyticsManifest");
}

#[tokio::test]
async fn given_config_schema_command_when_run_then_config_schema_json_is_returned() {
    let cli = Cli::parse_from(["aux-analytics", "config-schema"]);

    let output = run(cli)
        .await
        .expect("config schema command should run")
        .expect("output");

    let schema: serde_json::Value = serde_json::from_str(&output).expect("schema json");
    assert_eq!(schema["title"], "RootConfig");
}

#[tokio::test]
async fn given_openapi_command_when_run_then_openapi_json_is_returned() {
    let cli = Cli::parse_from(["aux-analytics", "openapi"]);

    let output = run(cli)
        .await
        .expect("openapi command should run")
        .expect("output");

    let document: serde_json::Value = serde_json::from_str(&output).expect("openapi json");
    assert_eq!(document["openapi"], "3.1.0");
}

#[tokio::test]
async fn given_tenant_query_command_when_run_then_only_target_tenant_rows_are_returned() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let db_path = tempdir.path().join("analytics.duckdb");
    let manifest_path = tempdir.path().join("manifest.json");
    let query_path = tempdir.path().join("query.json");
    let manifest = tenant_attribute_manifest();

    std::fs::write(
        manifest_path.as_path(),
        serde_json::to_string(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
    std::fs::write(
        query_path.as_path(),
        serde_json::to_string(&email_query()).expect("query json"),
    )
    .expect("write query");

    let engine =
        AnalyticsEngine::connect_duckdb(db_path.to_str().expect("db path")).expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    for (tenant_id, user_id, email) in [
        ("tenant-a", "user-a", "a@example.com"),
        ("tenant-b", "user-b", "b@example.com"),
    ] {
        engine
            .ingest_stream_record(
                &manifest,
                "users",
                user_id.as_bytes(),
                StorageStreamRecord {
                    sequence_number: user_id.to_string(),
                    keys: std::collections::HashMap::new(),
                    old_image: None,
                    new_image: Some(std::collections::HashMap::from([
                        (
                            "tenant_id".to_string(),
                            StorageValue::S(tenant_id.to_string()),
                        ),
                        ("user_id".to_string(), StorageValue::S(user_id.to_string())),
                        (
                            "profile".to_string(),
                            StorageValue::M(std::collections::HashMap::from([(
                                "email".to_string(),
                                StorageValue::S(email.to_string()),
                            )])),
                        ),
                    ])),
                },
            )
            .expect("ingest");
    }
    drop(engine);

    let cli = Cli::parse_from([
        "aux-analytics",
        "tenant-query",
        "--target-tenant-id",
        "tenant-a",
        "--manifest",
        manifest_path.to_str().expect("manifest path"),
        "--query",
        query_path.to_str().expect("query path"),
        "--duckdb",
        db_path.to_str().expect("db path"),
    ]);

    let output = run(cli)
        .await
        .expect("tenant query should run")
        .expect("output");
    let rows: serde_json::Value = serde_json::from_str(&output).expect("query rows");

    assert_eq!(rows.as_array().map(Vec::len), Some(1));
    assert_eq!(rows[0]["email"], "a@example.com");
}

fn tenant_attribute_manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "shared_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::Attribute {
            attribute_name: "tenant_id".to_string(),
        },
        row_identity: RowIdentity::Attribute {
            attribute_name: "user_id".to_string(),
        },
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(vec![ProjectionColumn {
            column_name: "email".to_string(),
            attribute_path: "profile.email".to_string(),
            column_type: Some(AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::VarChar,
            }),
        }]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }])
}

fn email_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "users".to_string(),
        select: vec![QuerySelect::Column {
            column_name: "email".to_string(),
            alias: None,
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
    }
}
