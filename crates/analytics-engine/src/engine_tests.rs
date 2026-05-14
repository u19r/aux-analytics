use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, QueryExpression,
    QueryPredicate, QuerySelect, RetentionPolicy, RetentionTimestamp, RowIdentity,
    StorageStreamRecord, StorageValue, StructuredQuery, TableRegistration, TenantSelector,
};
use analytics_fixtures::{generic_items_manifest, legacy_item, storage_key, users_manifest};

use super::{AnalyticsEngine, IngestOutcome, SourceCheckpoint};

#[test]
fn stream_records_are_projected_into_queryable_rows() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant_01".to_string()),
        tenant_selector: TenantSelector::TableName,
        row_identity: analytics_contract::RowIdentity::RecordKey,
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
    }]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let record = StorageStreamRecord {
        sequence_number: "1".to_string(),
        keys: HashMap::new(),
        old_image: None,
        new_image: Some(HashMap::from([(
            "profile".to_string(),
            StorageValue::M(HashMap::from([(
                "email".to_string(),
                StorageValue::S("a@example.com".to_string()),
            )])),
        )])),
    };
    let outcome = engine
        .ingest_stream_record(&manifest, "users", b"user-1", record)
        .expect("ingest");
    assert_eq!(outcome, IngestOutcome::Inserted);

    let rows = engine
        .query_unscoped_sql_json("select tenant_id, email from users")
        .expect("query");
    assert_eq!(rows[0]["tenant_id"], "tenant_01");
    assert_eq!(rows[0]["email"], "a@example.com");
}

#[test]
fn condition_expression_filters_single_source_into_separate_tables() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![
        filtered_entity_table("shared_items", "users", "USER"),
        filtered_entity_table("shared_items", "orders", "ORDER"),
    ]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let user_record = entity_record("1", "USER", "user@example.com");
    let order_record = entity_record("2", "ORDER", "order@example.com");

    assert_eq!(
        engine
            .ingest_stream_record(&manifest, "users", b"1", user_record.clone())
            .expect("ingest user into users"),
        IngestOutcome::Inserted
    );
    assert_eq!(
        engine
            .ingest_stream_record(&manifest, "orders", b"1", user_record)
            .expect("skip user for orders"),
        IngestOutcome::Skipped
    );
    assert_eq!(
        engine
            .ingest_stream_record(&manifest, "orders", b"2", order_record.clone())
            .expect("ingest order into orders"),
        IngestOutcome::Inserted
    );
    assert_eq!(
        engine
            .ingest_stream_record(&manifest, "users", b"2", order_record)
            .expect("skip order for users"),
        IngestOutcome::Skipped
    );

    let users = engine
        .query_unscoped_sql_json("select entity_id from users order by entity_id")
        .expect("query users");
    let orders = engine
        .query_unscoped_sql_json("select entity_id from orders order by entity_id")
        .expect("query orders");

    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["entity_id"], "1");
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0]["entity_id"], "2");
}

#[test]
fn condition_expression_removes_rows_when_updates_leave_filter() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest =
        AnalyticsManifest::new(vec![filtered_entity_table("shared_items", "users", "USER")]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    engine
        .ingest_stream_record(
            &manifest,
            "users",
            b"1",
            StorageStreamRecord {
                sequence_number: "1".to_string(),
                keys: HashMap::new(),
                old_image: None,
                new_image: Some(entity_item("1", "USER", "user@example.com")),
            },
        )
        .expect("insert matching row");

    let outcome = engine
        .ingest_stream_record(
            &manifest,
            "users",
            b"1",
            StorageStreamRecord {
                sequence_number: "2".to_string(),
                keys: HashMap::new(),
                old_image: Some(entity_item("1", "USER", "user@example.com")),
                new_image: Some(entity_item("1", "ORDER", "order@example.com")),
            },
        )
        .expect("update out of filtered table");

    assert_eq!(outcome, IngestOutcome::Deleted);
    let users = engine
        .query_unscoped_sql_json("select entity_id from users")
        .expect("query users");
    assert!(users.is_empty());
}

#[test]
fn source_checkpoints_are_persisted_in_analytics_database() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine
        .save_source_checkpoint(&SourceCheckpoint {
            source_table_name: "tenant_entities".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "12345".to_string(),
        })
        .expect("save checkpoint");

    let checkpoints = engine.load_source_checkpoints().expect("load checkpoints");

    assert_eq!(
        checkpoints,
        vec![SourceCheckpoint {
            source_table_name: "tenant_entities".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "12345".to_string(),
        }]
    );
}

#[test]
fn query_unscoped_sql_json_rejects_mutating_statements() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");

    let err = engine
        .query_unscoped_sql_json("drop table users")
        .expect_err("mutating query should fail");

    assert!(matches!(err, super::AnalyticsEngineError::InvalidQuery));
}

#[test]
fn query_unscoped_sql_json_interrupts_queries_that_exceed_timeout() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");

    let err = engine
        .query_unscoped_sql_json_with_timeout(
            "select sum(i * j) as total from range(100000000) a(i), range(100000000) b(j)",
            Duration::from_millis(1),
        )
        .expect_err("long query should be interrupted");

    assert!(matches!(
        err,
        super::AnalyticsEngineError::QueryTimeout { timeout_ms: 1 }
    ));

    let rows = engine
        .query_unscoped_sql_json("select 1 as ready")
        .expect("connection should remain usable after timeout");
    assert_eq!(rows[0]["ready"], 1);
}

#[test]
fn generic_document_tables_do_not_require_tenant_or_projection_layout() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "legacy_items".to_string(),
        analytics_table_name: "legacy_items".to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::None,
        row_identity: RowIdentity::StreamKeys,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let record = StorageStreamRecord {
        sequence_number: "1".to_string(),
        keys: HashMap::from([("pk".to_string(), StorageValue::S("USER#1".to_string()))]),
        old_image: None,
        new_image: Some(HashMap::from([
            ("pk".to_string(), StorageValue::S("USER#1".to_string())),
            (
                "profile".to_string(),
                StorageValue::M(HashMap::from([(
                    "email".to_string(),
                    StorageValue::S("legacy@example.com".to_string()),
                )])),
            ),
        ])),
    };
    let outcome = engine
        .ingest_stream_record(&manifest, "legacy_items", b"ignored", record)
        .expect("ingest");
    assert_eq!(outcome, IngestOutcome::Inserted);

    let rows = engine
        .query_unscoped_sql_json(
            "select tenant_id, item->'profile'->>'email' as email from legacy_items",
        )
        .expect("query");
    assert_eq!(rows[0]["tenant_id"], "");
    assert_eq!(rows[0]["email"], "legacy@example.com");
}

#[test]
fn structured_queries_compile_from_registered_manifest_columns() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let record = StorageStreamRecord {
        sequence_number: "1".to_string(),
        keys: storage_key("USER", "1"),
        old_image: None,
        new_image: Some(analytics_fixtures::user_item(
            "1",
            "structured@example.com",
            "org-a",
        )),
    };
    engine
        .ingest_stream_record(&manifest, "users", b"user-1", record)
        .expect("ingest");

    let rows = engine
        .query_unscoped_structured_json(
            &manifest,
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
                    value: serde_json::json!("org-a"),
                }],
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: Some(1),
            },
        )
        .expect("structured query");

    assert_eq!(rows[0]["email"], "structured@example.com");
}

#[test]
fn tenant_scoped_structured_queries_only_return_target_tenant_rows() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
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
    }]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    for (tenant_id, user_id, email) in [
        ("tenant-a", "user-a", "a@example.com"),
        ("tenant-b", "user-b", "b@example.com"),
    ] {
        let record = StorageStreamRecord {
            sequence_number: user_id.to_string(),
            keys: HashMap::new(),
            old_image: None,
            new_image: Some(HashMap::from([
                (
                    "tenant_id".to_string(),
                    StorageValue::S(tenant_id.to_string()),
                ),
                ("user_id".to_string(), StorageValue::S(user_id.to_string())),
                (
                    "profile".to_string(),
                    StorageValue::M(HashMap::from([(
                        "email".to_string(),
                        StorageValue::S(email.to_string()),
                    )])),
                ),
            ])),
        };
        engine
            .ingest_stream_record(&manifest, "users", user_id.as_bytes(), record)
            .expect("ingest");
    }

    let rows = engine
        .query_tenant_structured_json(
            &manifest,
            &StructuredQuery {
                analytics_table_name: "users".to_string(),
                select: vec![
                    QuerySelect::Column {
                        column_name: "tenant_id".to_string(),
                        alias: None,
                    },
                    QuerySelect::Column {
                        column_name: "email".to_string(),
                        alias: None,
                    },
                ],
                filters: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: None,
            },
            "tenant-a",
        )
        .expect("tenant-scoped structured query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["tenant_id"], "tenant-a");
    assert_eq!(rows[0]["email"], "a@example.com");
}

#[test]
fn structured_queries_read_document_paths_from_generic_tables() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = generic_items_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let record = StorageStreamRecord {
        sequence_number: "1".to_string(),
        keys: storage_key("USER", "1"),
        old_image: None,
        new_image: Some(legacy_item("1", "legacy-structured@example.com")),
    };
    engine
        .ingest_stream_record(&manifest, "legacy_items", b"ignored", record)
        .expect("ingest");

    let rows = engine
        .query_unscoped_structured_json(
            &manifest,
            &StructuredQuery {
                analytics_table_name: "legacy_items".to_string(),
                select: vec![QuerySelect::DocumentPath {
                    document_column: "item".to_string(),
                    path: "profile.email".to_string(),
                    alias: "email".to_string(),
                }],
                filters: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: Some(1),
            },
        )
        .expect("structured query");

    assert_eq!(rows[0]["email"], "legacy-structured@example.com");
}

#[test]
fn regex_selectors_extract_tenant_and_row_identity() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "shared_items".to_string(),
        analytics_table_name: "orders".to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::AttributeRegex {
            attribute_name: "pk".to_string(),
            regex: "^TENANT#(?<tenant_id>[^#]+)#ORDER#(?<order_id>[^#]+)$".to_string(),
            capture: "tenant_id".to_string(),
        },
        row_identity: RowIdentity::AttributeRegex {
            attribute_name: "pk".to_string(),
            regex: "^TENANT#(?<tenant_id>[^#]+)#ORDER#(?<order_id>[^#]+)$".to_string(),
            capture: "order_id".to_string(),
        },
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(vec![ProjectionColumn {
            column_name: "status".to_string(),
            attribute_path: "status".to_string(),
            column_type: Some(AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::VarChar,
            }),
        }]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let record = StorageStreamRecord {
        sequence_number: "1".to_string(),
        keys: storage_key("ORDER", "123"),
        old_image: None,
        new_image: Some(HashMap::from([
            (
                "pk".to_string(),
                StorageValue::S("TENANT#tenant-a#ORDER#123".to_string()),
            ),
            ("status".to_string(), StorageValue::S("paid".to_string())),
        ])),
    };
    engine
        .ingest_stream_record(&manifest, "orders", b"ignored", record)
        .expect("ingest");

    let rows = engine
        .query_unscoped_sql_json("select tenant_id, status from orders")
        .expect("query");

    assert_eq!(rows[0]["tenant_id"], "tenant-a");
    assert_eq!(rows[0]["status"], "paid");
}

#[test]
fn static_retention_computes_expiry_from_source_timestamp() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = filtered_entity_table("shared_items", "events", "EVENT");
    table.retention = Some(RetentionPolicy {
        period_ms: 1_000,
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: "created_at_ms".to_string(),
        },
    });
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let mut item = entity_item("1", "EVENT", "event@example.com");
    item.insert(
        "created_at_ms".to_string(),
        StorageValue::N("1000".to_string()),
    );
    engine
        .ingest_stream_record(
            &manifest,
            "events",
            b"1",
            StorageStreamRecord {
                sequence_number: "1".to_string(),
                keys: HashMap::new(),
                old_image: None,
                new_image: Some(item),
            },
        )
        .expect("ingest");

    let rows = engine
        .query_unscoped_sql_json("select __expiry, __missing_retention from events")
        .expect("query retention columns");
    assert_eq!(rows[0]["__expiry"], 2000);
    assert_eq!(rows[0]["__missing_retention"], false);
}

#[test]
fn bulk_expiry_delete_removes_only_expired_rows() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = filtered_entity_table("shared_items", "events", "EVENT");
    table.retention = Some(RetentionPolicy {
        period_ms: 1_000,
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: "created_at_ms".to_string(),
        },
    });
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    for (id, created_at_ms) in [("1", "1000"), ("2", "5000")] {
        let mut item = entity_item(id, "EVENT", "event@example.com");
        item.insert(
            "created_at_ms".to_string(),
            StorageValue::N(created_at_ms.to_string()),
        );
        engine
            .ingest_stream_record(
                &manifest,
                "events",
                id.as_bytes(),
                StorageStreamRecord {
                    sequence_number: id.to_string(),
                    keys: HashMap::new(),
                    old_image: None,
                    new_image: Some(item),
                },
            )
            .expect("ingest");
    }

    let deleted = engine
        .delete_expired_rows("events", 2_500, 100)
        .expect("delete expired");

    assert_eq!(deleted, 1);
    let rows = engine
        .query_unscoped_sql_json("select entity_id from events order by entity_id")
        .expect("query remaining");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["entity_id"], "2");
}

fn filtered_entity_table(
    source_table_name: &str,
    analytics_table_name: &str,
    entity_type: &str,
) -> TableRegistration {
    TableRegistration {
        source_table_name: source_table_name.to_string(),
        analytics_table_name: analytics_table_name.to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::None,
        row_identity: RowIdentity::Attribute {
            attribute_name: "entity_id".to_string(),
        },
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: Some("#entity_type = :entity_type".to_string()),
        expression_attribute_names: Some(HashMap::from([(
            "#entity_type".to_string(),
            "entity_type".to_string(),
        )])),
        expression_attribute_values: Some(BTreeMap::from([(
            ":entity_type".to_string(),
            StorageValue::S(entity_type.to_string()),
        )])),
        projection_attribute_names: Some(vec!["entity_id".to_string(), "email".to_string()]),
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }
}

fn entity_record(entity_id: &str, entity_type: &str, email: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: entity_id.to_string(),
        keys: HashMap::new(),
        old_image: None,
        new_image: Some(entity_item(entity_id, entity_type, email)),
    }
}

fn entity_item(entity_id: &str, entity_type: &str, email: &str) -> HashMap<String, StorageValue> {
    HashMap::from([
        (
            "entity_id".to_string(),
            StorageValue::S(entity_id.to_string()),
        ),
        (
            "entity_type".to_string(),
            StorageValue::S(entity_type.to_string()),
        ),
        ("email".to_string(), StorageValue::S(email.to_string())),
    ])
}
