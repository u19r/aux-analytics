use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, ClusteringKey, PrimitiveColumnType, PrivacyPolicy,
    ProjectionColumn, QueryExpression, QueryJoin, QueryJoinKind, QueryJoinPredicate,
    QueryPredicate, QuerySelect, RetentionPolicy, RetentionTimestamp, RowIdentity, SortOrder,
    SourcePosition, StorageStreamRecord, StorageValue, StructuredQuery, TableRegistration,
    TenantSelector,
};
use analytics_fixtures::{
    generic_items_manifest, legacy_item, metric_points_manifest, storage_key, user_item,
    users_manifest,
};
use config::{CatalogType, StorageBackend};

use super::{
    AnalyticsEngine, IngestOutcome, PrivacyTableRemediationMode, SourceCheckpoint,
    StreamRecordBatchItem,
};

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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
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
fn timestamp_projection_epoch_millis_ingests_as_duckdb_timestamp() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "sessions".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant_01".to_string()),
        tenant_selector: TenantSelector::TableName,
        row_identity: RowIdentity::RecordKey,
        document_column: None,
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(vec![ProjectionColumn {
            column_name: "created_at".to_string(),
            attribute_path: "created_at".to_string(),
            column_type: Some(AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::Timestamp,
            }),
        }]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let insert_outcome = engine
        .ingest_stream_record(
            &manifest,
            "sessions",
            b"session-1",
            timestamp_record("seq-1", "1781572733032"),
        )
        .expect("insert timestamp");
    assert_eq!(insert_outcome, IngestOutcome::Inserted);

    let update_outcome = engine
        .ingest_stream_record(
            &manifest,
            "sessions",
            b"session-1",
            timestamp_record("seq-2", "1781572733547"),
        )
        .expect("update timestamp");
    assert_eq!(update_outcome, IngestOutcome::Updated);

    let rows = engine
        .query_unscoped_sql_json("select epoch_ms(created_at) as created_at_ms from sessions")
        .expect("query timestamp");
    assert_eq!(rows[0]["created_at_ms"], 1_781_572_733_547_i64);
}

#[test]
fn existing_tables_gain_new_projection_columns_before_layout_is_applied() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let existing_manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "metric_events_raw".to_string(),
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
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }]);
    engine
        .ensure_manifest(&existing_manifest)
        .expect("ensure existing manifest");

    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "metric_events_raw".to_string(),
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
            column_name: "updated_at_ms".to_string(),
            attribute_path: "updated_at_ms".to_string(),
            column_type: Some(AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::BigInt,
            }),
        }]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: vec![ClusteringKey {
            column_name: "updated_at_ms".to_string(),
            order: Some(SortOrder::Desc),
        }],
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }]);

    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let rows = engine
        .query_unscoped_sql_json("select updated_at_ms from metric_events_raw")
        .expect("query added column");
    assert!(rows.is_empty());
}

#[test]
fn stream_record_batches_are_ingested_in_one_transaction_boundary() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let outcomes = engine
        .ingest_stream_record_batch(
            &manifest,
            vec![
                StreamRecordBatchItem {
                    analytics_table_name: "users".to_string(),
                    record_key: b"user-1".to_vec(),
                    record: email_record("1", "a@example.test"),
                },
                StreamRecordBatchItem {
                    analytics_table_name: "users".to_string(),
                    record_key: b"user-2".to_vec(),
                    record: email_record("2", "b@example.test"),
                },
            ],
        )
        .expect("batch ingest");

    assert_eq!(
        outcomes,
        vec![IngestOutcome::Inserted, IngestOutcome::Inserted]
    );
    let rows = engine
        .query_unscoped_sql_json("select count(*) as row_count from users")
        .expect("query");
    assert_eq!(rows[0]["row_count"], 2);
}

fn email_record(sequence_number: &str, email: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: HashMap::new(),
        old_image: None,
        new_image: Some(user_item(sequence_number, email, "org-a")),
    }
}

#[test]
fn privacy_policy_removes_denied_field_before_projection() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant_01".to_string()),
        tenant_selector: TenantSelector::TableName,
        row_identity: analytics_contract::RowIdentity::RecordKey,
        document_column: None,
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let policy = PrivacyPolicy {
        version: "privacy-v1".to_string(),
        denied_exact_paths: vec!["profile.email".to_string()],
        ..PrivacyPolicy::default()
    };

    let outcome = engine
        .ingest_stream_record_with_privacy_policy(
            &manifest,
            "users",
            b"user-1",
            StorageStreamRecord {
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
            },
            &policy,
        )
        .expect("privacy ingest");

    let rows = engine
        .query_unscoped_sql_json("select email from users")
        .expect("query");
    assert_eq!(outcome.dropped_fields, 1);
    assert_eq!(outcome.policy_version, "privacy-v1");
    assert!(rows[0]["email"].is_null());
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
fn source_checkpoints_are_persisted_in_ducklake_catalog() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let catalog_path = tempdir.path().join("catalog.sqlite");
    let data_path = tempdir.path().join("ducklake-data");
    std::fs::create_dir_all(&data_path).expect("create ducklake data dir");
    let engine = AnalyticsEngine::connect(&StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: catalog_path.to_string_lossy().to_string(),
        data_path: data_path.to_string_lossy().to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    })
    .expect("connect ducklake");
    engine
        .save_source_checkpoint(&SourceCheckpoint {
            source_table_name: "s00000".to_string(),
            shard_id: "aux-storage".to_string(),
            position: "12345".to_string(),
        })
        .expect("save checkpoint");

    let reopened = AnalyticsEngine::connect(&StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: catalog_path.to_string_lossy().to_string(),
        data_path: data_path.to_string_lossy().to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    })
    .expect("reopen ducklake");
    let checkpoints = reopened
        .load_source_checkpoints()
        .expect("load checkpoints");

    assert_eq!(
        checkpoints,
        vec![SourceCheckpoint {
            source_table_name: "s00000".to_string(),
            shard_id: "aux-storage".to_string(),
            position: "12345".to_string(),
        }]
    );
}

#[test]
fn source_checkpoints_are_replaced_without_unique_constraints() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut checkpoint = SourceCheckpoint {
        source_table_name: "tenant_entities".to_string(),
        shard_id: "shard-0001".to_string(),
        position: "12345".to_string(),
    };
    engine
        .save_source_checkpoint(&checkpoint)
        .expect("save initial checkpoint");

    checkpoint.position = "12346".to_string();
    engine
        .save_source_checkpoint(&checkpoint)
        .expect("replace checkpoint");

    let checkpoints = engine.load_source_checkpoints().expect("load checkpoints");

    assert_eq!(checkpoints, vec![checkpoint]);
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
fn query_unscoped_sql_json_rejects_queries_with_expired_timeout() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");

    let err = engine
        .query_unscoped_sql_json_with_timeout("select 1 as ready", Duration::ZERO)
        .expect_err("query should be rejected after its deadline");

    assert!(matches!(
        err,
        super::AnalyticsEngineError::QueryTimeout { timeout_ms: 0 }
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
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
fn privacy_policy_removes_denied_field_before_document_storage() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = generic_items_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let policy = PrivacyPolicy::new("privacy-v1")
        .unwrap()
        .with_denied_key_name("email");

    let outcome = engine
        .ingest_stream_record_with_privacy_policy(
            &manifest,
            "legacy_items",
            b"ignored",
            StorageStreamRecord {
                sequence_number: "1".to_string(),
                keys: HashMap::from([("pk".to_string(), StorageValue::S("USER#1".to_string()))]),
                old_image: None,
                new_image: Some(HashMap::from([
                    ("pk".to_string(), StorageValue::S("USER#1".to_string())),
                    (
                        "email".to_string(),
                        StorageValue::S("legacy@example.com".to_string()),
                    ),
                ])),
            },
            &policy,
        )
        .expect("privacy ingest");

    let rows = engine
        .query_unscoped_sql_json("select item->>'email' as email from legacy_items")
        .expect("query");
    assert_eq!(outcome.dropped_fields, 1);
    assert!(rows[0]["email"].is_null());
}

#[test]
fn privacy_table_scrub_dry_run_reports_projected_and_document_values_without_mutating() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    let record = user_record("user-1", "private@example.test");
    engine
        .ingest_stream_record(&manifest, "users", b"user-1", record)
        .expect("ingest");
    let policy = PrivacyPolicy::new("privacy-v1")
        .expect("policy")
        .with_denied_key_name("email");

    let report = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, true)
        .expect("scrub");
    let rows = engine
        .query_unscoped_sql_json("select email, item from users")
        .expect("query");

    assert_eq!(report.scanned_rows, 1);
    assert_eq!(report.affected_rows, 1);
    assert_eq!(report.projected_values_scrubbed, 1);
    assert_eq!(report.document_values_scrubbed, 1);
    assert!(!report.destination_mutated);
    assert_eq!(rows[0]["email"], "private@example.test");
    assert!(rows[0]["item"].to_string().contains("private@example.test"));
}

#[test]
fn privacy_table_scrub_apply_removes_projected_and_document_values() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    let record = user_record("user-1", "private@example.test");
    engine
        .ingest_stream_record(&manifest, "users", b"user-1", record)
        .expect("ingest");
    let policy = PrivacyPolicy::new("privacy-v1")
        .expect("policy")
        .with_denied_key_name("email");

    let report = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, false)
        .expect("scrub");
    let rows = engine
        .query_unscoped_sql_json("select email, item from users")
        .expect("query");

    assert!(report.destination_mutated);
    assert!(rows[0]["email"].is_null());
    assert!(rows[0]["item"].get("email").is_none());
}

#[test]
fn privacy_table_scrub_projection_only_filters_candidates_in_sql() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = users_manifest().tables.remove(0);
    table.document_column = None;
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .connection()
        .execute(
            "insert into users (tenant_id, __id, table_name, email) values
             ('tenant_01', 'user-1', 'users', 'private@example.test'),
             ('tenant_01', 'user-2', 'users', NULL)",
            [],
        )
        .expect("seed rows");
    let policy = PrivacyPolicy::new("privacy-v1")
        .expect("policy")
        .with_denied_key_name("email");

    let report = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, true)
        .expect("scrub");

    assert_eq!(report.scanned_rows, 1);
    assert_eq!(report.affected_rows, 1);
    assert_eq!(report.projected_values_scrubbed, 1);
}

#[test]
fn privacy_table_delete_dry_run_reports_violating_rows_without_mutating() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .ingest_stream_record(
            &manifest,
            "users",
            b"user-1",
            user_record("user-1", "private@example.test"),
        )
        .expect("ingest");
    let policy = PrivacyPolicy::new("privacy-v1")
        .expect("policy")
        .with_denied_key_name("email");

    let report = engine
        .remediate_table_with_privacy_policy(
            &manifest.tables[0],
            &policy,
            true,
            PrivacyTableRemediationMode::DeleteRows,
        )
        .expect("remediate");
    let rows = engine
        .query_unscoped_sql_json("select count(*) as count from users")
        .expect("query");

    assert_eq!(report.mode, PrivacyTableRemediationMode::DeleteRows);
    assert_eq!(report.affected_rows, 1);
    assert!(!report.destination_mutated);
    assert_eq!(rows[0]["count"], 1);
}

#[test]
fn privacy_table_delete_apply_removes_violating_rows() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .ingest_stream_record(
            &manifest,
            "users",
            b"user-1",
            user_record("user-1", "private@example.test"),
        )
        .expect("ingest");
    let policy = PrivacyPolicy::new("privacy-v1")
        .expect("policy")
        .with_denied_key_name("email");

    let report = engine
        .remediate_table_with_privacy_policy(
            &manifest.tables[0],
            &policy,
            false,
            PrivacyTableRemediationMode::DeleteRows,
        )
        .expect("remediate");
    let rows = engine
        .query_unscoped_sql_json("select count(*) as count from users")
        .expect("query");

    assert!(report.destination_mutated);
    assert_eq!(rows[0]["count"], 0);
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
fn stale_source_position_does_not_overwrite_newer_row() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "users",
            b"user-1",
            user_record("20", "newer@example.com"),
            source_cursor("20"),
        )
        .expect("ingest newer");

    let outcome = engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "users",
            b"user-1",
            user_record("10", "stale@example.com"),
            source_cursor("10"),
        )
        .expect("ingest stale");

    assert_eq!(outcome, IngestOutcome::Skipped);
    let rows = engine
        .query_unscoped_sql_json("select email from users")
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["email"], "newer@example.com");
}

#[test]
fn repeated_source_position_does_not_duplicate_or_rewrite_row() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let position = source_cursor("10");
    engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "users",
            b"user-1",
            user_record("10", "original@example.com"),
            position.clone(),
        )
        .expect("ingest original");

    let outcome = engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "users",
            b"user-1",
            user_record("10", "duplicate@example.com"),
            position,
        )
        .expect("ingest duplicate");

    assert_eq!(outcome, IngestOutcome::Skipped);
    let rows = engine
        .query_unscoped_sql_json("select email from users")
        .expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["email"], "original@example.com");
}

#[test]
fn delete_with_replayed_source_position_is_idempotent() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let position = source_cursor("10");
    let item = analytics_fixtures::user_item("1", "delete@example.com", "org-a");
    engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "users",
            b"user-1",
            StorageStreamRecord {
                sequence_number: "10".to_string(),
                keys: storage_key("USER", "1"),
                old_image: None,
                new_image: Some(item.clone()),
            },
            position.clone(),
        )
        .expect("insert");
    let delete = StorageStreamRecord {
        sequence_number: "11".to_string(),
        keys: storage_key("USER", "1"),
        old_image: Some(item),
        new_image: None,
    };
    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"user-1",
                delete.clone(),
                source_cursor("11"),
            )
            .expect("delete"),
        IngestOutcome::Deleted
    );
    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"user-1",
                delete,
                source_cursor("11"),
            )
            .expect("replay delete"),
        IngestOutcome::Skipped
    );
    let rows = engine
        .query_unscoped_sql_json("select count(*) as count from users")
        .expect("query");
    assert_eq!(rows[0]["count"], 0);
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
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
                table_alias: None,
                joins: Vec::new(),
                select: vec![
                    QuerySelect::Column {
                        table_alias: None,
                        column_name: "tenant_id".to_string(),
                        alias: None,
                    },
                    QuerySelect::Column {
                        table_alias: None,
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
fn tenant_structured_query_executes_joined_metric_scan() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = metric_points_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    for (event_id, model_name, value) in [("evt_1", "input", "100"), ("evt_2", "output", "25")] {
        engine
            .ingest_stream_record(
                &manifest,
                "metric_points_v1",
                event_id.as_bytes(),
                StorageStreamRecord {
                    sequence_number: event_id.to_string(),
                    keys: HashMap::new(),
                    old_image: None,
                    new_image: Some(metric_point_item(event_id, model_name, value)),
                },
            )
            .expect("ingest metric point");
    }
    for (model_name, rate_class) in [("input", "input_tokens"), ("output", "output_tokens")] {
        engine
            .ingest_stream_record(
                &manifest,
                "billing_rate_class_map_v1",
                model_name.as_bytes(),
                StorageStreamRecord {
                    sequence_number: model_name.to_string(),
                    keys: HashMap::new(),
                    old_image: None,
                    new_image: Some(rate_class_item(model_name, rate_class)),
                },
            )
            .expect("ingest reference row");
    }

    let rows = engine
        .query_tenant_structured_json(&manifest, &joined_metric_query(), "tenant_01")
        .expect("joined metric query");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["rate_class"], "input_tokens");
    assert_eq!(rows[0]["quantity"], 100);
    assert_eq!(rows[1]["rate_class"], "output_tokens");
    assert_eq!(rows[1]["quantity"], 25);
}

#[test]
fn expanded_metric_wal_point_uses_tenant_attribute_for_materialization() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = metric_points_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let mut item = metric_point_item("evt_wal_1", "input", "7");
    item.insert(
        "series_name".to_string(),
        StorageValue::S("canary_lambda_gzip".to_string()),
    );
    item.insert(
        "tenant_id".to_string(),
        StorageValue::S("t_metric_wal".to_string()),
    );
    item.insert(
        "wal_batch_id".to_string(),
        StorageValue::S("mwb_01".to_string()),
    );
    item.insert(
        "wal_row_ordinal".to_string(),
        StorageValue::N("0".to_string()),
    );

    let outcome = engine
        .ingest_stream_record(
            &manifest,
            "metric_points_v1",
            b"mwb_01:0",
            StorageStreamRecord {
                sequence_number: "mwb_01:0".to_string(),
                keys: HashMap::new(),
                old_image: None,
                new_image: Some(item),
            },
        )
        .expect("ingest expanded WAL metric point");

    assert_eq!(outcome, IngestOutcome::Inserted);
    let rows = engine
        .query_tenant_structured_json(
            &manifest,
            &metric_sum_query("canary_lambda_gzip"),
            "t_metric_wal",
        )
        .expect("query materialized WAL metric point");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["quantity"], 7);
}

#[test]
fn metric_fact_layout_executes_core_meter_query_shapes() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = metric_points_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    seed_metric_points(&engine, &manifest);

    let total = engine
        .query_tenant_structured_json(
            &manifest,
            &metric_sum_query("api_request_units"),
            "tenant_01",
        )
        .expect("simple total query");
    assert_eq!(total[0]["quantity"], 30);

    let grouped = engine
        .query_tenant_structured_json(&manifest, &metric_grouped_query(), "tenant_01")
        .expect("grouped query");
    assert_eq!(grouped[0]["region"], "eu-west-2");
    assert_eq!(grouped[0]["quantity"], 30);

    let grouped_three = engine
        .query_tenant_structured_json(&manifest, &metric_three_dimension_query(), "tenant_01")
        .expect("three dimension grouped query");
    assert_eq!(grouped_three.len(), 2);
    assert_eq!(grouped_three[0]["operation"], "read");
    assert_eq!(grouped_three[1]["operation"], "write");

    let distinct = engine
        .query_tenant_structured_json(&manifest, &metric_distinct_query(), "tenant_01")
        .expect("distinct query");
    assert_eq!(distinct[0]["active_regions"], 1);

    let decimal = engine
        .query_tenant_structured_json(&manifest, &metric_decimal_query(), "tenant_01")
        .expect("decimal query");
    assert_eq!(decimal[0]["quantity"], 42.5);

    let evidence = engine
        .query_tenant_structured_json(&manifest, &metric_evidence_query(), "tenant_01")
        .expect("evidence query");
    assert_eq!(evidence.len(), 2);
    assert_eq!(evidence[0]["event_id"], "evt_read");
    assert_eq!(evidence[0]["occurred_at_ms"], 1782863999000_i64);
}

#[test]
fn partition_key_prefix_selector_maps_aux_storage_namespace_prefix_to_tenant_id() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "s00000".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::PartitionKeyPrefix {
            attribute_name: "pk".to_string(),
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let record = StorageStreamRecord {
        sequence_number: "user-a".to_string(),
        keys: HashMap::new(),
        old_image: None,
        new_image: Some(HashMap::from([
            (
                "pk".to_string(),
                StorageValue::S("ns_2X6Z455H5ZBWSRK6D51ASR0#USER#user-a".to_string()),
            ),
            ("user_id".to_string(), StorageValue::S("user-a".to_string())),
            (
                "profile".to_string(),
                StorageValue::M(HashMap::from([(
                    "email".to_string(),
                    StorageValue::S("a@example.com".to_string()),
                )])),
            ),
        ])),
    };
    engine
        .ingest_stream_record(&manifest, "users", b"user-a", record)
        .expect("ingest");

    let rows = engine
        .query_tenant_structured_json(
            &manifest,
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
            "t_2X6Z455H5ZBWSRK6D51ASR0",
        )
        .expect("tenant-scoped structured query");

    assert_eq!(rows.len(), 1);
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
                table_alias: None,
                joins: Vec::new(),
                select: vec![QuerySelect::DocumentPath {
                    table_alias: None,
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
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

fn user_record(sequence_number: &str, email: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: storage_key("USER", "1"),
        old_image: None,
        new_image: Some(analytics_fixtures::user_item("1", email, "org-a")),
    }
}

fn timestamp_record(sequence_number: &str, created_at_ms: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: HashMap::new(),
        old_image: None,
        new_image: Some(HashMap::from([(
            "created_at".to_string(),
            StorageValue::N(created_at_ms.to_string()),
        )])),
    }
}

fn source_cursor(cursor: &str) -> SourcePosition {
    SourcePosition::AuxStorageStreamCursor {
        stream_name: "users".to_string(),
        cursor: cursor.to_string(),
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

fn metric_point_item(
    event_id: &str,
    model_name: &str,
    value: &str,
) -> HashMap<String, StorageValue> {
    HashMap::from([
        (
            "tenant_id".to_string(),
            StorageValue::S("tenant_01".to_string()),
        ),
        (
            "series_name".to_string(),
            StorageValue::S("ai_token_units".to_string()),
        ),
        (
            "customer_account_id".to_string(),
            StorageValue::S("bcus_123".to_string()),
        ),
        ("org_id".to_string(), StorageValue::NULL(true)),
        (
            "occurred_at_ms".to_string(),
            StorageValue::N("1782863999000".to_string()),
        ),
        (
            "ingested_at_ms".to_string(),
            StorageValue::N("1782864000000".to_string()),
        ),
        (
            "event_id".to_string(),
            StorageValue::S(event_id.to_string()),
        ),
        ("value_i64".to_string(), StorageValue::N(value.to_string())),
        ("value_decimal".to_string(), StorageValue::NULL(true)),
        (
            "dim_1".to_string(),
            StorageValue::S("eu-west-2".to_string()),
        ),
        ("dim_2".to_string(), StorageValue::S(model_name.to_string())),
        ("dim_3".to_string(), StorageValue::S("tokens".to_string())),
        ("source".to_string(), StorageValue::S("test".to_string())),
        (
            "schema_version".to_string(),
            StorageValue::N("1".to_string()),
        ),
    ])
}

fn rate_class_item(model_name: &str, rate_class: &str) -> HashMap<String, StorageValue> {
    HashMap::from([
        (
            "tenant_id".to_string(),
            StorageValue::S("tenant_01".to_string()),
        ),
        (
            "model_name".to_string(),
            StorageValue::S(model_name.to_string()),
        ),
        (
            "rate_class".to_string(),
            StorageValue::S(rate_class.to_string()),
        ),
        (
            "effective_from_ms".to_string(),
            StorageValue::N("0".to_string()),
        ),
        ("effective_to_ms".to_string(), StorageValue::NULL(true)),
    ])
}

fn seed_metric_points(engine: &AnalyticsEngine, manifest: &AnalyticsManifest) {
    for (event_id, series_name, operation, value) in [
        ("evt_read", "api_request_units", "read", "10"),
        ("evt_write", "api_request_units", "write", "20"),
    ] {
        engine
            .ingest_stream_record(
                manifest,
                "metric_points_v1",
                event_id.as_bytes(),
                StorageStreamRecord {
                    sequence_number: event_id.to_string(),
                    keys: HashMap::new(),
                    old_image: None,
                    new_image: Some(metric_point_item_for_series(
                        event_id,
                        series_name,
                        operation,
                        value,
                    )),
                },
            )
            .expect("ingest metric point");
    }
    engine
        .ingest_stream_record(
            manifest,
            "metric_points_v1",
            b"evt_decimal",
            StorageStreamRecord {
                sequence_number: "evt_decimal".to_string(),
                keys: HashMap::new(),
                old_image: None,
                new_image: Some(decimal_metric_point_item()),
            },
        )
        .expect("ingest decimal metric point");
}

fn metric_point_item_for_series(
    event_id: &str,
    series_name: &str,
    operation: &str,
    value: &str,
) -> HashMap<String, StorageValue> {
    let mut item = metric_point_item(event_id, "standard", value);
    item.insert(
        "series_name".to_string(),
        StorageValue::S(series_name.to_string()),
    );
    item.insert("dim_3".to_string(), StorageValue::S(operation.to_string()));
    item
}

fn decimal_metric_point_item() -> HashMap<String, StorageValue> {
    let mut item = metric_point_item("evt_decimal", "standard", "0");
    item.insert(
        "series_name".to_string(),
        StorageValue::S("storage_gb_hours".to_string()),
    );
    item.insert("value_i64".to_string(), StorageValue::NULL(true));
    item.insert(
        "value_decimal".to_string(),
        StorageValue::N("42.5".to_string()),
    );
    item
}

fn metric_sum_query(series_name: &str) -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "metric_points_v1".to_string(),
        table_alias: Some("m".to_string()),
        joins: Vec::new(),
        select: vec![QuerySelect::Sum {
            expression: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "value_i64".to_string(),
            },
            alias: "quantity".to_string(),
        }],
        filters: metric_series_filters(series_name),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(1),
    }
}

fn metric_grouped_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "metric_points_v1".to_string(),
        table_alias: Some("m".to_string()),
        joins: Vec::new(),
        select: vec![
            QuerySelect::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_1".to_string(),
                alias: Some("region".to_string()),
            },
            QuerySelect::Sum {
                expression: QueryExpression::Column {
                    table_alias: Some("m".to_string()),
                    column_name: "value_i64".to_string(),
                },
                alias: "quantity".to_string(),
            },
        ],
        filters: metric_series_filters("api_request_units"),
        group_by: vec![QueryExpression::Column {
            table_alias: Some("m".to_string()),
            column_name: "dim_1".to_string(),
        }],
        order_by: Vec::new(),
        limit: Some(1000),
    }
}

fn metric_three_dimension_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "metric_points_v1".to_string(),
        table_alias: Some("m".to_string()),
        joins: Vec::new(),
        select: vec![
            QuerySelect::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_1".to_string(),
                alias: Some("region".to_string()),
            },
            QuerySelect::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_2".to_string(),
                alias: Some("plan".to_string()),
            },
            QuerySelect::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_3".to_string(),
                alias: Some("operation".to_string()),
            },
            QuerySelect::Sum {
                expression: QueryExpression::Column {
                    table_alias: Some("m".to_string()),
                    column_name: "value_i64".to_string(),
                },
                alias: "quantity".to_string(),
            },
        ],
        filters: metric_series_filters("api_request_units"),
        group_by: vec![
            QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_1".to_string(),
            },
            QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_2".to_string(),
            },
            QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_3".to_string(),
            },
        ],
        order_by: vec![analytics_contract::QueryOrder {
            expression: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_3".to_string(),
            },
            direction: Some(analytics_contract::SortOrder::Asc),
        }],
        limit: Some(1000),
    }
}

fn metric_distinct_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "metric_points_v1".to_string(),
        table_alias: Some("m".to_string()),
        joins: Vec::new(),
        select: vec![QuerySelect::CountDistinct {
            expression: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "dim_1".to_string(),
            },
            alias: "active_regions".to_string(),
        }],
        filters: metric_series_filters("api_request_units"),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(1),
    }
}

fn metric_decimal_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "metric_points_v1".to_string(),
        table_alias: Some("m".to_string()),
        joins: Vec::new(),
        select: vec![QuerySelect::Sum {
            expression: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "value_decimal".to_string(),
            },
            alias: "quantity".to_string(),
        }],
        filters: metric_series_filters("storage_gb_hours"),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(1),
    }
}

fn metric_evidence_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "metric_points_v1".to_string(),
        table_alias: Some("m".to_string()),
        joins: Vec::new(),
        select: vec![
            QuerySelect::Column {
                table_alias: Some("m".to_string()),
                column_name: "event_id".to_string(),
                alias: Some("event_id".to_string()),
            },
            QuerySelect::Column {
                table_alias: Some("m".to_string()),
                column_name: "occurred_at_ms".to_string(),
                alias: Some("occurred_at_ms".to_string()),
            },
            QuerySelect::Column {
                table_alias: Some("m".to_string()),
                column_name: "value_i64".to_string(),
                alias: Some("quantity".to_string()),
            },
        ],
        filters: metric_series_filters("api_request_units"),
        group_by: Vec::new(),
        order_by: vec![analytics_contract::QueryOrder {
            expression: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "event_id".to_string(),
            },
            direction: Some(analytics_contract::SortOrder::Asc),
        }],
        limit: Some(100),
    }
}

fn metric_series_filters(series_name: &str) -> Vec<QueryPredicate> {
    vec![
        QueryPredicate::Eq {
            expression: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "series_name".to_string(),
            },
            value: serde_json::json!(series_name),
        },
        QueryPredicate::Eq {
            expression: QueryExpression::Column {
                table_alias: Some("m".to_string()),
                column_name: "customer_account_id".to_string(),
            },
            value: serde_json::json!("bcus_123"),
        },
    ]
}

fn joined_metric_query() -> StructuredQuery {
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
        filters: vec![
            QueryPredicate::Eq {
                expression: QueryExpression::Column {
                    table_alias: Some("m".to_string()),
                    column_name: "series_name".to_string(),
                },
                value: serde_json::json!("ai_token_units"),
            },
            QueryPredicate::Eq {
                expression: QueryExpression::Column {
                    table_alias: Some("m".to_string()),
                    column_name: "customer_account_id".to_string(),
                },
                value: serde_json::json!("bcus_123"),
            },
        ],
        group_by: vec![QueryExpression::Column {
            table_alias: Some("r".to_string()),
            column_name: "rate_class".to_string(),
        }],
        order_by: vec![analytics_contract::QueryOrder {
            expression: QueryExpression::Column {
                table_alias: Some("r".to_string()),
                column_name: "rate_class".to_string(),
            },
            direction: Some(analytics_contract::SortOrder::Asc),
        }],
        limit: Some(1000),
    }
}
