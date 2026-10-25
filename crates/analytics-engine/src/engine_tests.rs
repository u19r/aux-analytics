use std::{
    collections::BTreeMap,
    env,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use analytics_contract::{
    AnalyticsColumn, AnalyticsColumnType, AnalyticsManifest, ClusteringKey, PrimitiveColumnType,
    PrivacyPolicy, ProjectionColumn, QueryColumnComparison, QueryComparisonOperator,
    QueryConditionalBranch, QueryExpression, QueryJoin, QueryJoinKind, QueryJoinPredicate,
    QueryOrder, QueryPredicate, QuerySelect, RetentionPolicy, RetentionTimestamp, RowExpansion,
    RowIdentity, RowPathSegment, RowProjectionField, SortOrder, SourcePosition, StorageItem,
    StorageStreamRecord, StorageValue, StructuredQuery, TableRegistration, TenantRangePurgeRequest,
    TenantSelector,
};
use analytics_fixtures::{
    generic_items_manifest, legacy_item, metric_points_manifest, storage_key, user_item,
    users_manifest,
};
use config::{CatalogType, StorageBackend};
use storage_types::VecMap;

use super::{
    AnalyticsEngine, AnalyticsEngineError, IngestOutcome, MAX_QUERY_RESULT_BYTES,
    MaintenanceEffectApplyRequest, MaintenanceEffectAuthority, MaintenanceEffectDisposition,
    MaintenanceEffectVerifyRequest, PrivacyTableRemediationMode, RecordCheckpoint,
    SourceCheckpoint, StreamRecordBatchItem, TenantRangePurgeEffectDisposition,
    TenantRangePurgeEffectRequest,
};
use crate::{
    constants::{JSON_ARRAY_FRAMING_BYTES, MAX_QUERY_RESULT_ROWS},
    engine::ducklake_schema_convergence_message,
};

fn record_checkpoint(source_table_name: &str, shard_id: &str, position: &str) -> RecordCheckpoint {
    RecordCheckpoint::try_new(source_table_name, shard_id, position)
        .expect("test checkpoint must be a record checkpoint")
}

#[test]
fn given_ducklake_table_creation_conflicts_when_message_is_classified_then_schema_convergence_is_recognized()
 {
    assert!(ducklake_schema_convergence_message(
        "TransactionContext Error: Failed to commit DuckLake transaction. Transaction conflict - \
         invalid mutation: conflict creating table analytics_checkpoints: name already exists in \
         schema 0"
    ));
    assert!(ducklake_schema_convergence_message(
        "DuckLake transaction failed because the table has been created by another transaction \
         already"
    ));
}

#[test]
fn given_permission_or_unrelated_conflict_messages_when_classified_then_schema_convergence_is_rejected()
 {
    assert!(!ducklake_schema_convergence_message(
        "DuckLake transaction failed: permission denied"
    ));
    assert!(!ducklake_schema_convergence_message(
        "Transaction conflict - invalid mutation: name already exists"
    ));
}

#[test]
fn given_record_and_condition_errors_when_failure_is_classified_then_only_record_contract_errors_are_marked()
 {
    for error in [
        AnalyticsEngineError::MissingTenant,
        AnalyticsEngineError::MissingAttribute("tenant_id".to_string()),
        AnalyticsEngineError::RegexNoMatch {
            attribute_name: "tenant_id".to_string(),
        },
        AnalyticsEngineError::MissingIdentifier("tenant_id"),
        AnalyticsEngineError::AttributeConversion("tenant_id".to_string()),
        AnalyticsEngineError::InvalidRetentionTimestamp("created_at".to_string()),
    ] {
        assert!(error.is_record_contract_failure(), "{error}");
    }
    assert!(
        !AnalyticsEngineError::InvalidConditionExpression("invalid".to_string())
            .is_record_contract_failure()
    );
}

#[test]
fn given_unregistered_emitted_table_when_ingest_contract_is_validated_then_table_not_registered_is_returned()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(Vec::new());

    let error = engine
        .validate_ingest_contract(
            &manifest,
            "metric_points_v1",
            b"metric-1",
            insert_record(VecMap::new()),
        )
        .expect_err("emitted table must be registered");

    assert!(matches!(
        error,
        AnalyticsEngineError::TableNotRegistered(name) if name == "metric_points_v1"
    ));
}

#[test]
fn given_item_outside_registration_condition_when_ingest_contract_is_validated_then_record_mismatch_is_returned()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = generic_items_manifest().tables[0].clone();
    table.analytics_table_name = "sys_tenants".to_string();
    table.condition_expression = Some("#entity_type = :entity_type".to_string());
    table.expression_attribute_names = Some(VecMap::from_iter([(
        "#entity_type".to_string(),
        "entity_type".to_string(),
    )]));
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":entity_type".to_string(),
        StorageValue::S("TENANT_META".to_string()),
    )]));
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let error = engine
        .validate_ingest_contract(
            &manifest,
            "sys_tenants",
            b"tenant-1",
            insert_record(VecMap::from_iter([
                (
                    "entity_type".to_string(),
                    StorageValue::S("SYSTEM_TENANT".to_string()),
                ),
                (
                    "tenant_id".to_string(),
                    StorageValue::S("tenant-1".to_string()),
                ),
            ])),
        )
        .expect_err("serialized entity must match its registration");

    assert!(matches!(
        error,
        AnalyticsEngineError::RecordDoesNotMatchRegistration(name) if name == "sys_tenants"
    ));
}

#[test]
fn given_numeric_tenant_attribute_when_ingest_contract_is_validated_then_required_string_attribute_error_is_returned()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = generic_items_manifest().tables[0].clone();
    table.analytics_table_name = "sys_tenants".to_string();
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::Attribute {
        attribute_name: "tenant_id".to_string(),
    };
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let error = engine
        .validate_ingest_contract(
            &manifest,
            "sys_tenants",
            b"tenant-1",
            insert_record(VecMap::from_iter([(
                "tenant_id".to_string(),
                StorageValue::N("1".to_string()),
            )])),
        )
        .expect_err("tenant_id must use the registered string representation");

    assert!(matches!(
        error,
        AnalyticsEngineError::MissingAttribute(name) if name == "tenant_id"
    ));
}

#[test]
fn given_matching_serialized_item_when_ingest_contract_is_validated_then_destination_remains_empty()
{
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = generic_items_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    engine
        .validate_ingest_contract(
            &manifest,
            "legacy_items",
            b"item-1",
            insert_record(legacy_item("item-1", "value")),
        )
        .expect("matching contract");
    let rows = engine
        .query_unscoped_sql_json("select count(*) as row_count from legacy_items")
        .expect("query validation destination");
    assert_eq!(rows[0]["row_count"], 0);
}

#[test]
fn given_nested_email_projection_when_ingesting_record_then_tenant_and_email_are_queryable() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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
        keys: VecMap::new(),
        old_image: None,
        new_image: Some(VecMap::from_iter([(
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([(
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
fn given_reserved_position_output_shapes_when_ingesting_direct_and_batch_then_each_record_is_inserted_once()
 {
    for (shape, suffix) in [
        (0_u8, "document"),
        (1, "projection"),
        (2, "explicit"),
        (3, "projection_attributes"),
    ] {
        let table_name = format!("users_source_position_{suffix}");
        let manifest = source_position_output_manifest(table_name.as_str(), shape);
        let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
        engine.ensure_manifest(&manifest).expect("ensure manifest");

        assert_eq!(
            engine
                .ingest_stream_record(
                    &manifest,
                    table_name.as_str(),
                    b"direct-1",
                    email_record("direct-1", "direct@example.test"),
                )
                .expect("direct ingest"),
            IngestOutcome::Inserted,
            "{suffix} direct path",
        );
        let outcomes = engine
            .ingest_stream_record_batch(
                &manifest,
                vec![StreamRecordBatchItem {
                    analytics_table_name: table_name.clone(),
                    source_table_name: "tenant_01".to_string(),
                    shard_id: None,
                    system_position: None,
                    record_key: b"batch-1".to_vec(),
                    record: email_record("batch-1", "batch@example.test"),
                }],
            )
            .expect("batch ingest");
        assert_eq!(
            outcomes,
            vec![IngestOutcome::Inserted],
            "{suffix} batch path"
        );

        let rows = engine
            .query_unscoped_sql_json(
                format!(
                    "SELECT count(*) AS row_count FROM {}",
                    crate::sql::quote_identifier(table_name.as_str())
                )
                .as_str(),
            )
            .expect("query ingested rows");
        assert_eq!(rows, vec![serde_json::json!({"row_count": 2})], "{suffix}");
    }
}

fn source_position_output_manifest(table_name: &str, shape: u8) -> AnalyticsManifest {
    let mut table = users_manifest()
        .tables
        .into_iter()
        .next()
        .expect("users fixture contains a table");
    table.analytics_table_name = table_name.to_string();
    match shape {
        0 => table.document_column = Some("__source_position".to_string()),
        1 => {
            table.document_column = None;
            table.projection_columns = Some(vec![ProjectionColumn {
                column_name: "__source_position".to_string(),
                attribute_path: "profile.email".to_string(),
                column_type: Some(AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::Json,
                }),
            }]);
        }
        2 => {
            table.document_column = None;
            table.projection_columns = None;
            table.columns = vec![AnalyticsColumn {
                column_name: "__source_position".to_string(),
                column_type: AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::Json,
                },
            }];
        }
        3 => {
            table.document_column = None;
            table.projection_columns = None;
            table.projection_attribute_names =
                Some(vec!["__source_position".to_string(), "email".to_string()]);
        }
        _ => panic!("unknown source-position output shape"),
    }
    AnalyticsManifest::new(vec![table])
}

#[test]
fn given_existing_tenant_row_when_record_changes_tenant_then_only_new_tenant_row_remains() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = users_manifest().tables[0].clone();
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::Attribute {
        attribute_name: "tenant_id".to_string(),
    };
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let old_image = VecMap::from_iter([
        (
            "tenant_id".to_string(),
            StorageValue::S("tenant-a".to_string()),
        ),
        ("user_id".to_string(), StorageValue::S("user-1".to_string())),
        (
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([(
                "email".to_string(),
                StorageValue::S("a@example.com".to_string()),
            )])),
        ),
    ]);
    let new_image = VecMap::from_iter([
        (
            "tenant_id".to_string(),
            StorageValue::S("tenant-b".to_string()),
        ),
        ("user_id".to_string(), StorageValue::S("user-1".to_string())),
        (
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([(
                "email".to_string(),
                StorageValue::S("b@example.com".to_string()),
            )])),
        ),
    ]);
    engine
        .ingest_stream_record(
            &manifest,
            "users",
            b"user-1",
            StorageStreamRecord {
                sequence_number: "1".to_string(),
                keys: VecMap::new(),
                old_image: None,
                new_image: Some(old_image.clone()),
            },
        )
        .expect("insert initial tenant row");
    assert_eq!(
        engine
            .ingest_stream_record(
                &manifest,
                "users",
                b"user-1",
                StorageStreamRecord {
                    sequence_number: "2".to_string(),
                    keys: VecMap::new(),
                    old_image: Some(old_image),
                    new_image: Some(new_image),
                },
            )
            .expect("relocate tenant row"),
        IngestOutcome::Updated
    );

    let rows = engine
        .query_unscoped_sql_json("select tenant_id, email from users order by tenant_id")
        .expect("query relocated row");
    assert_eq!(
        rows,
        vec![serde_json::json!({
            "tenant_id": "tenant-b",
            "email": "b@example.com"
        })]
    );
}

fn insert_record(new_image: StorageItem) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: VecMap::new(),
        sequence_number: "1".to_string(),
        old_image: None,
        new_image: Some(new_image),
    }
}

#[test]
fn given_epoch_millisecond_projection_when_inserting_and_updating_then_timestamp_round_trips() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "sessions".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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
fn given_registered_table_when_adding_projection_column_then_new_column_is_queryable() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let existing_manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "metric_events_raw".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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
        row_expansion: None,
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
fn given_wide_registration_when_ingesting_two_records_then_staging_preserves_exact_rows() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut manifest = users_manifest();
    let table = &mut manifest.tables[0];
    table.condition_expression = Some(
        (0..8)
            .map(|index| format!("#profile_{index}.email <> :excluded_{index}"))
            .collect::<Vec<_>>()
            .join(" AND "),
    );
    table.expression_attribute_names =
        Some(VecMap::from_iter((0..8).map(|index| {
            (format!("#profile_{index}"), "profile".to_string())
        })));
    table.expression_attribute_values = Some(VecMap::from_iter((0..8).map(|index| {
        (
            format!(":excluded_{index}"),
            StorageValue::S("x".repeat(4096)),
        )
    })));
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let outcomes = engine
        .ingest_stream_record_batch(
            &manifest,
            vec![
                StreamRecordBatchItem {
                    analytics_table_name: "users".to_string(),
                    source_table_name: "source_users".to_string(),
                    shard_id: None,
                    system_position: None,
                    record_key: b"user-1".to_vec(),
                    record: email_record("1", "a@example.test"),
                },
                StreamRecordBatchItem {
                    analytics_table_name: "users".to_string(),
                    source_table_name: "source_users".to_string(),
                    shard_id: None,
                    system_position: None,
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
        .query_unscoped_sql_json("select email, item from users order by email")
        .expect("query");
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"email": "a@example.test", "item": {"user_id": "1", "profile": {"email": "a@example.test"}, "organization": {"id": "org-a"}}}),
            serde_json::json!({"email": "b@example.test", "item": {"user_id": "2", "profile": {"email": "b@example.test"}, "organization": {"id": "org-a"}}}),
        ]
    );
}

#[test]
fn given_existing_row_when_batch_updates_then_row_is_replaced() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    engine
        .ingest_stream_record(
            &manifest,
            "users",
            b"user-1",
            email_record("1", "before@example.test"),
        )
        .expect("initial ingest");

    let outcomes = engine
        .ingest_stream_record_batch(
            &manifest,
            vec![StreamRecordBatchItem {
                analytics_table_name: "users".to_string(),
                source_table_name: "tenant_01".to_string(),
                shard_id: None,
                system_position: None,
                record_key: b"user-1".to_vec(),
                record: StorageStreamRecord {
                    sequence_number: "2".to_string(),
                    keys: VecMap::new(),
                    old_image: Some(user_item("1", "before@example.test", "org-a")),
                    new_image: Some(user_item("1", "after@example.test", "org-a")),
                },
            }],
        )
        .expect("batch update");

    assert_eq!(outcomes, vec![IngestOutcome::Updated]);
    let rows = engine
        .query_unscoped_sql_json("select email from users")
        .expect("query");
    assert_eq!(
        rows,
        vec![serde_json::json!({"email": "after@example.test"})]
    );
}

#[test]
fn given_repeated_identity_when_batch_ends_with_delete_then_row_is_absent() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let updated = user_item("1", "after@example.test", "org-a");

    engine
        .ingest_stream_record_batch(
            &manifest,
            vec![
                StreamRecordBatchItem {
                    analytics_table_name: "users".to_string(),
                    source_table_name: "tenant_01".to_string(),
                    shard_id: None,
                    system_position: None,
                    record_key: b"user-1".to_vec(),
                    record: email_record("1", "before@example.test"),
                },
                StreamRecordBatchItem {
                    analytics_table_name: "users".to_string(),
                    source_table_name: "tenant_01".to_string(),
                    shard_id: None,
                    system_position: None,
                    record_key: b"user-1".to_vec(),
                    record: StorageStreamRecord {
                        sequence_number: "2".to_string(),
                        keys: VecMap::new(),
                        old_image: Some(user_item("1", "before@example.test", "org-a")),
                        new_image: Some(updated.clone()),
                    },
                },
                StreamRecordBatchItem {
                    analytics_table_name: "users".to_string(),
                    source_table_name: "tenant_01".to_string(),
                    shard_id: None,
                    system_position: None,
                    record_key: b"user-1".to_vec(),
                    record: StorageStreamRecord {
                        sequence_number: "3".to_string(),
                        keys: VecMap::new(),
                        old_image: Some(updated),
                        new_image: None,
                    },
                },
            ],
        )
        .expect("batch mutations");

    let rows = engine
        .query_unscoped_sql_json("select count(*) as row_count from users")
        .expect("query");
    assert_eq!(rows[0]["row_count"], 0);
}

#[test]
fn given_source_table_tenant_selector_when_ingesting_batch_then_actual_source_name_sets_tenant_and_table()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = users_manifest().tables.remove(0);
    table.source_table_name = "tenant_".to_string();
    table.source_table_name_prefix = Some("tenant_".to_string());
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::TableName;
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    engine
        .ingest_stream_record_batch(
            &manifest,
            vec![StreamRecordBatchItem {
                analytics_table_name: "users".to_string(),
                source_table_name: "tenant_42".to_string(),
                shard_id: None,
                system_position: None,
                record_key: b"user-1".to_vec(),
                record: email_record("1", "a@example.test"),
            }],
        )
        .expect("batch ingest");

    let rows = engine
        .query_unscoped_sql_json("select tenant_id, table_name from users")
        .expect("query");
    assert_eq!(rows[0]["tenant_id"], "tenant_42");
    assert_eq!(rows[0]["table_name"], "tenant_42");
}

#[test]
fn given_two_events_in_one_record_when_ingesting_expansion_then_both_quantities_and_ordinals_are_queryable()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = expanded_metric_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let outcome = engine
        .ingest_stream_record_batch(
            &manifest,
            vec![expanded_metric_batch_item(
                "batch-1",
                expanded_metric_item(&[
                    ("event-1", "api_requests", "10"),
                    ("event-2", "api_requests", "20"),
                ]),
            )],
        )
        .expect("ingest expanded metric batch");

    assert_eq!(outcome, vec![IngestOutcome::Inserted]);
    let rows = engine
        .query_unscoped_sql_json(
            "select event_id, value_i64, wal_batch_id, wal_row_ordinal from \
             metric_points_expanded order by wal_row_ordinal",
        )
        .expect("query expanded rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["event_id"], "event-1");
    assert_eq!(rows[0]["value_i64"], 10);
    assert_eq!(rows[0]["wal_batch_id"], "batch-1");
    assert_eq!(rows[0]["wal_row_ordinal"], 0);
    assert_eq!(rows[1]["event_id"], "event-2");
    assert_eq!(rows[1]["value_i64"], 20);
    assert_eq!(rows[1]["wal_row_ordinal"], 1);
}

#[test]
fn given_expanded_typed_null_and_retention_fields_when_ingesting_direct_and_batch_then_rows_match()
{
    let mut manifest = expanded_metric_manifest();
    let table = manifest.tables.first_mut().expect("expanded table");
    table.retention = Some(RetentionPolicy {
        period_ms: 1_000,
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: "created_at_ms".to_string(),
        },
    });
    table
        .projection_columns
        .as_mut()
        .expect("expanded projections")
        .push(varchar_projection("nullable"));
    table
        .row_expansion
        .as_mut()
        .expect("row expansion")
        .source_fields
        .push(row_field(
            "created_at_ms",
            vec![row_attribute("created_at_ms")],
        ));
    table
        .row_expansion
        .as_mut()
        .expect("row expansion")
        .element_fields
        .push(row_field("nullable", vec![row_index(3)]));

    let mut item = expanded_metric_item(&[("event-1", "api_requests", "10")]);
    item.insert(
        "created_at_ms".to_string(),
        StorageValue::N("1000".to_string()),
    );
    if let Some(StorageValue::L(levels)) = item.get_mut("p")
        && let Some(StorageValue::L(points)) = levels.get_mut(1)
        && let Some(StorageValue::L(point)) = points.first_mut()
    {
        point.push(StorageValue::NULL(true));
    }

    let direct_engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect direct");
    direct_engine
        .ensure_manifest(&manifest)
        .expect("ensure direct manifest");
    let direct_outcome = direct_engine
        .ingest_stream_record(
            &manifest,
            "metric_points_expanded",
            b"batch-typed",
            StorageStreamRecord {
                keys: VecMap::new(),
                sequence_number: "batch-typed".to_string(),
                old_image: None,
                new_image: Some(item.clone()),
            },
        )
        .expect("direct expanded ingest");

    let batch_engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect batch");
    batch_engine
        .ensure_manifest(&manifest)
        .expect("ensure batch manifest");
    let batch_outcome = batch_engine
        .ingest_stream_record_batch(
            &manifest,
            vec![expanded_metric_batch_item("batch-typed", item)],
        )
        .expect("batch expanded ingest");

    assert_eq!(direct_outcome, IngestOutcome::Inserted);
    assert_eq!(batch_outcome, vec![IngestOutcome::Inserted]);
    let query = "select event_id, nullable, __expiry, __missing_retention, tenant_id from \
                 metric_points_expanded";
    let direct_rows = direct_engine
        .query_unscoped_sql_json(query)
        .expect("direct query");
    let batch_rows = batch_engine
        .query_unscoped_sql_json(query)
        .expect("batch query");
    assert_eq!(direct_rows, batch_rows);
    assert_eq!(
        direct_rows,
        vec![serde_json::json!({
            "event_id": "event-1",
            "nullable": null,
            "__expiry": 2000,
            "__missing_retention": false,
            "tenant_id": "tenant-a"
        })]
    );
}

#[test]
fn given_duplicate_expanded_identity_when_ingesting_then_validation_rejects_without_rows() {
    let mut manifest = expanded_metric_manifest();
    manifest.tables[0].row_identity = RowIdentity::Attribute {
        attribute_name: "event_id".to_string(),
    };
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let error = engine
        .ingest_stream_record_batch(
            &manifest,
            vec![expanded_metric_batch_item(
                "batch-duplicate",
                expanded_metric_item(&[
                    ("event-1", "api_requests", "10"),
                    ("event-1", "api_requests", "20"),
                ]),
            )],
        )
        .expect_err("duplicate expanded identity must fail");
    assert!(matches!(
        error,
        AnalyticsEngineError::DuplicateExpandedRowIdentity
    ));
    let rows = engine
        .query_unscoped_sql_json("select count(*) as row_count from metric_points_expanded")
        .expect("query rows after rejected expansion");
    assert_eq!(rows[0]["row_count"], 0);
}

#[test]
fn given_expanded_numeric_columns_when_ingesting_edge_values_then_queries_preserve_numeric_ranges()
{
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut manifest = expanded_metric_manifest();
    let table = manifest.tables.first_mut().expect("expanded table");
    table
        .projection_columns
        .as_mut()
        .expect("expanded projections")
        .push(unsigned_bigint_projection("value_u64"));
    table
        .projection_columns
        .as_mut()
        .expect("expanded projections")
        .push(double_projection("value_f64"));
    table
        .projection_columns
        .as_mut()
        .expect("expanded projections")
        .push(decimal_projection("value_decimal"));
    table
        .row_expansion
        .as_mut()
        .expect("row expansion")
        .element_fields
        .push(row_field("value_u64", vec![row_index(3)]));
    table
        .row_expansion
        .as_mut()
        .expect("row expansion")
        .element_fields
        .push(row_field("value_f64", vec![row_index(4)]));
    table
        .row_expansion
        .as_mut()
        .expect("row expansion")
        .element_fields
        .push(row_field("value_decimal", vec![row_index(5)]));
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let mut item = expanded_metric_item(&[("event-1", "api_requests", "10")]);
    item.insert(
        "p".to_string(),
        StorageValue::L(vec![
            StorageValue::N("2".to_string()),
            StorageValue::L(vec![StorageValue::L(vec![
                StorageValue::S("api_requests".to_string()),
                StorageValue::S("event-1".to_string()),
                StorageValue::N("10".to_string()),
                StorageValue::N(u64::MAX.to_string()),
                StorageValue::N(f64::MAX.to_string()),
                StorageValue::N("1250.250000000000000001".to_string()),
            ])]),
        ]),
    );

    engine
        .ingest_stream_record_batch(
            &manifest,
            vec![expanded_metric_batch_item("batch-unsigned", item)],
        )
        .expect("ingest unsigned metric");

    let rows = engine
        .query_unscoped_sql_json(
            "select value_u64, value_f64, cast(value_decimal as varchar) as value_decimal from \
             metric_points_expanded",
        )
        .expect("query numeric values");
    assert_eq!(
        rows,
        vec![serde_json::json!({
            "value_u64": u64::MAX,
            "value_f64": f64::MAX,
            "value_decimal": "1250.250000000000000001"
        })]
    );
}

#[test]
fn given_expanded_entity_filter_when_ingesting_unrelated_entity_then_no_rows_are_written() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = expanded_metric_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let mut unrelated = expanded_metric_item(&[("event-1", "api_requests", "10")]);
    unrelated.insert("et".to_string(), StorageValue::S("USER".to_string()));

    let outcome = engine
        .ingest_stream_record_batch(
            &manifest,
            vec![expanded_metric_batch_item("user-1", unrelated)],
        )
        .expect("skip unrelated entity");

    assert_eq!(outcome, vec![IngestOutcome::Skipped]);
    let rows = engine
        .query_unscoped_sql_json("select count(*) as row_count from metric_points_expanded")
        .expect("query rows");
    assert_eq!(rows[0]["row_count"], 0);
}

fn expanded_metric_manifest() -> AnalyticsManifest {
    let mut table = filtered_entity_table("nsystem", "metric_points_expanded", "METRIC_WAL_BATCH");
    table.tenant_selector = TenantSelector::Attribute {
        attribute_name: "tenant_id".to_string(),
    };
    table.row_identity = RowIdentity::Attributes {
        attribute_names: vec!["wal_batch_id".to_string(), "wal_row_ordinal".to_string()],
    };
    table.document_column = None;
    table.skip_delete = true;
    table.projection_attribute_names = None;
    table.projection_columns = Some(vec![
        varchar_projection("event_id"),
        varchar_projection("series_name"),
        bigint_projection("value_i64"),
        varchar_projection("wal_batch_id"),
        bigint_projection("wal_row_ordinal"),
    ]);
    table.row_expansion = Some(RowExpansion {
        list_path: vec![row_attribute("p"), row_index(1)],
        source_fields: vec![
            row_field("tenant_id", vec![row_attribute("tenant_id")]),
            row_field("wal_batch_id", vec![row_attribute("b_id")]),
        ],
        element_fields: vec![
            row_field("series_name", vec![row_index(0)]),
            row_field("event_id", vec![row_index(1)]),
            row_field("value_i64", vec![row_index(2)]),
        ],
        ordinal_attribute_name: Some("wal_row_ordinal".to_string()),
    });
    AnalyticsManifest::new(vec![table])
}

fn expanded_metric_item(points: &[(&str, &str, &str)]) -> StorageItem {
    VecMap::from_iter([
        (
            "et".to_string(),
            StorageValue::S("METRIC_WAL_BATCH".to_string()),
        ),
        (
            "tenant_id".to_string(),
            StorageValue::S("tenant-a".to_string()),
        ),
        ("b_id".to_string(), StorageValue::S("batch-1".to_string())),
        (
            "p".to_string(),
            StorageValue::L(vec![
                StorageValue::N("2".to_string()),
                StorageValue::L(
                    points
                        .iter()
                        .map(|(event_id, series_name, value)| {
                            StorageValue::L(vec![
                                StorageValue::S((*series_name).to_string()),
                                StorageValue::S((*event_id).to_string()),
                                StorageValue::N((*value).to_string()),
                            ])
                        })
                        .collect(),
                ),
            ]),
        ),
    ])
}

fn expanded_metric_batch_item(sequence_number: &str, item: StorageItem) -> StreamRecordBatchItem {
    StreamRecordBatchItem {
        analytics_table_name: "metric_points_expanded".to_string(),
        source_table_name: "nsystem".to_string(),
        shard_id: None,
        system_position: None,
        record_key: sequence_number.as_bytes().to_vec(),
        record: StorageStreamRecord {
            keys: VecMap::new(),
            sequence_number: sequence_number.to_string(),
            old_image: None,
            new_image: Some(item),
        },
    }
}

fn row_field(output_attribute_name: &str, path: Vec<RowPathSegment>) -> RowProjectionField {
    RowProjectionField {
        output_attribute_name: output_attribute_name.to_string(),
        path,
    }
}

fn row_attribute(name: &str) -> RowPathSegment {
    RowPathSegment::Attribute {
        name: name.to_string(),
    }
}

const fn row_index(index: usize) -> RowPathSegment {
    RowPathSegment::Index { index }
}

fn varchar_projection(attribute_path: &str) -> ProjectionColumn {
    ProjectionColumn {
        column_name: attribute_path.to_string(),
        attribute_path: attribute_path.to_string(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::VarChar,
        }),
    }
}

fn bigint_projection(attribute_path: &str) -> ProjectionColumn {
    ProjectionColumn {
        column_name: attribute_path.to_string(),
        attribute_path: attribute_path.to_string(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::BigInt,
        }),
    }
}

fn unsigned_bigint_projection(attribute_path: &str) -> ProjectionColumn {
    ProjectionColumn {
        column_name: attribute_path.to_string(),
        attribute_path: attribute_path.to_string(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::UnsignedBigInt,
        }),
    }
}

fn decimal_projection(attribute_path: &str) -> ProjectionColumn {
    ProjectionColumn {
        column_name: attribute_path.to_string(),
        attribute_path: attribute_path.to_string(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::Decimal,
        }),
    }
}

fn double_projection(attribute_path: &str) -> ProjectionColumn {
    ProjectionColumn {
        column_name: attribute_path.to_string(),
        attribute_path: attribute_path.to_string(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::Double,
        }),
    }
}

fn email_record(sequence_number: &str, email: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: VecMap::new(),
        old_image: None,
        new_image: Some(user_item(sequence_number, email, "org-a")),
    }
}

#[test]
fn given_denied_email_policy_when_ingesting_projection_then_drop_is_reported_and_email_is_null() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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
                keys: VecMap::new(),
                old_image: None,
                new_image: Some(VecMap::from_iter([(
                    "profile".to_string(),
                    StorageValue::M(VecMap::from_iter([(
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
fn given_user_and_order_filters_when_ingesting_shared_source_then_entities_route_to_matching_tables()
 {
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
fn given_matching_entity_row_when_update_leaves_filter_then_row_is_deleted() {
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
                keys: VecMap::new(),
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
                keys: VecMap::new(),
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
fn given_record_checkpoint_when_saving_to_duckdb_then_loading_returns_same_checkpoint() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let checkpoint = record_checkpoint("tenant_entities", "shard-0001", "12345");
    engine
        .save_source_checkpoints(std::slice::from_ref(&checkpoint))
        .expect("save checkpoint");

    let checkpoints = engine.load_source_checkpoints().expect("load checkpoints");

    assert_eq!(checkpoints, vec![checkpoint]);
}

#[test]
fn given_iterator_checkpoint_when_saving_then_validation_rejects_without_runtime_tables() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let iterator_checkpoint = SourceCheckpoint {
        source_table_name: "tenant_entities".to_string(),
        shard_id: "__iterator:shard-0001".to_string(),
        position: "iterator-token".to_string(),
    };

    let error = engine
        .save_source_checkpoints_from_source(&[iterator_checkpoint])
        .expect_err("iterator progress is not durable record state");

    assert!(matches!(
        error,
        AnalyticsEngineError::IteratorCheckpointNotDurable
    ));
    let runtime_tables = engine
        .query_unscoped_sql_json(
            "select table_name from information_schema.tables
             where table_name = 'analytics_checkpoints'",
        )
        .expect("query runtime tables");
    assert!(runtime_tables.is_empty());
}

#[test]
fn given_saved_ducklake_checkpoint_when_reopening_catalog_then_checkpoint_is_preserved() {
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
    let checkpoint = record_checkpoint("s00000", "aux-storage", "12345");
    engine
        .save_source_checkpoints(std::slice::from_ref(&checkpoint))
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

    assert_eq!(checkpoints, vec![checkpoint]);
}

#[test]
#[ignore = "requires configured live AuxCatalog service"]
fn given_ambiguous_fdb_commit_when_later_join_row_ingests_then_reopen_queries_both_once() {
    assert!(
        std::env::var_os("AUX_ANALYTICS_DUCKLAKE_FDB_LIVE").is_some(),
        "configure AUX_ANALYTICS_DUCKLAKE_FDB_LIVE before explicitly running this live-service \
         test"
    );

    let tempdir = tempfile::tempdir().expect("tempdir");
    let catalog_path = tempdir.path().join("catalog.aux");
    let data_path = tempdir.path().join("ducklake-data");
    std::fs::create_dir_all(&data_path).expect("create ducklake data dir");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::AuxCatalog,
        catalog_path: catalog_path.to_string_lossy().to_string(),
        data_path: data_path.to_string_lossy().to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    let manifest = AnalyticsManifest::new(vec![
        filtered_entity_table("source_items", "memberships", "MEMBERSHIP"),
        filtered_entity_table("source_items", "users", "USER"),
    ]);
    let engine = AnalyticsEngine::connect(&backend).expect("connect aux catalog");
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let receipt = std::env::var_os("AUX_DUCKLAKE_TEST_FDB_COMMIT_UNKNOWN_RECEIPT")
        .expect("configure the commit ambiguity consumption receipt");
    std::fs::write(&receipt, "armed").expect("arm commit ambiguity after schema setup");
    let membership = entity_record("entity-1", "MEMBERSHIP", "member@example.test");
    let user = entity_record("entity-1", "USER", "user@example.test");
    assert_eq!(
        engine
            .ingest_stream_record(&manifest, "memberships", b"membership-1", membership)
            .expect("ingest membership through ambiguous commit"),
        IngestOutcome::Inserted
    );
    std::fs::read_to_string(&receipt)
        .expect("read commit ambiguity consumption receipt")
        .strip_prefix("after:")
        .expect("ambiguity must be injected after the durable commit")
        .parse::<u128>()
        .expect("membership ingest must consume an ambiguous commit attempt");
    assert_eq!(
        engine
            .ingest_stream_record(&manifest, "users", b"user-1", user)
            .expect("ingest later user"),
        IngestOutcome::Inserted
    );
    let checkpoint = record_checkpoint("source_items", "shard-1", "2");
    engine
        .save_source_checkpoints(std::slice::from_ref(&checkpoint))
        .expect("save source checkpoint");
    drop(engine);

    let reopened = AnalyticsEngine::connect(&backend).expect("reopen aux catalog");
    let rows = reopened
        .query_unscoped_sql_json(
            "select count(*) as joined_count from memberships m inner join users u on m.entity_id \
             = u.entity_id",
        )
        .expect("query joined rows");
    assert_eq!(rows[0]["joined_count"], 1);
    assert_eq!(
        reopened
            .load_source_checkpoints()
            .expect("load source checkpoints")[0]
            .position(),
        "2"
    );
}

#[test]
fn given_saved_checkpoint_when_saving_new_position_then_loading_returns_only_replacement() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut checkpoint = record_checkpoint("tenant_entities", "shard-0001", "12345");
    engine
        .save_source_checkpoints(std::slice::from_ref(&checkpoint))
        .expect("save initial checkpoint");

    checkpoint = record_checkpoint("tenant_entities", "shard-0001", "12346");
    engine
        .save_source_checkpoints(std::slice::from_ref(&checkpoint))
        .expect("replace checkpoint");

    let checkpoints = engine.load_source_checkpoints().expect("load checkpoints");

    assert_eq!(checkpoints, vec![checkpoint]);
}

#[test]
fn given_drop_table_statement_when_querying_read_only_engine_then_invalid_query_is_returned() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");

    let err = engine
        .query_unscoped_sql_json("drop table users")
        .expect_err("mutating query should fail");

    assert!(matches!(err, super::AnalyticsEngineError::InvalidQuery));
}

#[test]
fn given_sql_comments_quotes_and_separators_when_querying_then_single_reads_pass_and_multiple_statements_reject()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");

    for sql in [
        "select 1 as ready;",
        "select 1 as ready;   ",
        "select 1 as ready;\n",
        "select 1 as ready; -- trailing",
        "-- leading comment ;\nselect 1 as ready; -- trailing",
        "select 1 as ready -- trailing",
        "select 1 /* ; comment */ as ready",
    ] {
        let rows = engine
            .query_unscoped_sql_json(sql)
            .unwrap_or_else(|error| panic!("accepted read-only query failed: {sql:?}: {error}"));
        assert_eq!(rows[0]["ready"], 1, "query result for {sql:?}");
    }

    for sql in [
        "select 'semi;colon' as value",
        "select $$semi;colon$$ as value",
        "select $tag$semi;colon$tag$ as value",
    ] {
        let rows = engine
            .query_unscoped_sql_json(sql)
            .unwrap_or_else(|error| panic!("quoted query failed: {sql:?}: {error}"));
        assert_eq!(rows[0]["value"], "semi;colon", "query result for {sql:?}");
    }

    for sql in [
        "select 1 as ready;;",
        "select 1 as ready; select 2 as later",
        "with changed as (insert into users values (1)) select * from changed",
    ] {
        let error = engine
            .query_unscoped_sql_json(sql)
            .expect_err("invalid query should be rejected");
        assert!(
            matches!(&error, super::AnalyticsEngineError::InvalidQuery),
            "unexpected error for {sql:?}: {error}"
        );
    }

    let long_tag = "a".repeat(33);
    let error = engine
        .query_unscoped_sql_json(&format!(
            "select ${long_tag}$semi;colon${long_tag}$ as value"
        ))
        .expect_err("overlong dollar tag should be rejected");
    assert!(matches!(error, super::AnalyticsEngineError::InvalidQuery));
}

#[test]
fn given_zero_query_timeout_when_reading_then_timeout_is_returned_and_connection_remains_usable() {
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
fn given_oversized_json_row_when_materializing_query_then_size_error_is_public_safe() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let sql = format!(
        "select repeat('x', {}) as payload",
        MAX_QUERY_RESULT_BYTES.saturating_add(1)
    );

    let error = engine
        .query_unscoped_sql_json_without_timeout(sql.as_str())
        .expect_err("an oversized row must be rejected");

    assert!(matches!(
        error,
        AnalyticsEngineError::QueryResultTooLarge {
            actual_bytes,
            maximum_bytes,
        } if actual_bytes > maximum_bytes && maximum_bytes == MAX_QUERY_RESULT_BYTES
    ));
    assert!(!error.to_string().contains("payload"));
}

#[test]
fn given_oversized_aggregate_when_materializing_query_then_size_error_is_returned() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let chunk_bytes = 1024;
    let chunk_count = MAX_QUERY_RESULT_BYTES / chunk_bytes + 1;
    let sql = format!(
        "select string_agg(repeat('x', {chunk_bytes}), '') as payload from range({chunk_count})"
    );

    let error = engine
        .query_unscoped_sql_json_without_timeout(sql.as_str())
        .expect_err("an oversized aggregate must be rejected");

    assert!(matches!(
        error,
        AnalyticsEngineError::QueryResultTooLarge {
            actual_bytes,
            maximum_bytes,
        } if actual_bytes > maximum_bytes && maximum_bytes == MAX_QUERY_RESULT_BYTES
    ));
}

#[test]
fn given_array_framing_exceeds_the_budget_when_materializing_query_then_size_error_is_returned() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let row_overhead = serde_json::to_vec(&serde_json::json!({"payload": ""}))
        .expect("empty row json")
        .len();
    let payload_bytes = MAX_QUERY_RESULT_BYTES - row_overhead - 1;
    let sql = format!("select repeat('x', {payload_bytes}) as payload");

    let error = engine
        .query_unscoped_sql_json_without_timeout(sql.as_str())
        .expect_err("array framing must count against the result budget");

    assert!(matches!(
        error,
        AnalyticsEngineError::QueryResultTooLarge {
            actual_bytes,
            maximum_bytes,
        } if actual_bytes > maximum_bytes && maximum_bytes == MAX_QUERY_RESULT_BYTES
    ));
}

#[test]
fn given_array_encoding_exactly_at_the_budget_when_materializing_query_then_row_is_returned() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let row_overhead = serde_json::to_vec(&serde_json::json!({"payload": ""}))
        .expect("empty row json")
        .len();
    let payload_bytes = MAX_QUERY_RESULT_BYTES - row_overhead - JSON_ARRAY_FRAMING_BYTES;
    let sql = format!("select repeat('x', {payload_bytes}) as payload");

    let rows = engine
        .query_unscoped_sql_json_without_timeout(sql.as_str())
        .expect("exactly budgeted array must be accepted");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["payload"].as_str().map(str::len),
        Some(payload_bytes)
    );
}

#[test]
fn given_many_small_rows_when_materializing_query_then_row_budget_rejects_before_retention() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let row_count = MAX_QUERY_RESULT_ROWS + 1;
    let sql = format!("select range as row_id from range({row_count})");

    let error = engine
        .query_unscoped_sql_json_without_timeout(sql.as_str())
        .expect_err("many small rows must not be retained without a row budget");

    assert!(matches!(
        error,
        AnalyticsEngineError::QueryResultTooLarge {
            actual_bytes,
            maximum_bytes,
        } if actual_bytes > maximum_bytes && maximum_bytes == MAX_QUERY_RESULT_BYTES
    ));
}

#[test]
fn given_document_table_without_selectors_when_ingesting_then_document_is_queryable_with_empty_tenant()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "legacy_items".to_string(),
        analytics_table_name: "legacy_items".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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
        keys: VecMap::from_iter([("pk".to_string(), StorageValue::S("USER#1".to_string()))]),
        old_image: None,
        new_image: Some(VecMap::from_iter([
            ("pk".to_string(), StorageValue::S("USER#1".to_string())),
            (
                "profile".to_string(),
                StorageValue::M(VecMap::from_iter([(
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
fn given_denied_email_policy_when_ingesting_document_then_drop_is_reported_and_email_is_absent() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = generic_items_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .unwrap()
        .with_denied_key_name("email");

    let outcome = engine
        .ingest_stream_record_with_privacy_policy(
            &manifest,
            "legacy_items",
            b"ignored",
            StorageStreamRecord {
                sequence_number: "1".to_string(),
                keys: VecMap::from_iter([(
                    "pk".to_string(),
                    StorageValue::S("USER#1".to_string()),
                )]),
                old_image: None,
                new_image: Some(VecMap::from_iter([
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
fn given_denied_email_in_projection_and_document_when_dry_running_scrub_then_both_are_reported_and_preserved()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    let record = user_record("user-1", "private@example.test");
    engine
        .ingest_stream_record(&manifest, "users", b"user-1", record)
        .expect("ingest");
    let policy = PrivacyPolicy::try_new("privacy-v1")
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
fn given_denied_email_in_projection_and_document_when_applying_scrub_then_both_are_removed() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    let record = user_record("user-1", "private@example.test");
    engine
        .ingest_stream_record(&manifest, "users", b"user-1", record)
        .expect("ingest");
    let policy = PrivacyPolicy::try_new("privacy-v1")
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
fn given_projection_only_rows_with_null_and_denied_email_when_dry_running_scrub_then_only_denied_row_is_scanned()
 {
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
    let policy = PrivacyPolicy::try_new("privacy-v1")
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
fn given_row_with_denied_email_when_dry_running_privacy_delete_then_violation_is_reported_and_row_remains()
 {
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
    let policy = PrivacyPolicy::try_new("privacy-v1")
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
fn given_row_with_denied_email_when_applying_privacy_delete_then_row_is_removed() {
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
    let policy = PrivacyPolicy::try_new("privacy-v1")
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
fn given_shared_row_id_across_tenants_when_scrubbing_then_only_matching_identity_changes() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = users_manifest().tables.remove(0);
    table.document_column = Some("item".to_string());
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .connection()
        .execute(
            "insert into users (tenant_id, __id, table_name, item) values
             ('tenant-a', 'shared-id', 'source', '{\"email\":\"private@example.test\"}'),
             ('tenant-b', 'shared-id', 'source', '{\"email\":\"public@example.test\"}')",
            [],
        )
        .expect("seed rows");
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .expect("policy")
        .with_denied_value_regex("^private@example\\.test$");

    let report = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, false)
        .expect("scrub");
    let rows = engine
        .query_unscoped_sql_json("select tenant_id, item from users order by tenant_id")
        .expect("query");

    assert_eq!(report.affected_rows, 1);
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"tenant_id": "tenant-a", "item": {}}),
            serde_json::json!({"tenant_id": "tenant-b", "item": {"email": "public@example.test"}}),
        ]
    );
}

#[test]
fn given_shared_row_id_across_tenants_when_deleting_then_only_matching_identity_is_removed() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = users_manifest().tables.remove(0);
    table.document_column = Some("item".to_string());
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .connection()
        .execute(
            "insert into users (tenant_id, __id, table_name, item) values
             ('tenant-a', 'shared-id', 'source', '{\"email\":\"private@example.test\"}'),
             ('tenant-b', 'shared-id', 'source', '{\"email\":\"public@example.test\"}')",
            [],
        )
        .expect("seed rows");
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .expect("policy")
        .with_denied_value_regex("^private@example\\.test$");

    let report = engine
        .remediate_table_with_privacy_policy(
            &manifest.tables[0],
            &policy,
            false,
            PrivacyTableRemediationMode::DeleteRows,
        )
        .expect("delete");
    let rows = engine
        .query_unscoped_sql_json("select tenant_id, item from users")
        .expect("query");

    assert_eq!(report.affected_rows, 1);
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"tenant_id": "tenant-b", "item": {"email": "public@example.test"}})
        ]
    );
}

#[test]
fn given_shared_tenant_and_row_id_across_tables_when_scrubbing_then_only_matching_table_changes() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = users_manifest().tables.remove(0);
    table.document_column = Some("item".to_string());
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .connection()
        .execute(
            "insert into users (tenant_id, __id, table_name, item) values
             ('tenant-a', 'shared-id', 'source-a', '{\"email\":\"private@example.test\"}'),
             ('tenant-a', 'shared-id', 'source-b', '{\"email\":\"public@example.test\"}')",
            [],
        )
        .expect("seed rows");
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .expect("policy")
        .with_denied_value_regex("^private@example\\.test$");

    let report = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, false)
        .expect("scrub");
    let rows = engine
        .query_unscoped_sql_json("select table_name, item from users order by table_name")
        .expect("query");

    assert_eq!(report.affected_rows, 1);
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"table_name": "source-a", "item": {}}),
            serde_json::json!({"table_name": "source-b", "item": {"email": "public@example.test"}}),
        ]
    );
}

#[test]
fn given_shared_tenant_and_row_id_across_tables_when_deleting_then_only_matching_table_is_removed()
{
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = users_manifest().tables.remove(0);
    table.document_column = Some("item".to_string());
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .connection()
        .execute(
            "insert into users (tenant_id, __id, table_name, item) values
             ('tenant-a', 'shared-id', 'source-a', '{\"email\":\"private@example.test\"}'),
             ('tenant-a', 'shared-id', 'source-b', '{\"email\":\"public@example.test\"}')",
            [],
        )
        .expect("seed rows");
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .expect("policy")
        .with_denied_value_regex("^private@example\\.test$");

    let report = engine
        .remediate_table_with_privacy_policy(
            &manifest.tables[0],
            &policy,
            false,
            PrivacyTableRemediationMode::DeleteRows,
        )
        .expect("delete");
    let rows = engine
        .query_unscoped_sql_json("select table_name, item from users")
        .expect("query");

    assert_eq!(report.affected_rows, 1);
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"table_name": "source-b", "item": {"email": "public@example.test"}})
        ]
    );
}

#[test]
fn given_registered_email_and_org_columns_when_querying_org_filter_then_matching_email_is_returned()
{
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
                offset: None,
            },
        )
        .expect("structured query");

    assert_eq!(rows[0]["email"], "structured@example.com");
}

#[test]
fn given_newer_row_position_when_ingesting_stale_record_then_record_is_skipped_and_content_is_preserved()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "users",
            b"user-1",
            user_record("20", "newer@example.com"),
            &source_cursor("20"),
        )
        .expect("ingest newer");

    let outcome = engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "users",
            b"user-1",
            user_record("10", "stale@example.com"),
            &source_cursor("10"),
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
fn given_hidden_filtered_row_when_advancing_and_recovering_position_then_metadata_advances_and_new_match_returns()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest =
        AnalyticsManifest::new(vec![filtered_entity_table("shared_items", "users", "USER")]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"1",
                entity_record("1", "USER", "user@example.com"),
                &source_cursor("1"),
            )
            .expect("insert matching row"),
        IngestOutcome::Inserted
    );
    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"1",
                StorageStreamRecord {
                    sequence_number: "2".to_string(),
                    keys: VecMap::new(),
                    old_image: Some(entity_item("1", "USER", "user@example.com")),
                    new_image: Some(entity_item("1", "ORDER", "order@example.com")),
                },
                &source_cursor("2"),
            )
            .expect("hide matching row"),
        IngestOutcome::Deleted
    );

    for cursor in ["3", "3", "2"] {
        assert_eq!(
            engine
                .ingest_stream_record_at_source_position(
                    &manifest,
                    "users",
                    b"1",
                    StorageStreamRecord {
                        sequence_number: cursor.to_string(),
                        keys: VecMap::new(),
                        old_image: Some(entity_item("1", "ORDER", "order@example.com")),
                        new_image: Some(entity_item("1", "ORDER", "order@example.com")),
                    },
                    &source_cursor(cursor),
                )
                .expect("advance or suppress filtered row"),
            IngestOutcome::Skipped
        );
    }
    let metadata = engine
        .query_unscoped_sql_json(
            "select row_visible, source_position_json from __analytics_row_source_positions",
        )
        .expect("query hidden metadata");
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0]["row_visible"], false);
    assert_eq!(metadata[0]["source_position_json"]["cursor"], "3");
    assert!(
        engine
            .query_unscoped_sql_json("select * from users")
            .expect("query users")
            .is_empty()
    );

    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"1",
                StorageStreamRecord {
                    sequence_number: "4".to_string(),
                    keys: VecMap::new(),
                    old_image: Some(entity_item("1", "ORDER", "order@example.com")),
                    new_image: Some(entity_item("1", "USER", "recovered@example.com")),
                },
                &source_cursor("4"),
            )
            .expect("recover matching row"),
        IngestOutcome::Inserted
    );
    assert_eq!(
        engine
            .query_unscoped_sql_json("select email from users")
            .expect("query recovered row"),
        vec![serde_json::json!({"email": "recovered@example.com"})]
    );
}

#[test]
fn given_absent_row_when_ingesting_nonmatching_entity_then_record_is_skipped_without_position_metadata()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest =
        AnalyticsManifest::new(vec![filtered_entity_table("shared_items", "users", "USER")]);
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"1",
                StorageStreamRecord {
                    sequence_number: "1".to_string(),
                    keys: VecMap::new(),
                    old_image: None,
                    new_image: Some(entity_item("1", "ORDER", "order@example.com")),
                },
                &source_cursor("1"),
            )
            .expect("skip absent filtered row"),
        IngestOutcome::Skipped
    );
    assert_eq!(
        engine
            .query_unscoped_sql_json(
                "select count(*) as count from __analytics_row_source_positions",
            )
            .expect("query metadata count"),
        vec![serde_json::json!({"count": 0})]
    );
}

#[test]
fn given_existing_row_position_when_replaying_changed_content_then_record_is_skipped_and_original_row_remains()
 {
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
            &position,
        )
        .expect("ingest original");

    let outcome = engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "users",
            b"user-1",
            user_record("10", "duplicate@example.com"),
            &position,
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
fn given_deleted_row_position_when_replaying_delete_then_replay_is_skipped_and_table_remains_empty()
{
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
            &position,
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
                &source_cursor("11"),
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
                &source_cursor("11"),
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
fn given_rows_from_multiple_tenants_when_querying_one_tenant_then_only_target_rows_are_returned() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "shared_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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
            keys: VecMap::new(),
            old_image: None,
            new_image: Some(VecMap::from_iter([
                (
                    "tenant_id".to_string(),
                    StorageValue::S(tenant_id.to_string()),
                ),
                ("user_id".to_string(), StorageValue::S(user_id.to_string())),
                (
                    "profile".to_string(),
                    StorageValue::M(VecMap::from_iter([(
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
                offset: None,
            },
            "tenant-a",
        )
        .expect("tenant-scoped structured query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["tenant_id"], "tenant-a");
    assert_eq!(rows[0]["email"], "a@example.com");
}

#[test]
fn given_applied_repair_receipt_when_verifying_retries_and_tampering_then_identity_content_and_revision_are_enforced()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "shared_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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

    let record = StorageStreamRecord {
        sequence_number: "stream-event-1".to_string(),
        keys: VecMap::from_iter([(
            "transport_key".to_string(),
            StorageValue::S("transport-key".to_string()),
        )]),
        old_image: None,
        new_image: Some(VecMap::from_iter([
            (
                "tenant_id".to_string(),
                StorageValue::S("tenant-a".to_string()),
            ),
            (
                "user_id".to_string(),
                StorageValue::S("canonical-user".to_string()),
            ),
            (
                "profile".to_string(),
                StorageValue::M(VecMap::from_iter([(
                    "email".to_string(),
                    StorageValue::S("apply@example.test".to_string()),
                )])),
            ),
        ])),
    };
    let repair_key = "operation:chunk:transport-key";
    let source_revision = "object-1:checksum:1:256:source-users";
    let authority =
        MaintenanceEffectAuthority::try_new("execution-repair", 1).expect("repair authority");
    engine
        .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
            manifest: &manifest,
            analytics_table_name: "users",
            record_key: b"transport-key",
            record: record.clone(),
            repair_key,
            source_revision,
            authority: &authority,
        })
        .expect("apply repair");
    assert_eq!(repair_receipt_count(&engine, repair_key), 1);

    let duplicate = engine
        .connection()
        .execute(
            "INSERT INTO __analytics_row_repair_receipts
                (repair_key, analytics_table_name, source_table_name, tenant_id,
                 source_row_id, source_revision, expected_row_json, row_visible,
                 updated_at_ms)
             SELECT repair_key, analytics_table_name, source_table_name, tenant_id,
                    source_row_id, source_revision, expected_row_json, row_visible,
                    updated_at_ms
             FROM __analytics_row_repair_receipts
             WHERE repair_key = ?
             LIMIT 1",
            duckdb::params![repair_key],
        )
        .expect("duplicate receipt");
    assert_eq!(duplicate, 1);
    let ambiguous = engine
        .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
            manifest: &manifest,
            analytics_table_name: "users",
            record_key: b"transport-key",
            expected: &record,
            repair_key,
            source_revision,
            authority: &authority,
        })
        .expect_err("duplicate receipt must fail closed");
    assert!(matches!(
        ambiguous,
        AnalyticsEngineError::AmbiguousRepairReceipt(key) if key == repair_key
    ));
    assert_eq!(repair_receipt_count(&engine, repair_key), 2);
    engine
        .connection()
        .execute(
            "DELETE FROM __analytics_row_repair_receipts WHERE repair_key = ?",
            duckdb::params![repair_key],
        )
        .expect("clear duplicate receipts");
    engine
        .connection()
        .execute(
            "DELETE FROM __analytics_row_repair_effects WHERE repair_key = ?",
            duckdb::params![repair_key],
        )
        .expect("reset duplicate effect fixture");
    engine
        .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
            manifest: &manifest,
            analytics_table_name: "users",
            record_key: b"transport-key",
            record: record.clone(),
            repair_key,
            source_revision,
            authority: &authority,
        })
        .expect("recreate receipt after duplicate rejection");
    engine
        .connection()
        .execute(
            "UPDATE __analytics_row_repair_receipts
             SET source_revision = ?
             WHERE repair_key = ?",
            duckdb::params!["tampered-source-revision", repair_key],
        )
        .expect("tamper repair receipt");
    assert!(
        !engine
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &manifest,
                analytics_table_name: "users",
                record_key: b"transport-key",
                expected: &record,
                repair_key,
                source_revision,
                authority: &authority,
            })
            .expect("tampered receipt must fail verification")
    );
    assert_eq!(repair_receipt_count(&engine, repair_key), 1);
    engine
        .connection()
        .execute(
            "DELETE FROM __analytics_row_repair_receipts WHERE repair_key = ?",
            duckdb::params![repair_key],
        )
        .expect("clear tampered receipt");
    engine
        .connection()
        .execute(
            "DELETE FROM __analytics_row_repair_effects WHERE repair_key = ?",
            duckdb::params![repair_key],
        )
        .expect("reset tampered effect fixture");
    engine
        .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
            manifest: &manifest,
            analytics_table_name: "users",
            record_key: b"transport-key",
            record: record.clone(),
            repair_key,
            source_revision,
            authority: &authority,
        })
        .expect("recreate receipt after tamper rejection");

    let rows = engine
        .query_unscoped_sql_json("SELECT __id, tenant_id, email, item FROM users")
        .expect("query repaired row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["__id"], "63616e6f6e6963616c2d75736572");
    assert_eq!(rows[0]["tenant_id"], "tenant-a");
    assert_eq!(rows[0]["email"], "apply@example.test");
    assert_eq!(rows[0]["item"]["profile"]["email"], "apply@example.test");
    assert!(
        engine
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &manifest,
                analytics_table_name: "users",
                record_key: b"transport-key",
                expected: &record,
                repair_key,
                source_revision,
                authority: &authority,
            })
            .expect("verify repair")
    );
    assert_eq!(repair_receipt_count(&engine, repair_key), 0);

    engine
        .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
            manifest: &manifest,
            analytics_table_name: "users",
            record_key: b"transport-key",
            record: record.clone(),
            repair_key,
            source_revision,
            authority: &authority,
        })
        .expect("same repair retry is idempotent");
    assert_eq!(repair_receipt_count(&engine, repair_key), 0);
    assert!(
        engine
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &manifest,
                analytics_table_name: "users",
                record_key: b"transport-key",
                expected: &record,
                repair_key,
                source_revision,
                authority: &authority,
            })
            .expect("retry verification")
    );
    assert_eq!(repair_receipt_count(&engine, repair_key), 0);
    engine
        .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
            manifest: &manifest,
            analytics_table_name: "users",
            record_key: b"transport-key",
            record: record.clone(),
            repair_key,
            source_revision,
            authority: &authority,
        })
        .expect("same consumed repair remains idempotent before tamper check");
    assert_eq!(repair_receipt_count(&engine, repair_key), 0);
    engine
        .connection()
        .execute(
            "UPDATE users SET email = ?",
            duckdb::params!["tampered@example.test"],
        )
        .expect("tamper destination row");
    assert!(
        !engine
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &manifest,
                analytics_table_name: "users",
                record_key: b"transport-key",
                expected: &record,
                repair_key,
                source_revision,
                authority: &authority,
            })
            .expect("tampered content must fail verification")
    );
    assert_eq!(repair_receipt_count(&engine, repair_key), 0);

    let mut changed = record.clone();
    changed.new_image.as_mut().expect("new image").insert(
        "profile".to_string(),
        StorageValue::M(VecMap::from_iter([(
            "email".to_string(),
            StorageValue::S("changed@example.test".to_string()),
        )])),
    );
    assert!(
        !engine
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &manifest,
                analytics_table_name: "users",
                record_key: b"transport-key",
                expected: &changed,
                repair_key,
                source_revision,
                authority: &authority,
            })
            .expect("changed content must fail verification")
    );
    assert_eq!(repair_receipt_count(&engine, repair_key), 0);
    let conflict = engine
        .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
            manifest: &manifest,
            analytics_table_name: "users",
            record_key: b"transport-key",
            record: changed.clone(),
            repair_key,
            source_revision: "object-1:checksum:2:256:source-users",
            authority: &authority,
        })
        .expect_err("same repair key must reject changed source");
    assert!(matches!(
        conflict,
        AnalyticsEngineError::ConflictingMaintenanceEffectReceipt(key) if key == repair_key
    ));
    assert!(
        !engine
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &manifest,
                analytics_table_name: "users",
                record_key: b"transport-key",
                expected: &record,
                repair_key,
                source_revision: "object-1:checksum:2:256:source-users",
                authority: &authority,
            })
            .expect("changed source revision must fail verification")
    );
    assert_eq!(repair_receipt_count(&engine, repair_key), 0);
    assert!(
        !engine
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &manifest,
                analytics_table_name: "users",
                record_key: b"transport-key",
                expected: &record,
                repair_key: "missing-repair-receipt",
                source_revision,
                authority: &authority,
            })
            .expect("missing receipt must fail closed")
    );
}

fn repair_receipt_count(engine: &AnalyticsEngine, repair_key: &str) -> i64 {
    engine
        .connection()
        .query_row(
            "SELECT count(*) FROM __analytics_row_repair_receipts WHERE repair_key = ?",
            duckdb::params![repair_key],
            |row| row.get(0),
        )
        .expect("count repair receipts")
}

#[test]
fn given_expanded_table_when_applying_or_verifying_repair_receipt_then_unsupported_expansion_is_returned()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = expanded_metric_manifest();
    let item = expanded_metric_batch_item(
        "batch-1",
        expanded_metric_item(&[("event-1", "api_requests", "10")]),
    );
    let repair_key = "operation:chunk:batch-1";
    let source_revision = "object-1:checksum:1:256:source-metrics";
    let authority =
        MaintenanceEffectAuthority::try_new("execution-expansion", 1).expect("repair authority");

    let apply_error = engine
        .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
            manifest: &manifest,
            analytics_table_name: "metric_points_expanded",
            record_key: b"batch-1",
            record: item.record.clone(),
            repair_key,
            source_revision,
            authority: &authority,
        })
        .expect_err("row expansion receipt must be rejected");
    assert!(matches!(
        apply_error,
        AnalyticsEngineError::UnsupportedRepairReceiptExpansion(table)
            if table == "metric_points_expanded"
    ));

    let verify_error = engine
        .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
            manifest: &manifest,
            analytics_table_name: "metric_points_expanded",
            record_key: b"batch-1",
            expected: &item.record,
            repair_key,
            source_revision,
            authority: &authority,
        })
        .expect_err("row expansion receipt verification must be rejected");
    assert!(matches!(
        verify_error,
        AnalyticsEngineError::UnsupportedRepairReceiptExpansion(table)
            if table == "metric_points_expanded"
    ));
}

#[test]
fn given_remote_or_object_backed_ducklake_when_repairing_then_rejection_precedes_effects_and_receipts()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let remote_backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: tempdir
            .path()
            .join("remote-catalog.sqlite")
            .to_string_lossy()
            .into_owned(),
        data_path: "s3://analytics-bucket/repairs".to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    let object_backed_backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: tempdir
            .path()
            .join("object-catalog.sqlite")
            .to_string_lossy()
            .into_owned(),
        data_path: tempdir
            .path()
            .join("object-data")
            .to_string_lossy()
            .into_owned(),
        object_storage: Some(Box::new(config::AnalyticsObjectStorageConfig {
            bucket: Some("analytics-bucket".to_string()),
            credentials: Some(config::RemoteCredentialsConfig {
                r#static: Some(config::RemoteStaticCredentialsConfig {
                    access_key: "test-access-key".to_string(),
                    secret_key: "test-secret-key".to_string(),
                    session_token: None,
                }),
                instance_keys: None,
            }),
            ..Default::default()
        })),
        catalog_settings: Default::default(),
    };
    for (backend, repair_key) in [
        (remote_backend, "operation:chunk:remote"),
        (object_backed_backend, "operation:chunk:object-backed"),
    ] {
        assert_repair_rejected_without_effect(backend, repair_key);
    }
}

fn assert_repair_rejected_without_effect(backend: StorageBackend, repair_key: &str) {
    let engine = AnalyticsEngine::connect(&backend).expect("connect remote-path backend");
    let manifest = users_manifest();
    let record = repair_conflict_record("stream-remote", "remote@example.test");
    let authority =
        MaintenanceEffectAuthority::try_new("execution-remote", 1).expect("repair authority");
    let apply_error = engine
        .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
            manifest: &manifest,
            analytics_table_name: "users",
            record_key: b"transport-key",
            record: record.clone(),
            repair_key,
            source_revision: "source-revision-remote",
            authority: &authority,
        })
        .expect_err("remote DuckLake repair must be rejected before effects");
    assert!(matches!(
        apply_error,
        AnalyticsEngineError::RepairReceiptFenceUnavailable
    ));

    let verify_error = engine
        .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
            manifest: &manifest,
            analytics_table_name: "users",
            record_key: b"transport-key",
            expected: &record,
            repair_key,
            source_revision: "source-revision-remote",
            authority: &authority,
        })
        .expect_err("remote DuckLake verification must be rejected before effects");
    assert!(matches!(
        verify_error,
        AnalyticsEngineError::RepairReceiptFenceUnavailable
    ));

    let runtime_tables = engine
        .query_unscoped_sql_json(
            "SELECT table_name FROM information_schema.tables
             WHERE table_name IN ('users', '__analytics_row_repair_receipts',
                                  '__analytics_row_repair_effects')",
        )
        .expect("query runtime tables");
    assert!(
        runtime_tables.is_empty(),
        "remote repair must not create destination or receipt tables: {runtime_tables:?}"
    );
}

#[test]
fn given_file_duckdb_connections_when_applying_conflicting_repairs_then_one_content_wins_and_receipt_is_consumed_once()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let backend = StorageBackend::DuckDb {
        path: tempdir
            .path()
            .join("analytics.duckdb")
            .to_string_lossy()
            .to_string(),
    };
    concurrent_conflicting_repairs_are_fenced(backend);
}

#[test]
fn given_sqlite_ducklake_connections_when_applying_conflicting_repairs_then_one_content_wins_and_receipt_is_consumed_once()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let data_path = tempdir.path().join("ducklake-data");
    std::fs::create_dir_all(&data_path).expect("create ducklake data dir");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: tempdir
            .path()
            .join("catalog.sqlite")
            .to_string_lossy()
            .to_string(),
        data_path: data_path.to_string_lossy().to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    concurrent_conflicting_repairs_are_fenced(backend);
}

fn concurrent_conflicting_repairs_are_fenced(backend: StorageBackend) {
    let manifest = users_manifest();
    let setup = AnalyticsEngine::connect(&backend).expect("connect setup engine");
    setup
        .ensure_manifest(&manifest)
        .expect("ensure setup manifest");
    drop(setup);

    let record_a = repair_conflict_record("stream-a", "a@example.test");
    let record_b = repair_conflict_record("stream-b", "b@example.test");
    let repair_key = "operation:chunk:concurrent-conflict";
    let source_revision = "source-revision-1";
    let authority_a =
        MaintenanceEffectAuthority::try_new("execution-a", 1).expect("repair authority a");
    let authority_b =
        MaintenanceEffectAuthority::try_new("execution-b", 1).expect("repair authority b");
    let barrier = Arc::new(Barrier::new(2));

    let engine_a = AnalyticsEngine::connect(&backend).expect("connect engine a");
    let engine_b = AnalyticsEngine::connect(&backend).expect("connect engine b");
    let manifest_a = manifest.clone();
    let manifest_b = manifest.clone();
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);
    let apply_record_a = record_a.clone();
    let apply_record_b = record_b.clone();
    let authority_a_for_worker = authority_a.clone();
    let authority_b_for_worker = authority_b.clone();
    let handle_a = thread::spawn(move || {
        engine_a
            .with_repair_receipt_barrier(barrier_a)
            .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
                manifest: &manifest_a,
                analytics_table_name: "users",
                record_key: b"transport-key",
                record: apply_record_a,
                repair_key,
                source_revision,
                authority: &authority_a_for_worker,
            })
    });
    let handle_b = thread::spawn(move || {
        engine_b
            .with_repair_receipt_barrier(barrier_b)
            .apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
                manifest: &manifest_b,
                analytics_table_name: "users",
                record_key: b"transport-key",
                record: apply_record_b,
                repair_key,
                source_revision,
                authority: &authority_b_for_worker,
            })
    });
    let result_a = handle_a.join().expect("join engine a");
    let result_b = handle_b.join().expect("join engine b");
    assert_eq!(
        result_a.is_ok() as usize + result_b.is_ok() as usize,
        1,
        "same repair key must accept exactly one conflicting content: a={result_a:?}, \
         b={result_b:?}"
    );
    if matches!(
        &backend,
        StorageBackend::DuckLake {
            catalog: CatalogType::Sqlite,
            ..
        }
    ) {
        let conflict_count = [&result_a, &result_b]
            .into_iter()
            .filter(|result| {
                matches!(
                    result,
                    Err(AnalyticsEngineError::ConflictingMaintenanceEffectReceipt(key))
                        if key == repair_key
                )
            })
            .count();
        assert_eq!(
            conflict_count, 1,
            "SQLite-catalog loser must observe the durable winner, not a transient lock: \
             a={result_a:?}, b={result_b:?}"
        );
    }

    let winner = if result_a.is_ok() {
        &record_a
    } else {
        &record_b
    };
    let expected_email = if result_a.is_ok() {
        "a@example.test"
    } else {
        "b@example.test"
    };
    let winner_authority = if result_a.is_ok() {
        authority_a
    } else {
        authority_b
    };
    let observer = AnalyticsEngine::connect(&backend).expect("connect observer");
    assert_eq!(repair_receipt_count(&observer, repair_key), 1);
    let rows = observer
        .query_unscoped_sql_json("SELECT email FROM users")
        .expect("query winner row");
    assert_eq!(rows, vec![serde_json::json!({"email": expected_email})]);

    assert!(
        observer
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &manifest,
                analytics_table_name: "users",
                record_key: b"transport-key",
                expected: winner,
                repair_key,
                source_revision,
                authority: &winner_authority,
            })
            .expect("verify accepted repair")
    );
    assert_eq!(repair_receipt_count(&observer, repair_key), 0);
    assert!(
        !observer
            .verify_stream_record_with_receipt(MaintenanceEffectVerifyRequest {
                manifest: &manifest,
                analytics_table_name: "users",
                record_key: b"transport-key",
                expected: if result_a.is_ok() {
                    &record_b
                } else {
                    &record_a
                },
                repair_key,
                source_revision,
                authority: &winner_authority,
            })
            .expect("consumed repair must not be reusable")
    );
}

const RECEIPT_RACE_ROOT_ENV: &str = "ANALYTICS_ENGINE_RECEIPT_RACE_ROOT";
const RECEIPT_RACE_ROLE_ENV: &str = "ANALYTICS_ENGINE_RECEIPT_RACE_ROLE";
const RECEIPT_RACE_CATALOG_ENV: &str = "ANALYTICS_ENGINE_RECEIPT_RACE_CATALOG";
const RECEIPT_RACE_DATA_ENV: &str = "ANALYTICS_ENGINE_RECEIPT_RACE_DATA";

#[test]
fn given_independent_catalog_processes_when_applying_conflicting_repairs_then_one_durable_winner_is_observed()
 {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let catalog_path = tempdir.path().join("catalog.sqlite");
    let data_path = tempdir.path().join("ducklake-data");
    fs::create_dir_all(&data_path).expect("create ducklake data dir");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: catalog_path.to_string_lossy().to_string(),
        data_path: data_path.to_string_lossy().to_string(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    let manifest = users_manifest();
    let setup = AnalyticsEngine::connect(&backend).expect("connect setup engine");
    setup
        .ensure_manifest(&manifest)
        .expect("ensure setup manifest");
    drop(setup);
    #[cfg(unix)]
    std::os::unix::fs::symlink(&catalog_path, tempdir.path().join("catalog-alias.sqlite"))
        .expect("create catalog symlink alias");

    let control_dir = tempdir.path().join("receipt-race-control");
    fs::create_dir(&control_dir).expect("create process barrier directory");
    let executable = env::current_exe().expect("current test executable");
    let mut children = Vec::new();
    for role in ["a", "b"] {
        let catalog_for_role = if role == "a" {
            catalog_path.to_string_lossy().into_owned()
        } else {
            #[cfg(unix)]
            {
                "catalog-alias.sqlite".to_string()
            }
            #[cfg(not(unix))]
            {
                catalog_path.to_string_lossy().into_owned()
            }
        };
        children.push(
            Command::new(&executable)
                .args([
                    "--exact",
                    "engine_tests::given_receipt_race_environment_when_applying_worker_repair_then_result_is_written_for_parent_assertion",
                    "--nocapture",
                    "--ignored",
                ])
                .env(RECEIPT_RACE_ROOT_ENV, &control_dir)
                .env(RECEIPT_RACE_ROLE_ENV, role)
                .env(RECEIPT_RACE_CATALOG_ENV, catalog_for_role)
                .env(RECEIPT_RACE_DATA_ENV, &data_path)
                .current_dir(tempdir.path())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn receipt race worker"),
        );
    }

    wait_for_receipt_race_file(&control_dir.join("ready-a"));
    wait_for_receipt_race_file(&control_dir.join("ready-b"));
    fs::write(control_dir.join("go"), b"go").expect("release process barrier");

    let mut outputs = Vec::new();
    for child in children {
        outputs.push(
            child
                .wait_with_output()
                .expect("wait for receipt race worker"),
        );
    }
    for output in &outputs {
        assert!(
            output.status.success(),
            "receipt race worker failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let result_a = fs::read_to_string(control_dir.join("done-a")).expect("read worker a result");
    let result_b = fs::read_to_string(control_dir.join("done-b")).expect("read worker b result");
    assert_eq!(
        [result_a.as_str(), result_b.as_str()]
            .into_iter()
            .filter(|result| *result == "ok")
            .count(),
        1,
        "independent processes must accept one repair: a={result_a:?}, b={result_b:?}"
    );
    assert!(
        [result_a.as_str(), result_b.as_str()]
            .into_iter()
            .any(|result| result.contains("ConflictingMaintenanceEffectReceipt")),
        "the losing process must observe the durable winner: a={result_a:?}, b={result_b:?}"
    );
}

#[test]
#[ignore = "subprocess worker executed and asserted by the receipt-race parent"]
fn given_receipt_race_environment_when_applying_worker_repair_then_result_is_written_for_parent_assertion()
 {
    let control_dir = env::var_os(RECEIPT_RACE_ROOT_ENV)
        .map(PathBuf::from)
        .expect("receipt race worker requires parent-controlled environment");
    let role = env::var(RECEIPT_RACE_ROLE_ENV).expect("receipt race role");
    let catalog_path = env::var(RECEIPT_RACE_CATALOG_ENV).expect("receipt race catalog");
    let data_path = env::var(RECEIPT_RACE_DATA_ENV).expect("receipt race data");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path,
        data_path,
        object_storage: None,
        catalog_settings: Default::default(),
    };
    let engine = AnalyticsEngine::connect(&backend);
    let ready_path = control_dir.join(format!("ready-{role}"));
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&ready_path)
        .expect("signal worker ready");
    wait_for_receipt_race_file(&control_dir.join("go"));
    let authority = MaintenanceEffectAuthority::try_new(format!("subprocess-{role}"), 1)
        .expect("subprocess repair authority");

    let result = engine
        .and_then(|engine| {
            engine.apply_stream_record_with_receipt(MaintenanceEffectApplyRequest {
                manifest: &users_manifest(),
                analytics_table_name: "users",
                record_key: b"transport-key",
                record: repair_conflict_record(
                    if role == "a" { "stream-a" } else { "stream-b" },
                    if role == "a" {
                        "a@example.test"
                    } else {
                        "b@example.test"
                    },
                ),
                repair_key: "operation:chunk:subprocess-conflict",
                source_revision: "source-revision-1",
                authority: &authority,
            })
        })
        .map(|disposition| match disposition {
            MaintenanceEffectDisposition::Applied
            | MaintenanceEffectDisposition::AlreadyApplied => "ok".to_string(),
        })
        .unwrap_or_else(|error| format!("err:{error:?}"));
    fs::write(control_dir.join(format!("done-{role}")), result).expect("write worker result");
}

fn wait_for_receipt_race_file(path: &Path) {
    for _ in 0..600 {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for receipt race file {}", path.display());
}

fn repair_conflict_record(sequence_number: &str, email: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: VecMap::from_iter([(
            "pk".to_string(),
            StorageValue::S("transport-key".to_string()),
        )]),
        old_image: None,
        new_image: Some(analytics_fixtures::user_item(
            "canonical-user",
            email,
            "org-a",
        )),
    }
}

#[test]
fn given_joined_metric_tables_when_querying_tenant_then_rate_classes_and_quantities_are_returned() {
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
                    keys: VecMap::new(),
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
                    keys: VecMap::new(),
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
fn given_expanded_metric_wal_with_tenant_attribute_when_ingesting_then_tenant_quantity_is_queryable()
 {
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
                keys: VecMap::new(),
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

struct RangePurgeClaimHandler;

impl bg_jobs::JobHandler for RangePurgeClaimHandler {
    fn execute<'a>(
        &'a self,
        _: bg_jobs::ExecutionContext<'a>,
        _: &'a [u8],
    ) -> bg_jobs::HandlerFuture<'a> {
        Box::pin(async {
            Ok(bg_jobs::JobOutcome::Complete(
                bg_jobs::OutcomeCode::try_new("finished").unwrap(),
            ))
        })
    }
}

fn range_purge_claims() -> [bg_jobs::RemoteJobClaim; 3] {
    use bg_jobs::{
        EnqueueOptions, FairnessClass, JobCatalog, JobId, JobKind, JobSeed, JobSpec, Namespace,
        OwnerId, QueueName, Timestamp,
    };
    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        loop {
            if let std::task::Poll::Ready(result) = future.as_mut().poll(&mut context) {
                return result;
            }
            std::thread::yield_now();
        }
    }
    let mut catalog = JobCatalog::new();
    let kind = JobKind::try_new("range-purge").unwrap();
    catalog
        .register(
            JobSpec::bytes(
                kind.clone(),
                std::num::NonZeroU16::MIN,
                QueueName::try_new("purges").unwrap(),
                RangePurgeClaimHandler,
            )
            .policy(
                bg_jobs::EffectMode::Fenced,
                bg_jobs::FairnessPolicy::low_volume(),
                bg_jobs::RetrySelection::Preset(bg_jobs::RetryPreset::Maintenance),
                std::time::Duration::from_secs(1),
            ),
        )
        .unwrap();
    let system = bg_jobs::runtime::JobSystemBuilder::new(bg_jobs::test_support::FakeStore::new())
        .catalog(catalog)
        .namespace(Namespace::try_new("native-purge-proof").unwrap())
        .owner(OwnerId::from([1; 16]))
        .scheduler(std::sync::Arc::new(
            bg_jobs::test_support::TestProfile::new(),
        ))
        .build()
        .unwrap();
    let id = JobId::from([1; 16]);
    block_on(
        system.ensure_seed(
            JobSeed::new(
                kind,
                std::num::NonZeroU16::MIN,
                Default::default(),
                EnqueueOptions::new(FairnessClass::low_volume()).job_id(id),
                Timestamp::ZERO,
            )
            .unwrap(),
            Timestamp::ZERO,
        ),
    )
    .unwrap();
    std::array::from_fn(|i| {
        let claim = block_on(system.claim(id, Timestamp::from_millis(i as i64 * 1000))).unwrap();
        bg_jobs::RemoteJobClaim::from(&claim.context().unwrap())
    })
}

#[test]
fn given_multiple_tenant_series_when_purging_with_replay_and_takeover_then_cursor_resumes_and_other_series_remain()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = metric_points_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    for (tenant_id, event_id, series_name, occurred_at_ms) in [
        ("tenant_01", "purge-1", "requests", 1_000_i64),
        ("tenant_01", "purge-2", "requests", 2_000_i64),
        ("tenant_02", "purge-3", "requests", 1_000_i64),
        ("tenant_01", "purge-4", "other", 1_000_i64),
    ] {
        let mut item = metric_point_item(event_id, "standard", "1");
        item.insert(
            "tenant_id".to_string(),
            StorageValue::S(tenant_id.to_string()),
        );
        item.insert(
            "series_name".to_string(),
            StorageValue::S(series_name.to_string()),
        );
        item.insert(
            "occurred_at_ms".to_string(),
            StorageValue::N(occurred_at_ms.to_string()),
        );
        engine
            .ingest_stream_record(
                &manifest,
                "metric_points_v1",
                event_id.as_bytes(),
                StorageStreamRecord {
                    sequence_number: event_id.to_string(),
                    keys: VecMap::new(),
                    old_image: None,
                    new_image: Some(item),
                },
            )
            .expect("ingest metric point");
    }

    let request = TenantRangePurgeRequest {
        table_name: "metric_points_v1".to_string(),
        tenant_id: "tenant_01".to_string(),
        timestamp_column: "occurred_at_ms".to_string(),
        start_ms: 0,
        end_ms: 3_000,
        equals: BTreeMap::from([("series_name".to_string(), Some("requests".to_string()))]),
        cursor: None,
        limit: 1,
    };
    let [first_authority, reclaimed_authority, conflicting_authority] = range_purge_claims();
    assert!(matches!(
        engine.apply_tenant_range_purge(TenantRangePurgeEffectRequest {
            request: &request,
            authority: &first_authority,
            request_id: "range-purge-1",
        }),
        Err(AnalyticsEngineError::StaleMaintenanceEffectFence(_))
    ));
    engine
        .register_range_purge_claim(&first_authority)
        .expect("register first claim");
    engine
        .register_range_purge_claim(&first_authority)
        .expect("replay publication acknowledgement");
    let first = match engine
        .apply_tenant_range_purge(TenantRangePurgeEffectRequest {
            request: &request,
            authority: &first_authority,
            request_id: "range-purge-1",
        })
        .expect("first purge")
    {
        TenantRangePurgeEffectDisposition::Applied(response) => response,
        TenantRangePurgeEffectDisposition::AlreadyApplied(_) => panic!("unexpected replay"),
    };
    assert_eq!(first.rows_deleted, 1);
    assert!(!first.complete);
    let replay = engine
        .apply_tenant_range_purge(TenantRangePurgeEffectRequest {
            request: &request,
            authority: &first_authority,
            request_id: "range-purge-1",
        })
        .expect("replay first purge");
    assert_eq!(
        replay,
        TenantRangePurgeEffectDisposition::AlreadyApplied(first.clone())
    );
    engine
        .register_range_purge_claim(&reclaimed_authority)
        .expect("register replacement claim");
    assert!(matches!(
        engine.register_range_purge_claim(&first_authority),
        Err(AnalyticsEngineError::StaleMaintenanceEffectFence(_))
    ));
    assert_eq!(
        engine
            .apply_tenant_range_purge(TenantRangePurgeEffectRequest {
                request: &request,
                authority: &reclaimed_authority,
                request_id: "range-purge-1",
            })
            .expect("reclaimed replay"),
        TenantRangePurgeEffectDisposition::AlreadyApplied(first.clone())
    );
    assert!(matches!(
        engine.apply_tenant_range_purge(TenantRangePurgeEffectRequest {
            request: &request,
            authority: &first_authority,
            request_id: "range-purge-1",
        }),
        Err(AnalyticsEngineError::StaleMaintenanceEffectFence(_))
    ));
    let conflicting_request = TenantRangePurgeRequest {
        end_ms: 4_000,
        ..request.clone()
    };
    engine
        .register_range_purge_claim(&conflicting_authority)
        .expect("register next claim");
    assert!(matches!(
        engine.apply_tenant_range_purge(TenantRangePurgeEffectRequest {
            request: &conflicting_request,
            authority: &conflicting_authority,
            request_id: "range-purge-1",
        }),
        Err(AnalyticsEngineError::ConflictingMaintenanceEffectReceipt(_))
    ));
    let second_request = TenantRangePurgeRequest {
        cursor: first.cursor.clone(),
        ..request.clone()
    };

    let second = match engine
        .apply_tenant_range_purge(TenantRangePurgeEffectRequest {
            request: &second_request,
            authority: &conflicting_authority,
            request_id: "range-purge-2",
        })
        .expect("second purge")
    {
        TenantRangePurgeEffectDisposition::Applied(response) => response,
        TenantRangePurgeEffectDisposition::AlreadyApplied(_) => panic!("unexpected replay"),
    };
    assert_eq!(second.rows_deleted, 1);
    assert!(!second.complete);
    let third_request = TenantRangePurgeRequest {
        cursor: second.cursor.clone(),
        ..request
    };

    let third = match engine
        .apply_tenant_range_purge(TenantRangePurgeEffectRequest {
            request: &third_request,
            authority: &conflicting_authority,
            request_id: "range-purge-3",
        })
        .expect("completion check")
    {
        TenantRangePurgeEffectDisposition::Applied(response) => response,
        TenantRangePurgeEffectDisposition::AlreadyApplied(_) => panic!("unexpected replay"),
    };
    assert_eq!(third.rows_deleted, 0);
    assert!(third.complete);

    let rows = engine
        .query_unscoped_sql_json(
            "select tenant_id, series_name, event_id from metric_points_v1 order by tenant_id, \
             event_id",
        )
        .expect("query remaining rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["tenant_id"], "tenant_01");
    assert_eq!(rows[0]["series_name"], "other");
    assert_eq!(rows[1]["tenant_id"], "tenant_02");
}

#[test]
fn given_metric_facts_when_querying_aggregates_groups_and_evidence_then_quantities_dimensions_and_timestamps_match()
 {
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
fn given_boundary_and_late_arrival_metrics_when_grouping_conditionally_then_each_class_total_matches()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = metric_points_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    for (event_id, occurred_at_ms, ingested_at_ms, value) in [
        ("standard", 1_000_i64, 3_000_i64, "10"),
        ("late", 1_999_i64, 3_001_i64, "20"),
        ("next_window", 2_000_i64, 4_000_i64, "30"),
    ] {
        let mut item = metric_point_item(event_id, "standard", value);
        item.insert(
            "occurred_at_ms".to_string(),
            StorageValue::N(occurred_at_ms.to_string()),
        );
        item.insert(
            "ingested_at_ms".to_string(),
            StorageValue::N(ingested_at_ms.to_string()),
        );
        engine
            .ingest_stream_record(
                &manifest,
                "metric_points_v1",
                event_id.as_bytes(),
                StorageStreamRecord {
                    sequence_number: event_id.to_string(),
                    keys: VecMap::new(),
                    old_image: None,
                    new_image: Some(item),
                },
            )
            .expect("ingest classified metric point");
    }

    let classification = metric_late_arrival_classification();
    let rows = engine
        .query_tenant_structured_json(
            &manifest,
            &StructuredQuery {
                analytics_table_name: "metric_points_v1".to_string(),
                table_alias: Some("m".to_string()),
                joins: Vec::new(),
                select: vec![
                    QuerySelect::Expression {
                        expression: classification.clone(),
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
                group_by: vec![classification.clone()],
                order_by: vec![QueryOrder {
                    expression: classification,
                    direction: Some(SortOrder::Asc),
                }],
                limit: Some(2),
                offset: None,
            },
            "tenant_01",
        )
        .expect("conditional aggregate query");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["usage_class"], "late_arrival");
    assert_eq!(rows[0]["quantity"], 20);
    assert_eq!(rows[1]["usage_class"], "standard");
    assert_eq!(rows[1]["quantity"], 40);
}

#[test]
fn given_namespaced_partition_key_when_ingesting_then_prefix_selects_query_tenant() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "s00000".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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
        keys: VecMap::new(),
        old_image: None,
        new_image: Some(VecMap::from_iter([
            (
                "pk".to_string(),
                StorageValue::S("ns_2X6Z455H5ZBWSRK6D51ASR0#USER#user-a".to_string()),
            ),
            ("user_id".to_string(), StorageValue::S("user-a".to_string())),
            (
                "profile".to_string(),
                StorageValue::M(VecMap::from_iter([(
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
                offset: None,
            },
            "t_2X6Z455H5ZBWSRK6D51ASR0",
        )
        .expect("tenant-scoped structured query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["email"], "a@example.com");
}

#[test]
fn given_generic_document_table_when_querying_nested_email_path_then_email_is_returned() {
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
                offset: None,
            },
        )
        .expect("structured query");

    assert_eq!(rows[0]["email"], "legacy-structured@example.com");
}

#[test]
fn given_regex_tenant_and_row_selectors_when_ingesting_then_extracted_identity_and_status_are_queryable()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "shared_items".to_string(),
        analytics_table_name: "orders".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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
        new_image: Some(VecMap::from_iter([
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
fn given_static_retention_and_source_timestamp_when_ingesting_then_expiry_is_computed_without_missing_timestamp()
 {
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
                keys: VecMap::new(),
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

fn filtered_entity_table(
    source_table_name: &str,
    analytics_table_name: &str,
    entity_type: &str,
) -> TableRegistration {
    TableRegistration {
        source_table_name: source_table_name.to_string(),
        analytics_table_name: analytics_table_name.to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
        tenant_id: None,
        tenant_selector: TenantSelector::None,
        row_identity: RowIdentity::Attribute {
            attribute_name: "entity_id".to_string(),
        },
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: Some("#entity_type = :entity_type".to_string()),
        expression_attribute_names: Some(VecMap::from_iter([(
            "#entity_type".to_string(),
            "entity_type".to_string(),
        )])),
        expression_attribute_values: Some(VecMap::from_iter([(
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
        keys: VecMap::new(),
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
        keys: VecMap::new(),
        old_image: None,
        new_image: Some(VecMap::from_iter([(
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

fn entity_item(entity_id: &str, entity_type: &str, email: &str) -> StorageItem {
    VecMap::from_iter([
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

fn metric_point_item(event_id: &str, model_name: &str, value: &str) -> StorageItem {
    VecMap::from_iter([
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

fn rate_class_item(model_name: &str, rate_class: &str) -> StorageItem {
    VecMap::from_iter([
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
                    keys: VecMap::new(),
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
                keys: VecMap::new(),
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
) -> StorageItem {
    let mut item = metric_point_item(event_id, "standard", value);
    item.insert(
        "series_name".to_string(),
        StorageValue::S(series_name.to_string()),
    );
    item.insert("dim_3".to_string(), StorageValue::S(operation.to_string()));
    item
}

fn decimal_metric_point_item() -> StorageItem {
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
        offset: None,
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
        offset: None,
    }
}

fn metric_late_arrival_classification() -> QueryExpression {
    QueryExpression::Conditional {
        branches: vec![QueryConditionalBranch {
            all: vec![
                QueryColumnComparison {
                    table_alias: Some("m".to_string()),
                    column_name: "occurred_at_ms".to_string(),
                    operator: QueryComparisonOperator::Gte,
                    value: serde_json::json!(1_000),
                },
                QueryColumnComparison {
                    table_alias: Some("m".to_string()),
                    column_name: "occurred_at_ms".to_string(),
                    operator: QueryComparisonOperator::Lt,
                    value: serde_json::json!(2_000),
                },
                QueryColumnComparison {
                    table_alias: Some("m".to_string()),
                    column_name: "ingested_at_ms".to_string(),
                    operator: QueryComparisonOperator::Gt,
                    value: serde_json::json!(3_000),
                },
            ],
            then_value: serde_json::json!("late_arrival"),
        }],
        else_value: serde_json::json!("standard"),
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
        offset: None,
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
        offset: None,
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
        offset: None,
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
        offset: None,
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
        offset: None,
    }
}

#[test]
fn given_cached_manifest_condition_when_alias_and_value_change_then_ingestion_uses_current_policy()
{
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("engine");
    let mut manifest = users_manifest();
    manifest.tables[0].condition_expression = Some("#field = :value".to_string());
    manifest.tables[0].expression_attribute_names = Some(VecMap::from_iter([(
        "#field".to_string(),
        "status".to_string(),
    )]));
    manifest.tables[0].expression_attribute_values = Some(VecMap::from_iter([(
        ":value".to_string(),
        StorageValue::S("ready".to_string()),
    )]));
    engine.ensure_manifest(&manifest).expect("manifest");
    let mut image = user_item("1", "current@example.test", "org-a");
    image.insert("status".to_string(), StorageValue::S("ready".to_string()));
    image.insert("state".to_string(), StorageValue::S("paused".to_string()));
    for (key, expected) in [
        (b"first".as_slice(), IngestOutcome::Inserted),
        (b"second".as_slice(), IngestOutcome::Skipped),
        (b"third".as_slice(), IngestOutcome::Inserted),
    ] {
        let record = StorageStreamRecord {
            sequence_number: String::new(),
            keys: StorageItem::new(),
            old_image: None,
            new_image: Some(image.clone()),
        };
        assert_eq!(
            engine
                .ingest_stream_record(&manifest, "users", key, record)
                .expect("ingest"),
            expected
        );
        if key == b"first" {
            manifest.tables[0].expression_attribute_names = Some(VecMap::from_iter([(
                "#field".to_string(),
                "state".to_string(),
            )]));
        } else {
            manifest.tables[0].expression_attribute_values = Some(VecMap::from_iter([(
                ":value".to_string(),
                StorageValue::S("paused".to_string()),
            )]));
        }
    }
    let rows = engine
        .query_unscoped_sql_json("SELECT COUNT(*) AS count FROM users")
        .expect("query");
    assert_eq!(rows[0]["count"], 2);
}

#[test]
fn given_changed_projection_aliases_and_paths_when_ingesting_then_current_values_are_stored() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("memory database");
    let table = serde_json::from_value::<TableRegistration>(serde_json::json!({
        "source_table_name": "source_items",
        "analytics_table_name": "projection_items",
        "tenant_selector": {"kind": "attribute", "attribute_name": "tenant"},
        "expression_attribute_names": {"#chosen": "first"},
        "projection_columns": [{"column_name": "selected", "attribute_path": "#chosen"}]
    }))
    .expect("registration");
    let mut manifest = AnalyticsManifest::new(vec![table]);
    for (index, expected) in ["first-value", "second-value", "nested-value"]
        .iter()
        .enumerate()
    {
        if index == 1 {
            manifest.tables[0].expression_attribute_names = Some(VecMap::from_iter([(
                "#chosen".to_string(),
                "second".to_string(),
            )]));
        } else if index == 2 {
            manifest.tables[0]
                .projection_columns
                .as_mut()
                .expect("columns")[0]
                .attribute_path = "nested.child".to_string();
        }
        engine
            .ensure_manifest(&manifest)
            .expect("register current manifest");
        let image = VecMap::from_iter([
            (
                "tenant".to_string(),
                StorageValue::S("tenant-1".to_string()),
            ),
            (
                "first".to_string(),
                StorageValue::S("first-value".to_string()),
            ),
            (
                "second".to_string(),
                StorageValue::S("second-value".to_string()),
            ),
            (
                "nested".to_string(),
                StorageValue::M(VecMap::from_iter([(
                    "child".to_string(),
                    StorageValue::S("nested-value".to_string()),
                )])),
            ),
        ]);
        engine
            .ingest_stream_record(
                &manifest,
                "projection_items",
                &[index as u8],
                insert_record(image),
            )
            .expect("ingest current projection");
        let rows = engine
            .query_unscoped_sql_json("SELECT selected FROM projection_items ORDER BY __id")
            .expect("stored projection");
        assert_eq!(rows[index]["selected"], *expected);
    }
}

#[test]
fn given_exact_storage_numbers_when_single_and_batch_projections_are_read_then_tagged_json_round_trips()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    let mut manifest = users_manifest();
    let table = &mut manifest.tables[0];
    table.document_column = None;
    table.projection_columns = Some(vec![ProjectionColumn {
        column_name: "payload".into(),
        attribute_path: "payload".into(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::StorageJson,
        }),
    }]);
    engine.ensure_manifest(&manifest).unwrap();
    let payload = StorageValue::L(vec![
        StorageValue::N("1234567890123456789.123456789012345678".into()),
        StorageValue::N("18446744073709551615".into()),
        StorageValue::N("-9223372036854775808".into()),
        StorageValue::S("12.00".into()),
        StorageValue::NULL(true),
    ]);
    let record = StorageStreamRecord {
        sequence_number: "exact-storage".into(),
        keys: VecMap::new(),
        old_image: None,
        new_image: Some(VecMap::from_iter([("payload".into(), payload.clone())])),
    };
    engine
        .ingest_stream_record(&manifest, "users", b"single", record.clone())
        .unwrap();
    engine
        .ingest_stream_record_batch(
            &manifest,
            vec![StreamRecordBatchItem {
                analytics_table_name: "users".into(),
                source_table_name: "tenant_01".into(),
                shard_id: None,
                system_position: None,
                record_key: b"batch".to_vec(),
                record,
            }],
        )
        .unwrap();
    let rows = engine
        .query_unscoped_sql_json("SELECT payload FROM users")
        .unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        let restored: StorageValue = serde_json::from_value(row["payload"].clone()).unwrap();
        assert_eq!(restored, payload);
    }
}
