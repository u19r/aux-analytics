use std::{fs, path::PathBuf};

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, PrivacyPolicy, ProjectionColumn,
    RowExpansion, RowIdentity, RowPathSegment, RowProjectionField, SourcePosition, StorageItem,
    StorageStreamRecord, StorageValue, TenantSelector,
};
use analytics_fixtures::{user_item, users_manifest};
use config::{CatalogType, StorageBackend};
use duckdb::types::Value;
use storage_types::{SystemStreamCheckpointToken, SystemStreamPosition, SystemStreamScope, VecMap};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, RecordCheckpoint,
    SourceBatchRecord, SourceEffectAuthority, SourceEffectDisposition, SourceEffectRequest,
    SourceEffectRequestInput, SourceEffectScope, StreamRecordBatchItem, manifest_digest,
};

const SOURCE_TABLE_NAME: &str = "tenant_01";
const ANALYTICS_TABLE_NAME: &str = "users";
const SHARD_ID: &str = "source-shard";

#[test]
fn given_pending_system_writes_when_delete_and_recreate_share_batch_then_order_and_outcomes_are_preserved()
 {
    let (engine, mut manifest) = memory_engine();
    manifest.tables[0].row_identity = RowIdentity::StreamKeys;
    let scope = system_scope(1);
    apply_system(
        &engine,
        &manifest,
        authority(1, 1),
        scope.clone(),
        None,
        system_checkpoint('a'),
        vec![
            system_source_record("deleted", "warm", system_position("shard", "lane", "1")),
            system_source_record("reborn", "warm", system_position("shard", "lane", "2")),
        ],
    )
    .unwrap();
    let result = apply_system(
        &engine,
        &manifest,
        authority(2, 2),
        scope,
        Some(system_checkpoint('a')),
        system_checkpoint('b'),
        vec![
            system_source_record("deleted", "pending", system_position("shard", "lane", "3")),
            system_record_with_images(
                ANALYTICS_TABLE_NAME,
                SOURCE_TABLE_NAME,
                "deleted",
                Some(user_item("deleted", "pending@example.test", "org-a")),
                None,
                system_position("shard", "lane", "4"),
            ),
            system_record_with_images(
                ANALYTICS_TABLE_NAME,
                SOURCE_TABLE_NAME,
                "reborn",
                Some(user_item("reborn", "warm@example.test", "org-a")),
                None,
                system_position("shard", "lane", "5"),
            ),
            system_source_record("reborn", "final", system_position("shard", "lane", "6")),
        ],
    )
    .unwrap();
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT email FROM users")
            .unwrap(),
        [serde_json::json!({"email":"final@example.test"})]
    );
    let SourceEffectDisposition::Applied { summary } = result else {
        panic!("new batch must apply")
    };
    assert_eq!(summary.inserted(), 1);
    assert_eq!(summary.updated(), 1);
    assert_eq!(summary.deleted(), 2);
}

#[test]
#[ignore = "paired native system-source update throughput measurement"]
fn given_repeated_system_updates_when_native_batch_commits_then_latest_rows_and_positions_are_preserved()
 {
    let directory = tempfile::tempdir().unwrap();
    let (backend, _) = sqlite_ducklake_backend(directory.path());
    let engine = AnalyticsEngine::connect(&backend).unwrap();
    let mut manifest = users_manifest();
    manifest.tables[0].row_identity = RowIdentity::StreamKeys;
    let template = manifest.tables[0].clone();
    for index in 1..8 {
        let mut table = template.clone();
        table.analytics_table_name = format!("view_{index}");
        if index >= 2 {
            table.condition_expression = Some("#id = :never".into());
            table.expression_attribute_names =
                Some(VecMap::from_iter([("#id".into(), "user_id".into())]));
            table.expression_attribute_values = Some(VecMap::from_iter([(
                ":never".into(),
                StorageValue::S("not-a-key".into()),
            )]));
        }
        manifest.tables.push(table);
    }
    engine.ensure_manifest(&manifest).unwrap();
    let started = std::time::Instant::now();
    let mut previous = None;
    for batch in 0..4_u8 {
        let mut records = Vec::new();
        for index in 0..64 {
            let offset = usize::from(batch) * 64 + index;
            for table in &manifest.tables {
                let mut record = system_source_record(
                    &format!("key-{}", index % 8),
                    &format!("value-{offset}"),
                    system_position("shard", "lane", &format!("{offset:020}")),
                );
                record
                    .item
                    .analytics_table_name
                    .clone_from(&table.analytics_table_name);
                records.push(record);
            }
        }
        let checkpoint = system_checkpoint(char::from(b'a' + batch));
        let result = apply_system(
            &engine,
            &manifest,
            authority(batch + 1, u64::from(batch) + 1),
            system_scope(1),
            previous,
            checkpoint.clone(),
            records,
        )
        .unwrap();
        assert!(matches!(result, SourceEffectDisposition::Applied { .. }));
        previous = Some(checkpoint);
    }
    let elapsed = started.elapsed();
    for (index, table) in manifest.tables.iter().enumerate() {
        let rows = engine
            .query_unscoped_sql_json(&format!(
                "SELECT email FROM {} ORDER BY email",
                table.analytics_table_name
            ))
            .unwrap();
        let expected = if index < 2 {
            (248..256)
                .map(|index| serde_json::json!({"email":format!("value-{index}@example.test")}))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        assert_eq!(rows, expected);
    }
    println!(
        "SYSTEM_SOURCE_UPDATE_SAMPLE \
         {{\"logical_records\":256,\"destinations\":8,\"elapsed_ns\":{}}}",
        elapsed.as_nanos()
    );
}

#[test]
fn given_existing_temporary_owner_table_when_system_batch_staging_collides_then_table_and_durable_state_are_preserved()
 {
    let (engine, manifest) = memory_engine();
    engine
        .connection()
        .execute_batch(
            "CREATE TEMP TABLE __analytics_source_record_states AS
         SELECT * FROM main.__analytics_source_record_states WHERE FALSE;
         ALTER TABLE temp.main.__analytics_source_record_states ADD COLUMN sentinel INTEGER;
         INSERT INTO temp.main.__analytics_source_record_states (sentinel) VALUES (42)",
        )
        .unwrap();
    let before = system_destination_state(&engine).unwrap();
    assert!(
        apply_system(
            &engine,
            &manifest,
            authority(91, 1),
            system_scope(1),
            None,
            system_checkpoint('a'),
            vec![system_source_record(
                "user-1",
                "new",
                system_position("shard", "lane", "1")
            )],
        )
        .is_err()
    );
    let sentinel: i64 = engine
        .connection()
        .query_row(
            "SELECT sentinel FROM temp.main.__analytics_source_record_states",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sentinel, 42);
    assert_eq!(system_destination_state(&engine).unwrap(), before);
    engine
        .connection()
        .execute_batch("DROP TABLE temp.main.__analytics_source_record_states")
        .unwrap();
    assert!(matches!(
        apply_system(
            &engine,
            &manifest,
            authority(91, 1),
            system_scope(1),
            None,
            system_checkpoint('a'),
            vec![system_source_record(
                "user-1",
                "new",
                system_position("shard", "lane", "1")
            )],
        )
        .unwrap(),
        SourceEffectDisposition::Applied { .. }
    ));
}

#[derive(Debug, Eq, PartialEq)]
struct DestinationState {
    rows: Vec<serde_json::Value>,
    checkpoints: Vec<RecordCheckpoint>,
    failures: Vec<serde_json::Value>,
    authorities: Vec<serde_json::Value>,
    receipts: Vec<serde_json::Value>,
}

#[test]
fn given_runtime_retention_without_static_policy_when_mixed_lanes_apply_and_fail_then_expiry_order_replay_and_rollback_are_exact()
 {
    let (engine, mut manifest) = memory_engine();
    let mut orders = manifest.tables[0].clone();
    orders.analytics_table_name = "orders".into();
    manifest.tables.push(orders);
    engine.ensure_manifest(&manifest).unwrap();
    engine.ensure_retention_columns("users").unwrap();
    assert!(
        manifest
            .tables
            .iter()
            .all(|table| table.retention.is_none())
    );
    let mut order = source_record("order", "order");
    order.item.analytics_table_name = "orders".into();
    let records = vec![
        source_record("plain", "plain"),
        order,
        runtime_retention_record("retained", "1000"),
        source_record("unpadded", "unpadded"),
    ];
    let owner = authority(80, 80);
    assert!(matches!(
        apply(
            &engine,
            &manifest,
            owner.clone(),
            records.clone(),
            "retention-cursor"
        )
        .unwrap(),
        SourceEffectDisposition::Applied { .. }
    ));
    let expected = serde_json::json!([
        {"user_id": "plain", "email": "plain@example.test", "__expiry": null, "__missing_retention": null},
        {"user_id": "retained", "email": "retained@example.test", "__expiry": 1100, "__missing_retention": false},
        {"user_id": "unpadded", "email": "unpadded@example.test", "__expiry": null, "__missing_retention": null}
    ]);
    let select = "SELECT user_id, email, __expiry, __missing_retention FROM users ORDER BY user_id";
    assert_eq!(
        engine.query_unscoped_sql_json(select).unwrap().as_slice(),
        expected.as_array().unwrap()
    );
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT user_id, email FROM orders")
            .unwrap(),
        [serde_json::json!({"user_id": "order", "email": "order@example.test"})]
    );
    assert!(matches!(
        apply(&engine, &manifest, owner, records, "retention-cursor").unwrap(),
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
    let before = destination_state(&engine).unwrap();
    let mut late_order = source_record("late-order", "late-order");
    late_order.item.analytics_table_name = "orders".into();
    let error = apply_with_predecessor(
        &engine,
        &manifest,
        authority(81, 81),
        vec![
            source_record("late-plain", "late-plain"),
            late_order,
            runtime_retention_record("late-runtime", "1000"),
            runtime_retention_record("invalid", "invalid"),
        ],
        Some("retention-cursor"),
        "failed-retention-cursor",
    )
    .unwrap_err();
    assert!(matches!(
        error,
        AnalyticsEngineError::InvalidRetentionTimestamp(_)
    ));
    let after = destination_state(&engine).unwrap();
    assert_eq!(after.rows, before.rows);
    assert_eq!(after.checkpoints, before.checkpoints);
    assert_eq!(after.receipts, before.receipts);
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT user_id, email FROM orders")
            .unwrap(),
        [serde_json::json!({"user_id": "order", "email": "order@example.test"})]
    );
}

fn runtime_retention_record(key: &str, timestamp: &str) -> SourceBatchRecord {
    let mut record = source_record(key, key);
    record
        .item
        .record
        .new_image
        .as_mut()
        .unwrap()
        .insert("created_at_ms".into(), StorageValue::N(timestamp.into()));
    record.retention = Some(super::IngestRetention {
        period_ms: Some(100),
        timestamp: analytics_contract::RetentionTimestamp::Attribute {
            attribute_path: "created_at_ms".into(),
        },
        missing_retention: false,
    });
    record
}

#[test]
fn given_newer_owner_when_stale_worker_commits_then_all_destination_state_is_unchanged() {
    let (engine, manifest) = memory_engine();
    let newer = authority(11, 11);
    apply(
        &engine,
        &manifest,
        newer,
        vec![source_record("user-1", "newer")],
        "cursor-newer",
    )
    .expect("newer owner commit");
    let before = destination_state(&engine).expect("new owner state");

    let stale = apply(
        &engine,
        &manifest,
        authority(10, 10),
        vec![source_record("user-1", "stale")],
        "cursor-stale",
    )
    .expect("stale destination must return a disposition");

    assert_eq!(stale, SourceEffectDisposition::StaleFence);
    assert_eq!(destination_state(&engine).expect("stale state"), before);
}

#[test]
fn given_system_stream_fault_matrix_when_applied_then_state_is_atomic() {
    let (engine, manifest) = memory_engine();
    let scope = system_scope(7);
    let checkpoint = system_checkpoint('a');
    let records = vec![system_source_record(
        "user-1",
        "system",
        system_position("logical-shard", "lineage-a", "1"),
    )];

    let current_authority = authority(40, 10);
    let first = apply_system(
        &engine,
        &manifest,
        current_authority.clone(),
        scope.clone(),
        None,
        checkpoint.clone(),
        records.clone(),
    )
    .expect("system effect applies");
    let SourceEffectDisposition::Applied { summary } = first else {
        panic!("first system effect must apply");
    };
    assert_eq!(summary.inserted(), 1);
    assert_eq!(summary.checkpoint_count(), 1);
    let before = system_destination_state(&engine).expect("system destination state");
    assert_eq!(before.rows.len(), 1);
    assert_eq!(before.checkpoint, Some((scope.clone(), checkpoint.clone())));

    let replay = apply_system(
        &engine,
        &manifest,
        current_authority.clone(),
        scope.clone(),
        None,
        checkpoint.clone(),
        records.clone(),
    )
    .expect("exact system replay");
    assert!(matches!(
        replay,
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(
        system_destination_state(&engine).expect("replayed system state"),
        before
    );

    #[derive(Clone, Copy)]
    enum Fault {
        ChangedJobFence,
        StalePredecessor,
        ChangedContent,
        ChangedCheckpoint,
        ChangedScope,
    }
    let cases = [
        ("changed job fence", Fault::ChangedJobFence),
        ("stale predecessor", Fault::StalePredecessor),
        ("changed content", Fault::ChangedContent),
        ("changed checkpoint", Fault::ChangedCheckpoint),
        ("changed scope", Fault::ChangedScope),
    ];
    for (label, fault) in cases {
        let result = match fault {
            Fault::ChangedJobFence => apply_system(
                &engine,
                &manifest,
                authority(40, 9),
                scope.clone(),
                Some(checkpoint.clone()),
                system_checkpoint('b'),
                vec![system_source_record(
                    "user-1",
                    "changed-job-fence",
                    system_position("logical-shard", "lineage-a", "2"),
                )],
            ),
            Fault::StalePredecessor => apply_system(
                &engine,
                &manifest,
                authority(42, 11),
                scope.clone(),
                Some(system_checkpoint('f')),
                system_checkpoint('b'),
                vec![system_source_record(
                    "user-1",
                    "stale-predecessor",
                    system_position("logical-shard", "lineage-a", "2"),
                )],
            ),
            Fault::ChangedContent => {
                let mut changed = records.clone();
                changed[0].item.record.new_image = Some(VecMap::new());
                apply_system(
                    &engine,
                    &manifest,
                    current_authority.clone(),
                    scope.clone(),
                    None,
                    checkpoint.clone(),
                    changed,
                )
            }
            Fault::ChangedCheckpoint => apply_system(
                &engine,
                &manifest,
                current_authority.clone(),
                scope.clone(),
                None,
                system_checkpoint('b'),
                records.clone(),
            ),
            Fault::ChangedScope => apply_system(
                &engine,
                &manifest,
                current_authority.clone(),
                system_scope(8),
                None,
                checkpoint.clone(),
                records.clone(),
            ),
        };
        match fault {
            Fault::StalePredecessor => {
                assert_eq!(result.expect(label), SourceEffectDisposition::StaleFence);
            }
            Fault::ChangedJobFence
            | Fault::ChangedContent
            | Fault::ChangedCheckpoint
            | Fault::ChangedScope => {
                assert!(matches!(
                    result.expect_err(label),
                    AnalyticsEngineError::ConflictingSourceEffectReceipt
                ));
            }
        }
        assert_eq!(
            system_destination_state(&engine).expect("fault state"),
            before,
            "{label} must leave rows, frontier, fence, and receipt unchanged"
        );
    }

    assert_system_contract_failure_rolls_back();
}

#[test]
fn given_source_position_document_output_when_system_stream_is_applied_then_prefix_is_unique() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut manifest = users_manifest();
    manifest.tables[0].document_column = Some("__source_position".to_string());
    engine.ensure_manifest(&manifest).expect("ensure manifest");

    let result = apply_system(
        &engine,
        &manifest,
        authority(49, 1),
        system_scope(7),
        None,
        system_checkpoint('a'),
        vec![system_source_record(
            "user-1",
            "system",
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("system stream ingest");
    assert!(matches!(result, SourceEffectDisposition::Applied { .. }));

    let rows = engine
        .query_unscoped_sql_json("SELECT user_id, email FROM users")
        .expect("query system row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["email"], "system@example.test");
}

#[test]
fn given_retried_system_job_when_next_job_has_lower_fence_then_predecessor_orders_effects() {
    let (engine, manifest) = memory_engine();
    let scope = system_scope(7);
    for (id, fence, previous, checkpoint, offset) in
        [(70, 2, None, 'a', "1"), (71, 1, Some('a'), 'b', "2")]
    {
        let job = authority(id, fence);
        let records = vec![system_source_record(
            "user-1",
            offset,
            system_position("logical-shard", "lineage-a", offset),
        )];
        let apply_page = || {
            apply_system(
                &engine,
                &manifest,
                job.clone(),
                scope.clone(),
                previous.map(system_checkpoint),
                system_checkpoint(checkpoint),
                records.clone(),
            )
        };
        assert!(matches!(
            apply_page().expect("next job must apply regardless of the previous job fence"),
            SourceEffectDisposition::Applied { .. }
        ));
        let before = system_destination_state(&engine).expect("committed page state");
        assert_eq!(
            before.checkpoint,
            Some((scope.clone(), system_checkpoint(checkpoint)))
        );
        assert!(matches!(
            apply_page().expect("same job replay"),
            SourceEffectDisposition::AlreadyApplied { .. }
        ));
        for stale_fence in [1, 3] {
            let stale = apply_system(
                &engine,
                &manifest,
                authority(id + 10, stale_fence),
                scope.clone(),
                previous.map(system_checkpoint),
                system_checkpoint('c'),
                records.clone(),
            )
            .expect("obsolete predecessor disposition");
            assert_eq!(stale, SourceEffectDisposition::StaleFence);
        }
        assert_eq!(
            system_destination_state(&engine).expect("unchanged page"),
            before
        );
    }
}

#[test]
fn given_system_origin_request_after_durable_checkpoint_when_applied_then_predecessor_is_required()
{
    let (engine, manifest) = memory_engine();
    let scope = system_scope(7);
    apply_system(
        &engine,
        &manifest,
        authority(47, 1),
        scope.clone(),
        None,
        system_checkpoint('a'),
        vec![system_source_record(
            "user-1",
            "first",
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("origin system effect");

    let request = SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest: &manifest,
        manifest_digest: manifest_digest(&manifest),
        authority: authority(48, 2),
        scope: SourceEffectScope::try_new_system(scope, None, system_checkpoint('b'))
            .expect("system effect scope"),
        records: vec![system_source_record(
            "user-2",
            "second",
            system_position("logical-shard", "lineage-a", "2"),
        )],
        checkpoints: Vec::new(),
        privacy_policy: None,
    })
    .expect("system effect request");
    let result = engine
        .ingest_fenced_source_batch(request)
        .expect("system effect disposition");

    assert_eq!(result, SourceEffectDisposition::StaleFence);
}

#[test]
fn given_system_record_with_mismatched_logical_source_table_when_applied_then_destination_is_untouched()
 {
    let (engine, manifest) = memory_engine();
    let mut record = system_source_record(
        "user-1",
        "system",
        system_position("logical-shard", "lineage-a", "1"),
    );
    record.item.source_table_name = "other-source".to_string();

    let error = apply_system(
        &engine,
        &manifest,
        authority(41, 1),
        system_scope(7),
        None,
        system_checkpoint('a'),
        vec![record],
    )
    .expect_err("logical source table must match the selected registration");
    assert!(matches!(
        error,
        AnalyticsEngineError::RecordDoesNotMatchRegistration(name) if name == ANALYTICS_TABLE_NAME
    ));
    let state = system_destination_state(&engine).expect("mismatched source state");
    assert!(state.rows.is_empty());
    assert!(state.authorities.is_empty());
    assert!(state.receipts.is_empty());
    assert!(state.checkpoint.is_none());
}

fn assert_system_contract_failure_rolls_back() {
    let (engine, mut manifest) = memory_engine();
    manifest.tables[0].tenant_id = None;
    manifest.tables[0].tenant_selector = TenantSelector::Attribute {
        attribute_name: "user_id".to_string(),
    };
    engine
        .ensure_manifest(&manifest)
        .expect("manifest with required system tenant attribute");
    let mut poison = user_item("poison", "poison@example.test", "org-a");
    poison.remove("user_id");
    let error = apply_system(
        &engine,
        &manifest,
        authority(41, 1),
        system_scope(7),
        None,
        system_checkpoint('c'),
        vec![
            system_source_record(
                "user-1",
                "valid",
                system_position("logical-shard", "lineage-a", "1"),
            ),
            system_source_record_with_item(
                "poison",
                poison,
                system_position("logical-shard", "lineage-a", "2"),
            ),
        ],
    )
    .expect_err("system contract poison must fail closed");
    assert!(matches!(error, AnalyticsEngineError::MissingAttribute(_)));
    let state = system_destination_state(&engine).expect("rolled back system state");
    assert!(state.rows.is_empty());
    assert!(state.checkpoint.is_none());
    assert!(state.authorities.is_empty());
    assert!(state.receipts.is_empty());
}

#[test]
fn given_system_records_from_distinct_lineages_when_applied_then_metadata_keeps_each_lane_identity()
{
    let (engine, manifest) = memory_engine();
    let scope = system_scope(7);
    apply_system(
        &engine,
        &manifest,
        authority(42, 1),
        scope.clone(),
        None,
        system_checkpoint('d'),
        vec![system_source_record(
            "user-1",
            "lane-a",
            system_position("logical-shard", "lineage-a", "9"),
        )],
    )
    .expect("first logical lane");
    apply_system(
        &engine,
        &manifest,
        authority(43, 2),
        scope,
        Some(system_checkpoint('d')),
        system_checkpoint('e'),
        vec![system_source_record(
            "user-2",
            "lane-b",
            system_position("logical-shard", "lineage-b", "1"),
        )],
    )
    .expect("second logical lane");

    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT source_key_digest, source_position_json
             FROM __analytics_source_record_states
             ORDER BY source_key_digest",
        )
        .expect("logical lane metadata");
    assert_eq!(metadata.len(), 2);
    for (lineage, offset) in [("lineage-a", "9"), ("lineage-b", "1")] {
        let position = metadata
            .iter()
            .map(|row| &row["source_position_json"])
            .find(|position| position["position"]["LineageId"] == lineage)
            .expect("logical lane position");
        assert_eq!(position["source_scope"]["ScopeId"], "aux-system");
        assert_eq!(position["position"]["Offset"], offset);
    }
}

#[test]
fn given_system_prefix_registration_when_same_key_arrives_from_two_source_tables_then_owner_state_is_isolated()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut table = users_manifest()
        .tables
        .into_iter()
        .next()
        .expect("users table");
    table.source_table_name = "tenant_".to_string();
    table.source_table_name_prefix = Some("tenant_".to_string());
    let manifest = AnalyticsManifest::new(vec![table]);
    engine
        .ensure_manifest(&manifest)
        .expect("ensure prefix manifest");
    let mut first = system_source_record(
        "physical-key",
        "first",
        system_position("logical-shard", "lineage-a", "1"),
    );
    first.item.source_table_name = "tenant_a".to_string();
    let mut second = system_source_record(
        "physical-key",
        "second",
        system_position("logical-shard", "lineage-a", "2"),
    );
    second.item.source_table_name = "tenant_b".to_string();

    let result = apply_system(
        &engine,
        &manifest,
        authority(78, 1),
        system_scope(12),
        None,
        system_checkpoint('g'),
        vec![first, second],
    )
    .expect("prefix System batch");
    assert!(matches!(result, SourceEffectDisposition::Applied { .. }));
    let rows = engine
        .query_unscoped_sql_json("SELECT table_name, tenant_id FROM users ORDER BY table_name")
        .expect("prefix rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["table_name"], "tenant_a");
    assert_eq!(rows[1]["table_name"], "tenant_b");
    let owners = engine
        .query_unscoped_sql_json(
            "SELECT source_table_name, source_key_digest
             FROM __analytics_source_record_states
             ORDER BY source_table_name",
        )
        .expect("prefix owner state");
    assert_eq!(owners.len(), 2);
    assert_eq!(owners[0]["source_table_name"], "tenant_a");
    assert_eq!(owners[1]["source_table_name"], "tenant_b");
    assert_ne!(
        owners[0]["source_key_digest"],
        owners[1]["source_key_digest"]
    );
}

#[test]
fn given_system_record_replay_with_new_execution_when_offset_is_equal_or_stale_then_row_is_unchanged()
 {
    let (engine, manifest) = memory_engine();
    let scope = system_scope(7);
    apply_system(
        &engine,
        &manifest,
        authority(44, 1),
        scope.clone(),
        None,
        system_checkpoint('a'),
        vec![system_source_record(
            "user-1",
            "accepted",
            system_position("logical-shard", "lineage-a", "9"),
        )],
    )
    .expect("initial system record");

    let mut previous_checkpoint = system_checkpoint('a');
    for (seed, fence, suffix, offset) in [(45, 2, "stale", "8"), (46, 3, "equal", "9")] {
        let checkpoint = system_checkpoint(char::from(b'a' + fence as u8 - 1));
        let result = apply_system(
            &engine,
            &manifest,
            authority(seed, fence),
            scope.clone(),
            Some(previous_checkpoint),
            checkpoint.clone(),
            vec![system_source_record(
                "user-1",
                suffix,
                system_position("logical-shard", "lineage-a", offset),
            )],
        )
        .expect("new system execution");
        let SourceEffectDisposition::Applied { summary } = result else {
            panic!("new system execution should be applied");
        };
        assert_eq!(summary.skipped(), 1);
        previous_checkpoint = checkpoint;
    }

    let rows = engine
        .query_unscoped_sql_json("SELECT email FROM users ORDER BY __id")
        .expect("system replay row");
    assert_eq!(
        rows,
        vec![serde_json::json!({"email": "accepted@example.test"})]
    );
    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT source_position_json
             FROM __analytics_source_record_states",
        )
        .expect("system replay metadata");
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0]["source_position_json"]["position"]["Offset"],
        "9"
    );
}

#[test]
fn given_same_owner_in_two_incomparable_lanes_when_applied_in_one_system_batch_then_batch_rolls_back()
 {
    let (engine, manifest) = memory_engine();
    let scope = system_scope(8);
    apply_system(
        &engine,
        &manifest,
        authority(71, 1),
        scope.clone(),
        None,
        system_checkpoint('a'),
        vec![system_source_record(
            "user-1",
            "initial",
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("initial owner state");
    let before = system_destination_state(&engine).expect("initial system state");
    let owner_before = engine
        .query_unscoped_sql_json(
            "SELECT analytics_table_name, source_table_name, source_key_digest,
                    source_position_json, child_identities_json
             FROM __analytics_source_record_states
             ORDER BY source_key_digest",
        )
        .expect("initial owner row");

    let error = apply_system(
        &engine,
        &manifest,
        authority(72, 2),
        scope,
        Some(system_checkpoint('a')),
        system_checkpoint('b'),
        vec![
            system_source_record(
                "user-1",
                "same-lane",
                system_position("logical-shard", "lineage-a", "2"),
            ),
            system_source_record(
                "user-1",
                "sibling-lane",
                system_position("logical-shard", "lineage-b", "1"),
            ),
        ],
    )
    .expect_err("second incomparable lane must reject the whole system batch");
    assert!(matches!(
        error,
        AnalyticsEngineError::SystemSourceRecordRequiresValidation
    ));
    assert_eq!(
        system_destination_state(&engine).expect("rolled-back system state"),
        before
    );
    assert_eq!(
        engine
            .query_unscoped_sql_json(
                "SELECT analytics_table_name, source_table_name, source_key_digest,
                        source_position_json, child_identities_json
                 FROM __analytics_source_record_states
                 ORDER BY source_key_digest",
            )
            .expect("rolled-back owner row"),
        owner_before
    );
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM analytics_source_batch_failures")
            .expect("non-contract lane failure audit")
            .is_empty()
    );
}

#[test]
fn given_duplicate_system_owner_state_when_applied_then_destination_and_fence_are_unchanged() {
    let (engine, manifest) = memory_engine();
    let scope = system_scope(9);
    apply_system(
        &engine,
        &manifest,
        authority(73, 1),
        scope.clone(),
        None,
        system_checkpoint('c'),
        vec![system_source_record(
            "user-1",
            "initial",
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("initial owner state");
    engine
        .connection()
        .execute(
            "INSERT INTO __analytics_source_record_states
             SELECT analytics_table_name, source_table_name, source_key_digest,
                    source_position_json, child_identities_json, updated_at_ms
             FROM __analytics_source_record_states
             WHERE analytics_table_name = 'users'
               AND source_table_name = 'tenant_01'",
            [],
        )
        .expect("duplicate owner state");
    let before = system_destination_state(&engine).expect("corrupt system state");
    let owner_before = engine
        .query_unscoped_sql_json(
            "SELECT analytics_table_name, source_table_name, source_key_digest,
                    source_position_json, child_identities_json
             FROM __analytics_source_record_states
             ORDER BY source_key_digest, updated_at_ms",
        )
        .expect("duplicate owner rows");
    assert_eq!(owner_before.len(), 2);

    let error = apply_system(
        &engine,
        &manifest,
        authority(74, 2),
        scope,
        Some(system_checkpoint('c')),
        system_checkpoint('d'),
        vec![system_source_record(
            "user-1",
            "later",
            system_position("logical-shard", "lineage-a", "2"),
        )],
    )
    .expect_err("duplicate owner state must fail closed");
    assert!(matches!(
        error,
        AnalyticsEngineError::CorruptSystemSourceRecordState(_)
    ));
    assert_eq!(
        system_destination_state(&engine).expect("unchanged system state"),
        before
    );
    assert_eq!(
        engine
            .query_unscoped_sql_json(
                "SELECT analytics_table_name, source_table_name, source_key_digest,
                        source_position_json, child_identities_json
                 FROM __analytics_source_record_states
                 ORDER BY source_key_digest, updated_at_ms",
            )
            .expect("unchanged duplicate owner rows"),
        owner_before
    );
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM analytics_source_batch_failures")
            .expect("corrupt state must not be quarantined")
            .is_empty()
    );
}

#[test]
fn given_corrupt_system_owner_when_next_page_arrives_then_all_state_is_unchanged() {
    let wrong_kind = serde_json::to_string(&SourcePosition::RawBackupObjectOffset {
        object_id: "wrong-kind".to_string(),
        offset: 1,
    })
    .unwrap();
    let wrong_scope = serde_json::to_string(&SourcePosition::SystemStream {
        source_scope: system_scope(999),
        position: system_position("logical-shard", "lineage-a", "1"),
    })
    .unwrap();
    for (column, value) in [
        ("source_position_json", wrong_kind.as_str()),
        ("source_position_json", wrong_scope.as_str()),
        ("source_position_json", "{}"),
        ("child_identities_json", "{}"),
        (
            "child_identities_json",
            r#"[{"table_name":"other-table","tenant_id":"org-a","row_id":"id","row_visible":true}]"#,
        ),
        (
            "child_identities_json",
            r#"[{"table_name":"tenant_01","tenant_id":"org-a","row_id":"id","row_visible":true},{"table_name":"tenant_01","tenant_id":"org-a","row_id":"id","row_visible":false}]"#,
        ),
    ] {
        let (engine, manifest) = memory_engine();
        let scope = system_scope(9);
        apply_system(
            &engine,
            &manifest,
            authority(73, 1),
            scope.clone(),
            None,
            system_checkpoint('c'),
            vec![system_source_record(
                "user-1",
                "initial",
                system_position("logical-shard", "lineage-a", "1"),
            )],
        )
        .unwrap();
        engine
            .connection()
            .execute(
                &format!("UPDATE __analytics_source_record_states SET {column} = CAST(? AS JSON)"),
                [value],
            )
            .unwrap();
        let before = system_destination_state(&engine).unwrap();
        let owner_before = engine
            .query_unscoped_sql_json("SELECT * FROM __analytics_source_record_states")
            .unwrap();
        let error = apply_system(
            &engine,
            &manifest,
            authority(74, 2),
            scope,
            Some(system_checkpoint('c')),
            system_checkpoint('d'),
            vec![system_source_record(
                "user-1",
                "later",
                system_position("logical-shard", "lineage-a", "2"),
            )],
        )
        .expect_err("corrupt destination state must fail closed");
        assert!(
            matches!(
                error,
                AnalyticsEngineError::CorruptSystemSourceRecordState(_)
            ),
            "{column}: {error}"
        );
        assert_eq!(system_destination_state(&engine).unwrap(), before);
        assert_eq!(
            engine
                .query_unscoped_sql_json("SELECT * FROM __analytics_source_record_states")
                .unwrap(),
            owner_before
        );
        assert!(
            engine
                .query_unscoped_sql_json("SELECT * FROM analytics_source_batch_failures")
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn given_invalid_system_physical_key_when_applied_then_failure_is_quarantined_and_repair_retries() {
    let (engine, manifest) = memory_engine();
    let scope = system_scope(10);
    let position = system_position("logical-shard", "lineage-a", "1");
    let mut invalid = system_source_record("user-1", "invalid", position.clone());
    invalid.item.record.keys = VecMap::new();
    let error = apply_system(
        &engine,
        &manifest,
        authority(75, 1),
        scope.clone(),
        None,
        system_checkpoint('e'),
        vec![invalid],
    )
    .expect_err("empty System physical keys must fail closed");
    assert!(matches!(
        error,
        AnalyticsEngineError::InvalidSystemSourceKey(_)
    ));
    let state = system_destination_state(&engine).expect("invalid key state");
    assert!(state.rows.is_empty());
    assert!(state.checkpoint.is_none());
    assert!(state.authorities.is_empty());
    assert!(state.receipts.is_empty());
    let logical_key = logical_record_key(&position);
    let failures = engine
        .query_unscoped_sql_json(
            "SELECT source_table_name, shard_id, record_position, error_class,
                    repaired_at_ms
             FROM analytics_source_batch_failures",
        )
        .expect("invalid key failure audit");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0]["source_table_name"], SOURCE_TABLE_NAME);
    assert_eq!(failures[0]["shard_id"], "logical-shard");
    assert_eq!(failures[0]["record_position"], logical_key);
    assert_eq!(failures[0]["error_class"], "invalid_system_source_key");
    assert!(failures[0]["repaired_at_ms"].is_null());
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM __analytics_source_record_states")
            .expect("no owner state for invalid key")
            .is_empty()
    );

    assert!(
        engine
            .repair_source_batch_failure(SOURCE_TABLE_NAME, "logical-shard", logical_key.as_str())
            .expect("repair invalid key failure")
    );
    let retry = apply_system(
        &engine,
        &manifest,
        authority(76, 2),
        scope,
        None,
        system_checkpoint('e'),
        vec![system_source_record("user-1", "repaired", position)],
    )
    .expect("repaired physical key retry");
    assert!(matches!(retry, SourceEffectDisposition::Applied { .. }));
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT email FROM users")
            .expect("repaired row"),
        vec![serde_json::json!({"email": "repaired@example.test"})]
    );
    let failures = engine
        .query_unscoped_sql_json("SELECT repaired_at_ms FROM analytics_source_batch_failures")
        .expect("repaired failure audit");
    assert_eq!(failures.len(), 1);
    assert!(!failures[0]["repaired_at_ms"].is_null());
}

#[test]
fn given_privacy_denies_system_key_when_stream_keys_identity_is_used_then_raw_keys_remain_private_and_distinct()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut manifest = users_manifest();
    manifest.tables[0].row_identity = RowIdentity::StreamKeys;
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let policy = PrivacyPolicy::try_new("deny-pk")
        .expect("privacy policy")
        .with_denied_key_name("pk");
    let scope = system_scope(11);
    let request = SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest: &manifest,
        manifest_digest: manifest_digest(&manifest),
        authority: authority(77, 1),
        scope: SourceEffectScope::try_new_system(scope, None, system_checkpoint('f')).unwrap(),
        records: vec![
            system_source_record_with_item(
                "physical-a",
                user_item("user-a", "first@example.test", "org-a"),
                system_position("logical-shard", "lineage-a", "1"),
            ),
            system_source_record_with_item(
                "physical-b",
                user_item("user-b", "second@example.test", "org-a"),
                system_position("logical-shard", "lineage-a", "2"),
            ),
        ],
        checkpoints: Vec::new(),
        privacy_policy: Some(&policy),
    })
    .expect("privacy System request");
    let outcome = engine
        .ingest_fenced_source_batch(request)
        .expect("privacy filtered System batch");
    let SourceEffectDisposition::Applied { summary } = outcome else {
        panic!("privacy System effect must apply");
    };
    assert_eq!(summary.inserted(), 2);
    let rows = engine
        .query_unscoped_sql_json("SELECT __id, item FROM users ORDER BY __id")
        .expect("distinct StreamKeys rows");
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0]["__id"], rows[1]["__id"]);
    let serialized_rows = serde_json::to_string(&rows).expect("serialize rows");
    assert!(!serialized_rows.contains("physical-a"));
    assert!(!serialized_rows.contains("physical-b"));
    for private_key in [b"physical-a", b"physical-b"] {
        let encoded = private_key
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert!(
            !serialized_rows.contains(&encoded),
            "private key must not be recoverable from the row identity"
        );
    }
    let owner = engine
        .query_unscoped_sql_json(
            "SELECT source_key_digest, source_position_json
             FROM __analytics_source_record_states
             ORDER BY source_key_digest",
        )
        .expect("private owner state");
    assert_eq!(owner.len(), 2);
    let serialized_owner = serde_json::to_string(&owner).expect("serialize owner state");
    assert!(!serialized_owner.contains("physical-a"));
    assert!(!serialized_owner.contains("physical-b"));
}

#[test]
fn given_system_tombstone_then_filtered_create_when_delayed_match_arrives_then_metadata_blocks_revival()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = filtered_system_manifest(false, RowIdentity::RecordKey);
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let scope = system_scope(31);

    apply_system(
        &engine,
        &manifest,
        authority(51, 1),
        scope.clone(),
        None,
        system_checkpoint('a'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            None,
            Some(user_item("user-1", "p1", "org-a")),
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("matching insert");
    let old = user_item("user-1", "p1", "org-a");
    apply_system(
        &engine,
        &manifest,
        authority(52, 2),
        scope.clone(),
        Some(system_checkpoint('a')),
        system_checkpoint('b'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            Some(old),
            None,
            system_position("logical-shard", "lineage-a", "2"),
        )],
    )
    .expect("tombstone");
    apply_system(
        &engine,
        &manifest,
        authority(53, 3),
        scope.clone(),
        Some(system_checkpoint('b')),
        system_checkpoint('c'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            None,
            Some(user_item("other", "p4", "org-a")),
            system_position("logical-shard", "lineage-a", "4"),
        )],
    )
    .expect("filtered create");

    let delayed = apply_system(
        &engine,
        &manifest,
        authority(54, 4),
        scope,
        Some(system_checkpoint('c')),
        system_checkpoint('d'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            None,
            Some(user_item("user-1", "stale", "org-a")),
            system_position("logical-shard", "lineage-a", "3"),
        )],
    )
    .expect("delayed matching create");
    let SourceEffectDisposition::Applied { summary } = delayed else {
        panic!("delayed source effect should be applied");
    };
    assert_eq!(summary.skipped(), 1);
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM users")
            .expect("query rows")
            .is_empty()
    );
    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT child_identities_json, source_position_json
             FROM __analytics_source_record_states",
        )
        .expect("query metadata");
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0]["child_identities_json"][0]["row_visible"],
        false
    );
    assert_eq!(
        metadata[0]["source_position_json"]["position"]["Offset"],
        "4"
    );
}

#[test]
fn given_system_append_only_row_when_delete_is_ignored_then_delayed_match_preserves_row_and_position()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = filtered_system_manifest(true, RowIdentity::RecordKey);
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let scope = system_scope(32);
    let initial = user_item("user-1", "p1", "org-a");
    apply_system(
        &engine,
        &manifest,
        authority(55, 1),
        scope.clone(),
        None,
        system_checkpoint('e'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            None,
            Some(initial.clone()),
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("matching insert");
    apply_system(
        &engine,
        &manifest,
        authority(56, 2),
        scope.clone(),
        Some(system_checkpoint('e')),
        system_checkpoint('f'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            Some(initial),
            None,
            system_position("logical-shard", "lineage-a", "3"),
        )],
    )
    .expect("ignored delete");
    let filtered = user_item("other", "filtered", "org-a");
    apply_system(
        &engine,
        &manifest,
        authority(57, 3),
        scope.clone(),
        Some(system_checkpoint('f')),
        system_checkpoint('g'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            Some(filtered.clone()),
            Some(filtered),
            system_position("logical-shard", "lineage-a", "4"),
        )],
    )
    .expect("ignored filtered update");
    let delayed = apply_system(
        &engine,
        &manifest,
        authority(58, 4),
        scope,
        Some(system_checkpoint('g')),
        system_checkpoint('h'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            None,
            Some(user_item("user-1", "stale", "org-a")),
            system_position("logical-shard", "lineage-a", "2"),
        )],
    )
    .expect("delayed matching create");
    let SourceEffectDisposition::Applied { summary } = delayed else {
        panic!("delayed source effect should be applied");
    };
    assert_eq!(summary.skipped(), 1);
    let rows = engine
        .query_unscoped_sql_json("SELECT email FROM users")
        .expect("query rows");
    assert_eq!(rows, vec![serde_json::json!({"email": "p1"})]);
    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT child_identities_json, source_position_json
             FROM __analytics_source_record_states",
        )
        .expect("query metadata");
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0]["child_identities_json"][0]["row_visible"], true);
    assert_eq!(
        metadata[0]["source_position_json"]["position"]["Offset"],
        "4"
    );
}

#[test]
fn given_system_tombstone_then_filtered_delete_when_delayed_match_arrives_then_metadata_blocks_revival()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = filtered_system_manifest(false, RowIdentity::RecordKey);
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let scope = system_scope(37);
    let matching = user_item("user-1", "p1", "org-a");
    apply_system(
        &engine,
        &manifest,
        authority(67, 1),
        scope.clone(),
        None,
        system_checkpoint('q'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            None,
            Some(matching.clone()),
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("matching insert");
    apply_system(
        &engine,
        &manifest,
        authority(68, 2),
        scope.clone(),
        Some(system_checkpoint('q')),
        system_checkpoint('r'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            Some(matching),
            None,
            system_position("logical-shard", "lineage-a", "2"),
        )],
    )
    .expect("tombstone");
    let filtered = user_item("other", "filtered", "org-a");
    apply_system(
        &engine,
        &manifest,
        authority(69, 3),
        scope.clone(),
        Some(system_checkpoint('r')),
        system_checkpoint('s'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            Some(filtered),
            None,
            system_position("logical-shard", "lineage-a", "4"),
        )],
    )
    .expect("filtered delete");
    let delayed = apply_system(
        &engine,
        &manifest,
        authority(70, 4),
        scope,
        Some(system_checkpoint('s')),
        system_checkpoint('t'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "user-1",
            None,
            Some(user_item("user-1", "stale", "org-a")),
            system_position("logical-shard", "lineage-a", "3"),
        )],
    )
    .expect("delayed matching create");
    let SourceEffectDisposition::Applied { summary } = delayed else {
        panic!("delayed source effect should be applied");
    };
    assert_eq!(summary.skipped(), 1);
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM users")
            .expect("query rows")
            .is_empty()
    );
    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT child_identities_json, source_position_json
             FROM __analytics_source_record_states",
        )
        .expect("query metadata");
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0]["child_identities_json"][0]["row_visible"],
        false
    );
    assert_eq!(
        metadata[0]["source_position_json"]["position"]["Offset"],
        "4"
    );
}

#[test]
fn given_system_relocation_with_stale_old_identity_when_applying_source_batch_then_destination_is_unchanged()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let mut manifest = users_manifest();
    manifest.tables[0].row_identity = RowIdentity::Attribute {
        attribute_name: "user_id".to_string(),
    };
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let scope = system_scope(33);
    let old = user_item("old-id", "old", "org-a");
    apply_system(
        &engine,
        &manifest,
        authority(58, 1),
        scope.clone(),
        None,
        system_checkpoint('h'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "record-1",
            None,
            Some(old.clone()),
            system_position("logical-shard", "lineage-a", "5"),
        )],
    )
    .expect("initial row");
    let delayed = apply_system(
        &engine,
        &manifest,
        authority(59, 2),
        scope,
        Some(system_checkpoint('h')),
        system_checkpoint('i'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "record-1",
            Some(old),
            Some(user_item("new-id", "new", "org-a")),
            system_position("logical-shard", "lineage-a", "4"),
        )],
    )
    .expect("stale relocation");
    let SourceEffectDisposition::Applied { summary } = delayed else {
        panic!("stale relocation effect should be applied");
    };
    assert_eq!(summary.skipped(), 1);
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT user_id, email FROM users")
            .expect("query rows"),
        vec![serde_json::json!({"user_id": "old-id", "email": "old"})]
    );
    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT source_position_json FROM __analytics_source_record_states
             ORDER BY source_key_digest",
        )
        .expect("query metadata");
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0]["source_position_json"]["position"]["Offset"],
        "5"
    );
}

#[test]
fn given_system_expanded_filtered_transition_when_delayed_match_arrives_then_metadata_blocks_revival()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = expanded_system_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let scope = system_scope(34);
    let matching = expanded_item("MATCH", &[("point-1", "1")]);
    apply_system(
        &engine,
        &manifest,
        authority(60, 1),
        scope.clone(),
        None,
        system_checkpoint('j'),
        vec![system_record_with_images(
            "expanded_users",
            "expanded_source",
            "record-1",
            None,
            Some(matching.clone()),
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("expanded insert");
    let filtered = expanded_item("OTHER", &[("point-1", "2")]);
    apply_system(
        &engine,
        &manifest,
        authority(61, 2),
        scope.clone(),
        Some(system_checkpoint('j')),
        system_checkpoint('k'),
        vec![system_record_with_images(
            "expanded_users",
            "expanded_source",
            "record-1",
            Some(matching),
            Some(filtered.clone()),
            system_position("logical-shard", "lineage-a", "2"),
        )],
    )
    .expect("expanded hide");
    apply_system(
        &engine,
        &manifest,
        authority(62, 3),
        scope.clone(),
        Some(system_checkpoint('k')),
        system_checkpoint('l'),
        vec![system_record_with_images(
            "expanded_users",
            "expanded_source",
            "record-1",
            Some(filtered.clone()),
            Some(filtered.clone()),
            system_position("logical-shard", "lineage-a", "4"),
        )],
    )
    .expect("expanded filtered advance");
    let delayed = apply_system(
        &engine,
        &manifest,
        authority(63, 4),
        scope,
        Some(system_checkpoint('l')),
        system_checkpoint('m'),
        vec![system_record_with_images(
            "expanded_users",
            "expanded_source",
            "record-1",
            Some(filtered),
            Some(expanded_item("MATCH", &[("point-1", "3")])),
            system_position("logical-shard", "lineage-a", "3"),
        )],
    )
    .expect("delayed expanded match");
    let SourceEffectDisposition::Applied { summary } = delayed else {
        panic!("delayed expanded effect should be applied");
    };
    assert_eq!(summary.skipped(), 1);
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM expanded_users")
            .expect("query expanded rows")
            .is_empty()
    );
    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT child_identities_json, source_position_json
             FROM __analytics_source_record_states",
        )
        .expect("query expanded metadata");
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0]["child_identities_json"][0]["row_visible"],
        false
    );
    assert_eq!(
        metadata[0]["source_position_json"]["position"]["Offset"],
        "4"
    );
}

#[test]
fn given_system_unrelated_expanded_record_with_missing_list_when_applied_then_it_is_skipped() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = expanded_system_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let result = apply_system(
        &engine,
        &manifest,
        authority(66, 1),
        system_scope(36),
        None,
        system_checkpoint('p'),
        vec![system_record_with_images(
            "expanded_users",
            "expanded_source",
            "unrelated",
            None,
            Some(VecMap::from_iter([(
                "kind".to_string(),
                StorageValue::S("OTHER".to_string()),
            )])),
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("unrelated expanded record");
    let SourceEffectDisposition::Applied { summary } = result else {
        panic!("unrelated expanded effect should be applied");
    };
    assert_eq!(summary.skipped(), 1);
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM expanded_users")
            .expect("query unrelated rows")
            .is_empty()
    );
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM __analytics_row_source_positions")
            .expect("query unrelated metadata")
            .is_empty()
    );
}

#[test]
fn given_system_expanded_relocation_with_stale_old_identity_when_applying_source_batch_then_destination_is_unchanged()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = expanded_system_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let scope = system_scope(35);
    let old = expanded_item("MATCH", &[("old-point", "1")]);
    apply_system(
        &engine,
        &manifest,
        authority(64, 1),
        scope.clone(),
        None,
        system_checkpoint('n'),
        vec![system_record_with_images(
            "expanded_users",
            "expanded_source",
            "record-1",
            None,
            Some(old.clone()),
            system_position("logical-shard", "lineage-a", "5"),
        )],
    )
    .expect("expanded initial row");
    let delayed = apply_system(
        &engine,
        &manifest,
        authority(65, 2),
        scope,
        Some(system_checkpoint('n')),
        system_checkpoint('o'),
        vec![system_record_with_images(
            "expanded_users",
            "expanded_source",
            "record-1",
            Some(old),
            Some(expanded_item("MATCH", &[("new-point", "2")])),
            system_position("logical-shard", "lineage-a", "4"),
        )],
    )
    .expect("stale expanded relocation");
    let SourceEffectDisposition::Applied { summary } = delayed else {
        panic!("stale expanded relocation effect should be applied");
    };
    assert_eq!(summary.skipped(), 1);
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT event_id, value FROM expanded_users")
            .expect("query expanded rows"),
        vec![serde_json::json!({"event_id": "old-point", "value": 1})]
    );
    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT source_position_json FROM __analytics_source_record_states",
        )
        .expect("query expanded metadata");
    assert_eq!(metadata.len(), 1);
    assert_eq!(
        metadata[0]["source_position_json"]["position"]["Offset"],
        "5"
    );
}

#[test]
fn given_direct_expanded_late_storage_failure_when_applied_then_rows_and_metadata_roll_back() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = expanded_system_manifest();
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    let initial = expanded_item("MATCH", &[("point-1", "1"), ("point-2", "2")]);
    engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "expanded_users",
            b"record-1",
            StorageStreamRecord {
                sequence_number: "1".to_string(),
                keys: VecMap::new(),
                old_image: None,
                new_image: Some(initial.clone()),
            },
            &cursor_source_position("expanded", "1"),
        )
        .expect("initial expanded insert");
    let before_rows = engine
        .query_unscoped_sql_json("SELECT event_id, value FROM expanded_users ORDER BY event_id")
        .expect("query initial rows");
    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT source_row_id FROM __analytics_row_source_positions
             WHERE analytics_table_name = 'expanded_users' ORDER BY source_row_id",
        )
        .expect("query initial metadata");
    let second_id = metadata[1]["source_row_id"]
        .as_str()
        .expect("second expanded source identity");
    engine
        .connection()
        .execute(
            "UPDATE __analytics_row_source_positions
             SET source_position_json = CAST(? AS JSON)
             WHERE analytics_table_name = 'expanded_users' AND source_row_id = ?",
            duckdb::params!["{}", second_id],
        )
        .expect("corrupt second source position");

    let error = engine
        .ingest_stream_record_at_source_position(
            &manifest,
            "expanded_users",
            b"record-1",
            StorageStreamRecord {
                sequence_number: "2".to_string(),
                keys: VecMap::new(),
                old_image: None,
                new_image: Some(initial),
            },
            &cursor_source_position("expanded", "2"),
        )
        .expect_err("malformed later metadata must fail");
    assert!(error.to_string().contains("missing field") || error.to_string().contains("source"));
    assert_eq!(
        engine
            .query_unscoped_sql_json(
                "SELECT event_id, value FROM expanded_users ORDER BY event_id",
            )
            .expect("query rolled back rows"),
        before_rows
    );
    let metadata = engine
        .query_unscoped_sql_json(
            "SELECT source_row_id, source_position_json
             FROM __analytics_row_source_positions
             WHERE analytics_table_name = 'expanded_users' ORDER BY source_row_id",
        )
        .expect("query rolled back metadata");
    assert_eq!(metadata.len(), 2);
    assert_eq!(metadata[0]["source_position_json"]["cursor"], "1");
    assert_eq!(metadata[1]["source_position_json"], serde_json::json!({}));
}

#[test]
fn given_table_shard_fault_matrix_when_applied_then_state_is_atomic() {
    let (engine, manifest) = memory_engine();
    let current_authority = authority(1, 10);
    let records = vec![source_record("user-1", "first")];
    let first = apply(
        &engine,
        &manifest,
        current_authority.clone(),
        records.clone(),
        "cursor-1",
    )
    .expect("first apply");
    let SourceEffectDisposition::Applied { summary } = &first else {
        panic!("first table effect must apply");
    };
    assert_eq!(summary.inserted(), 1);
    assert_eq!(summary.checkpoint_count(), 1);
    let before = destination_state(&engine).expect("first state");
    let replay = apply(
        &engine,
        &manifest,
        current_authority.clone(),
        records.clone(),
        "cursor-1",
    )
    .expect("replay");

    assert!(matches!(first, SourceEffectDisposition::Applied { .. }));
    assert!(matches!(
        replay,
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(destination_state(&engine).expect("replay state"), before);
    assert_eq!(before.rows.len(), 1);
    assert_eq!(before.checkpoints.len(), 1);
    assert_eq!(before.receipts.len(), 1);
    assert_eq!(before.authorities.len(), 1);

    #[derive(Clone, Copy)]
    enum Fault {
        LowerFence,
        StalePredecessor,
        ChangedContent,
    }
    let cases = [
        ("lower fence", Fault::LowerFence),
        ("stale predecessor", Fault::StalePredecessor),
        ("changed content", Fault::ChangedContent),
    ];
    for (label, fault) in cases {
        let result = match fault {
            Fault::LowerFence => apply(
                &engine,
                &manifest,
                authority(2, 9),
                vec![source_record("user-1", "lower-fence")],
                "cursor-lower",
            ),
            Fault::StalePredecessor => apply_with_predecessor(
                &engine,
                &manifest,
                authority(3, 11),
                vec![source_record("user-1", "stale-predecessor")],
                Some("cursor-not-current"),
                "cursor-stale",
            ),
            Fault::ChangedContent => apply(
                &engine,
                &manifest,
                current_authority.clone(),
                vec![source_record("user-1", "changed-content")],
                "cursor-first",
            ),
        };
        match fault {
            Fault::LowerFence | Fault::StalePredecessor => {
                assert_eq!(result.expect(label), SourceEffectDisposition::StaleFence);
            }
            Fault::ChangedContent => {
                assert!(matches!(
                    result.expect_err(label),
                    AnalyticsEngineError::ConflictingSourceEffectReceipt
                ));
            }
        }
        assert_eq!(
            destination_state(&engine).expect("fault state"),
            before,
            "{label} must leave rows, checkpoint, fence, and receipt unchanged"
        );
    }

    assert_table_contract_failure_rolls_back();
}

#[test]
fn given_higher_fence_when_source_batch_updates_and_deletes_then_new_state_is_applied() {
    let (engine, manifest) = memory_engine();
    let initial_authority = authority(6, 6);
    let initial_records = vec![
        source_record("user-1", "before"),
        source_record("user-2", "removed"),
    ];
    apply(
        &engine,
        &manifest,
        initial_authority.clone(),
        initial_records.clone(),
        "cursor-before",
    )
    .expect("initial batch");
    let outcome = apply_with_predecessor(
        &engine,
        &manifest,
        authority(7, 7),
        vec![
            update_record("user-1", "before", "after"),
            delete_record("user-2", "removed"),
        ],
        Some("cursor-before"),
        "cursor-after",
    )
    .expect("higher-fence batch");

    let SourceEffectDisposition::Applied { summary } = outcome else {
        panic!("higher fence must apply");
    };
    assert_eq!(summary.inserted(), 0);
    assert_eq!(summary.updated(), 1);
    assert_eq!(summary.deleted(), 1);
    assert_eq!(summary.checkpoint_count(), 1);
    let state = destination_state(&engine).expect("updated state");
    assert_eq!(state.rows.len(), 1);
    assert_eq!(state.rows[0]["email"], "after@example.test");
    assert_eq!(state.checkpoints, vec![checkpoint("cursor-after")]);
    assert_eq!(state.authorities.len(), 1);
    assert_eq!(state.receipts.len(), 2);

    let before_replay = state;
    let replay = apply(
        &engine,
        &manifest,
        initial_authority,
        initial_records,
        "cursor-before",
    )
    .expect("historical receipt replay");
    assert!(matches!(
        replay,
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(
        destination_state(&engine).expect("historical replay state"),
        before_replay
    );
}

#[test]
fn given_next_table_page_when_job_fence_is_reused_then_predecessor_allows_progress() {
    let (engine, manifest) = memory_engine();
    apply(
        &engine,
        &manifest,
        authority(30, 1),
        vec![source_record("user-1", "first")],
        "cursor-first",
    )
    .expect("first page");

    let next = apply_with_predecessor(
        &engine,
        &manifest,
        authority(31, 1),
        vec![source_record("user-1", "second")],
        Some("cursor-first"),
        "cursor-second",
    )
    .expect("next page with a reused job fence");

    assert!(matches!(next, SourceEffectDisposition::Applied { .. }));
    let state = destination_state(&engine).expect("sequential table state");
    assert_eq!(state.checkpoints, vec![checkpoint("cursor-second")]);
    assert_eq!(state.rows[0]["email"], "second@example.test");
    assert_eq!(state.receipts.len(), 2);
}

#[test]
fn given_committed_destination_when_local_result_is_lost_then_receipt_reconciles_without_replay() {
    let (engine, manifest) = memory_engine();
    let authority = authority(8, 8);
    let records = vec![source_record("user-1", "committed")];
    let _lost_result = apply(
        &engine,
        &manifest,
        authority.clone(),
        records.clone(),
        "cursor-8",
    )
    .expect("destination commit");
    let before = destination_state(&engine).expect("committed state");

    let reconciled =
        apply(&engine, &manifest, authority, records, "cursor-8").expect("receipt-bound replay");
    assert!(matches!(
        reconciled,
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(
        destination_state(&engine).expect("reconciled state"),
        before
    );
}

#[test]
fn given_file_duckdb_when_reopened_then_source_effect_state_and_replay_are_durable() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let path = tempdir.path().join("analytics.duckdb");
    let backend = StorageBackend::DuckDb {
        path: path.to_string_lossy().into_owned(),
    };
    let manifest = users_manifest();
    let authority = authority(9, 9);
    let engine = AnalyticsEngine::connect(&backend).expect("connect file DuckDB");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    apply(
        &engine,
        &manifest,
        authority.clone(),
        vec![source_record("user-1", "file")],
        "cursor-file",
    )
    .expect("file apply");
    let before = destination_state(&engine).expect("file state");
    drop(engine);

    let reopened = AnalyticsEngine::connect(&backend).expect("reopen file DuckDB");
    let replay = apply(
        &reopened,
        &manifest,
        authority,
        vec![source_record("user-1", "file")],
        "cursor-file",
    )
    .expect("file replay");
    assert!(matches!(
        replay,
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(
        destination_state(&reopened).expect("reopened state"),
        before
    );
}

#[test]
fn given_file_duckdb_system_effect_when_reopened_then_scope_frontier_and_replay_are_durable() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let path = tempdir.path().join("analytics-system.duckdb");
    let backend = StorageBackend::DuckDb {
        path: path.to_string_lossy().into_owned(),
    };
    let manifest = users_manifest();
    let authority = authority(50, 50);
    let scope = system_scope(7);
    let checkpoint = system_checkpoint('f');
    let records = vec![system_source_record(
        "user-1",
        "system-restart",
        system_position("logical-shard", "lineage-a", "12"),
    )];
    let engine = AnalyticsEngine::connect(&backend).expect("connect file DuckDB");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    apply_system(
        &engine,
        &manifest,
        authority.clone(),
        scope.clone(),
        None,
        checkpoint.clone(),
        records.clone(),
    )
    .expect("system effect applies");
    drop(engine);

    let reopened = AnalyticsEngine::connect(&backend).expect("reopen file DuckDB");
    assert_eq!(
        reopened
            .load_system_stream_checkpoint()
            .expect("load system frontier"),
        Some((scope.clone(), checkpoint.clone()))
    );
    let replay = apply_system(
        &reopened,
        &manifest,
        authority,
        scope,
        Some(checkpoint.clone()),
        checkpoint,
        records,
    )
    .expect("exact system replay after restart");
    assert!(matches!(
        replay,
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
    let state = system_destination_state(&reopened).expect("restarted system destination state");
    assert_eq!(state.rows.len(), 1);
    assert_eq!(state.receipts.len(), 1);
}

#[test]
fn given_sqlite_catalog_ducklake_when_reopened_then_source_effect_state_is_durable() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let (backend, _paths) = sqlite_ducklake_backend(tempdir.path());
    let manifest = users_manifest();
    let authority = authority(12, 12);
    let engine = AnalyticsEngine::connect(&backend).expect("connect SQLite-catalog DuckLake");
    engine.ensure_manifest(&manifest).expect("ensure manifest");
    apply(
        &engine,
        &manifest,
        authority.clone(),
        vec![source_record("user-1", "ducklake")],
        "cursor-ducklake",
    )
    .expect("DuckLake apply");
    let before = destination_state(&engine).expect("DuckLake state");
    drop(engine);

    let reopened = AnalyticsEngine::connect(&backend).expect("reopen SQLite-catalog DuckLake");
    let replay = apply(
        &reopened,
        &manifest,
        authority,
        vec![source_record("user-1", "ducklake")],
        "cursor-ducklake",
    )
    .expect("DuckLake replay");
    assert!(matches!(
        replay,
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(
        destination_state(&reopened).expect("reopened DuckLake state"),
        before
    );
}

fn assert_table_contract_failure_rolls_back() {
    let (engine, mut manifest) = memory_engine();
    manifest.tables[0].tenant_id = None;
    manifest.tables[0].tenant_selector = TenantSelector::Attribute {
        attribute_name: "user_id".to_string(),
    };
    engine
        .ensure_manifest(&manifest)
        .expect("manifest with required tenant attribute");
    let mut poison = user_item("poison", "poison@example.test", "org-a");
    poison.remove("user_id");
    let error = apply(
        &engine,
        &manifest,
        authority(13, 13),
        vec![
            source_record("user-1", "valid"),
            source_record_with_item("poison", poison),
        ],
        "cursor-poison",
    )
    .expect_err("contract poison must fail closed");
    assert!(matches!(error, AnalyticsEngineError::MissingAttribute(_)));

    let state = destination_state(&engine).expect("poison state");
    assert!(state.rows.is_empty());
    assert!(state.checkpoints.is_empty());
    assert!(state.authorities.is_empty());
    assert!(state.receipts.is_empty());
    assert_eq!(state.failures.len(), 1);
    assert_eq!(state.failures[0]["record_position"], "poison");
    assert_eq!(state.failures[0]["fence"], "13");
    assert_eq!(state.failures[0]["execution_id"], hex_id(13));
}

#[test]
fn given_privacy_policy_when_fenced_batch_applies_then_drops_are_in_receipt_and_row_is_sanitized() {
    let (engine, manifest) = memory_engine();
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .expect("policy")
        .with_denied_key_name("email");
    let mut input = user_item("user-1", "secret@example.test", "org-a");
    input.insert(
        "public".to_string(),
        analytics_contract::StorageValue::S("ok".to_string()),
    );
    let outcome = apply_with_policy(
        &engine,
        &manifest,
        authority(14, 14),
        vec![source_record_with_item("user-1", input)],
        "cursor-privacy",
        Some(&policy),
    )
    .expect("privacy source batch");
    let SourceEffectDisposition::Applied { summary } = outcome else {
        panic!("privacy batch must apply");
    };
    assert_eq!(summary.dropped_fields(), 1);
    assert_eq!(summary.inserted(), 1);
    assert_eq!(summary.checkpoint_count(), 1);
    let state = destination_state(&engine).expect("privacy state");
    assert_eq!(state.rows[0]["email"], serde_json::Value::Null);
    assert_eq!(state.checkpoints, vec![checkpoint("cursor-privacy")]);
}

#[test]
fn given_malformed_request_when_validating_then_no_destination_state_is_created() {
    let (engine, manifest) = memory_engine();
    let invalid_execution = match SourceEffectAuthority::try_new([1_u8; 15], 1) {
        Err(error) => error,
        Ok(_) => panic!("execution identity must be 16 bytes"),
    };
    assert!(matches!(
        invalid_execution,
        AnalyticsEngineError::InvalidSourceEffectExecutionId { actual: 15 }
    ));
    let invalid_fence = match SourceEffectAuthority::try_new([1_u8; 16], 0) {
        Err(error) => error,
        Ok(_) => panic!("fence must be nonzero"),
    };
    assert!(matches!(
        invalid_fence,
        AnalyticsEngineError::InvalidSourceEffectFence
    ));
    let invalid_scope = match SourceEffectScope::try_new("\n", SHARD_ID) {
        Err(error) => error,
        Ok(_) => panic!("scope controls must be rejected"),
    };
    assert!(matches!(
        invalid_scope,
        AnalyticsEngineError::InvalidSourceEffectScope
    ));
    let iterator_checkpoint = RecordCheckpoint::try_new(
        SOURCE_TABLE_NAME,
        "__iterator:source-shard",
        "iterator-token",
    );
    assert!(matches!(
        iterator_checkpoint,
        Err(AnalyticsEngineError::IteratorCheckpointNotDurable)
    ));
    let scope = SourceEffectScope::try_new(SOURCE_TABLE_NAME, SHARD_ID).expect("scope");
    let duplicate = match SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest: &manifest,
        manifest_digest: manifest_digest(&manifest),
        authority: authority(15, 15),
        scope: scope.clone(),
        records: vec![source_record("user-1", "duplicate")],
        checkpoints: vec![checkpoint("one"), checkpoint("two")],
        privacy_policy: None,
    }) {
        Err(error) => error,
        Ok(_) => panic!("duplicate checkpoints must be rejected"),
    };
    assert!(matches!(
        duplicate,
        AnalyticsEngineError::DuplicateSourceEffectCheckpoint
    ));
    let missing_shard = match SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest: &manifest,
        manifest_digest: manifest_digest(&manifest),
        authority: authority(16, 16),
        scope: scope.clone(),
        records: vec![record_without_shard("user-1", "missing-shard")],
        checkpoints: vec![checkpoint("missing-shard")],
        privacy_policy: None,
    }) {
        Err(error) => error,
        Ok(_) => panic!("missing shard must be rejected"),
    };
    assert!(matches!(
        missing_shard,
        AnalyticsEngineError::MixedSourceEffectScope
    ));
    let mixed_table = match SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest: &manifest,
        manifest_digest: manifest_digest(&manifest),
        authority: authority(17, 17),
        scope,
        records: vec![record_from_source("other-source", "user-1", "mixed")],
        checkpoints: vec![checkpoint("mixed")],
        privacy_policy: None,
    }) {
        Err(error) => error,
        Ok(_) => panic!("mixed source table must be rejected"),
    };
    assert!(matches!(
        mixed_table,
        AnalyticsEngineError::MixedSourceEffectScope
    ));
    assert!(
        destination_state(&engine)
            .expect("state remains absent")
            .rows
            .is_empty()
    );

    let missing_table = match SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest: &manifest,
        manifest_digest: manifest_digest(&manifest),
        authority: authority(30, 30),
        scope: SourceEffectScope::try_new(SOURCE_TABLE_NAME, SHARD_ID).expect("scope"),
        records: vec![
            source_record("user-1", "valid-before-missing-table"),
            record_from_table("unregistered", "user-2", "missing-table"),
        ],
        checkpoints: vec![checkpoint("missing-table")],
        privacy_policy: None,
    }) {
        Ok(_) => panic!("unregistered destination table must fail before the effect"),
        Err(error) => error,
    };
    assert!(matches!(
        missing_table,
        AnalyticsEngineError::TableNotRegistered(name) if name == "unregistered"
    ));
    let state = destination_state(&engine).expect("missing table state");
    assert!(state.rows.is_empty());
    assert!(state.checkpoints.is_empty());
    assert!(state.authorities.is_empty());
    assert!(state.receipts.is_empty());
}

#[test]
fn given_malformed_or_ambiguous_durable_authority_when_called_then_effect_fails_closed() {
    for seed in [18_u8, 19, 20, 21] {
        let (engine, manifest) = memory_engine();
        let execution = Value::Blob(vec![seed; 16]);
        match seed {
            18 => {
                engine
                    .connection()
                    .execute(
                        "INSERT INTO __analytics_source_effect_authority
                         VALUES (1, ?, ?, ?, ?, 1)",
                        duckdb::params![SOURCE_TABLE_NAME, SHARD_ID, "bad", execution],
                    )
                    .expect("insert malformed authority");
            }
            19 => {
                for fence in ["19", "20"] {
                    engine
                        .connection()
                        .execute(
                            "INSERT INTO __analytics_source_effect_authority
                             VALUES (1, ?, ?, ?, ?, 1)",
                            duckdb::params![SOURCE_TABLE_NAME, SHARD_ID, fence, execution.clone()],
                        )
                        .expect("insert duplicate authority");
                }
            }
            20 => {
                engine
                    .connection()
                    .execute(
                        "INSERT INTO __analytics_source_effect_authority
                         VALUES (1, ?, ?, ?, ?, 1)",
                        duckdb::params![
                            SOURCE_TABLE_NAME,
                            SHARD_ID,
                            "20",
                            Value::Blob(vec![20; 15])
                        ],
                    )
                    .expect("insert short authority identity");
            }
            21 => {
                engine
                    .connection()
                    .execute(
                        "INSERT INTO __analytics_source_effect_authority
                         VALUES (1, ?, ?, ?, ?, 1)",
                        duckdb::params![SOURCE_TABLE_NAME, SHARD_ID, "0", execution],
                    )
                    .expect("insert zero authority fence");
            }
            _ => unreachable!(),
        }
        let error = apply(
            &engine,
            &manifest,
            authority(seed, u64::from(seed)),
            vec![source_record("user-1", "must-not-apply")],
            "must-not-apply",
        )
        .expect_err("durable authority corruption must fail closed");
        assert!(matches!(
            error,
            AnalyticsEngineError::InvalidSourceEffectFence
                | AnalyticsEngineError::InvalidSourceEffectExecutionId { .. }
                | AnalyticsEngineError::AmbiguousSourceEffectAuthority
        ));
        let state = destination_state(&engine).expect("authority failure state");
        assert!(state.rows.is_empty());
        assert!(state.checkpoints.is_empty());
        assert!(state.receipts.is_empty());
    }
}

#[test]
fn given_malformed_or_ambiguous_durable_receipt_when_called_then_effect_fails_closed() {
    let cases = [(22_u8, "bad", 1_i64), (23_u8, "23", -1_i64)];
    for (seed, fence, inserted) in cases {
        let (engine, manifest) = memory_engine();
        seed_receipt(&engine, seed, fence, inserted);
        let error = apply(
            &engine,
            &manifest,
            authority(seed, u64::from(seed)),
            vec![source_record("user-1", "must-not-apply")],
            "must-not-apply",
        )
        .expect_err("malformed receipt must fail closed");
        assert!(matches!(
            error,
            AnalyticsEngineError::InvalidSourceEffectFence
                | AnalyticsEngineError::InvalidSourceEffectReceipt
        ));
        assert!(
            destination_state(&engine)
                .expect("receipt failure state")
                .rows
                .is_empty()
        );
    }

    let (engine, manifest) = memory_engine();
    seed_receipt(&engine, 24, "24", 1);
    seed_receipt_with_scope(&engine, 24, "24", "other-table", SHARD_ID, 1);
    let error = apply(
        &engine,
        &manifest,
        authority(24, 24),
        vec![source_record("user-1", "must-not-apply")],
        "must-not-apply",
    )
    .expect_err("duplicate receipt must fail closed");
    assert!(matches!(
        error,
        AnalyticsEngineError::AmbiguousSourceEffectReceipt
    ));

    let (engine, manifest) = memory_engine();
    seed_receipt_with_scope(&engine, 25, "25", "other-table", SHARD_ID, 1);
    let error = apply(
        &engine,
        &manifest,
        authority(25, 25),
        vec![source_record("user-1", "must-not-apply")],
        "must-not-apply",
    )
    .expect_err("receipt from another scope must fail closed");
    assert!(matches!(
        error,
        AnalyticsEngineError::ConflictingSourceEffectReceipt
    ));
}

#[test]
fn given_same_execution_with_different_fence_when_replayed_then_receipt_conflict_fails_closed() {
    let (engine, manifest) = memory_engine();
    apply(
        &engine,
        &manifest,
        authority(31, 31),
        vec![source_record("user-1", "accepted")],
        "cursor-31",
    )
    .expect("initial source effect");

    let error = apply(
        &engine,
        &manifest,
        authority(31, 32),
        vec![source_record("user-1", "replayed-with-new-fence")],
        "cursor-31",
    )
    .expect_err("same execution with a different fence must fail closed");
    assert!(matches!(
        error,
        AnalyticsEngineError::ConflictingSourceEffectReceipt
    ));
    let state = destination_state(&engine).expect("fence conflict state");
    assert_eq!(state.rows.len(), 1);
    assert_eq!(state.checkpoints, vec![checkpoint("cursor-31")]);
    assert_eq!(state.receipts.len(), 1);
}

#[test]
fn given_receipt_without_authority_when_called_then_incomplete_state_fails_closed() {
    let (engine, manifest) = memory_engine();
    seed_receipt(&engine, 25, "25", 1);
    let error = apply(
        &engine,
        &manifest,
        authority(25, 25),
        vec![source_record("user-1", "must-not-apply")],
        "must-not-apply",
    )
    .expect_err("receipt-only state must fail closed");
    assert!(matches!(
        error,
        AnalyticsEngineError::IncompleteSourceEffectState
    ));
    assert!(
        destination_state(&engine)
            .expect("incomplete state")
            .rows
            .is_empty()
    );
}

#[test]
fn given_authority_without_receipt_when_called_then_incomplete_state_fails_closed() {
    let (engine, manifest) = memory_engine();
    seed_authority(&engine, 26, "26");
    let error = apply(
        &engine,
        &manifest,
        authority(26, 26),
        vec![source_record("user-1", "must-not-apply")],
        "must-not-apply",
    )
    .expect_err("authority-only state must fail closed");
    assert!(matches!(
        error,
        AnalyticsEngineError::IncompleteSourceEffectState
    ));
    assert!(
        destination_state(&engine)
            .expect("incomplete state")
            .rows
            .is_empty()
    );
}

#[test]
fn given_stale_contract_poison_when_newer_owner_exists_then_failure_audit_is_not_written() {
    let (engine, manifest) = memory_engine();
    apply(
        &engine,
        &manifest,
        authority(27, 27),
        vec![source_record("user-1", "newer")],
        "newer-cursor",
    )
    .expect("newer owner");
    let mut poison = user_item("poison", "poison@example.test", "org-a");
    poison.remove("user_id");
    let result = apply(
        &engine,
        &manifest,
        authority(28, 26),
        vec![source_record_with_item("poison", poison)],
        "stale-cursor",
    )
    .expect("stale poison disposition");
    assert_eq!(result, SourceEffectDisposition::StaleFence);
    let failures = destination_state(&engine).expect("failure state").failures;
    assert!(
        failures.is_empty(),
        "stale worker must not add failure audit"
    );
}

#[test]
fn given_unrelated_delete_without_projected_identity_when_delete_is_skipped_then_source_frontier_advances()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = filtered_system_manifest(
        true,
        RowIdentity::Attribute {
            attribute_name: "event_id".to_string(),
        },
    );
    engine.ensure_manifest(&manifest).expect("manifest");
    let scope = system_scope(81);
    let checkpoint = system_checkpoint('a');
    let result = apply_system(
        &engine,
        &manifest,
        authority(81, 1),
        scope.clone(),
        None,
        checkpoint.clone(),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "unrelated-key",
            Some(user_item("other", "unrelated", "org-a")),
            None,
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect("unrelated deletion must not require destination identity");
    let SourceEffectDisposition::Applied { summary } = result else {
        panic!("filtered deletion must advance source frontier");
    };
    assert_eq!(summary.skipped(), 1);
    assert_eq!(
        engine.load_system_stream_checkpoint().expect("checkpoint"),
        Some((scope, checkpoint))
    );
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM users")
            .expect("rows")
            .is_empty()
    );
    let owners = engine
        .query_unscoped_sql_json(
            "SELECT child_identities_json FROM __analytics_source_record_states",
        )
        .expect("filtered owner marker");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0]["child_identities_json"], serde_json::json!([]));
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM analytics_source_batch_failures")
            .expect("failures")
            .is_empty()
    );
}

#[test]
fn given_matching_delete_without_projected_identity_when_delete_is_skipped_then_source_frontier_is_unchanged()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect");
    let manifest = filtered_system_manifest(
        true,
        RowIdentity::Attribute {
            attribute_name: "event_id".to_string(),
        },
    );
    engine.ensure_manifest(&manifest).expect("manifest");
    let error = apply_system(
        &engine,
        &manifest,
        authority(82, 1),
        system_scope(82),
        None,
        system_checkpoint('b'),
        vec![system_record_with_images(
            "users",
            "tenant_01",
            "matching-key",
            Some(user_item("user-1", "matching", "org-a")),
            None,
            system_position("logical-shard", "lineage-a", "1"),
        )],
    )
    .expect_err("matching deletion must validate destination identity");
    assert!(matches!(
        error,
        AnalyticsEngineError::MissingAttribute(ref attribute) if attribute == "event_id"
    ));
    assert!(
        engine
            .load_system_stream_checkpoint()
            .expect("checkpoint")
            .is_none()
    );
    assert!(
        engine
            .query_unscoped_sql_json("SELECT * FROM __analytics_source_record_states")
            .expect("owners")
            .is_empty()
    );
}

#[derive(Debug, Eq, PartialEq)]
struct SystemDestinationState {
    rows: Vec<serde_json::Value>,
    checkpoint: Option<(SystemStreamScope, SystemStreamCheckpointToken)>,
    authorities: Vec<serde_json::Value>,
    receipts: Vec<serde_json::Value>,
}

fn system_destination_state(
    engine: &AnalyticsEngine,
) -> AnalyticsEngineResult<SystemDestinationState> {
    Ok(SystemDestinationState {
        rows: engine.query_unscoped_sql_json(
            "SELECT table_name, tenant_id, __id, user_id, email FROM users ORDER BY __id",
        )?,
        checkpoint: engine.load_system_stream_checkpoint()?,
        authorities: engine.query_unscoped_sql_json(
            "SELECT scope_version, scope_id, generation, fence, hex(execution_id) AS execution_id
             FROM __analytics_system_source_effect_authority
             ORDER BY scope_id, generation",
        )?,
        receipts: engine.query_unscoped_sql_json(
            "SELECT scope_version, scope_id, generation, \"checkpoint\", fence,
                    inserted_count, updated_count, deleted_count, skipped_count,
                    dropped_fields, checkpoint_count, hex(execution_id) AS execution_id
             FROM __analytics_system_source_effect_receipts
             ORDER BY scope_id, generation, execution_id",
        )?,
    })
}

fn apply_system(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    authority: SourceEffectAuthority,
    source_scope: SystemStreamScope,
    previous_checkpoint: Option<SystemStreamCheckpointToken>,
    checkpoint: SystemStreamCheckpointToken,
    records: Vec<SourceBatchRecord>,
) -> AnalyticsEngineResult<SourceEffectDisposition> {
    let request = SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest,
        manifest_digest: manifest_digest(manifest),
        authority,
        scope: SourceEffectScope::try_new_system(source_scope, previous_checkpoint, checkpoint)?,
        records,
        checkpoints: Vec::new(),
        privacy_policy: None,
    })?;
    engine.ingest_fenced_source_batch(request)
}

fn system_scope(generation: u64) -> SystemStreamScope {
    SystemStreamScope::try_new("aux-system", generation).expect("valid system scope")
}

fn system_checkpoint(nibble: char) -> SystemStreamCheckpointToken {
    let encoded = format!("{:02x}", u32::from(nibble));
    SystemStreamCheckpointToken::try_from(format!("v1.{}", encoded.repeat(16)))
        .expect("valid system checkpoint")
}

fn system_position(shard_id: &str, lineage_id: &str, offset: &str) -> SystemStreamPosition {
    SystemStreamPosition::try_new(shard_id, lineage_id, offset).expect("valid system position")
}

fn system_source_record(
    key: &str,
    email_suffix: &str,
    position: SystemStreamPosition,
) -> SourceBatchRecord {
    let email = format!("{email_suffix}@example.test");
    system_source_record_with_item(key, user_item(key, email.as_str(), "org-a"), position)
}

fn system_source_record_with_item(
    key: &str,
    item: StorageItem,
    position: SystemStreamPosition,
) -> SourceBatchRecord {
    let record_key = logical_record_key(&position);
    SourceBatchRecord {
        item: StreamRecordBatchItem {
            analytics_table_name: ANALYTICS_TABLE_NAME.to_string(),
            source_table_name: SOURCE_TABLE_NAME.to_string(),
            shard_id: Some(position.shard_id().as_str().to_string()),
            system_position: Some(position),
            record_key: record_key.as_bytes().to_vec(),
            record: StorageStreamRecord {
                sequence_number: record_key,
                keys: VecMap::from_iter([("pk".to_string(), StorageValue::S(key.to_string()))]),
                old_image: None,
                new_image: Some(item),
            },
        },
        retention: None,
    }
}

fn memory_engine() -> (AnalyticsEngine, AnalyticsManifest) {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect memory DuckDB");
    let manifest = users_manifest();
    engine
        .ensure_manifest(&manifest)
        .expect("ensure memory manifest");
    (engine, manifest)
}

fn filtered_system_manifest(skip_delete: bool, row_identity: RowIdentity) -> AnalyticsManifest {
    let mut table = users_manifest()
        .tables
        .into_iter()
        .next()
        .expect("users table");
    table.skip_delete = skip_delete;
    table.row_identity = row_identity;
    table.condition_expression = Some("#user_id = :user_id".to_string());
    table.expression_attribute_names = Some(VecMap::from_iter([(
        "#user_id".to_string(),
        "user_id".to_string(),
    )]));
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":user_id".to_string(),
        StorageValue::S("user-1".to_string()),
    )]));
    AnalyticsManifest::new(vec![table])
}

fn expanded_system_manifest() -> AnalyticsManifest {
    let mut table = users_manifest()
        .tables
        .into_iter()
        .next()
        .expect("users table");
    table.source_table_name = "expanded_source".to_string();
    table.analytics_table_name = "expanded_users".to_string();
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::Attribute {
        attribute_name: "tenant_id".to_string(),
    };
    table.row_identity = RowIdentity::Attributes {
        attribute_names: vec!["event_id".to_string(), "ordinal".to_string()],
    };
    table.document_column = None;
    table.condition_expression = Some("#kind = :kind".to_string());
    table.expression_attribute_names = Some(VecMap::from_iter([(
        "#kind".to_string(),
        "kind".to_string(),
    )]));
    table.expression_attribute_values = Some(VecMap::from_iter([(
        ":kind".to_string(),
        StorageValue::S("MATCH".to_string()),
    )]));
    table.projection_columns = Some(vec![
        projection_column_with_type("event_id", PrimitiveColumnType::VarChar),
        projection_column_with_type("value", PrimitiveColumnType::BigInt),
        projection_column_with_type("batch_id", PrimitiveColumnType::VarChar),
        projection_column_with_type("ordinal", PrimitiveColumnType::BigInt),
    ]);
    table.projection_attribute_names = None;
    table.columns = Vec::new();
    table.row_expansion = Some(RowExpansion {
        list_path: vec![attribute_segment("points")],
        source_fields: vec![
            expansion_field("tenant_id", "tenant_id"),
            expansion_field("batch_id", "batch_id"),
        ],
        element_fields: vec![
            expansion_index_field("event_id", 0),
            expansion_index_field("value", 1),
        ],
        ordinal_attribute_name: Some("ordinal".to_string()),
    });
    AnalyticsManifest::new(vec![table])
}

fn expanded_item(kind: &str, points: &[(&str, &str)]) -> StorageItem {
    VecMap::from_iter([
        ("kind".to_string(), StorageValue::S(kind.to_string())),
        (
            "tenant_id".to_string(),
            StorageValue::S("tenant-a".to_string()),
        ),
        (
            "batch_id".to_string(),
            StorageValue::S("batch-1".to_string()),
        ),
        (
            "points".to_string(),
            StorageValue::L(
                points
                    .iter()
                    .map(|(event_id, value)| {
                        StorageValue::L(vec![
                            StorageValue::S((*event_id).to_string()),
                            StorageValue::N((*value).to_string()),
                        ])
                    })
                    .collect(),
            ),
        ),
    ])
}

fn projection_column_with_type(
    column_name: &str,
    primitive: PrimitiveColumnType,
) -> ProjectionColumn {
    ProjectionColumn {
        column_name: column_name.to_string(),
        attribute_path: column_name.to_string(),
        column_type: Some(AnalyticsColumnType::Primitive { primitive }),
    }
}

fn expansion_field(output_attribute_name: &str, path_attribute: &str) -> RowProjectionField {
    RowProjectionField {
        output_attribute_name: output_attribute_name.to_string(),
        path: vec![attribute_segment(path_attribute)],
    }
}

fn expansion_index_field(output_attribute_name: &str, index: usize) -> RowProjectionField {
    RowProjectionField {
        output_attribute_name: output_attribute_name.to_string(),
        path: vec![RowPathSegment::Index { index }],
    }
}

fn attribute_segment(name: &str) -> RowPathSegment {
    RowPathSegment::Attribute {
        name: name.to_string(),
    }
}

fn system_record_with_images(
    analytics_table_name: &str,
    source_table_name: &str,
    key: &str,
    old_image: Option<StorageItem>,
    new_image: Option<StorageItem>,
    position: SystemStreamPosition,
) -> SourceBatchRecord {
    let shard_id = position.shard_id().as_str().to_string();
    let record_key = logical_record_key(&position);
    SourceBatchRecord {
        item: StreamRecordBatchItem {
            analytics_table_name: analytics_table_name.to_string(),
            source_table_name: source_table_name.to_string(),
            shard_id: Some(shard_id),
            system_position: Some(position),
            record_key: record_key.as_bytes().to_vec(),
            record: StorageStreamRecord {
                sequence_number: record_key,
                keys: VecMap::from_iter([("pk".to_string(), StorageValue::S(key.to_string()))]),
                old_image,
                new_image,
            },
        },
        retention: None,
    }
}

fn logical_record_key(position: &SystemStreamPosition) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        position.shard_id().as_str(),
        position.lineage_id().as_str(),
        position.offset().as_str()
    )
}

fn cursor_source_position(stream_name: &str, cursor: &str) -> SourcePosition {
    SourcePosition::AuxStorageStreamCursor {
        stream_name: stream_name.to_string(),
        cursor: cursor.to_string(),
    }
}

fn apply(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    authority: SourceEffectAuthority,
    records: Vec<SourceBatchRecord>,
    position: &str,
) -> AnalyticsEngineResult<SourceEffectDisposition> {
    apply_with_predecessor(engine, manifest, authority, records, None, position)
}

fn apply_with_predecessor(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    authority: SourceEffectAuthority,
    records: Vec<SourceBatchRecord>,
    previous_position: Option<&str>,
    position: &str,
) -> AnalyticsEngineResult<SourceEffectDisposition> {
    apply_with_policy_and_predecessor(
        engine,
        manifest,
        authority,
        records,
        previous_position,
        position,
        None,
    )
}

fn apply_with_policy<'a>(
    engine: &AnalyticsEngine,
    manifest: &'a AnalyticsManifest,
    authority: SourceEffectAuthority,
    records: Vec<SourceBatchRecord>,
    position: &str,
    privacy_policy: Option<&'a PrivacyPolicy>,
) -> AnalyticsEngineResult<SourceEffectDisposition> {
    apply_with_policy_and_predecessor(
        engine,
        manifest,
        authority,
        records,
        None,
        position,
        privacy_policy,
    )
}

fn apply_with_policy_and_predecessor<'a>(
    engine: &AnalyticsEngine,
    manifest: &'a AnalyticsManifest,
    authority: SourceEffectAuthority,
    records: Vec<SourceBatchRecord>,
    previous_position: Option<&str>,
    position: &str,
    privacy_policy: Option<&'a PrivacyPolicy>,
) -> AnalyticsEngineResult<SourceEffectDisposition> {
    let request = SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest,
        manifest_digest: manifest_digest(manifest),
        authority,
        scope: SourceEffectScope::try_new_with_predecessor(
            SOURCE_TABLE_NAME,
            SHARD_ID,
            previous_position.map(str::to_owned),
        )?,
        records,
        checkpoints: vec![checkpoint(position)],
        privacy_policy,
    })?;
    engine.ingest_fenced_source_batch(request)
}

fn authority(seed: u8, fence: u64) -> SourceEffectAuthority {
    SourceEffectAuthority::try_new([seed; 16], fence).expect("valid test authority")
}

fn checkpoint(position: &str) -> RecordCheckpoint {
    RecordCheckpoint::try_new(SOURCE_TABLE_NAME, SHARD_ID, position).expect("valid checkpoint")
}

fn source_record(key: &str, email_suffix: &str) -> SourceBatchRecord {
    let email = format!("{email_suffix}@example.test");
    source_record_with_item(key, user_item(key, email.as_str(), "org-a"))
}

fn source_record_with_item(key: &str, item: StorageItem) -> SourceBatchRecord {
    SourceBatchRecord {
        item: StreamRecordBatchItem {
            analytics_table_name: ANALYTICS_TABLE_NAME.to_string(),
            source_table_name: SOURCE_TABLE_NAME.to_string(),
            shard_id: Some(SHARD_ID.to_string()),
            system_position: None,
            record_key: key.as_bytes().to_vec(),
            record: StorageStreamRecord {
                sequence_number: key.to_string(),
                keys: VecMap::new(),
                old_image: None,
                new_image: Some(item),
            },
        },
        retention: None,
    }
}

fn update_record(key: &str, old_email: &str, new_email: &str) -> SourceBatchRecord {
    let old_email = format!("{old_email}@example.test");
    let new_email = format!("{new_email}@example.test");
    SourceBatchRecord {
        item: StreamRecordBatchItem {
            analytics_table_name: ANALYTICS_TABLE_NAME.to_string(),
            source_table_name: SOURCE_TABLE_NAME.to_string(),
            shard_id: Some(SHARD_ID.to_string()),
            system_position: None,
            record_key: key.as_bytes().to_vec(),
            record: StorageStreamRecord {
                sequence_number: format!("{key}-update"),
                keys: VecMap::new(),
                old_image: Some(user_item(key, old_email.as_str(), "org-a")),
                new_image: Some(user_item(key, new_email.as_str(), "org-a")),
            },
        },
        retention: None,
    }
}

fn delete_record(key: &str, email: &str) -> SourceBatchRecord {
    let email = format!("{email}@example.test");
    SourceBatchRecord {
        item: StreamRecordBatchItem {
            analytics_table_name: ANALYTICS_TABLE_NAME.to_string(),
            source_table_name: SOURCE_TABLE_NAME.to_string(),
            shard_id: Some(SHARD_ID.to_string()),
            system_position: None,
            record_key: key.as_bytes().to_vec(),
            record: StorageStreamRecord {
                sequence_number: format!("{key}-delete"),
                keys: VecMap::new(),
                old_image: Some(user_item(key, email.as_str(), "org-a")),
                new_image: None,
            },
        },
        retention: None,
    }
}

fn record_without_shard(key: &str, email_suffix: &str) -> SourceBatchRecord {
    let mut record = source_record(key, email_suffix);
    record.item.shard_id = None;
    record
}

fn record_from_source(source_table_name: &str, key: &str, email_suffix: &str) -> SourceBatchRecord {
    let mut record = source_record(key, email_suffix);
    record.item.source_table_name = source_table_name.to_string();
    record
}

fn record_from_table(
    analytics_table_name: &str,
    key: &str,
    email_suffix: &str,
) -> SourceBatchRecord {
    let mut record = source_record(key, email_suffix);
    record.item.analytics_table_name = analytics_table_name.to_string();
    record
}

fn destination_state(engine: &AnalyticsEngine) -> AnalyticsEngineResult<DestinationState> {
    Ok(DestinationState {
        rows: engine.query_unscoped_sql_json(
            "SELECT table_name, tenant_id, __id, user_id, email FROM users ORDER BY __id",
        )?,
        checkpoints: engine.load_source_checkpoints()?,
        failures: engine.query_unscoped_sql_json(
            "SELECT source_table_name, shard_id, record_position, fence, hex(execution_id) AS \
             execution_id,
                    repaired_at_ms
             FROM analytics_source_batch_failures
             ORDER BY source_table_name, shard_id, record_position, failed_at_ms",
        )?,
        authorities: engine.query_unscoped_sql_json(
            "SELECT source_table_name, shard_id, fence, hex(execution_id) AS execution_id
             FROM __analytics_source_effect_authority
             ORDER BY source_table_name, shard_id",
        )?,
        receipts: engine.query_unscoped_sql_json(
            "SELECT source_table_name, shard_id, fence, hex(execution_id) AS execution_id,
                    inserted_count, updated_count, deleted_count, skipped_count,
                    dropped_fields, checkpoint_count
             FROM __analytics_source_effect_receipts
             ORDER BY source_table_name, shard_id, execution_id",
        )?,
    })
}

fn seed_authority(engine: &AnalyticsEngine, seed: u8, fence: &str) {
    engine
        .connection()
        .execute(
            "INSERT INTO __analytics_source_effect_authority
             VALUES (1, ?, ?, ?, ?, 1)",
            duckdb::params![
                SOURCE_TABLE_NAME,
                SHARD_ID,
                fence,
                Value::Blob(vec![seed; 16])
            ],
        )
        .expect("seed authority");
}

fn seed_receipt(engine: &AnalyticsEngine, seed: u8, fence: &str, inserted: i64) {
    seed_receipt_with_scope(engine, seed, fence, SOURCE_TABLE_NAME, SHARD_ID, inserted);
}

fn seed_receipt_with_scope(
    engine: &AnalyticsEngine,
    seed: u8,
    fence: &str,
    source_table_name: &str,
    shard_id: &str,
    inserted: i64,
) {
    engine
        .connection()
        .execute(
            "INSERT INTO __analytics_source_effect_receipts
             (receipt_version, execution_id, source_table_name, shard_id, fence,
              inserted_count, updated_count, deleted_count, skipped_count,
              dropped_fields, checkpoint_count, committed_at_ms)
             VALUES (1, ?, ?, ?, ?, ?, 0, 0, 0, 0, 1, 1)",
            duckdb::params![
                Value::Blob(vec![seed; 16]),
                source_table_name,
                shard_id,
                fence,
                inserted,
            ],
        )
        .expect("seed receipt");
}

fn hex_id(seed: u8) -> String {
    let digits = b"0123456789ABCDEF";
    let mut result = String::with_capacity(32);
    for byte in [seed; 16] {
        result.push(char::from(digits[usize::from(byte >> 4)]));
        result.push(char::from(digits[usize::from(byte & 0x0f)]));
    }
    result
}

fn sqlite_ducklake_backend(root: &std::path::Path) -> (StorageBackend, (PathBuf, PathBuf)) {
    let catalog_path = root.join("catalog.sqlite");
    let data_path = root.join("ducklake-data");
    fs::create_dir_all(&data_path).expect("create DuckLake data directory");
    (
        StorageBackend::DuckLake {
            catalog: CatalogType::Sqlite,
            catalog_path: catalog_path.to_string_lossy().into_owned(),
            data_path: data_path.to_string_lossy().into_owned(),
            object_storage: None,
            catalog_settings: Default::default(),
        },
        (catalog_path, data_path),
    )
}

#[test]
fn given_ducklake_mixed_batch_when_committed_then_deletes_and_upserts_are_atomic() {
    let directory = tempfile::tempdir().expect("native DuckLake storage");
    let (backend, _paths) = sqlite_ducklake_backend(directory.path());
    let engine = AnalyticsEngine::connect(&backend).expect("native DuckLake");
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).expect("registered table");
    let original = source_record("remove", "before").item;
    engine
        .ingest_stream_record_batch(&manifest, vec![original.clone()])
        .expect("initial row");
    let mut deleted = original;
    deleted.record.old_image = deleted.record.new_image.take();
    let inserted = source_record("keep", "after").item;
    engine
        .ingest_stream_record_batch(&manifest, vec![inserted, deleted])
        .expect("mixed batch");
    let state = destination_state(&engine).expect("stored rows and source metadata");
    assert_eq!(state.rows.len(), 1);
    assert!(
        state.rows[0]["email"]
            .as_str()
            .expect("email")
            .contains("after")
    );
    let stages: i64 = engine
        .connection()
        .query_row(
            "SELECT count(*) FROM duckdb_tables() WHERE temporary AND table_name LIKE \
             '__analytics_ingest_stage_%'",
            [],
            |row| row.get(0),
        )
        .expect("temporary stages");
    assert_eq!(stages, 0);
}
