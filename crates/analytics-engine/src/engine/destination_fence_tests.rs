use analytics_contract::{AnalyticsManifest, StorageStreamRecord};
use analytics_fixtures::{user_item, users_manifest};
use storage_types::VecMap;

use super::{
    AnalyticsEngine, RecordCheckpoint, SourceBatchRecord, StreamRecordBatchItem,
    destination_fence::DestinationFence,
};
use crate::{
    SourceEffectAuthority, SourceEffectDisposition, SourceEffectRequest, SourceEffectRequestInput,
    SourceEffectScope, manifest_digest,
};

#[test]
fn given_missing_catalog_fence_function_when_source_effect_is_requested_then_no_effect_or_transaction_remains()
 {
    let mut engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest).unwrap();
    engine.destination_fence = DestinationFence::CatalogSnapshot;
    let error = engine
        .ingest_fenced_source_batch(source_request(&manifest, 1))
        .unwrap_err();
    assert!(error.to_string().contains("ducklake_set_transaction_fence"));
    for table in [
        "users",
        "analytics_checkpoints",
        "__analytics_source_effect_authority",
        "__analytics_source_effect_receipts",
    ] {
        let count: i64 = engine
            .connection()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
    engine
        .connection()
        .execute_batch("BEGIN; ROLLBACK")
        .expect("failed fence must roll back its transaction");
    assert!(engine.verify_destination_fence().is_err());
    engine
        .connection()
        .execute_batch("BEGIN; ROLLBACK")
        .unwrap();
}

#[test]
#[ignore = "requires an isolated FoundationDB-backed DuckLake catalog and explicit destination \
            configuration"]
fn given_remote_destination_when_source_effect_commits_and_replays_then_exact_receipt_and_frontier_are_durable()
 {
    let path = std::env::var_os("ANALYTICS_FENCE_TEST_BACKEND")
        .expect("explicit isolated backend configuration");
    let fixture: RemoteDestinationFixture =
        serde_json::from_slice(&std::fs::read(path).unwrap()).expect("valid backend configuration");
    let backend = config::StorageBackend::DuckLake {
        catalog: config::CatalogType::AuxCatalog,
        catalog_path: fixture.catalog_path,
        data_path: fixture.data_path,
        object_storage: fixture.object_storage,
        catalog_settings: Default::default(),
    };
    let manifest = users_manifest();
    let engine = AnalyticsEngine::connect(&backend).unwrap();
    engine.ensure_manifest(&manifest).unwrap();
    engine.verify_destination_fence().unwrap();
    assert_eq!(row_count(&engine), 0, "requires an empty dedicated catalog");
    assert!(matches!(
        engine
            .ingest_fenced_source_batch(source_request(&manifest, 1))
            .unwrap(),
        SourceEffectDisposition::Applied { .. }
    ));
    assert_receipt_bytes(&engine, "before flush");
    engine
        .connection()
        .execute_batch("CALL ducklake_flush_inlined_data('dlake');")
        .unwrap();
    assert_receipt_bytes(&engine, "after flush");
    let reopened = AnalyticsEngine::connect(&backend).unwrap();
    assert_receipt_bytes(&reopened, "reopened");
    let replay = reopened
        .ingest_fenced_source_batch(source_request(&manifest, 1))
        .unwrap();
    assert!(
        matches!(replay, SourceEffectDisposition::AlreadyApplied { .. }),
        "{replay:?}"
    );
    assert_eq!(
        reopened
            .ingest_fenced_source_batch(source_request(&manifest, 2))
            .unwrap(),
        SourceEffectDisposition::StaleFence
    );
    assert_eq!(row_count(&reopened), 1);
    let checkpoint: String = reopened
        .connection()
        .query_row(
            "SELECT position FROM analytics_checkpoints WHERE source_table_name='tenant_01' AND \
             shard_id='shard-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(checkpoint, "1");
    let receipts: i64 = reopened
        .connection()
        .query_row(
            "SELECT count(*) FROM __analytics_source_effect_receipts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 1);
}

fn row_count(engine: &AnalyticsEngine) -> i64 {
    engine
        .connection()
        .query_row("SELECT count(*) FROM users", [], |row| row.get(0))
        .unwrap()
}

fn source_request(manifest: &AnalyticsManifest, seed: u8) -> SourceEffectRequest<'_> {
    SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest,
        manifest_digest: manifest_digest(manifest),
        authority: SourceEffectAuthority::try_new([seed; 16], u64::from(seed)).unwrap(),
        scope: SourceEffectScope::try_new("tenant_01", "shard-1").unwrap(),
        records: vec![SourceBatchRecord {
            item: StreamRecordBatchItem {
                analytics_table_name: "users".into(),
                source_table_name: "tenant_01".into(),
                shard_id: Some("shard-1".into()),
                system_position: None,
                record_key: b"user-1".to_vec(),
                record: StorageStreamRecord {
                    sequence_number: "1".into(),
                    keys: VecMap::new(),
                    old_image: None,
                    new_image: Some(user_item("user-1", "user@example.test", "org-a")),
                },
            },
            retention: None,
        }],
        checkpoints: vec![RecordCheckpoint::try_new("tenant_01", "shard-1", "1").unwrap()],
        privacy_policy: None,
    })
    .unwrap()
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteDestinationFixture {
    catalog_path: String,
    data_path: String,
    object_storage: Option<Box<config::AnalyticsObjectStorageConfig>>,
}

fn assert_receipt_bytes(engine: &AnalyticsEngine, stage: &str) {
    let values: (i64, String) = engine
        .connection()
        .query_row(
            "SELECT count(*), coalesce(max(hex(execution_id)), '') FROM \
             __analytics_source_effect_receipts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(values, (1, "01".repeat(16)), "receipt bytes {stage}");
    let filtered: i64 = engine
        .connection()
        .query_row(
            "SELECT count(*) FROM __analytics_source_effect_receipts WHERE execution_id = ?",
            [duckdb::types::Value::Blob(vec![1; 16])],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(filtered, 1, "binary equality filter {stage}");
}
