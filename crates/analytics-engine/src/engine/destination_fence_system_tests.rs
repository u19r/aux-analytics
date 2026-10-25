use std::time::Instant;

use analytics_contract::{AnalyticsManifest, StorageStreamRecord};
use storage_types::{SystemStreamCheckpointToken, SystemStreamPosition, SystemStreamScope};

use super::{AnalyticsEngine, SourceBatchRecord, StreamRecordBatchItem};
use crate::{
    SourceEffectAuthority, SourceEffectDisposition, SourceEffectRequest, SourceEffectRequestInput,
    SourceEffectScope, manifest_digest,
};

#[test]
#[ignore = "requires an isolated FoundationDB-backed DuckLake catalog and explicit caller fixture"]
fn given_remote_system_destination_when_caller_batch_commits_then_replay_and_frontier_are_durable()
{
    let path = std::env::var_os("ANALYTICS_SYSTEM_FENCE_TEST_FIXTURE")
        .expect("explicit isolated caller fixture");
    let fixture: SystemDestinationFixture =
        serde_json::from_slice(&std::fs::read(path).unwrap()).expect("valid caller fixture");
    let backend = fixture.storage_backend();
    let engine = AnalyticsEngine::connect(&backend).unwrap();
    let started = Instant::now();
    engine.ensure_manifest(&fixture.manifest).unwrap();
    println!(
        "system_fence_stage=manifest elapsed_ms={}",
        started.elapsed().as_millis()
    );
    engine.verify_destination_fence().unwrap();
    assert_eq!(
        fixture.row_count(&engine),
        0,
        "requires an empty dedicated catalog"
    );
    fixture.seed_existing_owners(&engine);
    let started = Instant::now();
    let outcome = engine
        .ingest_fenced_source_batch(fixture.source_request(0))
        .unwrap();
    println!(
        "system_fence_stage=ingest elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let SourceEffectDisposition::Applied { summary } = outcome else {
        panic!("first source effect must apply");
    };
    assert!(summary.inserted() >= fixture.expected_rows);
    assert_eq!(summary.checkpoint_count(), 1);
    assert_eq!(fixture.row_count(&engine), fixture.expected_rows);
    let reopened = AnalyticsEngine::connect(&backend).unwrap();
    assert_eq!(
        reopened.load_system_stream_checkpoint().unwrap(),
        Some(fixture.frontier(0))
    );
    assert!(matches!(
        reopened
            .ingest_fenced_source_batch(fixture.source_request(0))
            .unwrap(),
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
    assert_eq!(fixture.row_count(&reopened), fixture.expected_rows);
    assert!(matches!(
        reopened
            .ingest_fenced_source_batch(fixture.source_request(1))
            .unwrap(),
        SourceEffectDisposition::Applied { .. }
    ));
    assert_eq!(
        fixture.row_count(&reopened),
        fixture.expected_rows_after_page(1),
        "second page must follow the registered append or replacement policy"
    );
    let owners_before_flush = fixture.owner_state_digest(&reopened);
    reopened
        .connection()
        .execute_batch("CALL ducklake_flush_inlined_data('dlake')")
        .unwrap();
    let persisted = AnalyticsEngine::connect(&backend).unwrap();
    fixture.assert_flushed_deletions(&persisted);
    assert_eq!(
        fixture.owner_state_digest(&persisted),
        owners_before_flush,
        "flush must preserve all owner state"
    );
    assert_eq!(
        persisted.load_system_stream_checkpoint().unwrap(),
        Some(fixture.frontier(1))
    );
    assert_eq!(
        fixture.row_count(&persisted),
        fixture.expected_rows_after_page(1)
    );
    assert!(matches!(
        persisted
            .ingest_fenced_source_batch(fixture.source_request(1))
            .unwrap(),
        SourceEffectDisposition::AlreadyApplied { .. }
    ));
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemDestinationFixture {
    catalog_path: String,
    data_path: String,
    object_storage: Option<Box<config::AnalyticsObjectStorageConfig>>,
    manifest: AnalyticsManifest,
    source_table_name: String,
    analytics_table_names: Vec<String>,
    records: Vec<StorageStreamRecord>,
    expected_table: String,
    expected_rows: u64,
    #[serde(default)]
    preexisting_owner_rows: usize,
}

impl SystemDestinationFixture {
    fn seed_existing_owners(&self, engine: &AnalyticsEngine) {
        let started = Instant::now();
        let position = serde_json::to_string(&analytics_contract::SourcePosition::SystemStream {
            source_scope: self.frontier(0).0,
            position: SystemStreamPosition::try_new("existing-shard", "existing-lineage", "0")
                .unwrap(),
        })
        .unwrap();
        let mut statement = engine
            .connection()
            .prepare(
                "INSERT INTO __analytics_source_record_states
             (analytics_table_name, source_table_name, source_key_digest,
              source_position_json, child_identities_json, updated_at_ms)
             VALUES ('existing_projection', 'existing_source', sha256(?), CAST(? AS JSON), '[]', 1)",
            )
            .unwrap();
        for start in (0..self.preexisting_owner_rows).step_by(1_000) {
            engine.connection().execute_batch("BEGIN").unwrap();
            for index in start..(start + 1_000).min(self.preexisting_owner_rows) {
                assert_eq!(
                    statement
                        .execute(duckdb::params![format!("existing-owner-{index}"), position])
                        .unwrap(),
                    1
                );
            }
            engine.connection().execute_batch("COMMIT").unwrap();
        }
        let (rows, unique_keys): (usize, usize) = engine
            .connection()
            .query_row(
                "SELECT count(*), count(DISTINCT source_key_digest) FROM \
                 __analytics_source_record_states",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(rows, self.preexisting_owner_rows, "fixture owner count");
        assert_eq!(unique_keys, rows, "fixture owner keys must be unique");
        println!(
            "system_fence_stage=existing_owners elapsed_ms={}",
            started.elapsed().as_millis()
        );
    }

    fn storage_backend(&self) -> config::StorageBackend {
        config::StorageBackend::DuckLake {
            catalog: config::CatalogType::AuxCatalog,
            catalog_path: self.catalog_path.clone(),
            data_path: self.data_path.clone(),
            object_storage: self.object_storage.clone(),
            catalog_settings: Default::default(),
        }
    }

    fn frontier(&self, page: u8) -> (SystemStreamScope, SystemStreamCheckpointToken) {
        (
            SystemStreamScope::try_new("native-system-proof", 1).unwrap(),
            SystemStreamCheckpointToken::try_from(format!(
                "v1.{}",
                format!("{:02x}", 0xab + page).repeat(16)
            ))
            .unwrap(),
        )
    }

    fn source_request(&self, page: u8) -> SourceEffectRequest<'_> {
        let (scope, checkpoint) = self.frontier(page);
        SourceEffectRequest::try_new(SourceEffectRequestInput {
            manifest: &self.manifest,
            manifest_digest: manifest_digest(&self.manifest),
            authority: SourceEffectAuthority::try_new([61 + page; 16], 2 - u64::from(page))
                .unwrap(),
            scope: SourceEffectScope::try_new_system(
                scope,
                page.checked_sub(1)
                    .map(|previous| self.frontier(previous).1),
                checkpoint,
            )
            .unwrap(),
            records: self.source_records(page),
            checkpoints: Vec::new(),
            privacy_policy: None,
        })
        .unwrap()
    }

    fn source_records(&self, page: u8) -> Vec<SourceBatchRecord> {
        self.records
            .iter()
            .enumerate()
            .flat_map(|(index, record)| {
                self.analytics_table_names
                    .iter()
                    .map(move |name| SourceBatchRecord {
                        item: StreamRecordBatchItem {
                            analytics_table_name: name.clone(),
                            source_table_name: self.source_table_name.clone(),
                            shard_id: Some("native-shard".into()),
                            system_position: Some(
                                SystemStreamPosition::try_new(
                                    "native-shard",
                                    "native-lineage",
                                    (index + 1 + usize::from(page) * self.records.len())
                                        .to_string(),
                                )
                                .unwrap(),
                            ),
                            record_key: serde_json::to_vec(&record.keys).unwrap(),
                            record: record.clone(),
                        },
                        retention: None,
                    })
            })
            .collect()
    }

    fn owner_state_digest(&self, engine: &AnalyticsEngine) -> (u64, u64, u64) {
        let digest: (u64, u64, u64) = engine
            .connection()
            .query_row(
                "SELECT count(*), count(DISTINCT source_key_digest),
             coalesce(bit_xor(hash(source_key_digest, source_position_json, child_identities_json, \
                 updated_at_ms)), 0)
             FROM __analytics_source_record_states",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            digest.0, digest.1,
            "each owner key must have one visible state"
        );
        digest
    }

    fn is_expected_table_append_only(&self) -> bool {
        self.manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == self.expected_table)
            .expect("expected projection must be registered")
            .skip_delete
    }

    fn expected_rows_after_page(&self, page: u8) -> u64 {
        if self.is_expected_table_append_only() {
            self.expected_rows * (u64::from(page) + 1)
        } else {
            self.expected_rows
        }
    }

    fn assert_flushed_deletions(&self, engine: &AnalyticsEngine) {
        let (files, deletions): (u64, u64) = engine
            .connection()
            .query_row(
                "SELECT count(*), count(delete_file) FROM ducklake_list_files('dlake', ?)",
                [&self.expected_table],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        println!("system_fence_files={files} system_fence_delete_files={deletions}");
        assert!(files > 0, "flush must persist actual data files");
        if !self.is_expected_table_append_only() {
            assert!(
                deletions > 0,
                "replaced inline rows require persisted deletion masks"
            );
        }
    }

    fn row_count(&self, engine: &AnalyticsEngine) -> u64 {
        assert!(
            self.expected_table
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        );
        let count = engine
            .connection()
            .query_row(
                &format!("SELECT count(*) FROM {}", self.expected_table),
                [],
                |row| row.get(0),
            )
            .unwrap();
        let scanned = engine
            .query_unscoped_sql_json(&format!("SELECT * FROM {}", self.expected_table))
            .unwrap()
            .len() as u64;
        assert_eq!(
            count, scanned,
            "metadata count must match scanned visible rows"
        );
        count
    }
}
