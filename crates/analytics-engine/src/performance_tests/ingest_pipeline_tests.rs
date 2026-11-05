//! Ignored ingest-pipeline probes.
//!
//! These tests are deliberately ignored: they print measurements and are not
//! correctness gates. Run them with `--ignored --nocapture` in a stable build
//! profile while the machine is otherwise quiet enough for relative comparison.
//!
//! The probes separate three costs that the live service combines:
//! - per-record row materialization and staging,
//! - per-batch merge and DuckDB commit,
//! - source-effect content canonicalization.

use std::time::{Duration, Instant};

use analytics_contract::{
    AnalyticsManifest, RowIdentity, StorageItem, StorageStreamRecord, StorageValue,
};
use analytics_fixtures::{user_item, users_manifest};
use config::{CatalogType, StorageBackend};
use storage_types::VecMap;

use crate::{
    AnalyticsEngine, SourceBatchRecord, StreamRecordBatchItem,
    engine::SourceBatchExecution,
    manifest_digest,
    source_effect::{self, ContentDigestInput},
};

const SOURCE_TABLE_NAME: &str = "tenant_01";
const ANALYTICS_TABLE_NAME: &str = "users";

fn stream_keys_manifest() -> AnalyticsManifest {
    let mut manifest = users_manifest();
    manifest.tables[0].row_identity = RowIdentity::StreamKeys;
    manifest
}

fn item(index: usize) -> StorageItem {
    let key = format!("key-{index:09}");
    user_item(
        key.as_str(),
        format!("user-{index:09}@example.test").as_str(),
        "org-perf",
    )
}

fn record(index: usize) -> SourceBatchRecord {
    let key = format!("key-{index:09}");
    SourceBatchRecord {
        item: StreamRecordBatchItem {
            analytics_table_name: ANALYTICS_TABLE_NAME.to_string(),
            source_table_name: SOURCE_TABLE_NAME.to_string(),
            shard_id: Some("perf-shard".to_string()),
            system_position: None,
            record_key: key.as_bytes().to_vec(),
            record: StorageStreamRecord {
                sequence_number: format!("{index:020}"),
                keys: VecMap::from_iter([("pk".to_string(), StorageValue::S(key))]),
                old_image: None,
                new_image: Some(item(index)),
            },
        },
        retention: None,
    }
}

fn records(start: usize, count: usize) -> Vec<SourceBatchRecord> {
    (start..start + count).map(record).collect()
}

fn system_update_records(
    keyspace: usize,
    batch_size: usize,
    generation: usize,
) -> Vec<SourceBatchRecord> {
    (0..batch_size)
        .map(|index| system_update_record(index % keyspace, generation))
        .collect()
}

fn system_update_record(index: usize, generation: usize) -> SourceBatchRecord {
    let key = format!("key-{index:09}");
    let mut value = record(index);
    value.item.record.new_image = Some(user_item(
        key.as_str(),
        format!("user-{index:09}-{generation:09}@example.test").as_str(),
        "org-perf",
    ));
    let offset = format!("{generation:08}{index:08}");
    value.item.record.sequence_number = offset.clone();
    let position =
        storage_types::SystemStreamPosition::try_new("perf-shard", "perf-lineage", offset.as_str())
            .expect("system position");
    value.item.shard_id = Some(position.shard_id().as_str().to_string());
    value.item.record_key = format!("perf-shard\u{1f}perf-lineage\u{1f}{offset}").into_bytes();
    value.item.system_position = Some(position);
    value
}

fn system_records(start: usize, count: usize) -> Vec<SourceBatchRecord> {
    (start..start + count)
        .map(|index| {
            let mut value = record(index);
            let offset = format!("{index:020}");
            let position = storage_types::SystemStreamPosition::try_new(
                "perf-shard",
                "perf-lineage",
                offset.as_str(),
            )
            .expect("system position");
            value.item.shard_id = Some(position.shard_id().as_str().to_string());
            value.item.record_key =
                format!("perf-shard\u{1f}perf-lineage\u{1f}{offset}").into_bytes();
            value.item.system_position = Some(position);
            value
        })
        .collect()
}

fn commit_system_batch(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    digest: [u8; crate::MANIFEST_DIGEST_BYTES],
    source_scope: &storage_types::SystemStreamScope,
    fence: &mut u64,
    previous_checkpoint: &mut Option<storage_types::SystemStreamCheckpointToken>,
    records: Vec<SourceBatchRecord>,
) -> Duration {
    *fence += 1;
    let checkpoint =
        storage_types::SystemStreamCheckpointToken::try_from(format!("v1.{:032x}", *fence))
            .expect("checkpoint");
    let mut execution_id = [7_u8; 16];
    execution_id[..8].copy_from_slice(&fence.to_be_bytes());
    let request = crate::SourceEffectRequest::try_new(crate::SourceEffectRequestInput {
        manifest,
        manifest_digest: digest,
        authority: crate::SourceEffectAuthority::try_new(execution_id, *fence).expect("authority"),
        scope: crate::SourceEffectScope::try_new_system(
            source_scope.clone(),
            previous_checkpoint.clone(),
            checkpoint.clone(),
        )
        .expect("effect scope"),
        records,
        checkpoints: Vec::new(),
        privacy_policy: None,
    })
    .expect("request");
    let started = Instant::now();
    engine
        .ingest_fenced_source_batch(request)
        .expect("live fenced batch");
    let elapsed = started.elapsed();
    *previous_checkpoint = Some(checkpoint);
    elapsed
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn print_sample(
    label: &str,
    batch_size: usize,
    batches: usize,
    elapsed: Duration,
    digest: Option<Duration>,
) {
    let records = batch_size;
    let per_batch_ms = elapsed.as_secs_f64() * 1000.0;
    let per_record_us = elapsed.as_secs_f64() * 1_000_000.0 / records as f64;
    let records_per_second = records as f64 / elapsed.as_secs_f64();
    let digest_us = digest.map(|value| value.as_secs_f64() * 1_000_000.0 / records as f64);
    println!(
        "INGEST_PIPELINE_SAMPLE \
         {{\"label\":\"{label}\",\"batch_size\":{batch_size},\"batches\":{batches},\"records\":\
         {records},\"median_batch_ms\":{:.3},\"per_batch_ms\":{:.3},\"per_record_us\":{:.3},\"\
         records_per_second\":{:.1},\"digest_per_record_us\":{}}}",
        elapsed.as_secs_f64() * 1000.0,
        per_batch_ms,
        per_record_us,
        records_per_second,
        digest_us.map_or_else(|| "null".to_string(), |value| format!("{value:.3}")),
    );
}

/// Measures the engine's per-record and per-batch work on an in-memory DuckDB
/// destination. This removes DuckLake catalog commits from the sample.
#[test]
#[ignore = "prints an ingest throughput measurement; run explicitly"]
fn given_duckdb_memory_destination_when_batches_ingest_then_report_batch_and_record_cost() {
    let manifest = stream_keys_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("memory DuckDB");
    engine.ensure_manifest(&manifest).expect("manifest");
    let mut next_record = 0usize;

    for batch_size in [1usize, 8, 32, 128, 512, 1_024] {
        // One warmup batch is enough to create the staging path; keep the
        // measured batches on fresh keys so each one inserts.
        let warmup = records(next_record, batch_size);
        next_record += batch_size;
        engine
            .ingest_source_batch_for_test(SourceBatchExecution {
                manifest: &manifest,
                records: warmup,
                checkpoints: Vec::new(),
                policy: None,
                fence: None,
            })
            .expect("warmup batch");

        let batches = if batch_size >= 512 { 8 } else { 32 };
        let mut samples = Vec::with_capacity(batches);
        for _ in 0..batches {
            let batch = records(next_record, batch_size);
            next_record += batch_size;
            let started = Instant::now();
            engine
                .ingest_source_batch_for_test(SourceBatchExecution {
                    manifest: &manifest,
                    records: batch,
                    checkpoints: Vec::new(),
                    policy: None,
                    fence: None,
                })
                .expect("measured batch");
            samples.push(started.elapsed());
        }
        print_sample(
            "duckdb_memory_insert",
            batch_size,
            batches,
            median(samples),
            None,
        );
    }
}

/// Measures repeated updates to the same keys, which is the live workload's
/// dominant shape after the initial preload.
#[test]
#[ignore = "prints an ingest throughput measurement; run explicitly"]
fn given_repeated_keys_when_duckdb_batches_update_then_report_batch_and_record_cost() {
    let manifest = stream_keys_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("memory DuckDB");
    engine.ensure_manifest(&manifest).expect("manifest");

    for batch_size in [8usize, 128, 512, 1_024] {
        let keyspace = batch_size.max(1_024);
        for iteration in 0..=8usize {
            let batch = (0..batch_size)
                .map(|index| record(index % keyspace))
                .collect::<Vec<_>>();
            let started = Instant::now();
            engine
                .ingest_source_batch_for_test(SourceBatchExecution {
                    manifest: &manifest,
                    records: batch,
                    checkpoints: Vec::new(),
                    policy: None,
                    fence: None,
                })
                .expect("update batch");
            if iteration > 0 {
                let _ = started.elapsed();
            }
        }
        let batches = 16;
        let mut samples = Vec::with_capacity(batches);
        for _ in 0..batches {
            let batch = (0..batch_size)
                .map(|index| record(index % keyspace))
                .collect::<Vec<_>>();
            let started = Instant::now();
            engine
                .ingest_source_batch_for_test(SourceBatchExecution {
                    manifest: &manifest,
                    records: batch,
                    checkpoints: Vec::new(),
                    policy: None,
                    fence: None,
                })
                .expect("measured update batch");
            samples.push(started.elapsed());
        }
        print_sample(
            "duckdb_memory_update",
            batch_size,
            batches,
            median(samples),
            None,
        );
    }
}

/// Measures the canonical-JSON source-effect digest independently from staging
/// and destination writes.
#[test]
#[ignore = "prints an ingest throughput measurement; run explicitly"]
fn given_source_batch_when_digested_then_report_canonicalization_cost() {
    let manifest = stream_keys_manifest();
    let digest = manifest_digest(&manifest);
    for batch_size in [1usize, 128, 1_024] {
        let batch = records(0, batch_size);
        let batches = 32;
        let mut samples = Vec::with_capacity(batches);
        for _ in 0..batches {
            let started = Instant::now();
            let value = source_effect::content_digest(ContentDigestInput {
                manifest_digest: &digest,
                previous_position: None,
                records: &batch,
                checkpoints: &[],
                policy: None,
            });
            std::hint::black_box(value);
            samples.push(started.elapsed());
        }
        print_sample(
            "source_effect_digest",
            batch_size,
            batches,
            median(samples.clone()),
            Some(median(samples)),
        );
    }
}

/// Optional live DuckLake probe. Set `AUX_ANALYTICS_DUCKLAKE_FDB_LIVE=1` and
/// the same `AUX_DUCKLAKE_*` variables used by the service to include the real
/// catalog commit in the sample.
#[test]
#[ignore = "requires AUX_ANALYTICS_DUCKLAKE_FDB_LIVE and a local FoundationDB catalog"]
fn given_live_aux_catalog_when_fenced_batches_commit_then_report_end_to_end_cost() {
    if std::env::var_os("AUX_ANALYTICS_DUCKLAKE_FDB_LIVE").is_none() {
        println!("INGEST_PIPELINE_SAMPLE_SKIPPED live DuckLake probe is opt-in");
        return;
    }
    let directory = tempfile::tempdir().expect("catalog directory");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::AuxCatalog,
        catalog_path: directory
            .path()
            .join("metadata.duckdb")
            .to_string_lossy()
            .into_owned(),
        data_path: directory.path().join("data").to_string_lossy().into_owned(),
        object_storage: None,
        catalog_settings: config::DuckLakeCatalogSettings::default(),
    };
    let engine = AnalyticsEngine::connect(&backend).expect("live DuckLake");
    let manifest = stream_keys_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    let digest = manifest_digest(&manifest);
    let source_scope = storage_types::SystemStreamScope::try_new("perf-system", 1).expect("scope");
    let mut fence = 1_u64;
    let mut previous_checkpoint = None;

    let mut next_record = 0usize;
    for batch_size in [8usize, 128, 512, 1_024] {
        let batches = 8;
        let mut samples = Vec::with_capacity(batches);
        for _ in 0..batches {
            let batch = system_records(next_record, batch_size);
            next_record += batch_size;
            samples.push(commit_system_batch(
                &engine,
                &manifest,
                digest,
                &source_scope,
                &mut fence,
                &mut previous_checkpoint,
                batch,
            ));
        }
        print_sample(
            "ducklake_aux_insert",
            batch_size,
            batches,
            median(samples),
            None,
        );
    }
}

/// Measures repeated updates of a stable key set, which is the live write
/// shape after the initial load. DuckLake may rewrite or delete files here.
#[test]
#[ignore = "requires AUX_ANALYTICS_DUCKLAKE_FDB_LIVE and a local FoundationDB catalog"]
fn given_live_aux_catalog_when_fenced_updates_commit_then_report_steady_state_cost() {
    if std::env::var_os("AUX_ANALYTICS_DUCKLAKE_FDB_LIVE").is_none() {
        println!("INGEST_PIPELINE_SAMPLE_SKIPPED live DuckLake probe is opt-in");
        return;
    }
    let directory = tempfile::tempdir().expect("catalog directory");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::AuxCatalog,
        catalog_path: directory
            .path()
            .join("metadata.duckdb")
            .to_string_lossy()
            .into_owned(),
        data_path: directory.path().join("data").to_string_lossy().into_owned(),
        object_storage: None,
        catalog_settings: config::DuckLakeCatalogSettings::default(),
    };
    let engine = AnalyticsEngine::connect(&backend).expect("live DuckLake");
    let manifest = stream_keys_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    let digest = manifest_digest(&manifest);
    let source_scope = storage_types::SystemStreamScope::try_new("perf-system", 1).expect("scope");
    let mut fence = 0_u64;
    let mut previous_checkpoint = None;
    let keyspace = 20_000usize;

    // Seed the key set once.
    let mut inserted = 0usize;
    while inserted < keyspace {
        let count = (keyspace - inserted).min(1_024);
        commit_system_batch(
            &engine,
            &manifest,
            digest,
            &source_scope,
            &mut fence,
            &mut previous_checkpoint,
            (inserted..inserted + count)
                .map(|index| system_update_record(index, 0))
                .collect(),
        );
        inserted += count;
    }

    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT count(*) AS n FROM users")
            .unwrap()[0]["n"],
        keyspace
    );

    for batch_size in [128usize, 512, 1_024] {
        let batches = 16;
        let mut samples = Vec::with_capacity(batches);
        for generation in 1..=batches {
            samples.push(commit_system_batch(
                &engine,
                &manifest,
                digest,
                &source_scope,
                &mut fence,
                &mut previous_checkpoint,
                system_update_records(keyspace, batch_size, generation),
            ));
        }
        print_sample(
            "ducklake_aux_steady_update",
            batch_size,
            batches,
            median(samples),
            None,
        );
    }
}

#[test]
#[ignore = "prints a system-source owner-state measurement; run explicitly"]
fn given_system_source_updates_when_batches_commit_then_report_owner_state_cost() {
    let manifest = stream_keys_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    engine.ensure_manifest(&manifest).unwrap();
    let digest = manifest_digest(&manifest);
    let scope = storage_types::SystemStreamScope::try_new("perf-system", 1).unwrap();
    let mut fence = 0;
    let mut previous = None;
    for batch_size in [128, 512, 1_024] {
        let mut samples = Vec::new();
        for generation in 0..9 {
            let records = system_update_records(1_024, batch_size, generation + fence as usize + 1);
            let elapsed = commit_system_batch(
                &engine,
                &manifest,
                digest,
                &scope,
                &mut fence,
                &mut previous,
                records,
            );
            if generation > 0 {
                samples.push(elapsed);
            }
        }
        print_sample(
            "duckdb_system_owner_update",
            batch_size,
            samples.len(),
            median(samples),
            None,
        );
        assert_eq!(
            engine
                .query_unscoped_sql_json("SELECT count(*) AS n FROM users")
                .unwrap()[0]["n"],
            batch_size
        );
    }
}
