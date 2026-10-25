use std::time::Instant;

use alloc_counter::AllocationGuard;
use analytics_contract::{AnalyticsManifest, StorageItem, StorageStreamRecord};
use analytics_engine::{AnalyticsEngine, IngestOutcome, StreamRecordBatchItem};
use analytics_storage::{PollBatch, PolledRecord, SourceCheckpoint};
use serde_json::{Value, json};
use storage_types::SystemStreamPosition;

use super::{
    super::observation::SourceJobShared, SourceJobCheckpoint, SourceJobKind, SourceJobPayload,
    SourceJobRecord, SourceJobScope, batch_identity, source_payload::BatchIdentityInput,
    validate_checkpoints, validate_deadline, validate_kind_scope, validate_payload_bounds,
    validate_records, validate_scope,
};

// Reconstructs the former owned layout without adding a per-record Vec absent
// from that implementation. No production alternative or instrumentation
// exists.
struct FormerSourceRecord {
    source_table_name: String,
    shard_id: String,
    analytics_table_name: String,
    record_key: String,
    record: StorageStreamRecord,
    system_position: Option<SystemStreamPosition>,
}

struct SourceJobWorkload {
    manifest: AnalyticsManifest,
    batch: PollBatch,
    expected_rows: Vec<Value>,
    expected_wire: Vec<u8>,
}

struct SourceJobCycle {
    wire: Vec<u8>,
    outcomes: Vec<IngestOutcome>,
    checkpoints: Vec<SourceCheckpoint>,
    retained_wide_value_bytes: usize,
}

#[test]
#[ignore = "paired source routing/persistence/decode/native-ingestion measurement"]
fn given_equal_source_jobs_when_logical_and_expanded_ownership_run_then_full_native_rows_and_wire_match()
 {
    let selected = std::env::var_os("ANALYTICS_SOURCE_JOB_PROBE_MODE").map(|value| {
        value
            .into_string()
            .expect("source job probe mode must be UTF-8")
    });
    assert!(
        selected
            .as_deref()
            .is_none_or(|mode| matches!(mode, "expanded" | "logical")),
        "unsupported ANALYTICS_SOURCE_JOB_PROBE_MODE"
    );
    let allocations = std::env::var_os("ANALYTICS_SOURCE_JOB_PROBE_ALLOCATIONS")
        .map(|value| {
            match value
                .into_string()
                .expect("allocation selector must be UTF-8")
                .as_str()
            {
                "true" => true,
                "false" => false,
                _ => panic!("unsupported allocation selector"),
            }
        })
        .unwrap_or(true);
    let shapes = if selected.is_some() {
        vec![(64, 8192, 16)]
    } else {
        [1, 32]
            .into_iter()
            .flat_map(|rows| {
                [64, 8192].into_iter().flat_map(move |width| {
                    [1, 4, 16]
                        .into_iter()
                        .map(move |destinations| (rows, width, destinations))
                })
            })
            .collect()
    };
    for (rows, width, destinations) in shapes {
        for pair in 0..3 {
            let modes = if pair % 2 == 0 {
                ["expanded", "logical"]
            } else {
                ["logical", "expanded"]
            };
            for mode in modes {
                if selected.as_deref().is_some_and(|selected| selected != mode) {
                    continue;
                }
                let workload = SourceJobWorkload::new(rows, width, destinations);
                let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
                engine.ensure_manifest(&workload.manifest).unwrap();
                let digest = analytics_engine::manifest_digest(&workload.manifest);
                let guard = allocations.then(|| {
                    AllocationGuard::start(
                        module_path!(),
                        "source-job-routing-native",
                        file!(),
                        line!(),
                        Some(mode),
                    )
                });
                let started = Instant::now();
                let cycle = match mode {
                    "expanded" => {
                        run_former_source_job(&engine, &workload.manifest, workload.batch, digest)
                    }
                    _ => {
                        run_logical_source_job(&engine, &workload.manifest, workload.batch, digest)
                    }
                };
                let elapsed = started.elapsed().as_nanos();
                let report = guard.map(AllocationGuard::finish);
                assert_eq!(cycle.wire, workload.expected_wire);
                assert_eq!(
                    cycle.outcomes,
                    vec![IngestOutcome::Updated; rows * destinations]
                );
                assert_eq!(
                    cycle.checkpoints,
                    [SourceCheckpoint {
                        source_table_name: "tenant_01".into(),
                        shard_id: "shard".into(),
                        position: "checkpoint".into()
                    }]
                );
                for table in &workload.manifest.tables {
                    assert_eq!(
                        engine
                            .query_unscoped_sql_json(&format!(
                                "SELECT __id, table_name, tenant_id, email, item FROM {} ORDER BY \
                                 email",
                                table.analytics_table_name
                            ))
                            .unwrap(),
                        workload.expected_rows
                    );
                }
                println!(
                    "SOURCE_JOB_SAMPLE {}",
                    json!({"mode": mode, "pair": pair, "rows": rows,
                    "width": width, "destinations": destinations, "elapsed_ns": elapsed,
                    "allocations": report.as_ref().map(|report| report.allocation_count),
                    "allocated_bytes": report.as_ref().map(|report| report.allocated_bytes),
                    "retained_wide_value_bytes": cycle.retained_wide_value_bytes,
                    "wire_bytes": cycle.wire.len(), "wire_capacity": cycle.wire.capacity()})
                );
            }
        }
    }
}

impl SourceJobWorkload {
    fn new(rows: usize, width: usize, destinations: usize) -> Self {
        let mut manifest = analytics_fixtures::users_manifest();
        let template = manifest.tables.pop().unwrap();
        for index in 0..destinations {
            let mut table = template.clone();
            table.analytics_table_name = format!("destination_{index}");
            manifest.tables.push(table);
        }
        let names = manifest
            .tables
            .iter()
            .map(|table| table.analytics_table_name.clone())
            .collect::<Vec<_>>();
        let text = "x".repeat(width);
        let mut records = Vec::new();
        let mut expected_rows = Vec::new();
        for index in 0..rows {
            let email = format!("user_{index:04}@example.test");
            let key = format!("{index:04}");
            let image = json!({"profile": {"M": {"email": {"S": email}}}, "payload": {"M": {
                "text": {"S": text}, "tags": {"SS": ["alpha", "beta"]}, "numbers": {"NS": ["1", "2"]},
                "binary": {"BS": ["AAE=", "AgM="]}, "values": {"L": [{"N": "12.3400"}, {"NULL": true}, {"B": "AAE="}]}, "enabled": {"BOOL": true}
            }}});
            let new_image: StorageItem = serde_json::from_value(image).unwrap();
            records.push(PolledRecord {
                source_table_name: "tenant_01".into(),
                shard_id: "shard".into(),
                analytics_table_names: names.clone(),
                record_key: key.clone(),
                system_position: None,
                record: StorageStreamRecord {
                    sequence_number: key.clone(),
                    keys: serde_json::from_value(json!({"pk": {"S": key}})).unwrap(),
                    old_image: Some(new_image.clone()),
                    new_image: Some(new_image),
                },
            });
            expected_rows.push(json!({"__id": key.bytes().map(|byte| format!("{byte:02x}")).collect::<String>(),
                "table_name": "tenant_01", "tenant_id": "tenant_01", "email": email,
                "item": {"profile": {"email": email}, "payload": {"text": text, "tags": ["alpha", "beta"],
                    "numbers": [1, 2], "binary": ["AAE=", "AgM="], "values": [12.34, null, "AAE="], "enabled": true}}}));
        }
        let batch = PollBatch {
            source_response_count: 1,
            source_nonempty_response_count: 1,
            source_record_count: rows,
            source_encoded_bytes: 0,
            records,
            checkpoints: vec![SourceCheckpoint {
                source_table_name: "tenant_01".into(),
                shard_id: "shard".into(),
                position: "checkpoint".into(),
            }],
            source_predecessor: None,
            system_stream: None,
        };
        let expected_wire = SourceJobPayload::from_batch(
            SourceJobKind::StorageStream,
            batch.clone(),
            1000,
            analytics_engine::manifest_digest(&manifest),
        )
        .unwrap()
        .encode()
        .unwrap();
        Self {
            manifest,
            batch,
            expected_rows,
            expected_wire,
        }
    }
}

fn run_logical_source_job(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    batch: PollBatch,
    digest: [u8; analytics_engine::MANIFEST_DIGEST_BYTES],
) -> SourceJobCycle {
    let retained_wide_value_bytes = batch
        .records
        .iter()
        .map(|record| wide_value_capacity(&record.record))
        .sum();
    let payload =
        SourceJobPayload::from_batch(SourceJobKind::StorageStream, batch, 1000, digest).unwrap();
    let wire = payload.encode().unwrap();
    let batch = SourceJobPayload::decode(&wire).unwrap().into_batch();
    let records = batch
        .records
        .into_iter()
        .flat_map(SourceJobShared::into_destination_records)
        .collect();
    let outcomes = engine
        .ingest_stream_record_batch(manifest, records)
        .unwrap();
    SourceJobCycle {
        wire,
        outcomes,
        checkpoints: batch.checkpoints,
        retained_wide_value_bytes,
    }
}

fn run_former_source_job(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    batch: PollBatch,
    digest: [u8; analytics_engine::MANIFEST_DIGEST_BYTES],
) -> SourceJobCycle {
    // Scope derivation uses the logical input in both modes. This
    // conservatively excludes the former extra scope-validation scan over
    // fan-out copies.
    let scope = SourceJobScope::from_poll_batch(&batch).unwrap();
    let expanded = expand_former_records(batch.records);
    let retained_wide_value_bytes = expanded
        .iter()
        .map(|record| wide_value_capacity(&record.record))
        .sum();
    let records = coalesce_former_records(expanded);
    let checkpoints = batch
        .checkpoints
        .into_iter()
        .map(SourceJobCheckpoint::from_storage_checkpoint)
        .collect::<Vec<_>>();
    validate_kind_scope(SourceJobKind::StorageStream, &scope).unwrap();
    validate_scope(&scope).unwrap();
    validate_deadline(1000).unwrap();
    validate_payload_bounds(&scope, &records, &checkpoints).unwrap();
    validate_records(&scope, &records).unwrap();
    validate_checkpoints(&checkpoints).unwrap();
    let batch_id = batch_identity(BatchIdentityInput {
        source_kind: SourceJobKind::StorageStream,
        manifest_digest: &digest,
        scope: &scope,
        records: &records,
        checkpoints: &checkpoints,
    });
    let payload = SourceJobPayload::V1 {
        source_kind: SourceJobKind::StorageStream,
        manifest_digest: digest,
        scope,
        batch_id,
        deadline_at_ms: 1000,
        records,
        checkpoints,
    };
    let wire = payload.encode().unwrap();
    let SourceJobPayload::V1 {
        scope,
        records,
        checkpoints,
        ..
    } = SourceJobPayload::decode(&wire).unwrap();
    let SourceJobScope::Table {
        source_table_name,
        shard_id,
        ..
    } = scope
    else {
        panic!("table probe scope")
    };
    let table_scope = Some((source_table_name.as_str(), shard_id.as_str()));
    let checkpoints = checkpoints
        .into_iter()
        .map(|checkpoint| checkpoint.into_storage_checkpoint(table_scope))
        .collect();
    let expanded = expand_former_payload(records, &shard_id);
    let records = expanded
        .into_iter()
        .map(|record| StreamRecordBatchItem {
            source_table_name: record.source_table_name,
            analytics_table_name: record.analytics_table_name,
            shard_id: Some(record.shard_id),
            record_key: record.record_key.into_bytes(),
            record: record.record,
            system_position: record.system_position,
        })
        .collect();
    let outcomes = engine
        .ingest_stream_record_batch(manifest, records)
        .unwrap();
    SourceJobCycle {
        wire,
        outcomes,
        checkpoints,
        retained_wide_value_bytes,
    }
}

fn expand_former_records(records: Vec<PolledRecord>) -> Vec<FormerSourceRecord> {
    let mut output = Vec::new();
    for mut record in records {
        let last = record.analytics_table_names.pop().unwrap();
        let final_record = FormerSourceRecord {
            source_table_name: record.source_table_name,
            shard_id: record.shard_id,
            analytics_table_name: last,
            record_key: record.record_key,
            record: record.record,
            system_position: record.system_position,
        };
        for analytics_table_name in record.analytics_table_names {
            output.push(FormerSourceRecord {
                analytics_table_name,
                source_table_name: final_record.source_table_name.clone(),
                shard_id: final_record.shard_id.clone(),
                record_key: final_record.record_key.clone(),
                record: final_record.record.clone(),
                system_position: final_record.system_position.clone(),
            });
        }
        output.push(final_record);
    }
    output
}

fn coalesce_former_records(records: Vec<FormerSourceRecord>) -> Vec<SourceJobRecord> {
    let mut grouped: Vec<SourceJobRecord> = Vec::new();
    for record in records {
        if let Some(previous) = grouped.last_mut().filter(|previous| {
            previous.source_table_name == record.source_table_name
                && previous.record_key == record.record_key.as_bytes()
                && previous.system_position == record.system_position
                && previous.record == record.record
        }) {
            previous
                .analytics_table_names
                .push(record.analytics_table_name);
            continue;
        }
        grouped.push(SourceJobRecord {
            source_table_name: record.source_table_name,
            analytics_table_names: vec![record.analytics_table_name],
            record_key: record.record_key.into_bytes(),
            record: record.record,
            system_position: record.system_position,
        });
    }
    grouped
}

fn expand_former_payload(records: Vec<SourceJobRecord>, shard_id: &str) -> Vec<FormerSourceRecord> {
    let capacity = records
        .iter()
        .map(|record| record.analytics_table_names.len())
        .sum();
    let mut expanded = Vec::with_capacity(capacity);
    for mut record in records {
        let last = record.analytics_table_names.pop().unwrap();
        let final_record = FormerSourceRecord {
            source_table_name: record.source_table_name,
            shard_id: shard_id.into(),
            analytics_table_name: last,
            record_key: String::from_utf8(record.record_key).unwrap(),
            record: record.record,
            system_position: record.system_position,
        };
        expanded.extend(
            record
                .analytics_table_names
                .into_iter()
                .map(|analytics_table_name| FormerSourceRecord {
                    analytics_table_name,
                    source_table_name: final_record.source_table_name.clone(),
                    shard_id: final_record.shard_id.clone(),
                    record_key: final_record.record_key.clone(),
                    record: final_record.record.clone(),
                    system_position: final_record.system_position.clone(),
                }),
        );
        expanded.push(final_record);
    }
    expanded
}

// A lower bound on retained source data, not total heap or peak process memory.
fn wide_value_capacity(record: &StorageStreamRecord) -> usize {
    [&record.old_image, &record.new_image]
        .into_iter()
        .flatten()
        .map(|image| {
            let analytics_contract::StorageValue::M(payload) = image.get("payload").unwrap() else {
                panic!("payload map")
            };
            let analytics_contract::StorageValue::S(text) = payload.get("text").unwrap() else {
                panic!("payload text")
            };
            text.capacity()
        })
        .sum()
}
