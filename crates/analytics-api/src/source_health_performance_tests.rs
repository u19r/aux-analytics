use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use analytics_storage::SourceCheckpoint;

use crate::{
    CheckpointHealth, SourceHealth,
    source_polling::{
        SourceBatchOutcome, apply_source_durable_checkpoints, upsert_checkpoint_health,
    },
};

const RECORD_COUNT: usize = 500;
const ATTRIBUTE_COUNT: usize = 10;
const KEY_BYTES: usize = 100;
const PAYLOAD_BYTES: usize = 1_200;
const BATCH_SIZES: [usize; 4] = [1, 25, 100, 500];
const PAIRED_SAMPLES: usize = 3;
const WARMUP_RUNS: usize = 2;
const MEASURE_REPETITIONS: usize = 32;
const TIMING_TRIALS: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Probe {
    digest: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct AllocationSample {
    count: u64,
    bytes: u64,
}

#[test]
#[ignore = "debug-only source health allocation regression"]
fn given_existing_checkpoint_capacity_when_health_updated_then_capacity_is_reused() {
    let records = (0..RECORD_COUNT).map(realistic_record).collect::<Vec<_>>();
    assert_realistic_fixture(records.as_slice());
    let checkpoints = (0..RECORD_COUNT)
        .map(|index| SourceCheckpoint {
            source_table_name: format!("source_{:03}", index % 8),
            shard_id: format!("shard-{index:04}"),
            position: format!("position-{index:020}"),
        })
        .collect::<Vec<_>>();

    let durable_kept = measure_path(
        "durable checkpoint capacity",
        "source_polling::apply_source_durable_checkpoints",
        || durable_checkpoint_probe(checkpoints.as_slice(), false),
        || durable_checkpoint_probe(checkpoints.as_slice(), true),
    );
    let successful_kept = measure_path(
        "successful checkpoint capacity",
        "source_polling::apply_source_success_health",
        || successful_checkpoint_probe(checkpoints.as_slice(), false),
        || successful_checkpoint_probe(checkpoints.as_slice(), true),
    );

    assert!(
        durable_kept && successful_kept,
        "retained source-health capacity paths must satisfy the 5% CPU or strict-allocation \
         threshold"
    );
}

fn measure_path(
    name: &'static str,
    owner: &'static str,
    mut baseline: impl FnMut() -> Probe,
    mut candidate: impl FnMut() -> Probe,
) -> bool {
    for warmup in 0..WARMUP_RUNS {
        let (before, after) = if warmup.is_multiple_of(2) {
            (timed_probe(&mut baseline).1, timed_probe(&mut candidate).1)
        } else {
            let after = timed_probe(&mut candidate).1;
            let before = timed_probe(&mut baseline).1;
            (before, after)
        };
        assert_eq!(before, after, "{name} changed source-health semantics");
    }

    let mut baseline_times = [Duration::ZERO; PAIRED_SAMPLES];
    let mut candidate_times = [Duration::ZERO; PAIRED_SAMPLES];
    for sample in 0..PAIRED_SAMPLES {
        let (before, after) = if sample.is_multiple_of(2) {
            let before = timed_probe(&mut baseline);
            let after = timed_probe(&mut candidate);
            baseline_times[sample] = before.0;
            candidate_times[sample] = after.0;
            (before.1, after.1)
        } else {
            let after = timed_probe(&mut candidate);
            let before = timed_probe(&mut baseline);
            baseline_times[sample] = before.0;
            candidate_times[sample] = after.0;
            (before.1, after.1)
        };
        assert_eq!(before, after, "{name} changed source-health semantics");
    }

    let mut baseline_allocations = [AllocationSample { count: 0, bytes: 0 }; PAIRED_SAMPLES];
    let mut candidate_allocations = [AllocationSample { count: 0, bytes: 0 }; PAIRED_SAMPLES];
    for sample in 0..PAIRED_SAMPLES {
        let (before, after) = if sample.is_multiple_of(2) {
            let before = allocation_probe(name, "baseline", &mut baseline);
            let after = allocation_probe(name, "candidate", &mut candidate);
            baseline_allocations[sample] = before.0;
            candidate_allocations[sample] = after.0;
            (before.1, after.1)
        } else {
            let after = allocation_probe(name, "candidate", &mut candidate);
            let before = allocation_probe(name, "baseline", &mut baseline);
            baseline_allocations[sample] = before.0;
            candidate_allocations[sample] = after.0;
            (before.1, after.1)
        };
        assert_eq!(before, after, "{name} changed source-health semantics");
    }

    let deltas: [f64; PAIRED_SAMPLES] = std::array::from_fn(|index| {
        let baseline = baseline_times[index].as_secs_f64();
        ((baseline - candidate_times[index].as_secs_f64()) / baseline) * 100.0
    });
    let cpu_win = deltas.iter().all(|delta| *delta >= 5.0);
    let allocation_win = strict_allocation_reduction(&baseline_allocations, &candidate_allocations);
    let minimum = deltas.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = deltas.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let median = deltas.iter().sum::<f64>() - minimum - maximum;
    let allocation_cpu_stable = minimum >= -10.0 && median >= -5.0;
    let kept = cpu_win || (allocation_win && allocation_cpu_stable);
    println!(
        "source-health | name={name} | owner={owner} | pair_delta_percent={deltas:?} | \
         baseline_allocations={baseline_allocations:?} | \
         candidate_allocations={candidate_allocations:?} | kept={kept}"
    );
    kept
}

fn timed_probe(run: &mut impl FnMut() -> Probe) -> (Duration, Probe) {
    let mut observed = None;
    let mut trials = [Duration::ZERO; TIMING_TRIALS];
    for trial in &mut trials {
        let started = Instant::now();
        for _ in 0..MEASURE_REPETITIONS {
            observed = Some(black_box(run()));
        }
        *trial = started.elapsed();
    }
    trials.sort_unstable();
    (
        trials[TIMING_TRIALS / 2],
        observed.expect("source-health probe ran"),
    )
}

fn allocation_probe(
    name: &'static str,
    label: &'static str,
    run: &mut impl FnMut() -> Probe,
) -> (AllocationSample, Probe) {
    let guard = AllocationGuard::start(module_path!(), name, file!(), line!(), Some(label));
    let mut observed = None;
    for _ in 0..MEASURE_REPETITIONS {
        observed = Some(black_box(run()));
    }
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("source-health allocation report");
    (
        AllocationSample {
            count: report.allocation_count,
            bytes: report.allocated_bytes,
        },
        observed.expect("source-health probe ran"),
    )
}

fn strict_allocation_reduction(
    baseline: &[AllocationSample; PAIRED_SAMPLES],
    candidate: &[AllocationSample; PAIRED_SAMPLES],
) -> bool {
    let non_increasing = baseline
        .iter()
        .zip(candidate)
        .all(|(before, after)| after.count <= before.count && after.bytes <= before.bytes);
    let strict = baseline
        .iter()
        .zip(candidate)
        .any(|(before, after)| after.count < before.count || after.bytes < before.bytes);
    non_increasing && strict
}

fn durable_checkpoint_probe(checkpoints: &[SourceCheckpoint], reserve: bool) -> Probe {
    let mut digest = 1469598103934665603_u64;
    let mut bytes = 0_u64;
    for &size in &BATCH_SIZES {
        let mut health = SourceHealth::starting(8);
        let checkpoints = &checkpoints[..size];
        if reserve {
            apply_source_durable_checkpoints(&mut health, checkpoints);
        } else {
            apply_durable_checkpoints_without_capacity_hint(&mut health, checkpoints);
        }
        digest = digest_health(digest, &health);
        bytes = bytes.saturating_add(health.checkpoints.len() as u64);
    }
    Probe { digest, bytes }
}

fn successful_checkpoint_probe(checkpoints: &[SourceCheckpoint], reserve: bool) -> Probe {
    let mut digest = 1469598103934665603_u64;
    let mut bytes = 0_u64;
    for &size in &BATCH_SIZES {
        let mut health = SourceHealth::starting(8);
        let checkpoints = &checkpoints[..size];
        if reserve {
            health.apply_source_batch_health(
                SourceBatchOutcome {
                    is_committed: true,
                    records_ingested: checkpoints.len() as u64,
                    ingest_errors: 0,
                    checkpoints_saved: checkpoints.len() as u64,
                    checkpoint_errors: 0,
                },
                checkpoints,
                1_900_000_000_000,
            );
        } else {
            apply_success_health_without_capacity_hint(&mut health, checkpoints);
        }
        digest = digest_health(digest, &health);
        bytes = bytes.saturating_add(health.checkpoints.len() as u64);
    }
    Probe { digest, bytes }
}

fn apply_durable_checkpoints_without_capacity_hint(
    health: &mut SourceHealth,
    checkpoints: &[SourceCheckpoint],
) {
    health.checkpoints.clear();
    for checkpoint in checkpoints {
        if checkpoint.is_iterator() {
            continue;
        }
        upsert_checkpoint_health(
            &mut health.checkpoints,
            CheckpointHealth {
                source_table_name: checkpoint.source_table_name.clone(),
                shard_id: checkpoint.shard_id.clone(),
                position: checkpoint.position.clone(),
                updated_at_ms: None,
            },
        );
    }
}

fn apply_success_health_without_capacity_hint(
    health: &mut SourceHealth,
    checkpoints: &[SourceCheckpoint],
) {
    health.status = crate::http_contract::SourceHealthStatus::Healthy;
    health.phase = crate::http_contract::SourcePollingPhase::Healthy;
    health.phase_started_at_ms = Some(1_900_000_000_000);
    health.last_success_at_ms = Some(1_900_000_000_000);
    health.last_error = None;
    health.total_records_ingested = health
        .total_records_ingested
        .saturating_add(checkpoints.len() as u64);
    health.total_checkpoints_saved = health
        .total_checkpoints_saved
        .saturating_add(checkpoints.len() as u64);
    for checkpoint in checkpoints {
        if checkpoint.is_iterator() {
            continue;
        }
        upsert_checkpoint_health(
            &mut health.checkpoints,
            CheckpointHealth {
                source_table_name: checkpoint.source_table_name.clone(),
                shard_id: checkpoint.shard_id.clone(),
                position: checkpoint.position.clone(),
                updated_at_ms: Some(1_900_000_000_000),
            },
        );
    }
}

fn digest_health(mut digest: u64, health: &SourceHealth) -> u64 {
    for checkpoint in &health.checkpoints {
        digest = digest_bytes(digest, checkpoint.source_table_name.as_bytes());
        digest = digest_bytes(digest, checkpoint.shard_id.as_bytes());
        digest = digest_bytes(digest, checkpoint.position.as_bytes());
    }
    digest
}

fn assert_realistic_fixture(records: &[StorageStreamRecord]) {
    let record = &records[0];
    let item = record.new_image.as_ref().expect("source-health image");
    assert_eq!(item.len(), ATTRIBUTE_COUNT);
    assert_eq!(payload_bytes(item), PAYLOAD_BYTES);
    assert_eq!(record_key_bytes(&record.keys), KEY_BYTES);
}

fn realistic_record(index: usize) -> StorageStreamRecord {
    let tenant = if index.is_multiple_of(2) {
        "tenant-a"
    } else {
        "tenant-b"
    };
    let key = fixed_string(&format!("{tenant}#item-{index:08}-"), KEY_BYTES);
    let keys = StorageItem::from_iter([
        ("pk".to_owned(), StorageValue::S(key.clone())),
        (
            "sk".to_owned(),
            StorageValue::S(fixed_string(&format!("item-{index:08}-"), KEY_BYTES)),
        ),
    ]);
    let image = StorageItem::from_iter([
        ("tenant_id".to_owned(), StorageValue::S(tenant.to_owned())),
        (
            "gsi1_pk".to_owned(),
            StorageValue::S(fixed_string(&format!("gsi1-pk-{index}-"), KEY_BYTES)),
        ),
        (
            "gsi1_sk".to_owned(),
            StorageValue::S(fixed_string(&format!("gsi1-sk-{index}-"), KEY_BYTES)),
        ),
        (
            "gsi2_pk".to_owned(),
            StorageValue::S(fixed_string(&format!("gsi2-pk-{index}-"), KEY_BYTES)),
        ),
        (
            "gsi2_sk".to_owned(),
            StorageValue::S(fixed_string(&format!("gsi2-sk-{index}-"), KEY_BYTES)),
        ),
        (
            "payload_a".to_owned(),
            StorageValue::S(fixed_string("payload-a-", 360)),
        ),
        (
            "payload_b".to_owned(),
            StorageValue::S(fixed_string("payload-b-", 400)),
        ),
        (
            "payload_c".to_owned(),
            StorageValue::S(fixed_string("payload-c-", 440)),
        ),
        (
            "status".to_owned(),
            StorageValue::S(format!("state-{}", index % 3)),
        ),
        (
            "ttl".to_owned(),
            StorageValue::N((1_900_000_000_i64 + index as i64).to_string()),
        ),
    ]);
    StorageStreamRecord {
        keys,
        sequence_number: format!("{index:020}"),
        old_image: None,
        new_image: Some(image),
    }
}

fn fixed_string(prefix: &str, length: usize) -> String {
    let mut value = String::with_capacity(length);
    value.push_str(prefix);
    while value.len() < length {
        value.push(char::from(b'a' + (value.len() % 26) as u8));
    }
    value.truncate(length);
    value
}

fn payload_bytes(item: &StorageItem) -> usize {
    ["payload_a", "payload_b", "payload_c"]
        .into_iter()
        .map(
            |name| match item.get(name).expect("source-health payload") {
                StorageValue::S(value) => value.len(),
                _ => 0,
            },
        )
        .sum()
}

fn record_key_bytes(keys: &StorageItem) -> usize {
    keys.get("pk")
        .and_then(|value| match value {
            StorageValue::S(value) => Some(value.len()),
            _ => None,
        })
        .expect("source-health partition key")
}

fn digest_bytes(mut digest: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        digest = digest
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(*byte));
    }
    digest
}
