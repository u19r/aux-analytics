//! Allocation regression for the poller's logical source-record identity key.
//!
//! The workload is intentionally local to this string-building hot path. It
//! does not model transport, providers, polling, routing, or engine effects.

use std::hint::black_box;

use alloc_counter::AllocationGuard;
use storage_types::SystemStreamPosition;

use crate::constants::{
    LOGICAL_KEY_BYTES, LOGICAL_KEY_PAIRED_SAMPLES, LOGICAL_KEY_REPETITIONS,
    LOGICAL_KEY_WARMUP_RUNS, LOGICAL_RECORD_COUNT,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Probe {
    digest: u64,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AllocationSample {
    count: u64,
    bytes: u64,
}

#[test]
#[ignore = "debug-only logical source-record identity allocation regression"]
fn given_source_positions_when_logical_keys_built_then_keys_match_and_allocations_drop() {
    let positions = (0..LOGICAL_RECORD_COUNT)
        .map(logical_position)
        .collect::<Vec<_>>();
    assert_eq!(positions.len(), LOGICAL_RECORD_COUNT);
    assert!(
        positions
            .iter()
            .all(|position| logical_record_key_baseline(position).len() == LOGICAL_KEY_BYTES)
    );

    for position in &positions {
        assert_eq!(
            logical_record_key_baseline(position),
            logical_record_key_production(position)
        );
    }
    let changed = logical_position(LOGICAL_RECORD_COUNT + 1);
    assert_ne!(
        logical_record_key_baseline(&positions[0]),
        logical_record_key_baseline(&changed)
    );

    for warmup in 0..LOGICAL_KEY_WARMUP_RUNS {
        let (baseline, production) = if warmup.is_multiple_of(2) {
            (
                probe_logical_keys(false, LOGICAL_RECORD_COUNT),
                probe_logical_keys(true, LOGICAL_RECORD_COUNT),
            )
        } else {
            (
                probe_logical_keys(true, LOGICAL_RECORD_COUNT),
                probe_logical_keys(false, LOGICAL_RECORD_COUNT),
            )
        };
        assert_eq!(baseline, production);
    }

    let mut baseline = [AllocationSample { count: 0, bytes: 0 }; LOGICAL_KEY_PAIRED_SAMPLES];
    let mut production = [AllocationSample { count: 0, bytes: 0 }; LOGICAL_KEY_PAIRED_SAMPLES];
    for index in 0..LOGICAL_KEY_PAIRED_SAMPLES {
        let (before, after) = if index.is_multiple_of(2) {
            (
                allocation_sample("baseline", || {
                    probe_logical_keys(false, LOGICAL_RECORD_COUNT)
                }),
                allocation_sample("production", || {
                    probe_logical_keys(true, LOGICAL_RECORD_COUNT)
                }),
            )
        } else {
            (
                allocation_sample("production", || {
                    probe_logical_keys(true, LOGICAL_RECORD_COUNT)
                }),
                allocation_sample("baseline", || {
                    probe_logical_keys(false, LOGICAL_RECORD_COUNT)
                }),
            )
        };
        assert_eq!(before.1, after.1);
        if index.is_multiple_of(2) {
            baseline[index] = before.0;
            production[index] = after.0;
        } else {
            production[index] = before.0;
            baseline[index] = after.0;
        }
    }

    println!(
        "logical-record-key | key_bytes={} | records={} | warmups={} | repetitions={} | \
         baseline_allocations={baseline:?} | production_allocations={production:?} | \
         threshold=strict_allocation_reduction",
        LOGICAL_KEY_BYTES, LOGICAL_RECORD_COUNT, LOGICAL_KEY_WARMUP_RUNS, LOGICAL_KEY_REPETITIONS,
    );
    assert!(strict_allocation_reduction(&baseline, &production));
}

fn logical_position(index: usize) -> SystemStreamPosition {
    SystemStreamPosition::try_new(
        fixed_string(&format!("shard-{index:04}-"), 30),
        fixed_string(&format!("lineage-{:04}-", index % 3), 30),
        fixed_string(&format!("offset-{index:020}-"), 38),
    )
    .expect("valid logical position")
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

fn logical_record_key_baseline(position: &SystemStreamPosition) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        position.shard_id().as_str(),
        position.lineage_id().as_str(),
        position.offset().as_str()
    )
}

fn logical_record_key_production(position: &SystemStreamPosition) -> String {
    crate::poller::logical_record_key(position)
}

fn probe_logical_keys(production: bool, records: usize) -> Probe {
    let mut digest = 1469598103934665603_u64;
    let mut bytes = 0_u64;
    for index in 0..records {
        let position = logical_position(index);
        let key = if production {
            logical_record_key_production(&position)
        } else {
            logical_record_key_baseline(&position)
        };
        digest = digest_bytes(digest, key.as_bytes());
        bytes = bytes.saturating_add(key.len() as u64);
        black_box(&key);
    }
    Probe { digest, bytes }
}

fn allocation_sample(
    label: &'static str,
    mut run: impl FnMut() -> Probe,
) -> (AllocationSample, Probe) {
    let guard = AllocationGuard::start(
        module_path!(),
        "logical-record-key",
        file!(),
        line!(),
        Some(label),
    );
    let mut observed = None;
    for _ in 0..LOGICAL_KEY_REPETITIONS {
        observed = Some(black_box(run()));
    }
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("allocation report");
    (
        AllocationSample {
            count: report.allocation_count,
            bytes: report.allocated_bytes,
        },
        observed.expect("probe ran"),
    )
}

fn strict_allocation_reduction(
    baseline: &[AllocationSample; LOGICAL_KEY_PAIRED_SAMPLES],
    production: &[AllocationSample; LOGICAL_KEY_PAIRED_SAMPLES],
) -> bool {
    let non_increasing = baseline
        .iter()
        .zip(production)
        .all(|(before, after)| after.count <= before.count && after.bytes <= before.bytes);
    let strict = baseline
        .iter()
        .zip(production)
        .any(|(before, after)| after.count < before.count || after.bytes < before.bytes);
    non_increasing && strict
}

fn digest_bytes(mut digest: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        digest = digest
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(*byte));
    }
    digest
}
