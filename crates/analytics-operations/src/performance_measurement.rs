use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};
use storage_types::VecMap;

use crate::{CheckRow, RawBackupRecord};

pub(super) const RECORD_COUNT: usize = 500;
pub(super) const ATTRIBUTE_COUNT: usize = 10;
pub(super) const KEY_BYTES: usize = 100;
pub(super) const PAYLOAD_BYTES: usize = 1_200;
pub(super) const QUERY_RETURN_LENGTHS: [usize; 4] = [1, 25, 100, 500];
pub(super) const PAIRED_SAMPLES: usize = 3;
pub(super) const WARMUP_RUNS: usize = 2;
pub(super) const MEASURE_REPETITIONS: usize = 32;
pub(super) const TIMING_TRIALS: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Probe {
    pub(super) digest: u64,
    pub(super) bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AllocationSample {
    pub(super) count: u64,
    pub(super) bytes: u64,
}

#[derive(Debug)]
pub(super) struct CandidateResult {
    pub(super) name: &'static str,
    pub(super) owner: &'static str,
    pub(super) baseline_samples: [Duration; PAIRED_SAMPLES],
    pub(super) candidate_samples: [Duration; PAIRED_SAMPLES],
    pub(super) baseline_allocations: [AllocationSample; PAIRED_SAMPLES],
    pub(super) candidate_allocations: [AllocationSample; PAIRED_SAMPLES],
    pub(super) pair_deltas: [f64; PAIRED_SAMPLES],
    pub(super) semantically_equal: bool,
    pub(super) kept: bool,
}

pub(super) fn workload() -> (Vec<CheckRow>, Vec<RawBackupRecord>) {
    let rows = (0..RECORD_COUNT)
        .map(|index| CheckRow {
            table: format!("table_{:02}", index % 16),
            key: fixed_string(&format!("key-{index}-"), KEY_BYTES),
            row_hash: (index as u64).wrapping_mul(1_099_511_628_211),
            source_position: crate::check_test_support::backup_position(index as u64),
            contains_private_data: index % 17 == 0,
        })
        .collect();
    let records = (0..RECORD_COUNT).map(realistic_record).collect();
    (rows, records)
}

fn realistic_record(index: usize) -> RawBackupRecord {
    let item = realistic_item(index);
    RawBackupRecord::try_new(
        format!("table_{:02}", index % 16),
        format!("key-{index}"),
        None,
        StorageStreamRecord {
            keys: VecMap::from_iter([(
                "pk".to_string(),
                StorageValue::S(fixed_string(&format!("pk-{index}-"), KEY_BYTES)),
            )]),
            sequence_number: index.to_string(),
            old_image: Some(item.clone()),
            new_image: Some(item),
        },
    )
    .expect("realistic raw-backup record")
}

pub(super) fn realistic_item(index: usize) -> StorageItem {
    let mut item = VecMap::with_capacity(ATTRIBUTE_COUNT);
    item.insert(
        "entity_type".to_string(),
        StorageValue::S("item".to_string()),
    );
    for family in 1..=2 {
        item.insert(
            format!("gsi{family}_pk"),
            StorageValue::S(fixed_string(&format!("gsi{family}-pk-{index}-"), KEY_BYTES)),
        );
        item.insert(
            format!("gsi{family}_sk"),
            StorageValue::S(fixed_string(&format!("gsi{family}-sk-{index}-"), KEY_BYTES)),
        );
    }
    for (name, length) in [("payload_a", 360), ("payload_b", 400), ("payload_c", 440)] {
        item.insert(
            name.to_string(),
            StorageValue::S(fixed_string(&format!("{name}-{index}-"), length)),
        );
    }
    item.insert(
        "status".to_string(),
        StorageValue::S(format!("state-{}", index % 3)),
    );
    item.insert(
        "ttl".to_string(),
        StorageValue::N((1_900_000_000_i64 + index as i64).to_string()),
    );
    item
}

pub(super) fn assert_realistic_fixture(records: &[RawBackupRecord]) {
    let record = &records[0];
    let item = record.record.new_image.as_ref().expect("new image");
    assert_eq!(item.len(), ATTRIBUTE_COUNT);
    let payload_bytes = ["payload_a", "payload_b", "payload_c"]
        .into_iter()
        .map(|name| match item.get(name).expect("payload") {
            StorageValue::S(value) => value.len(),
            _ => 0,
        })
        .sum::<usize>();
    assert_eq!(payload_bytes, PAYLOAD_BYTES);
    assert_eq!(
        record
            .record
            .keys
            .get("pk")
            .expect("pk key")
            .to_json_value()
            .unwrap()
            .as_str()
            .unwrap()
            .len(),
        KEY_BYTES
    );
}

pub(super) fn fixed_string(prefix: &str, length: usize) -> String {
    let mut value = String::with_capacity(length);
    value.push_str(prefix);
    while value.len() < length {
        value.push(char::from(b'a' + (value.len() % 26) as u8));
    }
    value.truncate(length);
    value
}

pub(super) fn privacy_filter_probe(
    records: &[RawBackupRecord],
    policy: &PrivacyPolicy,
    reuse_matcher: bool,
) -> Probe {
    let matcher = reuse_matcher.then(|| policy.validated().expect("policy matcher"));
    let mut digest = 1469598103934665603_u64;
    let mut bytes = 0_u64;
    for record in records {
        for item in [
            &record.record.keys,
            record.record.old_image.as_ref().expect("old image"),
            record.record.new_image.as_ref().expect("new image"),
        ] {
            let filtered = if let Some(matcher) = matcher.as_ref() {
                matcher.filter_item(item.clone())
            } else {
                policy.filter_item(item.clone()).expect("filter item")
            };
            let mut fields = filtered.item.iter().collect::<Vec<_>>();
            fields.sort_unstable_by(|left, right| left.0.cmp(right.0));
            for (key, value) in fields {
                let encoded = serde_json::to_vec(value).expect("filtered value");
                digest = digest_bytes(digest, key.as_bytes());
                digest = digest_bytes(digest, &encoded);
                bytes = bytes.saturating_add(key.len() as u64 + encoded.len() as u64);
            }
            bytes = bytes.saturating_add(filtered.dropped_fields);
        }
    }
    Probe { digest, bytes }
}

pub(super) fn measure_candidate(
    name: &'static str,
    owner: &'static str,
    mut baseline: impl FnMut() -> Probe,
    mut candidate: impl FnMut() -> Probe,
) -> CandidateResult {
    let mut baseline_samples = [Duration::ZERO; PAIRED_SAMPLES];
    let mut candidate_samples = [Duration::ZERO; PAIRED_SAMPLES];
    let mut semantically_equal = true;
    for index in 0..WARMUP_RUNS {
        let (baseline_probe, candidate_probe) = if index.is_multiple_of(2) {
            let (_, baseline_probe) = timed_probe(&mut baseline);
            let (_, candidate_probe) = timed_probe(&mut candidate);
            (baseline_probe, candidate_probe)
        } else {
            let (_, candidate_probe) = timed_probe(&mut candidate);
            let (_, baseline_probe) = timed_probe(&mut baseline);
            (baseline_probe, candidate_probe)
        };
        semantically_equal &= baseline_probe == candidate_probe;
    }
    for index in 0..PAIRED_SAMPLES {
        let (baseline_time, baseline_probe, candidate_time, candidate_probe) =
            if index.is_multiple_of(2) {
                let (baseline_time, baseline_probe) = timed_probe(&mut baseline);
                let (candidate_time, candidate_probe) = timed_probe(&mut candidate);
                (
                    baseline_time,
                    baseline_probe,
                    candidate_time,
                    candidate_probe,
                )
            } else {
                let (candidate_time, candidate_probe) = timed_probe(&mut candidate);
                let (baseline_time, baseline_probe) = timed_probe(&mut baseline);
                (
                    baseline_time,
                    baseline_probe,
                    candidate_time,
                    candidate_probe,
                )
            };
        baseline_samples[index] = baseline_time;
        candidate_samples[index] = candidate_time;
        semantically_equal &= baseline_probe == candidate_probe;
    }
    let mut baseline_allocations = [AllocationSample { count: 0, bytes: 0 }; PAIRED_SAMPLES];
    let mut candidate_allocations = [AllocationSample { count: 0, bytes: 0 }; PAIRED_SAMPLES];
    for index in 0..PAIRED_SAMPLES {
        let (baseline_sample, baseline_probe, candidate_sample, candidate_probe) =
            if index.is_multiple_of(2) {
                let (baseline_sample, baseline_probe) =
                    allocation_sample(name, "baseline", &mut baseline);
                let (candidate_sample, candidate_probe) =
                    allocation_sample(name, "candidate", &mut candidate);
                (
                    baseline_sample,
                    baseline_probe,
                    candidate_sample,
                    candidate_probe,
                )
            } else {
                let (candidate_sample, candidate_probe) =
                    allocation_sample(name, "candidate", &mut candidate);
                let (baseline_sample, baseline_probe) =
                    allocation_sample(name, "baseline", &mut baseline);
                (
                    baseline_sample,
                    baseline_probe,
                    candidate_sample,
                    candidate_probe,
                )
            };
        semantically_equal &= baseline_probe == candidate_probe;
        baseline_allocations[index] = baseline_sample;
        candidate_allocations[index] = candidate_sample;
    }
    let pair_deltas =
        std::array::from_fn(|index| cpu_delta(baseline_samples[index], candidate_samples[index]));
    let cpu_win = pair_deltas.iter().all(|delta| *delta >= 5.0);
    let allocation_win = allocation_reduction(&baseline_allocations, &candidate_allocations);
    let minimum = pair_deltas.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = pair_deltas
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let median = pair_deltas.iter().sum::<f64>() - minimum - maximum;
    // Allocation-only wins tolerate one bounded timing outlier, but the
    // paired median may not regress by more than five percent and no pair may
    // regress by more than ten percent.
    let cpu_regression = minimum < -10.0 || median < -5.0;
    CandidateResult {
        name,
        owner,
        baseline_samples,
        candidate_samples,
        baseline_allocations,
        candidate_allocations,
        pair_deltas,
        semantically_equal,
        kept: semantically_equal && (cpu_win || (allocation_win && !cpu_regression)),
    }
}

fn timed_probe(run: &mut impl FnMut() -> Probe) -> (Duration, Probe) {
    let mut observed = None;
    let mut samples = [Duration::ZERO; TIMING_TRIALS];
    for sample in &mut samples {
        let started = Instant::now();
        for _ in 0..MEASURE_REPETITIONS {
            observed = Some(black_box(run()));
        }
        *sample = started.elapsed();
    }
    samples.sort_unstable();
    (
        samples[TIMING_TRIALS / 2],
        observed.expect("paired probe ran"),
    )
}

fn allocation_sample(
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
    alloc_counter::emit_report(&report).expect("allocation report");
    (
        AllocationSample {
            count: report.allocation_count,
            bytes: report.allocated_bytes,
        },
        observed.expect("paired probe ran"),
    )
}

pub(super) fn allocation_reduction(
    baseline: &[AllocationSample; PAIRED_SAMPLES],
    candidate: &[AllocationSample; PAIRED_SAMPLES],
) -> bool {
    let count_non_increasing = candidate
        .iter()
        .zip(baseline)
        .all(|(candidate, baseline)| candidate.count <= baseline.count);
    let bytes_non_increasing = candidate
        .iter()
        .zip(baseline)
        .all(|(candidate, baseline)| candidate.bytes <= baseline.bytes);
    let strict_reduction = candidate.iter().zip(baseline).any(|(candidate, baseline)| {
        candidate.count < baseline.count || candidate.bytes < baseline.bytes
    });
    count_non_increasing && bytes_non_increasing && strict_reduction
}

pub(super) fn cpu_delta(baseline: Duration, candidate: Duration) -> f64 {
    ((baseline.as_secs_f64() - candidate.as_secs_f64()) / baseline.as_secs_f64()) * 100.0
}

pub(super) fn digest_bytes(mut digest: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        digest = digest
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(*byte));
    }
    digest
}
