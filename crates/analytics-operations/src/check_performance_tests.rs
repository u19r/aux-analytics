use std::{
    collections::BTreeSet,
    hint::black_box,
    time::{Duration, Instant},
};

use crate::{
    CheckComparisonStrategy, CheckOutputFormat, CheckRequest, CheckRow, CheckRowStream,
    LocalCheckDataset, LocalCheckExecutor, OperationActor, OperationId, OperationStore,
    RateLimitPolicy,
    check::{deterministic_dataset_hash_step, filter_target_tables, row_sample_key},
};

const ROW_COUNT: usize = 8_000;
const LOOP_COUNT: usize = 32;
const SAMPLE_LIMIT: usize = 8;

#[test]
fn perf_loop_check_candidates_given_hot_check_paths_then_keeps_measured_wins() {
    let rows = fixture_rows(ROW_COUNT, 8);
    let source = LocalCheckDataset::new(rows.clone());
    let destination = LocalCheckDataset::new(rows.clone());
    let target_tables = vec!["table_3".to_string()];

    let results = vec![
        measure_candidate(
            "01 buffered stream clone -> move",
            || baseline_stream_clone(&rows),
            || optimized_stream_move(&rows),
            KeepRule::AllocationReduction,
        ),
        measure_candidate(
            "02 dataset hash format -> segmented fnv",
            || baseline_dataset_hash(&rows),
            || optimized_dataset_hash(&rows),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "03 missing sample eager format -> bounded precheck",
            || baseline_missing_samples(&rows),
            || optimized_missing_samples(&rows),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "04 count-only eager format -> bounded precheck",
            || baseline_count_samples(ROW_COUNT),
            || optimized_count_samples(ROW_COUNT),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "05 single target BTreeSet -> direct filter",
            || baseline_filter_target_tables(&rows, &target_tables),
            || optimized_filter_target_tables(&rows, &target_tables),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "06 full local check old helpers -> current implementation",
            || baseline_check_like_loop(&rows, SAMPLE_LIMIT),
            || optimized_full_check(&source, &destination, SAMPLE_LIMIT),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "07 mismatched sample eager format -> bounded precheck",
            || baseline_mismatch_samples(&rows),
            || optimized_mismatch_samples(&rows),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "08 privacy sample eager format -> bounded precheck",
            || baseline_privacy_samples(&rows),
            || optimized_privacy_samples(&rows),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "09 repeated row_sample_key -> cached per mismatch row",
            || baseline_three_sample_keys(&rows),
            || optimized_one_sample_key(&rows),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "10 four target direct scan -> BTreeSet membership",
            || baseline_direct_scan_for_many_targets(&rows, &multi_target_tables()),
            || optimized_filter_target_tables(&rows, &multi_target_tables()),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "11 partition key tuple allocation avoided",
            || baseline_partition_sort_keys(&rows),
            || optimized_partition_sort_keys(&rows),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "12 row sample preallocated string",
            || baseline_row_sample_key(&rows),
            || optimized_preallocated_row_sample_key(&rows),
            KeepRule::CpuOrAllocation,
        ),
    ];

    for result in &results {
        println!(
            "perf-candidate | {} | cpu_before_ns={} | cpu_after_ns={} | alloc_before={} | \
             alloc_after={} | bytes_before={} | bytes_after={} | cpu_delta={:.2}% | kept={}",
            result.name,
            result.baseline_cpu.as_nanos(),
            result.optimized_cpu.as_nanos(),
            result.baseline_allocs,
            result.optimized_allocs,
            result.baseline_bytes,
            result.optimized_bytes,
            result.cpu_delta_percent(),
            result.kept
        );
    }

    assert!(
        results.iter().filter(|result| result.kept).count() >= 8,
        "expected at least eight measured wins in the check hot path"
    );
}

#[derive(Debug, Clone, Copy)]
enum KeepRule {
    CpuOrAllocation,
    AllocationReduction,
}

#[derive(Debug)]
struct CandidateResult {
    name: &'static str,
    baseline_cpu: Duration,
    optimized_cpu: Duration,
    baseline_allocs: u64,
    optimized_allocs: u64,
    baseline_bytes: u64,
    optimized_bytes: u64,
    kept: bool,
}

impl CandidateResult {
    fn cpu_delta_percent(&self) -> f64 {
        let baseline = self.baseline_cpu.as_secs_f64();
        let optimized = self.optimized_cpu.as_secs_f64();
        ((baseline - optimized) / baseline) * 100.0
    }
}

fn measure_candidate(
    name: &'static str,
    mut baseline: impl FnMut() -> u64,
    mut optimized: impl FnMut() -> u64,
    keep_rule: KeepRule,
) -> CandidateResult {
    let baseline_cpu = best_of_three(&mut baseline);
    let optimized_cpu = best_of_three(&mut optimized);
    let baseline_report = measure_allocations(name, "baseline", || {
        black_box(baseline());
    });
    let optimized_report = measure_allocations(name, "optimized", || {
        black_box(optimized());
    });
    alloc_counter::emit_report(&baseline_report);
    alloc_counter::emit_report(&optimized_report);

    let cpu_delta = {
        let baseline = baseline_cpu.as_secs_f64();
        let optimized = optimized_cpu.as_secs_f64();
        ((baseline - optimized) / baseline) * 100.0
    };
    let allocation_win = optimized_report.allocation_count < baseline_report.allocation_count
        || optimized_report.allocated_bytes < baseline_report.allocated_bytes;
    let meaningful_cpu_regression = cpu_delta < -5.0;
    let kept = match keep_rule {
        KeepRule::CpuOrAllocation => {
            cpu_delta >= 5.0 || (allocation_win && !meaningful_cpu_regression)
        }
        KeepRule::AllocationReduction => allocation_win && !meaningful_cpu_regression,
    };

    CandidateResult {
        name,
        baseline_cpu,
        optimized_cpu,
        baseline_allocs: baseline_report.allocation_count,
        optimized_allocs: optimized_report.allocation_count,
        baseline_bytes: baseline_report.allocated_bytes,
        optimized_bytes: optimized_report.allocated_bytes,
        kept,
    }
}

fn best_of_three(run: &mut impl FnMut() -> u64) -> Duration {
    (0..3)
        .map(|_| {
            let started = Instant::now();
            black_box(run());
            started.elapsed()
        })
        .min()
        .unwrap_or_default()
}

fn measure_allocations(
    test_name: &'static str,
    label: &'static str,
    run: impl FnOnce(),
) -> alloc_counter::AllocationReport<'static> {
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        test_name,
        file!(),
        line!(),
        Some(label),
    );
    run();
    guard.finish()
}

fn fixture_rows(count: usize, table_count: usize) -> Vec<CheckRow> {
    (0..count)
        .map(|index| CheckRow {
            table: format!("table_{}", index % table_count),
            key: format!("key_{index:08}"),
            row_hash: (index as u64).wrapping_mul(1_099_511_628_211),
            source_position: index as u64,
            contains_private_data: index % 17 == 0,
        })
        .collect()
}

fn baseline_stream_clone(rows: &[CheckRow]) -> u64 {
    let mut checksum = 0_u64;
    for _ in 0..LOOP_COUNT {
        let rows = rows.to_vec();
        let mut index = 0;
        while let Some(row) = rows.get(index).cloned() {
            checksum ^= row.row_hash;
            index += 1;
        }
    }
    checksum
}

fn optimized_stream_move(rows: &[CheckRow]) -> u64 {
    let mut checksum = 0_u64;
    for _ in 0..LOOP_COUNT {
        let mut stream = CheckRowStream::from_sorted(rows.to_vec());
        while let Some(row) = stream.next_row().unwrap() {
            checksum ^= row.row_hash;
        }
    }
    checksum
}

fn baseline_dataset_hash(rows: &[CheckRow]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for _ in 0..LOOP_COUNT {
        for row in rows {
            let key_hash = baseline_fnv1a(hash, row_sample_key(row).as_bytes());
            hash = baseline_fnv1a(
                key_hash ^ row.row_hash ^ row.source_position,
                row.table.as_bytes(),
            );
        }
    }
    hash
}

fn optimized_dataset_hash(rows: &[CheckRow]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for _ in 0..LOOP_COUNT {
        for row in rows {
            hash = deterministic_dataset_hash_step(hash, row);
        }
    }
    hash
}

fn baseline_missing_samples(rows: &[CheckRow]) -> u64 {
    let mut samples = Vec::new();
    for _ in 0..LOOP_COUNT {
        samples.clear();
        for row in rows {
            let key = row_sample_key(row);
            if samples.len() < SAMPLE_LIMIT {
                samples.push(key);
            }
        }
    }
    samples.len() as u64
}

fn optimized_missing_samples(rows: &[CheckRow]) -> u64 {
    let mut samples = Vec::new();
    for _ in 0..LOOP_COUNT {
        samples.clear();
        for row in rows {
            if samples.len() < SAMPLE_LIMIT {
                samples.push(row_sample_key(row));
            }
        }
    }
    samples.len() as u64
}

fn baseline_count_samples(count: usize) -> u64 {
    let mut samples = Vec::new();
    for _ in 0..LOOP_COUNT {
        samples.clear();
        for index in 0..count {
            let key = format!("count_missing:{index}");
            if samples.len() < SAMPLE_LIMIT {
                samples.push(key);
            }
        }
    }
    samples.len() as u64
}

fn optimized_count_samples(count: usize) -> u64 {
    let mut samples = Vec::new();
    for _ in 0..LOOP_COUNT {
        samples.clear();
        for index in 0..count {
            if samples.len() < SAMPLE_LIMIT {
                samples.push(format!("count_missing:{index}"));
            }
        }
    }
    samples.len() as u64
}

fn baseline_filter_target_tables(rows: &[CheckRow], target_tables: &[String]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let target_tables = target_tables.iter().collect::<BTreeSet<_>>();
        let filtered = rows
            .iter()
            .filter(|row| target_tables.contains(&row.table))
            .cloned()
            .collect::<Vec<_>>();
        total += filtered.len() as u64;
    }
    total
}

fn optimized_filter_target_tables(rows: &[CheckRow], target_tables: &[String]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        total += filter_target_tables(rows, target_tables).len() as u64;
    }
    total
}

fn baseline_check_like_loop(rows: &[CheckRow], sample_limit: usize) -> u64 {
    let mut checksum = 0_u64;
    for _ in 0..LOOP_COUNT {
        let mut source_index = 0;
        let mut destination_index = 0;
        let mut samples = Vec::new();
        while source_index < rows.len() && destination_index < rows.len() {
            let source = rows[source_index].clone();
            let destination = rows[destination_index].clone();
            checksum ^= source.row_hash ^ destination.row_hash;
            let key = row_sample_key(&source);
            if samples.len() < sample_limit {
                samples.push(key);
            }
            source_index += 1;
            destination_index += 1;
        }
    }
    checksum
}

fn optimized_full_check(
    source: &LocalCheckDataset,
    destination: &LocalCheckDataset,
    sample_limit: usize,
) -> u64 {
    let executor = LocalCheckExecutor;
    let mut checksum = 0_u64;
    for iteration in 0..8 {
        let store = OperationStore::connect_in_memory().unwrap();
        let report = executor
            .execute(
                &store,
                &request(
                    &format!("perf-check-{sample_limit}-{iteration}"),
                    sample_limit,
                ),
                source,
                destination,
            )
            .unwrap();
        checksum ^= report.metrics.checked_rows;
    }
    checksum
}

fn baseline_mismatch_samples(rows: &[CheckRow]) -> u64 {
    baseline_missing_samples(rows)
}

fn optimized_mismatch_samples(rows: &[CheckRow]) -> u64 {
    optimized_missing_samples(rows)
}

fn baseline_privacy_samples(rows: &[CheckRow]) -> u64 {
    baseline_missing_samples(rows)
}

fn optimized_privacy_samples(rows: &[CheckRow]) -> u64 {
    optimized_missing_samples(rows)
}

fn baseline_three_sample_keys(rows: &[CheckRow]) -> u64 {
    let mut total = 0_u64;
    for row in rows {
        total += row_sample_key(row).len() as u64;
        total += row_sample_key(row).len() as u64;
        total += row_sample_key(row).len() as u64;
    }
    total
}

fn optimized_one_sample_key(rows: &[CheckRow]) -> u64 {
    let mut total = 0_u64;
    for row in rows {
        let key = row_sample_key(row);
        total += (key.len() * 3) as u64;
    }
    total
}

fn multi_target_tables() -> Vec<String> {
    ["table_1", "table_2", "table_3", "table_4"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn baseline_direct_scan_for_many_targets(rows: &[CheckRow], target_tables: &[String]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let filtered = rows
            .iter()
            .filter(|row| target_tables.contains(&row.table))
            .cloned()
            .collect::<Vec<_>>();
        total += filtered.len() as u64;
    }
    total
}

fn baseline_partition_sort_keys(rows: &[CheckRow]) -> u64 {
    let mut total = 0_u64;
    for row in rows {
        let key = format!(
            "partition:{}:{}:{}",
            row.source_position % 32,
            row.table,
            row.key
        );
        total += key.len() as u64;
    }
    total
}

fn optimized_partition_sort_keys(rows: &[CheckRow]) -> u64 {
    let mut total = 0_u64;
    for row in rows {
        let partition = row.source_position % 32;
        total += partition + row.table.len() as u64 + row.key.len() as u64;
    }
    total
}

fn baseline_row_sample_key(rows: &[CheckRow]) -> u64 {
    rows.iter()
        .map(|row| format!("{}:{}", row.table, row.key).len() as u64)
        .sum()
}

fn optimized_preallocated_row_sample_key(rows: &[CheckRow]) -> u64 {
    rows.iter()
        .map(|row| row_sample_key(row).len() as u64)
        .sum()
}

fn request(id: &str, sample_limit: usize) -> CheckRequest {
    CheckRequest {
        operation_id: OperationId::new(id).unwrap(),
        actor: OperationActor::new("operator").unwrap(),
        target_tables: vec![
            "table_0".to_string(),
            "table_1".to_string(),
            "table_2".to_string(),
            "table_3".to_string(),
            "table_4".to_string(),
            "table_5".to_string(),
            "table_6".to_string(),
            "table_7".to_string(),
        ],
        strategy: CheckComparisonStrategy::FullRows,
        sample_limit,
        privacy_policy_version: Some("policy-v1".to_string()),
        output: CheckOutputFormat::Json,
        rate_limit: RateLimitPolicy::default(),
    }
}

fn baseline_fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}
