use std::{
    fs,
    hint::black_box,
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use super::{confinement, mutation, validation};
use crate::constants::{
    PLANNER_MEASURE_REPETITIONS, PLANNER_MIN_ALLOCATION_CPU_DELTA_PERCENT, PLANNER_PAIRED_SAMPLES,
    PLANNER_SIZES, PLANNER_TIMING_TRIALS, PLANNER_WARMUP_RUNS,
};

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

#[derive(Debug)]
struct CandidateResult {
    name: &'static str,
    owner: &'static str,
    baseline_samples: [Duration; PLANNER_PAIRED_SAMPLES],
    candidate_samples: [Duration; PLANNER_PAIRED_SAMPLES],
    baseline_allocations: [AllocationSample; PLANNER_PAIRED_SAMPLES],
    candidate_allocations: [AllocationSample; PLANNER_PAIRED_SAMPLES],
    pair_deltas: [f64; PLANNER_PAIRED_SAMPLES],
    kept: bool,
}

#[test]
#[ignore = "debug-only Quint planner allocation regression"]
fn given_planner_inputs_when_validation_paths_are_measured_then_outputs_match_and_each_has_a_cpu_or_allocation_win()
 {
    let root = workspace_root();
    let model_display = "specs/source_position_mbt.qnt";
    let model_path = root.join(model_display);
    let adapter_paths = PLANNER_SIZES.map(adapter_path);
    let model_paths = PLANNER_SIZES.map(|_| model_display.to_owned());

    println!(
        "quint-planner-fixture | source=checker/plan/validation/mutation | \
         input_sizes=1,25,100,500 | paired_samples={PLANNER_PAIRED_SAMPLES}"
    );
    let results = vec![
        measure_candidate(
            "path component validation",
            "validation.rs::path_component_checks::iterator_validation",
            || path_component_probe(&adapter_paths, &model_paths, false),
            || path_component_probe(&adapter_paths, &model_paths, true),
        ),
        measure_candidate(
            "bounded model reads",
            "validation::read_model_source + mutation::read_bounded_bytes",
            || bounded_read_probe(&root, &model_path, model_display, false),
            || bounded_read_probe(&root, &model_path, model_display, true),
        ),
    ];
    print_results(&results);
    let retained = results
        .iter()
        .filter(|result| result.kept)
        .map(|result| result.name)
        .collect::<Vec<_>>();
    assert_eq!(
        retained,
        ["path component validation", "bounded model reads"]
    );
    println!(
        "quint-planner-summary | measurements={} | retained={} | paired_samples={}",
        results.len(),
        retained.len(),
        PLANNER_PAIRED_SAMPLES,
    );
    assert_eq!(results.len(), 2);
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn adapter_path(size: usize) -> String {
    format!("crates/analytics-contract/src/module_{size:04}.rs")
}

fn path_component_probe(
    adapter_paths: &[String; 4],
    model_paths: &[String; 4],
    optimized: bool,
) -> Probe {
    combine(
        PLANNER_SIZES
            .into_iter()
            .zip(adapter_paths)
            .zip(model_paths)
            .map(|((size, adapter), model)| {
                let package = if optimized {
                    confinement::performance_adapter_path_component(adapter)
                } else {
                    baseline_adapter_component(adapter)
                };
                let model_valid = if optimized {
                    confinement::performance_model_path_components(model)
                } else {
                    baseline_model_components(model)
                };
                let package = package.unwrap_or_default();
                Probe {
                    digest: digest_bytes(
                        digest_bytes(1469598103934665603_u64, package.as_bytes()),
                        &[u8::from(model_valid)],
                    ),
                    bytes: package.len() as u64 + size as u64,
                }
            }),
    )
}

fn baseline_adapter_component(path: &str) -> Option<&str> {
    let components = path.split('/').collect::<Vec<_>>();
    if components.len() < 3
        || path.contains('\\')
        || components
            .iter()
            .any(|component| component.is_empty() || matches!(*component, "." | ".."))
        || components.first().copied() != Some("crates")
    {
        return None;
    }
    let package = components.get(1).copied()?;
    if package.is_empty()
        || !package
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return None;
    }
    Some(package)
}

fn baseline_model_components(path: &str) -> bool {
    let components = path.split('/').collect::<Vec<_>>();
    components.len() >= 2 && components.first().copied() == Some("specs")
}

fn bounded_read_probe(root: &Path, model_path: &Path, display: &str, optimized: bool) -> Probe {
    combine(PLANNER_SIZES.into_iter().map(|size| {
        let source = if optimized {
            validation::performance_read_model_source(root, model_path, display)
                .expect("model source")
        } else {
            baseline_model_source(model_path)
        };
        let bounded = if optimized {
            mutation::counterexample::performance_read_bounded_bytes(model_path, 1_048_576)
                .expect("bounded model")
        } else {
            baseline_bounded_bytes(model_path, 1_048_576)
        };
        let mut digest = digest_bytes(1469598103934665603_u64, source.as_bytes());
        digest = digest_bytes(digest, &bounded);
        black_box((&source, &bounded));
        Probe {
            digest,
            bytes: source.len() as u64 + bounded.len() as u64 + size as u64,
        }
    }))
}

fn baseline_model_source(path: &Path) -> String {
    let metadata = fs::metadata(path).expect("model metadata");
    assert!(metadata.is_file());
    assert!(metadata.len() <= 1_048_576);
    let mut bytes = Vec::new();
    fs::File::open(path)
        .expect("model open")
        .take(1_048_577)
        .read_to_end(&mut bytes)
        .expect("model read");
    assert!(bytes.len() <= 1_048_576);
    String::from_utf8(bytes).expect("model utf8")
}

fn baseline_bounded_bytes(path: &Path, limit: u64) -> Vec<u8> {
    let metadata = fs::metadata(path).expect("bounded metadata");
    assert!(metadata.is_file());
    assert!(metadata.len() <= limit);
    let mut bytes = Vec::new();
    fs::File::open(path)
        .expect("bounded open")
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .expect("bounded read");
    assert!(bytes.len() as u64 <= limit);
    bytes
}

fn combine(probes: impl IntoIterator<Item = Probe>) -> Probe {
    probes.into_iter().fold(
        Probe {
            digest: 1469598103934665603_u64,
            bytes: 0,
        },
        |combined, probe| Probe {
            digest: digest_bytes(combined.digest, &probe.digest.to_le_bytes()),
            bytes: combined.bytes.saturating_add(probe.bytes),
        },
    )
}

fn digest_bytes(mut digest: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        digest = digest
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(*byte));
    }
    digest
}

fn measure_candidate(
    name: &'static str,
    owner: &'static str,
    mut baseline: impl FnMut() -> Probe,
    mut candidate: impl FnMut() -> Probe,
) -> CandidateResult {
    for index in 0..PLANNER_WARMUP_RUNS {
        let (baseline_probe, candidate_probe) = if index.is_multiple_of(2) {
            let (_, baseline_probe) = timed_probe(&mut baseline);
            let (_, candidate_probe) = timed_probe(&mut candidate);
            (baseline_probe, candidate_probe)
        } else {
            let (_, candidate_probe) = timed_probe(&mut candidate);
            let (_, baseline_probe) = timed_probe(&mut baseline);
            (baseline_probe, candidate_probe)
        };
        assert_eq!(
            baseline_probe, candidate_probe,
            "semantic output changed for {name} during warmup"
        );
    }
    let mut baseline_samples = [Duration::ZERO; PLANNER_PAIRED_SAMPLES];
    let mut candidate_samples = [Duration::ZERO; PLANNER_PAIRED_SAMPLES];
    for index in 0..PLANNER_PAIRED_SAMPLES {
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
        assert_eq!(
            baseline_probe, candidate_probe,
            "semantic output changed for {name}"
        );
    }
    let mut baseline_allocations =
        [AllocationSample { count: 0, bytes: 0 }; PLANNER_PAIRED_SAMPLES];
    let mut candidate_allocations =
        [AllocationSample { count: 0, bytes: 0 }; PLANNER_PAIRED_SAMPLES];
    for index in 0..PLANNER_PAIRED_SAMPLES {
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
        assert_eq!(
            baseline_probe, candidate_probe,
            "semantic output changed for {name} during allocation sample"
        );
        baseline_allocations[index] = baseline_sample;
        candidate_allocations[index] = candidate_sample;
    }
    let pair_deltas = std::array::from_fn(|index| {
        let baseline = baseline_samples[index].as_secs_f64();
        let candidate = candidate_samples[index].as_secs_f64();
        ((baseline - candidate) / baseline) * 100.0
    });
    let allocation_win = allocation_reduction(&baseline_allocations, &candidate_allocations);
    let cpu_win = pair_deltas.iter().all(|delta| *delta >= 5.0);
    // Allocation-only wins may not trade away CPU on any paired sample. A
    // negative pair is a real regression even when it is smaller than the
    // broad campaign's five-percent rejection band.
    let allocation_cpu_stable = allocation_cpu_stable(&pair_deltas);
    CandidateResult {
        name,
        owner,
        baseline_samples,
        candidate_samples,
        baseline_allocations,
        candidate_allocations,
        pair_deltas,
        kept: cpu_win || (allocation_win && allocation_cpu_stable),
    }
}

fn timed_probe(run: &mut impl FnMut() -> Probe) -> (Duration, Probe) {
    let mut observed = None;
    let mut samples = [Duration::ZERO; PLANNER_TIMING_TRIALS];
    for sample in &mut samples {
        let started = Instant::now();
        for _ in 0..PLANNER_MEASURE_REPETITIONS {
            observed = Some(black_box(run()));
        }
        *sample = started.elapsed();
    }
    samples.sort_unstable();
    (
        samples[PLANNER_TIMING_TRIALS / 2],
        observed.expect("repeated performance probe ran"),
    )
}

fn allocation_sample(
    name: &'static str,
    label: &'static str,
    run: &mut impl FnMut() -> Probe,
) -> (AllocationSample, Probe) {
    let mut observed = None;
    let guard =
        alloc_counter::AllocationGuard::start(module_path!(), name, file!(), line!(), Some(label));
    for _ in 0..PLANNER_MEASURE_REPETITIONS {
        observed = Some(black_box(run()));
    }
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("allocation report");
    (
        AllocationSample {
            count: report.allocation_count,
            bytes: report.allocated_bytes,
        },
        observed.expect("allocation probe ran"),
    )
}

fn allocation_reduction(
    baseline: &[AllocationSample; PLANNER_PAIRED_SAMPLES],
    candidate: &[AllocationSample; PLANNER_PAIRED_SAMPLES],
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

fn allocation_cpu_stable(deltas: &[f64]) -> bool {
    if deltas.is_empty() {
        return false;
    }
    let minimum = deltas.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = deltas.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let median = deltas.iter().sum::<f64>() - minimum - maximum;
    minimum >= 0.0 && median >= PLANNER_MIN_ALLOCATION_CPU_DELTA_PERCENT
}

fn print_results(results: &[CandidateResult]) {
    for result in results {
        println!(
            "quint-planner-candidate | name={} | owner={} | baseline_ns={:?} | candidate_ns={:?} \
             | pair_delta_percent={:?} | baseline_allocations={:?} | candidate_allocations={:?} | \
             kept={}",
            result.name,
            result.owner,
            result
                .baseline_samples
                .iter()
                .map(Duration::as_nanos)
                .collect::<Vec<_>>(),
            result
                .candidate_samples
                .iter()
                .map(Duration::as_nanos)
                .collect::<Vec<_>>(),
            result.pair_deltas,
            result.baseline_allocations,
            result.candidate_allocations,
            result.kept,
        );
    }
}
