//! Equal-work replay measurements with explicit warm or cache-cleared inputs.

use std::time::Instant;

use alloc_counter::AllocationGuard;
use sha2::{Digest, Sha256};

use super::{
    RawBackupCompression,
    test_support::{ReplayFixture, count_object_reads},
};

#[test]
#[ignore = "manual equal-work filesystem replay allocation and timing probe"]
fn given_equal_committed_objects_when_verification_is_fused_then_outputs_match_and_object_reads_drop()
 {
    let selected = std::env::var("ANALYTICS_BACKUP_PROBE_MODE").ok();
    assert!(
        selected
            .as_deref()
            .is_none_or(|mode| matches!(mode, "separate" | "fused"))
    );
    let cache = std::env::var("ANALYTICS_BACKUP_PROBE_CACHE").unwrap_or_else(|_| "warm".into());
    assert!(matches!(cache.as_str(), "warm" | "cold"));
    let allocations = std::env::var_os("ANALYTICS_BACKUP_PROBE_ALLOCATIONS").is_some();
    for rows in [1, 4096] {
        for codec in [RawBackupCompression::None, RawBackupCompression::RunLength] {
            let fixture = ReplayFixture::new(rows, codec, true);
            let expected = Sha256::digest(serde_json::to_vec(&fixture.expected).unwrap());
            for pair in 0..3 {
                let modes = if pair % 2 == 0 {
                    ["separate", "fused"]
                } else {
                    ["fused", "separate"]
                };
                for mode in modes {
                    if selected.as_deref().is_some_and(|selected| selected != mode) {
                        continue;
                    }
                    if cache == "cold" {
                        assert!(
                            std::process::Command::new("/usr/sbin/purge")
                                .status()
                                .unwrap()
                                .success(),
                            "cache clearing failed; do not report this as a cold trial"
                        );
                    }
                    let guard = allocations.then(|| {
                        AllocationGuard::start(
                            module_path!(),
                            "verified replay",
                            file!(),
                            line!(),
                            None,
                        )
                    });
                    let started = Instant::now();
                    let (output, reads) = count_object_reads(|| match mode {
                        "separate" => fixture.replay_with_separate_verification(),
                        _ => fixture.request.execute(&fixture.store).unwrap(),
                    });
                    let elapsed = started.elapsed();
                    let report = guard.map(AllocationGuard::finish);
                    assert_eq!(reads, if mode == "separate" { 2 } else { 1 });
                    assert_eq!(output.records.len(), rows);
                    assert_eq!(output.summary.metrics.policy_drops, rows as u64);
                    assert_eq!(
                        Sha256::digest(serde_json::to_vec(&output.records).unwrap()),
                        expected
                    );
                    println!(
                        "backup-replay | mode={mode} | cache={cache} | rows={rows} | \
                         codec={codec:?} | pair={pair} | reads={reads} | elapsed_ns={} | \
                         allocation_count={:?} | allocated_bytes={:?} | sha256_bytes={expected:?}",
                        elapsed.as_nanos(),
                        report.as_ref().map(|report| report.allocation_count),
                        report.as_ref().map(|report| report.allocated_bytes)
                    );
                }
            }
        }
    }
}
