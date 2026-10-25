use std::time::Duration;

use analytics_contract::PrivacyPolicy;

use super::performance_measurement::{
    ATTRIBUTE_COUNT, KEY_BYTES, PAIRED_SAMPLES, PAYLOAD_BYTES, Probe, QUERY_RETURN_LENGTHS,
    RECORD_COUNT, assert_realistic_fixture, digest_bytes, measure_candidate, privacy_filter_probe,
    workload,
};
use crate::{
    OperationId, RawBackupObjectId, RawBackupWriteRequest,
    maintenance::{calculate_materialized_fingerprint_baseline, calculate_request_fingerprint},
};

const FINGERPRINT_RECORD_COUNT: usize = 100;

#[test]
#[ignore = "debug-only privacy matcher allocation regression"]
fn given_repeated_privacy_records_when_matcher_paths_are_measured_then_outputs_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let (rows, items) = workload();
    drop(rows);
    assert_realistic_fixture(&items);
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .expect("policy")
        .with_denied_value_regex("secret");
    println!(
        "privacy-filter-fixture | records={} | attributes={} | key_bytes={} | payload_bytes={} | \
         query_return_lengths={:?} | paired_samples={}",
        RECORD_COUNT,
        ATTRIBUTE_COUNT,
        KEY_BYTES,
        PAYLOAD_BYTES,
        QUERY_RETURN_LENGTHS,
        PAIRED_SAMPLES,
    );

    let result = measure_candidate(
        "privacy matcher reuse",
        "raw_backup::filter_raw_backup_record",
        || privacy_filter_probe(&items, &policy, false),
        || privacy_filter_probe(&items, &policy, true),
    );

    println!(
        "privacy-filter-summary | kept={} | semantic_equality={} | paired_samples={}",
        result.kept, result.semantically_equal, PAIRED_SAMPLES,
    );
    println!("privacy-filter-measurements | {result:?}");
    assert!(result.kept);
}

#[test]
#[ignore = "debug-only maintenance executor allocation regression"]
fn given_backup_indexes_when_object_identity_paths_are_measured_then_outputs_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let (_rows, items) = workload();
    assert_realistic_fixture(&items);
    let operation_id = OperationId::try_new("maintenance/fixture").expect("operation id");
    let source_identities = (0..RECORD_COUNT)
        .map(|index| format!("source/items/{index:04}"))
        .collect::<Vec<_>>();
    println!(
        "maintenance-executor-fixture | records={} | attributes={} | key_bytes={} | \
         payload_bytes={} | query_return_lengths={:?} | paired_samples={}",
        RECORD_COUNT,
        ATTRIBUTE_COUNT,
        KEY_BYTES,
        PAYLOAD_BYTES,
        QUERY_RETURN_LENGTHS,
        PAIRED_SAMPLES,
    );

    let result = measure_candidate(
        "raw backup object identity",
        "raw_backup::RawBackupObjectId::for_operation",
        || object_id_probe(&operation_id, &source_identities, false),
        || object_id_probe(&operation_id, &source_identities, true),
    );
    println!(
        "maintenance-executor-candidate | name={} | owner={} | baseline_ns={:?} | \
         candidate_ns={:?} | pair_delta_percent={:?} | baseline_allocations={:?} | \
         candidate_allocations={:?} | semantic_equality={} | kept={}",
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
        result.semantically_equal,
        result.kept,
    );
    assert!(result.semantically_equal);
    assert!(result.kept);
}

fn object_id_probe(
    operation_id: &OperationId,
    source_identities: &[String],
    single_buffer: bool,
) -> Probe {
    let mut digest = 1469598103934665603_u64;
    let mut bytes = 0_u64;
    for &size in &QUERY_RETURN_LENGTHS {
        for (ordinal, source_identity) in source_identities.iter().take(size).enumerate() {
            let object_id = if single_buffer {
                RawBackupObjectId::for_operation(operation_id, source_identity, ordinal as u64)
                    .expect("object id")
            } else {
                object_id_with_format(operation_id, source_identity, ordinal as u64)
            };
            digest = digest_bytes(digest, object_id.as_str().as_bytes());
            bytes = bytes.saturating_add(object_id.as_str().len() as u64);
        }
    }
    Probe { digest, bytes }
}

fn object_id_with_format(
    operation_id: &OperationId,
    source_identity: &str,
    ordinal: u64,
) -> RawBackupObjectId {
    let value = format!(
        "{}_{}_{}",
        sanitize_object_id_part(operation_id.as_str()),
        sanitize_object_id_part(source_identity),
        ordinal
    );
    RawBackupObjectId::try_new(value).expect("object id")
}

fn sanitize_object_id_part(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[test]
#[ignore = "debug-only maintenance request fingerprint allocation regression"]
fn given_maintenance_request_when_fingerprint_streamed_then_digest_and_allocation_budget_hold() {
    let (_rows, records) = workload();
    assert_realistic_fixture(&records);
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("maintenance/fingerprint-fixture").expect("operation id"),
        RawBackupObjectId::try_new("object-fingerprint-fixture").expect("object id"),
        "source/items",
        Some("privacy-v1".to_string()),
        records,
    )
    .expect("raw backup request");
    let mut request = request;
    request.records.truncate(FINGERPRINT_RECORD_COUNT);
    let requests = vec![request];
    let serialized_bytes = requests
        .iter()
        .map(|request| {
            calculate_materialized_fingerprint_baseline(request)
                .expect("materialized fingerprint baseline")
                .1 as u64
        })
        .sum::<u64>();
    println!(
        "maintenance-fingerprint-fixture | records={} | attributes={} | key_bytes={} | \
         payload_bytes={} | paired_samples={}",
        FINGERPRINT_RECORD_COUNT, ATTRIBUTE_COUNT, KEY_BYTES, PAYLOAD_BYTES, PAIRED_SAMPLES,
    );

    let result = measure_candidate(
        "maintenance request fingerprint",
        "maintenance/fingerprint.rs::calculate_value_fingerprint",
        || fingerprint_probe(&requests, false, serialized_bytes),
        || fingerprint_probe(&requests, true, serialized_bytes),
    );
    println!(
        "maintenance-fingerprint-candidate | name={} | owner={} | baseline_ns={:?} | \
         candidate_ns={:?} | pair_delta_percent={:?} | baseline_allocations={:?} | \
         candidate_allocations={:?} | serialized_bytes_baseline={} | \
         serialized_bytes_candidate={} | semantic_equality={} | kept={}",
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
        serialized_bytes,
        serialized_bytes,
        result.semantically_equal,
        result.kept,
    );
    assert!(result.semantically_equal);
    assert!(result.kept);
}

fn fingerprint_probe(
    requests: &[RawBackupWriteRequest],
    streaming: bool,
    serialized_bytes: u64,
) -> Probe {
    let mut digest = 1469598103934665603_u64;
    for request in requests {
        let fingerprint = if streaming {
            calculate_request_fingerprint(request).expect("streaming fingerprint")
        } else {
            calculate_materialized_fingerprint_baseline(request)
                .expect("materialized fingerprint baseline")
                .0
        };
        digest = digest_bytes(digest, fingerprint.as_bytes());
    }
    Probe {
        digest,
        bytes: serialized_bytes,
    }
}
