use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use sha2::{Digest, Sha256};
use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};

use super::{
    BatchIdentityInput, SourceJobCheckpoint, SourceJobKind, SourceJobRecord, SourceJobScope,
    identity,
};

#[test]
fn given_nested_record_when_canonical_bytes_are_written_then_v3_bytes_match() {
    let record = representative_record(false, false);
    let expected = reference_canonical_record_bytes(&record);
    let mut actual = Vec::new();

    record.write_canonical_json(&mut actual);

    assert_eq!(actual, expected);
}

#[test]
fn given_nested_record_when_batch_identity_is_derived_then_v3_id_matches() {
    let record = representative_record(false, false);
    let records = [source_record(record)];
    let checkpoints = [SourceJobCheckpoint {
        iterator: false,
        position: "checkpoint-1".to_owned(),
    }];
    let scope = SourceJobScope::table_with_predecessor(
        "source-events",
        "shard-1",
        Some("previous-position".to_owned()),
    );
    let manifest_digest = [0x2a; analytics_engine::MANIFEST_DIGEST_BYTES];

    let expected = reference_batch_identity(identity_input(
        &manifest_digest,
        &scope,
        &records,
        &checkpoints,
    ));
    let actual = identity::batch_identity(identity_input(
        &manifest_digest,
        &scope,
        &records,
        &checkpoints,
    ));

    assert_eq!(actual, expected);
}

#[test]
fn given_system_scope_when_batch_identity_is_derived_then_v3_id_matches() {
    let record = representative_record(false, false);
    let records = [source_record(record)];
    let checkpoints = [SourceJobCheckpoint {
        iterator: true,
        position: "iterator-position".to_owned(),
    }];
    let scope = SourceJobScope::System {
        source_scope: SystemStreamScope::try_new("system-events", 7).expect("valid system scope"),
        previous_checkpoint: super::SystemStreamCheckpointPredecessor::Token {
            checkpoint: SystemStreamCheckpointToken::try_from(
                "v1.0123456789abcdef0123456789abcdef",
            )
            .expect("valid previous checkpoint"),
        },
        checkpoint: SystemStreamCheckpointToken::try_from("v1.abcdef0123456789abcdef0123456789")
            .expect("valid checkpoint"),
    };
    let manifest_digest = [0x2a; analytics_engine::MANIFEST_DIGEST_BYTES];

    let expected = reference_batch_identity(identity_input(
        &manifest_digest,
        &scope,
        &records,
        &checkpoints,
    ));
    let actual = identity::batch_identity(identity_input(
        &manifest_digest,
        &scope,
        &records,
        &checkpoints,
    ));

    assert_eq!(actual, expected);
}

#[test]
fn given_map_insertion_orders_when_canonical_bytes_are_written_then_output_is_stable() {
    let first = representative_record(false, true);
    let second = representative_record(true, true);
    let first_reference = reference_canonical_record_bytes(&first);
    let second_reference = reference_canonical_record_bytes(&second);
    let mut first_actual = Vec::new();
    let mut second_actual = Vec::new();

    first.write_canonical_json(&mut first_actual);
    second.write_canonical_json(&mut second_actual);

    assert_eq!(first_reference, second_reference);
    assert_eq!(first_actual, first_reference);
    assert_eq!(second_actual, second_reference);
    assert_eq!(first_actual, second_actual);
    assert_eq!(batch_id_for_record(&first), batch_id_for_record(&second));
}

#[test]
fn given_none_and_present_images_when_canonical_bytes_are_written_then_presence_is_preserved() {
    let without_images = StorageStreamRecord {
        keys: StorageItem::new(),
        sequence_number: "sequence".to_owned(),
        old_image: None,
        new_image: None,
    };
    let with_old_image = representative_record(false, true);
    let mut without_actual = Vec::new();
    let mut with_old_actual = Vec::new();

    without_images.write_canonical_json(&mut without_actual);
    with_old_image.write_canonical_json(&mut with_old_actual);

    assert_eq!(
        without_actual,
        reference_canonical_record_bytes(&without_images)
    );
    assert_eq!(
        with_old_actual,
        reference_canonical_record_bytes(&with_old_image)
    );
    assert!(
        !without_actual
            .windows(b"OldImage".len())
            .any(|window| window == b"OldImage")
    );
    assert!(
        with_old_actual
            .windows(b"OldImage".len())
            .any(|window| window == b"OldImage")
    );
}

#[test]
fn given_changed_nested_value_when_batch_identity_is_derived_then_v3_id_changes() {
    let original = representative_record(false, false);
    let mut changed = original.clone();
    changed
        .new_image
        .as_mut()
        .expect("representative record has a new image")
        .insert("changed".to_owned(), StorageValue::S("value".to_owned()));

    let original_bytes = reference_canonical_record_bytes(&original);
    let changed_bytes = reference_canonical_record_bytes(&changed);
    let mut original_actual = Vec::new();
    let mut changed_actual = Vec::new();
    original.write_canonical_json(&mut original_actual);
    changed.write_canonical_json(&mut changed_actual);

    assert_eq!(original_actual, original_bytes);
    assert_eq!(changed_actual, changed_bytes);
    assert_ne!(original_actual, changed_actual);
    assert_ne!(
        batch_id_for_record(&original),
        batch_id_for_record(&changed)
    );
}

#[test]
fn given_changed_set_member_when_batch_identity_is_derived_then_v3_id_changes() {
    let original = representative_record(false, false);
    let mut changed = original.clone();
    changed
        .new_image
        .as_mut()
        .expect("representative record has a new image")
        .insert(
            "strings".to_owned(),
            StorageValue::SS(vec!["z".to_owned(), "changed".to_owned()]),
        );

    let original_bytes = reference_canonical_record_bytes(&original);
    let changed_bytes = reference_canonical_record_bytes(&changed);
    let mut original_actual = Vec::new();
    let mut changed_actual = Vec::new();
    original.write_canonical_json(&mut original_actual);
    changed.write_canonical_json(&mut changed_actual);

    assert_eq!(original_actual, original_bytes);
    assert_eq!(changed_actual, changed_bytes);
    assert_ne!(original_actual, changed_actual);
    assert_ne!(
        batch_id_for_record(&original),
        batch_id_for_record(&changed)
    );
}

#[test]
#[ignore = "debug-only identity serialization allocation and CPU comparison"]
fn given_representative_record_when_identity_serializer_is_probed_then_candidate_is_measured() {
    const REPETITIONS: usize = 1_000;
    let record = representative_record(false, true);
    let baseline = measure_baseline(&record, REPETITIONS);
    let candidate = measure_candidate(&record, REPETITIONS);

    assert_eq!(baseline.checksum, candidate.checksum);
    println!(
        "identity-serialization | repetitions={REPETITIONS} | baseline_elapsed={:?} | \
         candidate_elapsed={:?} | baseline_allocations={} | candidate_allocations={} | \
         baseline_bytes={} | candidate_bytes={}",
        baseline.elapsed,
        candidate.elapsed,
        baseline.allocations,
        candidate.allocations,
        baseline.bytes,
        candidate.bytes,
    );
}

#[derive(Debug, Clone, Copy)]
struct ProbeResult {
    elapsed: std::time::Duration,
    allocations: u64,
    bytes: u64,
    checksum: usize,
}

fn measure_baseline(record: &StorageStreamRecord, repetitions: usize) -> ProbeResult {
    let guard = AllocationGuard::start(
        module_path!(),
        "identity-serialization",
        file!(),
        line!(),
        Some("baseline"),
    );
    let started = Instant::now();
    let mut checksum = 0;
    for _ in 0..repetitions {
        let bytes = reference_canonical_record_bytes(record);
        checksum ^= black_box(bytes).len();
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("identity baseline allocation report");
    ProbeResult {
        elapsed,
        allocations: report.allocation_count,
        bytes: report.allocated_bytes,
        checksum,
    }
}

fn measure_candidate(record: &StorageStreamRecord, repetitions: usize) -> ProbeResult {
    let mut output = Vec::with_capacity(4_096);
    let guard = AllocationGuard::start(
        module_path!(),
        "identity-serialization",
        file!(),
        line!(),
        Some("candidate"),
    );
    let started = Instant::now();
    let mut checksum = 0;
    for _ in 0..repetitions {
        output.clear();
        record.write_canonical_json(&mut output);
        checksum ^= black_box(output.as_slice()).len();
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("identity candidate allocation report");
    ProbeResult {
        elapsed,
        allocations: report.allocation_count,
        bytes: report.allocated_bytes,
        checksum,
    }
}

fn batch_id_for_record(record: &StorageStreamRecord) -> String {
    let records = [source_record(record.clone())];
    let checkpoints = [SourceJobCheckpoint {
        iterator: true,
        position: "checkpoint-1".to_owned(),
    }];
    let scope = SourceJobScope::table_with_predecessor("source-events", "shard-1", None);
    let manifest_digest = [0x2a; analytics_engine::MANIFEST_DIGEST_BYTES];
    identity::batch_identity(identity_input(
        &manifest_digest,
        &scope,
        &records,
        &checkpoints,
    ))
}

fn identity_input<'a>(
    manifest_digest: &'a [u8; analytics_engine::MANIFEST_DIGEST_BYTES],
    scope: &'a SourceJobScope,
    records: &'a [SourceJobRecord],
    checkpoints: &'a [SourceJobCheckpoint],
) -> BatchIdentityInput<'a> {
    BatchIdentityInput {
        source_kind: SourceJobKind::StorageStream,
        manifest_digest,
        scope,
        records,
        checkpoints,
    }
}

fn source_record(record: StorageStreamRecord) -> SourceJobRecord {
    SourceJobRecord {
        source_table_name: "source-events".to_owned(),
        analytics_table_names: vec!["analytics-events".to_owned()],
        record_key: b"key-1".to_vec(),
        record,
        system_position: None,
    }
}

fn representative_record(reverse: bool, include_old_image: bool) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: ordered_item(
            reverse,
            vec![
                (
                    "pk",
                    StorageValue::S("USER#\"escaped\"\\\n\u{0001}é".to_owned()),
                ),
                ("sk", StorageValue::N("00042.500".to_owned())),
            ],
        ),
        sequence_number: "seq-\\\"\n\u{0002}é".to_owned(),
        old_image: include_old_image.then(|| {
            ordered_item(
                !reverse,
                vec![("old", StorageValue::S("old-value".to_owned()))],
            )
        }),
        new_image: Some(image_item(reverse)),
    }
}

fn image_item(reverse: bool) -> StorageItem {
    ordered_item(
        reverse,
        vec![
            ("binary", StorageValue::B("AA+/=".to_owned())),
            (
                "strings",
                StorageValue::SS(vec!["z".to_owned(), "a\"".to_owned()]),
            ),
            (
                "numbers",
                StorageValue::NS(vec!["10".to_owned(), "1.5".to_owned()]),
            ),
            (
                "binaries",
                StorageValue::BS(vec!["AQ==".to_owned(), "Ag==".to_owned()]),
            ),
            ("boolean", StorageValue::BOOL(true)),
            ("null", StorageValue::NULL(false)),
            (
                "list",
                StorageValue::L(vec![
                    StorageValue::S("line\nvalue".to_owned()),
                    StorageValue::M(nested_item(!reverse)),
                ]),
            ),
            ("nested", StorageValue::M(nested_item(reverse))),
        ],
    )
}

fn nested_item(reverse: bool) -> StorageItem {
    ordered_item(
        reverse,
        vec![
            (
                "deep",
                StorageValue::M(ordered_item(
                    !reverse,
                    vec![("value", StorageValue::S("deep-value".to_owned()))],
                )),
            ),
            ("text", StorageValue::S("nested\ttext".to_owned())),
        ],
    )
}

fn ordered_item(reverse: bool, mut entries: Vec<(&str, StorageValue)>) -> StorageItem {
    if reverse {
        entries.reverse();
    }
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

fn reference_batch_identity(input: BatchIdentityInput<'_>) -> String {
    let BatchIdentityInput {
        source_kind,
        manifest_digest,
        scope,
        records,
        checkpoints,
    } = input;
    let mut bytes = Vec::new();
    reference_append_field(&mut bytes, b"analytics-api:source-batch:v1");
    reference_append_field(&mut bytes, source_kind.job_kind().as_bytes());
    reference_append_field(&mut bytes, manifest_digest);
    reference_append_scope(&mut bytes, scope);
    reference_append_count(&mut bytes, records.len());
    for record in records {
        reference_append_field(&mut bytes, record.source_table_name.as_bytes());
        reference_append_count(&mut bytes, record.analytics_table_names.len());
        for name in &record.analytics_table_names {
            reference_append_field(&mut bytes, name.as_bytes());
        }
        reference_append_field(&mut bytes, &record.record_key);
        reference_append_field(&mut bytes, record.record.sequence_number.as_bytes());
        reference_append_field(
            &mut bytes,
            &reference_canonical_record_bytes(&record.record),
        );
        if let Some(position) = record.system_position.as_ref() {
            reference_append_field(&mut bytes, position.shard_id().as_str().as_bytes());
            reference_append_field(&mut bytes, position.lineage_id().as_str().as_bytes());
            reference_append_field(&mut bytes, position.offset().as_str().as_bytes());
        }
    }
    reference_append_count(&mut bytes, checkpoints.len());
    for checkpoint in checkpoints {
        bytes.push(u8::from(checkpoint.iterator));
        reference_append_field(&mut bytes, checkpoint.position.as_bytes());
    }
    format_digest(Sha256::digest(bytes))
}

fn reference_append_scope(bytes: &mut Vec<u8>, scope: &SourceJobScope) {
    match scope {
        SourceJobScope::Table {
            source_table_name,
            shard_id,
            previous_position,
        } => {
            reference_append_field(bytes, b"table");
            reference_append_field(bytes, source_table_name.as_bytes());
            reference_append_field(bytes, shard_id.as_bytes());
            match previous_position {
                Some(position) => reference_append_field(bytes, position.as_bytes()),
                None => bytes.push(0),
            }
        }
        SourceJobScope::System {
            source_scope,
            previous_checkpoint,
            checkpoint,
        } => {
            reference_append_field(bytes, b"system");
            bytes.push(source_scope.version());
            reference_append_field(bytes, source_scope.scope_id().as_str().as_bytes());
            reference_append_field(bytes, source_scope.generation().to_be_bytes().as_slice());
            match previous_checkpoint {
                super::SystemStreamCheckpointPredecessor::Initial => bytes.push(0),
                super::SystemStreamCheckpointPredecessor::Token { checkpoint } => {
                    bytes.push(2);
                    reference_append_field(bytes, checkpoint.as_str().as_bytes());
                }
            }
            reference_append_field(bytes, checkpoint.as_str().as_bytes());
        }
    }
}

fn reference_append_field(bytes: &mut Vec<u8>, field: &[u8]) {
    reference_append_count(bytes, field.len());
    bytes.extend_from_slice(field);
}

fn reference_append_count(bytes: &mut Vec<u8>, count: usize) {
    bytes.extend_from_slice(&u64::try_from(count).unwrap_or(u64::MAX).to_be_bytes());
}

fn format_digest(digest: sha2::digest::Output<Sha256>) -> String {
    let mut identity = String::with_capacity(3 + digest.len() * 2);
    identity.push_str("v1-");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        identity.push(HEX[usize::from(byte >> 4)] as char);
        identity.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    identity
}

fn reference_canonical_record_bytes(record: &StorageStreamRecord) -> Vec<u8> {
    let value = serde_json::to_value(record).expect("source stream records are serializable");
    serde_json::to_vec(&reference_canonical_json(value))
        .expect("canonical source records are serializable")
}

fn reference_canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            let mut canonical = serde_json::Map::with_capacity(entries.len());
            for (key, value) in entries {
                canonical.insert(key, reference_canonical_json(value));
            }
            serde_json::Value::Object(canonical)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(reference_canonical_json).collect())
        }
        scalar => scalar,
    }
}
