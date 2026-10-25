use analytics_contract::{
    PrivacyPolicy, RetentionTimestamp, StorageItem, StorageStreamRecord, StorageValue,
};
use sha2::Digest;
use storage_types::{SystemStreamPosition, VecMap};

use super::*;
use crate::{IngestRetention, RecordCheckpoint, SourceBatchRecord, StreamRecordBatchItem};

const PROBE_RECORD_COUNT: usize = 256;

#[test]
#[ignore = "debug-only source effect digest allocation regression"]
fn given_source_records_when_effect_digest_is_measured_then_outputs_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let records = realistic_source_records();
    let checkpoints = vec![
        RecordCheckpoint::try_new("source-items", "shard-1", "cursor-256")
            .expect("checkpoint is valid"),
    ];
    let manifest_digest = [17; super::super::MANIFEST_DIGEST_BYTES];
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .expect("policy is valid")
        .with_denied_key_name("secret");
    let result = super::measure_production_candidate(
        "source effect content digest",
        "source_effect/digest.rs::content_digest::canonical_stream",
        || baseline_content_digest_probe(&manifest_digest, &records, &checkpoints, Some(&policy)),
        || production_content_digest_probe(&manifest_digest, &records, &checkpoints, Some(&policy)),
    );
    super::print_production_candidate_results(
        "source-effect-digest-candidate",
        std::slice::from_ref(&result),
    );
    assert!(result.kept);
    assert_eq!(result.serialized_bytes, PROBE_RECORD_COUNT as u64);
}

fn realistic_source_records() -> Vec<SourceBatchRecord> {
    (0..PROBE_RECORD_COUNT)
        .map(|index| SourceBatchRecord {
            item: StreamRecordBatchItem {
                analytics_table_name: "analytics_items".to_string(),
                source_table_name: "source_items".to_string(),
                shard_id: Some("shard-1".to_string()),
                record_key: format!("record-key-{index:04}").into_bytes(),
                record: StorageStreamRecord {
                    sequence_number: format!("sequence-{index:04}"),
                    keys: item(&format!("key-{index}")),
                    old_image: (index > 0).then(|| item(&format!("old-{index}"))),
                    new_image: Some(item(&format!("new-{index}"))),
                },
                system_position: (index % 2 == 0).then(|| {
                    SystemStreamPosition::try_new(
                        format!("shard-{}", index % 4),
                        format!("lineage-{}", index % 3),
                        format!("offset-{index:04}"),
                    )
                    .expect("valid system position")
                }),
            },
            retention: (index % 3 != 0).then(|| IngestRetention {
                period_ms: Some(86_400_000 + index as u64),
                timestamp: if index.is_multiple_of(2) {
                    RetentionTimestamp::Attribute {
                        attribute_path: "created_at_ms".to_owned(),
                    }
                } else {
                    RetentionTimestamp::IngestedAt
                },
                missing_retention: index.is_multiple_of(5),
            }),
        })
        .collect()
}

fn item(seed: &str) -> StorageItem {
    VecMap::from_iter([
        ("pk".to_string(), StorageValue::S(format!("ITEM#{seed}"))),
        (
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([
                (
                    "z-last".to_string(),
                    StorageValue::S("quote \" slash \\\\ newline\n".to_string()),
                ),
                (
                    "a-first".to_string(),
                    StorageValue::S(format!("{seed}@example.test")),
                ),
            ])),
        ),
        (
            "values".to_string(),
            StorageValue::L(vec![
                StorageValue::N("1.2300".to_string()),
                StorageValue::BOOL(true),
                StorageValue::NULL(true),
            ]),
        ),
    ])
}

fn production_content_digest_probe(
    manifest_digest: &[u8; super::super::MANIFEST_DIGEST_BYTES],
    records: &[SourceBatchRecord],
    checkpoints: &[RecordCheckpoint],
    policy: Option<&PrivacyPolicy>,
) -> ProductionProbe {
    let digest = crate::source_effect::content_digest(crate::source_effect::ContentDigestInput {
        manifest_digest,
        previous_position: Some("cursor-0"),
        records,
        checkpoints,
        policy,
    });
    ProductionProbe {
        output_digest: u64::from_be_bytes(digest[..8].try_into().expect("digest prefix")),
        serialized_bytes: records.len() as u64,
    }
}

fn baseline_content_digest_probe(
    manifest_digest: &[u8; super::super::MANIFEST_DIGEST_BYTES],
    records: &[SourceBatchRecord],
    checkpoints: &[RecordCheckpoint],
    policy: Option<&PrivacyPolicy>,
) -> ProductionProbe {
    let digest = legacy_content_digest(
        manifest_digest,
        Some("cursor-0"),
        records,
        checkpoints,
        policy,
    );
    ProductionProbe {
        output_digest: u64::from_be_bytes(digest[..8].try_into().expect("digest prefix")),
        serialized_bytes: records.len() as u64,
    }
}

fn legacy_content_digest(
    manifest_digest: &[u8; super::super::MANIFEST_DIGEST_BYTES],
    previous_position: Option<&str>,
    records: &[SourceBatchRecord],
    checkpoints: &[RecordCheckpoint],
    policy: Option<&analytics_contract::PrivacyPolicy>,
) -> [u8; crate::source_effect::CONTENT_DIGEST_BYTES] {
    let mut input = Vec::new();
    legacy_append_digest_field(&mut input, b"analytics-engine:source-effect:v1");
    legacy_append_digest_field(&mut input, manifest_digest);
    legacy_append_digest_option(&mut input, previous_position.map(str::as_bytes));
    legacy_append_digest_count(&mut input, records.len());
    for source_record in records {
        let item = &source_record.item;
        legacy_append_digest_field(&mut input, item.source_table_name.as_bytes());
        legacy_append_digest_field(&mut input, item.analytics_table_name.as_bytes());
        legacy_append_digest_option(&mut input, item.shard_id.as_deref().map(str::as_bytes));
        legacy_append_digest_field(&mut input, item.record_key.as_slice());
        legacy_append_digest_field(&mut input, item.record.sequence_number.as_bytes());
        legacy_append_digest_field(
            &mut input,
            &legacy_canonical_json_bytes(
                serde_json::to_value(&item.record).expect("source stream records are serializable"),
            ),
        );
        if let Some(position) = item.system_position.as_ref() {
            input.push(1);
            legacy_append_digest_field(&mut input, position.shard_id().as_str().as_bytes());
            legacy_append_digest_field(&mut input, position.lineage_id().as_str().as_bytes());
            legacy_append_digest_field(&mut input, position.offset().as_str().as_bytes());
        } else {
            input.push(0);
        }
        if let Some(retention) = source_record.retention.as_ref() {
            input.push(1);
            input.extend_from_slice(&retention.period_ms.unwrap_or_default().to_be_bytes());
            input.push(u8::from(retention.missing_retention));
            legacy_append_digest_field(
                &mut input,
                &legacy_canonical_json_bytes(
                    serde_json::to_value(&retention.timestamp)
                        .expect("retention timestamps are serializable"),
                ),
            );
        } else {
            input.push(0);
        }
    }
    legacy_append_digest_count(&mut input, checkpoints.len());
    for checkpoint in checkpoints {
        legacy_append_digest_field(&mut input, checkpoint.source_table_name().as_bytes());
        legacy_append_digest_field(&mut input, checkpoint.shard_id().as_bytes());
        legacy_append_digest_field(&mut input, checkpoint.position().as_bytes());
    }
    match policy {
        Some(policy) => {
            input.push(1);
            legacy_append_digest_field(
                &mut input,
                &legacy_canonical_json_bytes(
                    serde_json::to_value(policy).expect("privacy policies are serializable"),
                ),
            );
        }
        None => input.push(0),
    }
    sha2::Sha256::digest(input).into()
}

fn legacy_append_digest_option(input: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            input.push(1);
            legacy_append_digest_field(input, value);
        }
        None => input.push(0),
    }
}

fn legacy_append_digest_count(input: &mut Vec<u8>, count: usize) {
    input.extend_from_slice(&u64::try_from(count).unwrap_or(u64::MAX).to_be_bytes());
}

fn legacy_append_digest_field(input: &mut Vec<u8>, field: &[u8]) {
    legacy_append_digest_count(input, field.len());
    input.extend_from_slice(field);
}

fn legacy_canonical_json_bytes(value: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&legacy_canonical_json(value)).expect("canonical JSON is serializable")
}

fn legacy_canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            let mut canonical = serde_json::Map::with_capacity(entries.len());
            for (key, value) in entries {
                canonical.insert(key, legacy_canonical_json(value));
            }
            serde_json::Value::Object(canonical)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(legacy_canonical_json).collect())
        }
        scalar => scalar,
    }
}
