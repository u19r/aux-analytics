use analytics_contract::{
    AnalyticsManifest, PrivacyPolicy, RetentionTimestamp, StorageStreamRecord, StorageValue,
};
use sha2::{Digest, Sha256};
use storage_types::{SystemStreamPosition, VecMap};

use super::digest::{
    ContentDigestInput, canonical_json_bytes, content_digest, manifest_digest,
    write_retention_timestamp,
};
use crate::{
    constants::{CONTENT_DIGEST_BYTES, MANIFEST_DIGEST_BYTES},
    engine::{IngestRetention, RecordCheckpoint, SourceBatchRecord, StreamRecordBatchItem},
};

#[test]
fn given_nested_json_with_escaped_scalars_when_canonical_bytes_are_written_then_sorted_reference_bytes_match()
 {
    let value = serde_json::json!({
        "z": [
            "quote \" slash \\\\ newline\n",
            1.2300,
            -0.0,
        ],
        "a": {
            "nested-z": true,
            "nested-a": null,
        },
    });
    let actual = canonical_json_bytes(&value);
    let expected = legacy_canonical_json_bytes(value);
    assert_eq!(actual, expected);
}

#[test]
fn given_empty_manifest_when_digest_is_calculated_then_sorted_reference_hash_matches() {
    let manifest = AnalyticsManifest::new(Vec::new());
    let value = serde_json::to_value(&manifest).expect("manifest serializes");
    let expected: [u8; MANIFEST_DIGEST_BYTES] =
        Sha256::digest(legacy_canonical_json_bytes(value)).into();

    assert_eq!(manifest_digest(&manifest), expected);
}

#[test]
fn given_escaped_retention_attribute_when_timestamp_is_written_then_sorted_json_is_preserved() {
    let timestamp = RetentionTimestamp::Attribute {
        attribute_path: "created_at\"\\\n".to_string(),
    };
    let mut actual = Vec::new();

    write_retention_timestamp(&timestamp, &mut actual);

    assert_eq!(
        actual,
        br#"{"attribute_path":"created_at\"\\\n","kind":"attribute"}"#
    );
    assert_eq!(
        actual,
        legacy_canonical_json_bytes(serde_json::to_value(timestamp).expect("timestamp serializes"))
    );
}

#[test]
fn given_ingested_at_retention_when_timestamp_is_written_then_kind_is_fixed() {
    let mut actual = Vec::new();

    write_retention_timestamp(&RetentionTimestamp::IngestedAt, &mut actual);

    assert_eq!(actual, br#"{"kind":"ingested_at"}"#);
}

#[test]
fn given_records_checkpoints_and_privacy_policy_when_content_digest_is_calculated_then_fixed_framed_hash_matches()
 {
    let records = vec![fixture_source_record()];
    let checkpoints = vec![
        RecordCheckpoint::try_new("source-items", "shard-1", "cursor-9")
            .expect("checkpoint is valid"),
    ];
    let policy = PrivacyPolicy::try_new("privacy-v1")
        .expect("policy is valid")
        .with_denied_key_name("secret");
    let manifest_digest = [13; MANIFEST_DIGEST_BYTES];

    let actual = content_digest(ContentDigestInput {
        manifest_digest: &manifest_digest,
        previous_position: Some("cursor-8"),
        records: &records,
        checkpoints: &checkpoints,
        policy: Some(&policy),
    });
    let legacy = legacy_content_digest(
        &manifest_digest,
        Some("cursor-8"),
        &records,
        &checkpoints,
        Some(&policy),
    );
    let expected = [
        175, 145, 71, 109, 22, 218, 162, 63, 231, 57, 45, 113, 168, 195, 246, 36, 145, 194, 130,
        99, 32, 233, 189, 157, 6, 130, 215, 64, 71, 100, 225, 207,
    ];
    assert_eq!(actual, expected);
    assert_eq!(actual, legacy);
}

fn fixture_source_record() -> SourceBatchRecord {
    let image = VecMap::from_iter([
        ("pk".to_string(), StorageValue::S("ITEM#1".to_string())),
        (
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([
                (
                    "z-last".to_string(),
                    StorageValue::S("quote \" slash \\\\ line\n".to_string()),
                ),
                ("a-first".to_string(), StorageValue::N("1.2300".to_string())),
            ])),
        ),
    ]);
    SourceBatchRecord {
        item: StreamRecordBatchItem {
            analytics_table_name: "items".to_string(),
            source_table_name: "source-items".to_string(),
            shard_id: Some("shard-1".to_string()),
            record_key: b"record-key-1".to_vec(),
            record: StorageStreamRecord {
                sequence_number: "sequence-9".to_string(),
                keys: VecMap::from_iter([(
                    "pk".to_string(),
                    StorageValue::S("ITEM#1".to_string()),
                )]),
                old_image: Some(VecMap::from_iter([(
                    "pk".to_string(),
                    StorageValue::S("ITEM#1-old".to_string()),
                )])),
                new_image: Some(image),
            },
            system_position: Some(
                SystemStreamPosition::try_new("shard-1", "lineage-1", "offset-9")
                    .expect("system position is valid"),
            ),
        },
        retention: Some(IngestRetention {
            period_ms: Some(86_400_000),
            timestamp: RetentionTimestamp::Attribute {
                attribute_path: "created_at_ms".to_string(),
            },
            missing_retention: false,
        }),
    }
}

fn legacy_canonical_json_bytes(value: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&legacy_canonical_json(value)).expect("canonical JSON is serializable")
}

fn legacy_content_digest(
    manifest_digest: &[u8; MANIFEST_DIGEST_BYTES],
    previous_position: Option<&str>,
    records: &[SourceBatchRecord],
    checkpoints: &[RecordCheckpoint],
    policy: Option<&PrivacyPolicy>,
) -> [u8; CONTENT_DIGEST_BYTES] {
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
        let record = serde_json::to_value(&item.record).expect("record serializes");
        legacy_append_digest_field(&mut input, &legacy_canonical_json_bytes(record));
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
            let timestamp =
                serde_json::to_value(&retention.timestamp).expect("retention timestamp serializes");
            legacy_append_digest_field(&mut input, &legacy_canonical_json_bytes(timestamp));
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
            let policy = serde_json::to_value(policy).expect("privacy policy serializes");
            legacy_append_digest_field(&mut input, &legacy_canonical_json_bytes(policy));
        }
        None => input.push(0),
    }
    Sha256::digest(input).into()
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
