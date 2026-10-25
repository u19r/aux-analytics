use analytics_contract::{AnalyticsManifest, PrivacyPolicy, RetentionTimestamp};
use sha2::{Digest, Sha256};
use storage_types::SystemStreamPosition;

use crate::{
    constants::{CONTENT_DIGEST_BYTES, MANIFEST_DIGEST_BYTES},
    engine::{IngestRetention, RecordCheckpoint, SourceBatchRecord},
};

pub(crate) struct ContentDigestInput<'a> {
    pub(crate) manifest_digest: &'a [u8; MANIFEST_DIGEST_BYTES],
    pub(crate) previous_position: Option<&'a str>,
    pub(crate) records: &'a [SourceBatchRecord],
    pub(crate) checkpoints: &'a [RecordCheckpoint],
    pub(crate) policy: Option<&'a PrivacyPolicy>,
}

pub(crate) fn content_digest(input: ContentDigestInput<'_>) -> [u8; CONTENT_DIGEST_BYTES] {
    let ContentDigestInput {
        manifest_digest,
        previous_position,
        records,
        checkpoints,
        policy,
    } = input;
    let mut digest = Sha256::new();
    append_digest_field(&mut digest, b"analytics-engine:source-effect:v1");
    append_digest_field(&mut digest, manifest_digest);
    append_digest_option(&mut digest, previous_position.map(str::as_bytes));
    append_digest_records(&mut digest, records);
    append_digest_checkpoints(&mut digest, checkpoints);
    append_digest_policy(&mut digest, policy);
    digest.finalize().into()
}

fn append_digest_records(digest: &mut Sha256, records: &[SourceBatchRecord]) {
    append_digest_count(digest, records.len());
    let mut record_bytes = Vec::new();
    for source_record in records {
        append_digest_record(digest, source_record, &mut record_bytes);
    }
}

fn append_digest_record(
    digest: &mut Sha256,
    source_record: &SourceBatchRecord,
    record_bytes: &mut Vec<u8>,
) {
    let item = &source_record.item;
    append_digest_field(digest, item.source_table_name.as_bytes());
    append_digest_field(digest, item.analytics_table_name.as_bytes());
    append_digest_option(digest, item.shard_id.as_deref().map(str::as_bytes));
    append_digest_field(digest, item.record_key.as_slice());
    append_digest_field(digest, item.record.sequence_number.as_bytes());
    record_bytes.clear();
    item.record.write_canonical_json(record_bytes);
    append_digest_field(digest, record_bytes);
    append_digest_system_position(digest, item.system_position.as_ref());
    append_digest_retention(digest, source_record.retention.as_ref(), record_bytes);
}

fn append_digest_system_position(digest: &mut Sha256, position: Option<&SystemStreamPosition>) {
    let Some(position) = position else {
        digest.update([0]);
        return;
    };
    digest.update([1]);
    append_digest_field(digest, position.shard_id().as_str().as_bytes());
    append_digest_field(digest, position.lineage_id().as_str().as_bytes());
    append_digest_field(digest, position.offset().as_str().as_bytes());
}

fn append_digest_retention(
    digest: &mut Sha256,
    retention: Option<&IngestRetention>,
    record_bytes: &mut Vec<u8>,
) {
    let Some(retention) = retention else {
        digest.update([0]);
        return;
    };
    digest.update([1]);
    digest.update(retention.period_ms.unwrap_or_default().to_be_bytes());
    digest.update([u8::from(retention.missing_retention)]);
    record_bytes.clear();
    write_retention_timestamp(&retention.timestamp, record_bytes);
    append_digest_field(digest, record_bytes);
}

pub(crate) fn write_retention_timestamp(timestamp: &RetentionTimestamp, output: &mut Vec<u8>) {
    match timestamp {
        RetentionTimestamp::Attribute { attribute_path } => {
            output.extend_from_slice(b"{\"attribute_path\":");
            serde_json::to_writer(&mut *output, attribute_path)
                .expect("retention attribute paths are serializable");
            output.extend_from_slice(b",\"kind\":\"attribute\"}");
        }
        RetentionTimestamp::IngestedAt => output.extend_from_slice(b"{\"kind\":\"ingested_at\"}"),
    }
}

fn append_digest_checkpoints(digest: &mut Sha256, checkpoints: &[RecordCheckpoint]) {
    append_digest_count(digest, checkpoints.len());
    for checkpoint in checkpoints {
        append_digest_field(digest, checkpoint.source_table_name().as_bytes());
        append_digest_field(digest, checkpoint.shard_id().as_bytes());
        append_digest_field(digest, checkpoint.position().as_bytes());
    }
}

fn append_digest_policy(digest: &mut Sha256, policy: Option<&PrivacyPolicy>) {
    // Policy is a batch boundary, so retain one serde conversion after record
    // frames.
    let Some(policy) = policy else {
        digest.update([0]);
        return;
    };
    digest.update([1]);
    let policy = serde_json::to_value(policy).expect("privacy policies are serializable");
    append_canonical_json_field(digest, &policy);
}

/// Computes the canonical mutation-relevant manifest identity. Object keys
/// are sorted recursively so equivalent decoded manifests cannot produce
/// different effect authorities merely because their JSON ordering differs.
/// Manifest conversion remains a required serde boundary and happens once.
#[must_use]
pub fn manifest_digest(manifest: &AnalyticsManifest) -> [u8; MANIFEST_DIGEST_BYTES] {
    Sha256::digest(canonical_json_bytes(
        &serde_json::to_value(manifest).expect("analytics manifests are serializable"),
    ))
    .into()
}

fn append_digest_option(digest: &mut Sha256, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            digest.update([1]);
            append_digest_field(digest, value);
        }
        None => digest.update([0]),
    }
}

fn append_digest_count(digest: &mut Sha256, count: usize) {
    digest.update(u64::try_from(count).unwrap_or(u64::MAX).to_be_bytes());
}

fn append_digest_field(digest: &mut Sha256, field: &[u8]) {
    append_digest_count(digest, field.len());
    digest.update(field);
}

fn append_canonical_json_field(digest: &mut Sha256, value: &serde_json::Value) {
    // The persisted frame prefixes each canonical field with its byte length,
    // so retain only that field while writing it once; hashing a direct stream
    // would require a slower length pass before the digest pass.
    let bytes = canonical_json_bytes(value);
    append_digest_field(digest, &bytes);
}

pub(super) fn canonical_json_bytes(value: &serde_json::Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_canonical_json(value, &mut bytes);
    bytes
}

fn write_canonical_json(value: &serde_json::Value, output: &mut Vec<u8>) {
    match value {
        serde_json::Value::Object(object) => write_canonical_object(object, output),
        serde_json::Value::Array(values) => write_canonical_array(values, output),
        scalar => write_canonical_scalar(scalar, output),
    }
}

fn write_canonical_object(
    object: &serde_json::Map<String, serde_json::Value>,
    output: &mut Vec<u8>,
) {
    output.push(b'{');
    let mut entries = object.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(left, _)| *left);
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        serde_json::to_writer(&mut *output, key).expect("canonical JSON keys are serializable");
        output.push(b':');
        write_canonical_json(value, output);
    }
    output.push(b'}');
}

fn write_canonical_array(values: &[serde_json::Value], output: &mut Vec<u8>) {
    output.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        write_canonical_json(value, output);
    }
    output.push(b']');
}

fn write_canonical_scalar(value: &serde_json::Value, output: &mut Vec<u8>) {
    match value {
        serde_json::Value::String(value) => serde_json::to_writer(&mut *output, value)
            .expect("canonical JSON strings are serializable"),
        serde_json::Value::Number(value) => serde_json::to_writer(&mut *output, value)
            .expect("canonical JSON numbers are serializable"),
        serde_json::Value::Bool(value) => serde_json::to_writer(&mut *output, value)
            .expect("canonical JSON booleans are serializable"),
        serde_json::Value::Null => output.extend_from_slice(b"null"),
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            unreachable!("canonical scalar writer received a container")
        }
    }
}
