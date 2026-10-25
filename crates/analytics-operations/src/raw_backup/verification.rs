use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

use analytics_contract::{PrivacyPolicy, PrivacyPolicyMatcher};

use super::{
    RawBackupError, RawBackupMetrics, RawBackupObjectIndex, RawBackupRecord, RawBackupReplayRecord,
    RawBackupReplayReport, RawBackupResult, RawBackupVerifyReport, RawBackupWriteRequest,
    checksum_hex, decode_object_bytes, encode_object_bytes,
};
use crate::constants::HEX;

pub(crate) fn path_matches_store_path(candidate: &Path, expected: &Path) -> bool {
    if candidate == expected {
        return true;
    }
    if !candidate.is_absolute() || candidate.file_name() != expected.file_name() {
        return false;
    }
    let Some(candidate_parent) = candidate.parent() else {
        return false;
    };
    let Some(expected_parent) = expected.parent() else {
        return false;
    };
    let candidate_parent = fs::canonicalize(candidate_parent).ok();
    let expected_parent = fs::canonicalize(expected_parent).ok();
    candidate_parent.is_some() && candidate_parent == expected_parent
}

pub(crate) fn create_or_verify_marker(path: &Path, bytes: &[u8]) -> RawBackupResult<()> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = fs::read(path)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(RawBackupError::ObjectStore(format!(
                    "raw backup publication marker differs for {}",
                    path.display()
                )))
            }
        }
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn write_or_verify_file(path: &Path, bytes: &[u8]) -> RawBackupResult<()> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = fs::read(path)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(RawBackupError::ObjectAlreadyExists(
                    path.to_string_lossy().into_owned(),
                ))
            }
        }
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn remove_file_if_present(path: &Path) -> RawBackupResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn path_exists(path: &Path) -> RawBackupResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) struct EncodedRawBackupObject {
    pub(crate) bytes: Vec<u8>,
    pub(crate) uncompressed_len: usize,
}

pub(crate) fn encode_records(
    request: &RawBackupWriteRequest,
) -> RawBackupResult<EncodedRawBackupObject> {
    let mut encoded = Vec::new();
    for record in &request.records {
        serde_json::to_writer(&mut encoded, record)?;
        encoded.push(b'\n');
    }
    let uncompressed_len = encoded.len();
    Ok(EncodedRawBackupObject {
        bytes: encode_object_bytes(encoded, request.layout.compression),
        uncompressed_len,
    })
}

pub(crate) fn verify_object_bytes(
    index: &RawBackupObjectIndex,
    bytes: &[u8],
) -> RawBackupResult<RawBackupVerifyReport> {
    index.validate_schema_version()?;
    let actual_checksum = checksum_hex(bytes);
    let checksum_valid = actual_checksum == index.checksum;
    Ok(RawBackupVerifyReport {
        object_id: index.object_id.clone(),
        checksum_valid,
        expected_checksum: index.checksum.clone(),
        actual_checksum,
        byte_count: bytes.len() as u64,
        metrics: RawBackupMetrics {
            checksum_failures: u64::from(!checksum_valid),
            ..RawBackupMetrics::default()
        },
    })
}

pub(crate) fn same_raw_backup_fingerprint(
    left: &RawBackupObjectIndex,
    right: &RawBackupObjectIndex,
) -> bool {
    left.operation_id == right.operation_id
        && left.source_identity == right.source_identity
        && left.layout == right.layout
        && left.record_count == right.record_count
        && left.byte_count == right.byte_count
        && left.uncompressed_byte_count == right.uncompressed_byte_count
        && left.checksum == right.checksum
        && left.privacy_policy_version == right.privacy_policy_version
        && left.compacted_from_object_ids == right.compacted_from_object_ids
}

pub(crate) fn replay_verified_bytes(
    index: &RawBackupObjectIndex,
    bytes: &[u8],
) -> RawBackupResult<RawBackupReplayReport> {
    index.validate_schema_version()?;
    let mut records = Vec::new();
    let decoded = decode_object_bytes(bytes, index.layout.compression)?;
    let reader = BufReader::new(decoded.as_ref());
    for line in reader.lines() {
        let record: RawBackupRecord = serde_json::from_str(line?.as_str())?;
        record.validate()?;
        records.push(RawBackupReplayRecord {
            table_name: record.table_name,
            record_key: record.record_key.into_bytes(),
            source_position: record.source_position,
            record: record.record,
        });
    }
    Ok(RawBackupReplayReport {
        metrics: RawBackupMetrics {
            replayed_records: records.len() as u64,
            ..RawBackupMetrics::default()
        },
        records,
    })
}

pub(crate) fn append_sanitized_object_id_part(output: &mut String, value: &str) {
    for character in value.chars() {
        output.push(
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            },
        );
    }
}

pub(crate) fn hex_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(crate) fn filter_raw_backup_record(
    mut record: RawBackupRecord,
    matcher: &PrivacyPolicyMatcher<'_>,
    policy_drops: &mut u64,
) -> RawBackupResult<RawBackupRecord> {
    if matcher.denies_key(record.record_key.as_str())
        || matcher.denies_path(record.record_key.as_str())
        || matcher.denies_value(record.record_key.as_str())
    {
        return Err(RawBackupError::PrivacyIdentityDenied);
    }
    let keys = matcher.filter_item(std::mem::take(&mut record.record.keys));
    *policy_drops += keys.dropped_fields;
    record.record.keys = keys.item;
    if let Some(old_image) = record.record.old_image.take() {
        let filtered = matcher.filter_item(old_image);
        *policy_drops += filtered.dropped_fields;
        record.record.old_image = Some(filtered.item);
    }
    if let Some(new_image) = record.record.new_image.take() {
        let filtered = matcher.filter_item(new_image);
        *policy_drops += filtered.dropped_fields;
        record.record.new_image = Some(filtered.item);
    }
    Ok(record)
}

fn filter_raw_backup_replay_record(
    record: RawBackupReplayRecord,
    matcher: &PrivacyPolicyMatcher<'_>,
    policy_drops: &mut u64,
) -> RawBackupResult<RawBackupReplayRecord> {
    let record_key =
        String::from_utf8(record.record_key).map_err(|_| RawBackupError::PrivacyIdentityDenied)?;
    let filtered = filter_raw_backup_record(
        RawBackupRecord {
            table_name: record.table_name,
            record_key,
            source_position: record.source_position,
            record: record.record,
        },
        matcher,
        policy_drops,
    )?;
    Ok(RawBackupReplayRecord {
        table_name: filtered.table_name,
        record_key: filtered.record_key.into_bytes(),
        source_position: filtered.source_position,
        record: filtered.record,
    })
}

pub(crate) fn apply_raw_backup_replay_privacy_policy(
    replay: RawBackupReplayReport,
    policy: &PrivacyPolicy,
) -> RawBackupResult<RawBackupReplayReport> {
    let matcher = policy.validated()?;
    let mut policy_drops = 0_u64;
    let mut records = Vec::with_capacity(replay.records.len());
    for record in replay.records {
        records.push(filter_raw_backup_replay_record(
            record,
            &matcher,
            &mut policy_drops,
        )?);
    }
    Ok(RawBackupReplayReport {
        metrics: RawBackupMetrics {
            replayed_records: records.len() as u64,
            policy_drops,
            ..replay.metrics
        },
        records,
    })
}
