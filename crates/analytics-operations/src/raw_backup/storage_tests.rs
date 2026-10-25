use std::{borrow::Cow, fs};

use super::{
    RawBackupCompression, RawBackupError, checksum_hex,
    codec::{decode_object_bytes, encode_object_bytes},
    test_support::{ReplayFixture, count_object_reads},
};

#[test]
fn given_committed_objects_with_each_codec_and_privacy_when_replayed_then_one_read_preserves_records_and_summary()
 {
    for codec in [RawBackupCompression::None, RawBackupCompression::RunLength] {
        for privacy in [false, true] {
            let fixture = ReplayFixture::new(3, codec, privacy);
            let (baseline, baseline_reads) =
                count_object_reads(|| fixture.replay_with_separate_verification());
            let (fused, fused_reads) =
                count_object_reads(|| fixture.request.execute(&fixture.store).unwrap());
            assert_eq!(baseline_reads, 2);
            assert_eq!(fused_reads, 1);
            assert_eq!(fused, baseline);
            assert_eq!(fused.records, fixture.expected);
            assert!(fused.summary.dry_run);
            assert!(fused.summary.verification.checksum_valid);
            assert_eq!(fused.summary.record_count, 3);
            assert_eq!(
                fused.summary.metrics.policy_drops,
                if privacy { 3 } else { 0 }
            );
            let summary = serde_json::to_value(&fused.summary).unwrap();
            assert!(summary.get("records").is_none());
        }
    }
}

#[test]
fn given_uncompressed_bytes_when_encoded_and_decoded_then_owned_buffer_moves_and_decode_borrows() {
    let bytes = vec![b'x'; 8192];
    let allocation = bytes.as_ptr();
    let encoded = encode_object_bytes(bytes, RawBackupCompression::None);
    assert_eq!(encoded.as_ptr(), allocation);
    let decoded = decode_object_bytes(&encoded, RawBackupCompression::None).unwrap();
    assert!(matches!(decoded, Cow::Borrowed(_)));
    assert_eq!(decoded.as_ptr(), allocation);
    assert_eq!(decoded.len(), 8192);
}

#[test]
fn given_corrupt_truncated_or_invalid_objects_when_replay_requested_then_error_exposes_no_records()
{
    let mut failures = Vec::new();
    for case in [
        "checksum",
        "truncated",
        "invalid_record",
        "invalid_compression",
        "invalid_index",
    ] {
        let compression = if case == "invalid_compression" {
            RawBackupCompression::RunLength
        } else {
            RawBackupCompression::None
        };
        let fixture = ReplayFixture::new(1, compression, false);
        let mut index = fixture
            .store
            .read_index(&fixture.request.object_id)
            .unwrap();
        let bytes = match case {
            "invalid_record" => b"{\"table_name\":\"\",\"record_key\":\"key\",\"source_position\":null,\"record\":{\"Keys\":{},\"SequenceNumber\":\"1\",\"OldImage\":null,\"NewImage\":null}}\n".to_vec(),
            "invalid_compression" => vec![1],
            "truncated" => b"{\"table_name\":\"entries\"".to_vec(),
            _ => b"corrupt".to_vec(),
        };
        if case != "checksum" {
            index.checksum = checksum_hex(&bytes);
        }
        if case == "invalid_index" {
            index.object_path = fixture
                .root
                .path()
                .join("outside.jsonl")
                .display()
                .to_string();
        }
        fs::write(fixture.store.object_path(&index.object_id), bytes).unwrap();
        fs::write(
            fixture.store.index_path(&index.object_id),
            serde_json::to_vec(&index).unwrap(),
        )
        .unwrap();
        let (result, reads) = count_object_reads(|| fixture.request.execute(&fixture.store));
        let expected_reads = if case == "invalid_index" { 0 } else { 1 };
        match result {
            Err(error) => {
                let correct_error = match case {
                    "checksum" => matches!(error, RawBackupError::ChecksumMismatch { .. }),
                    "truncated" => matches!(error, RawBackupError::Json(_)),
                    "invalid_record" => matches!(error, RawBackupError::EmptyTableName),
                    "invalid_compression" => matches!(error, RawBackupError::Compression(_)),
                    _ => matches!(error, RawBackupError::ObjectStore(_)),
                };
                if !correct_error || reads != expected_reads {
                    failures.push(format!(
                        "{case}: {error:?}, reads={reads}, expected_reads={expected_reads}"
                    ));
                }
            }
            Ok(output) => {
                failures.push(format!("{case}: exposed {} records", output.records.len()))
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
