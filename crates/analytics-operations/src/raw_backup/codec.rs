use std::borrow::Cow;

use super::{RawBackupCompression, RawBackupError, RawBackupResult};

pub(super) fn encode_object_bytes(bytes: Vec<u8>, compression: RawBackupCompression) -> Vec<u8> {
    match compression {
        RawBackupCompression::None => bytes,
        RawBackupCompression::RunLength => run_length_encode(&bytes),
    }
}

pub(super) fn decode_object_bytes(
    bytes: &[u8],
    compression: RawBackupCompression,
) -> RawBackupResult<Cow<'_, [u8]>> {
    match compression {
        RawBackupCompression::None => Ok(Cow::Borrowed(bytes)),
        RawBackupCompression::RunLength => run_length_decode(bytes).map(Cow::Owned),
    }
}

pub(super) fn compression_ratio_per_mille(encoded_len: usize, uncompressed_len: usize) -> u64 {
    if uncompressed_len == 0 {
        return 1000;
    }
    ((encoded_len as u64) * 1000) / (uncompressed_len as u64)
}

fn run_length_encode(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    let mut index = 0;
    while let Some(byte) = bytes.get(index).copied() {
        let mut count = 1_u8;
        while count < u8::MAX && bytes.get(index + usize::from(count)).copied() == Some(byte) {
            count += 1;
        }
        encoded.push(count);
        encoded.push(byte);
        index += usize::from(count);
    }
    encoded
}

fn run_length_decode(bytes: &[u8]) -> RawBackupResult<Vec<u8>> {
    if !bytes.len().is_multiple_of(2) {
        return Err(RawBackupError::Compression(
            "run-length data has an odd byte count".to_string(),
        ));
    }
    let mut decoded = Vec::new();
    for pair in bytes.as_chunks::<2>().0 {
        decoded.extend(std::iter::repeat_n(pair[1], usize::from(pair[0])));
    }
    Ok(decoded)
}
