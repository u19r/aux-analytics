use std::io::{self, Write};

use bg_jobs::MAX_INLINE_PAYLOAD_BYTES;

use super::SourceJobPayload;
use crate::{
    background_jobs::error::BackgroundJobError,
    constants::{MAX_SOURCE_DECODED_BYTES, SOURCE_PAYLOAD_HEADER},
};

pub(super) fn encode_payload(payload: &SourceJobPayload) -> Result<Vec<u8>, BackgroundJobError> {
    let mut json = PayloadBuffer {
        bytes: Vec::new(),
        exceeded: None,
    };
    if serde_json::to_writer(&mut json, payload).is_err() {
        return Err(match json.exceeded {
            Some(actual) => BackgroundJobError::PayloadTooLarge {
                actual,
                maximum: MAX_SOURCE_DECODED_BYTES,
            },
            None => BackgroundJobError::InvalidPayload("source job payload cannot be encoded"),
        });
    }
    let mut encoded = vec![0; 8 + lz4_flex::block::get_maximum_output_size(json.bytes.len())];
    encoded[..4].copy_from_slice(SOURCE_PAYLOAD_HEADER);
    encoded[4..8].copy_from_slice(&(json.bytes.len() as u32).to_be_bytes());
    let length = lz4_flex::block::compress_into(&json.bytes, &mut encoded[8..]).map_err(|_| {
        BackgroundJobError::InvalidPayload("source job payload cannot be compressed")
    })?;
    encoded.truncate(8 + length);
    if encoded.len() > MAX_INLINE_PAYLOAD_BYTES {
        return Err(BackgroundJobError::PayloadTooLarge {
            actual: encoded.len(),
            maximum: MAX_INLINE_PAYLOAD_BYTES,
        });
    }
    Ok(encoded)
}

pub(super) fn decode_payload(encoded: &[u8]) -> Result<SourceJobPayload, BackgroundJobError> {
    if encoded.len() > MAX_INLINE_PAYLOAD_BYTES {
        return Err(BackgroundJobError::PayloadTooLarge {
            actual: encoded.len(),
            maximum: MAX_INLINE_PAYLOAD_BYTES,
        });
    }
    if encoded.len() < 8 || &encoded[..4] != SOURCE_PAYLOAD_HEADER {
        return Err(BackgroundJobError::InvalidPayload(
            "source job payload has an invalid envelope",
        ));
    }
    let length = u32::from_be_bytes(encoded[4..8].try_into().expect("checked length")) as usize;
    if length == 0 || length > MAX_SOURCE_DECODED_BYTES {
        return Err(BackgroundJobError::InvalidPayload(
            "source job decoded size is outside its bound",
        ));
    }
    let mut json = vec![0; length];
    let actual = lz4_flex::block::decompress_into(&encoded[8..], &mut json).map_err(|_| {
        BackgroundJobError::InvalidPayload("source job compressed payload is malformed")
    })?;
    if actual != length {
        return Err(BackgroundJobError::InvalidPayload(
            "source job decoded size does not match its envelope",
        ));
    }
    serde_json::from_slice(&json)
        .map_err(|_| BackgroundJobError::InvalidPayload("source job payload is malformed"))
}

struct PayloadBuffer {
    bytes: Vec<u8>,
    exceeded: Option<usize>,
}

impl Write for PayloadBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self.bytes.len().saturating_add(bytes.len());
        if length > MAX_SOURCE_DECODED_BYTES {
            self.exceeded = Some(length);
            return Err(io::Error::other(
                "source job decoded size exceeds its bound",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
