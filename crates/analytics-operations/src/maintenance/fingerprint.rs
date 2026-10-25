use std::io::{self, Write};

use sha2::{Digest, Sha256};

use crate::constants::HEX;

pub(crate) fn calculate_request_fingerprint<T: serde::Serialize>(
    request: &T,
) -> Result<String, serde_json::Error> {
    // Materialize first so object keys use serde_json's canonical ordering;
    // operation identity must remain stable across request field declaration
    // order. The final bytes are streamed directly into the digest below.
    calculate_value_fingerprint(serde_json::to_value(request)?)
}

pub(super) fn calculate_value_fingerprint(
    value: serde_json::Value,
) -> Result<String, serde_json::Error> {
    let mut digest = Sha256::new();
    let mut writer = io::BufWriter::new(Sha256Writer(&mut digest));
    serde_json::to_writer(&mut writer, &value)?;
    writer.flush().map_err(serde_json::Error::io)?;
    drop(writer);
    Ok(encode_request_fingerprint(digest.finalize().as_slice()))
}

struct Sha256Writer<'a>(&'a mut Sha256);

impl Write for Sha256Writer<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn calculate_materialized_fingerprint_baseline<T: serde::Serialize>(
    request: &T,
) -> Result<(String, usize), serde_json::Error> {
    let value = serde_json::to_value(request)?;
    let bytes = serde_json::to_vec(&value)?;
    let digest = Sha256::digest(bytes.as_slice());
    Ok((encode_request_fingerprint(digest.as_slice()), bytes.len()))
}

fn encode_request_fingerprint(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(7 + bytes.len() * 2);
    output.push_str("sha256:");
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
