use analytics_contract::{SourcePosition, StorageItem, StorageValue};
use sha2::{Digest, Sha256};
use storage_types::{AttributeValue as StorageAttributeValue, KeyAttributes};

use super::{AnalyticsEngineError, AnalyticsEngineResult};
use crate::constants::HEX_DIGITS;

pub(super) fn source_key_digest(
    analytics_table_name: &str,
    source_table_name: &str,
    source_position: &SourcePosition,
    record_keys: &StorageItem,
) -> AnalyticsEngineResult<String> {
    let canonical_keys = canonical_source_keys(record_keys)?;
    let scope = source_position_scope(source_position)?;
    let mut digest = Sha256::new();
    digest.update(b"analytics-engine:source-record-owner:v1");
    append_digest_field(&mut digest, analytics_table_name.as_bytes());
    append_digest_field(&mut digest, source_table_name.as_bytes());
    append_digest_field(&mut digest, scope.as_bytes());
    append_digest_field(&mut digest, canonical_keys.as_bytes());
    Ok(hex_digest(&digest.finalize()))
}

fn canonical_source_keys(record_keys: &StorageItem) -> AnalyticsEngineResult<String> {
    if record_keys.is_empty() {
        return Err(AnalyticsEngineError::InvalidSystemSourceKey(
            "keys are empty".to_string(),
        ));
    }
    let mut key = KeyAttributes::with_capacity(record_keys.len());
    for (name, value) in record_keys {
        if name.trim().is_empty() {
            return Err(AnalyticsEngineError::InvalidSystemSourceKey(
                "a key name is empty".to_string(),
            ));
        }
        let value = match value {
            StorageValue::S(value) => StorageAttributeValue::S(value.clone()),
            StorageValue::N(value) => StorageAttributeValue::N(value.clone()),
            StorageValue::B(value) => StorageAttributeValue::B(value.clone()),
            _ => {
                return Err(AnalyticsEngineError::InvalidSystemSourceKey(
                    "keys must use S, N, or B values".to_string(),
                ));
            }
        };
        key.insert(name.to_string(), value);
    }
    key.canonical_dynamo_json()
        .map_err(|error| AnalyticsEngineError::InvalidSystemSourceKey(error.to_string()))
}

fn source_position_scope(source_position: &SourcePosition) -> AnalyticsEngineResult<String> {
    let SourcePosition::SystemStream { source_scope, .. } = source_position else {
        return Err(AnalyticsEngineError::IncompleteSourceEffectState);
    };
    Ok(format!(
        "system:{}:{}:{}",
        source_scope.version(),
        source_scope.scope_id(),
        source_scope.generation()
    ))
}

fn append_digest_field(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}
