use std::{fs, io::Write};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use config::{AnalyticsLambdaResponseCompression, RootConfig};
use flate2::{Compression, write::GzEncoder};
use serde::Serialize;
use serde_json::value::RawValue;

use super::{AnalyticsLambdaError, LambdaResponseConfig};

pub(super) fn encode_limited_query_response(
    value: impl Serialize,
    config: LambdaResponseConfig,
) -> Result<Box<RawValue>, AnalyticsLambdaError> {
    let Some(limit_bytes) = config.max_response_size_kb.map(kib_to_bytes) else {
        return serialize_response(value);
    };
    let raw = serde_json::to_vec(&value)?;
    if raw.len() <= limit_bytes {
        return raw_json_from_bytes(raw);
    }
    if config.response_compression != AnalyticsLambdaResponseCompression::GzipBase64 {
        return Err(AnalyticsLambdaError::ResponseTooLarge {
            actual_bytes: raw.len(),
            limit_bytes,
        });
    }
    let compressed = gzip_bytes(raw.as_slice())?;
    let payload = BASE64.encode(compressed);
    let envelope = CompressedQueryResponse {
        encoding: "gzip+base64",
        payload,
    };
    let envelope = serde_json::to_vec(&envelope)?;
    let envelope_size = envelope.len();
    if envelope_size > limit_bytes {
        return Err(AnalyticsLambdaError::ResponseTooLarge {
            actual_bytes: envelope_size,
            limit_bytes,
        });
    }
    raw_json_from_bytes(envelope)
}

#[derive(Serialize)]
struct CompressedQueryResponse {
    encoding: &'static str,
    payload: String,
}

pub(super) fn serialize_response<T: Serialize>(
    value: T,
) -> Result<Box<RawValue>, AnalyticsLambdaError> {
    raw_json_from_bytes(serde_json::to_vec(&value)?)
}

fn raw_json_from_bytes(bytes: Vec<u8>) -> Result<Box<RawValue>, AnalyticsLambdaError> {
    let json = String::from_utf8(bytes).expect("serde_json output is UTF-8");
    // SAFETY: `json` was produced by serde_json, so it is one valid JSON value
    // with no surrounding whitespace.
    Ok(unsafe { RawValue::from_string_unchecked(json) })
}

fn gzip_bytes(bytes: &[u8]) -> Result<Vec<u8>, AnalyticsLambdaError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

fn kib_to_bytes(kib: usize) -> usize {
    kib.saturating_mul(1024)
}

pub(super) fn load_privacy_policy(
    root: &RootConfig,
) -> Result<Option<PrivacyPolicy>, AnalyticsLambdaError> {
    let Some(path) = root.analytics.privacy.policy_path.as_deref() else {
        return Ok(None);
    };
    let policy: PrivacyPolicy = serde_json::from_str(fs::read_to_string(path)?.as_str())?;
    policy.validate()?;
    Ok(Some(policy))
}

use analytics_contract::PrivacyPolicy;

impl super::AnalyticsLambdaHandler {
    pub(super) fn query_response<T: Serialize>(
        &self,
        response: T,
    ) -> Result<Box<RawValue>, AnalyticsLambdaError> {
        encode_limited_query_response(response, self.response_config)
    }
}
