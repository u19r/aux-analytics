use aws_config::{BehaviorVersion, retry::RetryConfig};
use aws_sdk_s3::{Client, config::Region, error::ProvideErrorMetadata};
use config::{ConfigRequest, RegistrationReference, RegistrationSource};
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use crate::{
    ApiError, ApiResult,
    constants::{ENV_REGISTRATION_REFERENCE, REGISTRATION_MAX_ATTEMPTS, REGISTRATION_STARTUP_TIMEOUT},
    error::{ApiErrorDebug, ApiErrorKind},
};

/// API and Lambda run this once before configuration resolution and readiness.
/// Local-file consumers retain their normal request when no remote input exists.
pub async fn prepare_runtime_registration(request: ConfigRequest) -> ApiResult<ConfigRequest> {
    let value = match std::env::var(ENV_REGISTRATION_REFERENCE) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(request),
        Err(_) => return Err(reject_registration("reference_not_unicode")),
    };
    let reference = RegistrationReference::try_from(value.as_str())?;
    let sdk = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(reference.region.clone()))
        .retry_config(RetryConfig::standard().with_max_attempts(REGISTRATION_MAX_ATTEMPTS))
        .load().await;
    prepare_registration_request(request, &Client::new(&sdk), &reference).await
}

pub(super) async fn prepare_registration_request(request: ConfigRequest, client: &Client, reference: &RegistrationReference) -> ApiResult<ConfigRequest> {
    // GetObject returns a streaming body. The deadline covers both SDK attempts
    // and body collection so an incomplete response cannot hold readiness open.
    let bytes = tokio::time::timeout(REGISTRATION_STARTUP_TIMEOUT, fetch_registration_object(client, reference))
        .await.map_err(|_| reject_registration("object_read_timeout"))??;
    let document = RegistrationDocument::parse_verified(&bytes, reference)?;
    let request = request.with_manifest_json(document.manifest.get().as_bytes())?;
    Ok(request)
}

async fn fetch_registration_object(client: &Client, reference: &RegistrationReference) -> ApiResult<bytes::Bytes> {
    let response = client.get_object().bucket(&reference.bucket).key(&reference.key)
        .version_id(&reference.version).send().await.map_err(|error| {
            let code = error.as_service_error().and_then(|error| error.code()).unwrap_or("transport_error");
            reject_registration(&format!("get_object:{code}"))
        })?;
    if response.version_id() != Some(reference.version.as_str())
        || response.content_length().and_then(|length| u64::try_from(length).ok()) != Some(reference.bytes) {
        return Err(reject_registration("object_version_or_size_mismatch"));
    }
    Ok(response.body.collect().await.map_err(|_| reject_registration("object_body_read_failed"))?.into_bytes())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrationDocument {
    source: RegistrationSource,
    manifest: Box<RawValue>,
}

impl RegistrationDocument {
    fn parse_verified(bytes: &[u8], reference: &RegistrationReference) -> ApiResult<Self> {
        if bytes.len() as u64 != reference.bytes || hex::encode(Sha256::digest(bytes)) != reference.sha256 {
            return Err(reject_registration("object_checksum_or_size_mismatch"));
        }
        let document: Self = serde_json::from_slice(bytes).map_err(|_| reject_registration("invalid_document_json"))?;
        if document.source != reference.source {
            return Err(reject_registration("document_source_mismatch"));
        }
        Ok(document)
    }
}

fn reject_registration(code: &str) -> ApiError {
    ApiError::with_debug(ApiErrorKind::RuntimeRegistration, ApiErrorDebug::Message(code.into()))
}
