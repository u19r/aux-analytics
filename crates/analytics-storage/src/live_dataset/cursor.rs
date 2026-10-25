use analytics_contract::DatasetQuery;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{LiveDatasetError, catalog::StoredDataset};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DatasetCursor {
    pub tenant: String,
    pub dataset_id: String,
    pub version: u64,
    pub fingerprint: String,
    pub watermark: String,
    pub source_cursor: String,
}

impl DatasetCursor {
    pub(super) fn encode(&self, secret: &str) -> Result<String, LiveDatasetError> {
        let bytes = serde_json::to_vec(self).map_err(|_| LiveDatasetError::Storage)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .map_err(|_| LiveDatasetError::Storage)?;
        mac.update(&bytes);
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&bytes),
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        ))
    }

    pub(super) fn decode(
        encoded: &str,
        stored: &StoredDataset,
        fingerprint: &str,
    ) -> Result<Self, LiveDatasetError> {
        if encoded.len() > 16384 {
            return Err(LiveDatasetError::InvalidQuery);
        }
        let (payload, signature) = encoded
            .split_once('.')
            .ok_or(LiveDatasetError::InvalidQuery)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| LiveDatasetError::InvalidQuery)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| LiveDatasetError::InvalidQuery)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(stored.cursor_secret.as_bytes())
            .map_err(|_| LiveDatasetError::Storage)?;
        mac.update(&bytes);
        mac.verify_slice(&signature)
            .map_err(|_| LiveDatasetError::InvalidQuery)?;
        let cursor: Self =
            serde_json::from_slice(&bytes).map_err(|_| LiveDatasetError::InvalidQuery)?;
        if cursor.tenant != stored.dataset.tenant
            || cursor.dataset_id != stored.dataset.dataset_id
            || cursor.version != stored.dataset.version
            || cursor.fingerprint != fingerprint
        {
            return Err(LiveDatasetError::InvalidQuery);
        }
        Ok(cursor)
    }
}

pub(super) fn fingerprint_query(request: &DatasetQuery) -> Result<String, LiveDatasetError> {
    let mut query = request.clone();
    query.cursor = None;
    let bytes = serde_json::to_vec(&query).map_err(|_| LiveDatasetError::InvalidQuery)?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}
