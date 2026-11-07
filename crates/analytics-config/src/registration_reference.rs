use serde::{Deserialize, Serialize};

use crate::ConfigError;

/// Immutable deployment input. Startup reads one exact version, never latest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationReference {
    pub schema_version: u16,
    pub region: String,
    pub bucket: String,
    pub key: String,
    pub version: String,
    pub sha256: String,
    pub bytes: u64,
    pub source: RegistrationSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationSource {
    pub repository: String,
    pub revision: String,
    pub tree: String,
}

impl TryFrom<&str> for RegistrationReference {
    type Error = ConfigError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let reference: Self = serde_json::from_str(value)
            .map_err(|error| ConfigError::argument(format!("invalid registration reference JSON: {error}")))?;
        let required = [&reference.region, &reference.bucket, &reference.key, &reference.version, &reference.source.repository];
        if reference.schema_version != 1 || reference.bytes == 0 || reference.version == "null"
            || required.iter().any(|value| value.trim().is_empty())
            || !is_hex_digest(&reference.sha256, 64)
            || !is_hex_digest(&reference.source.revision, 40)
            || !is_hex_digest(&reference.source.tree, 40) {
            return Err(ConfigError::argument("registration reference requires schema 1, an exact S3 version, size, checksum and source identity"));
        }
        Ok(reference)
    }
}

fn is_hex_digest(value: &str, length: usize) -> bool {
    value.len() == length && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
