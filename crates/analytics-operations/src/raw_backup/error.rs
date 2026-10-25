use std::fmt;

use thiserror::Error;

use super::RawBackupObjectId;
pub use crate::constants::RAW_BACKUP_SCHEMA_VERSION;
use crate::operation_contract::OperationTypeError;

#[derive(Debug, Error)]
pub enum RawBackupError {
    #[error(transparent)]
    OperationType(#[from] OperationTypeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    PrivacyPolicy(#[from] analytics_contract::PrivacyPolicyError),
    #[error("raw backup object id cannot be empty")]
    EmptyObjectId,
    #[error("raw backup object id contains an unsafe path component")]
    UnsafeObjectId,
    #[error("raw backup table name cannot be empty")]
    EmptyTableName,
    #[error("raw backup source identity cannot be empty")]
    EmptySourceIdentity,
    #[error("raw backup record key cannot be empty")]
    EmptyRecordKey,
    #[error("raw backup object must contain at least one record")]
    EmptyObject,
    #[error("raw backup encryption key id cannot be empty")]
    EmptyEncryptionKeyId,
    #[error("raw backup compression failed: {0}")]
    Compression(String),
    #[error("raw backup checksum mismatch for {object_id}: expected {expected}, actual {actual}")]
    ChecksumMismatch {
        object_id: RawBackupObjectId,
        expected: String,
        actual: String,
    },
    #[error("raw backup schema version {actual} is not supported; expected {expected}")]
    UnsupportedSchemaVersion { expected: u16, actual: u16 },
    #[error("raw backup object already exists: {0}")]
    ObjectAlreadyExists(String),
    #[error("raw backup object store error: {0}")]
    ObjectStore(String),
    #[error("raw backup destination {destination} does not support atomic maintenance fencing")]
    DestinationFenceUnavailable {
        destination: RawBackupDestinationKind,
    },
    #[error("raw backup index object id {actual} does not match requested object id {requested}")]
    ObjectIdMismatch {
        requested: RawBackupObjectId,
        actual: RawBackupObjectId,
    },
    #[error("raw backup store root must be an absolute path")]
    RelativeStoreRoot,
    #[error("raw backup store root cannot be represented as a stable identity")]
    NonUtf8StoreRoot,
    #[error("raw backup cleanup progress integrity check failed")]
    CleanupProgressIntegrity,
    #[error("privacy policy denied a raw backup record identity")]
    PrivacyIdentityDenied,
}

pub type RawBackupResult<T> = Result<T, RawBackupError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawBackupDestinationKind {
    Filesystem,
    Generic,
}

impl fmt::Display for RawBackupDestinationKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Filesystem => "filesystem",
            Self::Generic => "generic raw-backup store",
        };
        formatter.write_str(name)
    }
}
