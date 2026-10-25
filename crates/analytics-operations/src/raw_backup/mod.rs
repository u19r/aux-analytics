mod checksum;
mod cleanup;
mod cleanup_store;
mod codec;
mod error;
mod objects;
mod replay;
mod storage;
mod verification;

pub(crate) use checksum::checksum_hex;
pub(crate) use cleanup::RawBackupCleanupProgressEnvelope;
pub use cleanup::{RawBackupCleanupAuthority, RawBackupCleanupMetadata, RawBackupCleanupProgress};
use codec::{compression_ratio_per_mille, decode_object_bytes, encode_object_bytes};
pub use error::{
    RAW_BACKUP_SCHEMA_VERSION, RawBackupDestinationKind, RawBackupError, RawBackupResult,
};
pub use objects::{
    RawBackupChecksumAlgorithm, RawBackupCompression, RawBackupEncryption, RawBackupMetrics,
    RawBackupObjectId, RawBackupObjectIndex, RawBackupObjectLayout, RawBackupRecord,
    RawBackupReplayRecord, RawBackupWriteRequest,
};
pub use replay::{
    RawBackupObjectStore, RawBackupReplayExecution, RawBackupReplayReport, RawBackupReplayRequest,
    RawBackupReplaySummary, RawBackupVerifyReport, RawBackupWriteReport,
};
pub use storage::FilesystemRawBackupStore;
pub(crate) use verification::{
    append_sanitized_object_id_part, apply_raw_backup_replay_privacy_policy,
    create_or_verify_marker, encode_records, filter_raw_backup_record, hex_component, path_exists,
    path_matches_store_path, remove_file_if_present, replay_verified_bytes,
    same_raw_backup_fingerprint, verify_object_bytes, write_or_verify_file,
};

#[cfg(test)]
mod replay_performance_tests;
#[cfg(test)]
mod storage_tests;
#[cfg(test)]
mod test_support;
