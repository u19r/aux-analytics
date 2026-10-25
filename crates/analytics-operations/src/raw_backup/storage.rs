#[cfg(test)]
use std::sync::{Arc, atomic::AtomicBool};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

use analytics_contract::PrivacyPolicy;

use super::{
    RawBackupDestinationKind, RawBackupError, RawBackupMetrics, RawBackupObjectId,
    RawBackupObjectIndex, RawBackupObjectStore, RawBackupReplayRecord, RawBackupReplayReport,
    RawBackupResult, RawBackupVerifyReport, RawBackupWriteReport, RawBackupWriteRequest,
    apply_raw_backup_replay_privacy_policy, checksum_hex, compression_ratio_per_mille,
    create_or_verify_marker, encode_records, hex_component, path_matches_store_path,
    remove_file_if_present, replay_verified_bytes, verify_object_bytes, write_or_verify_file,
};
use crate::operation_contract::OperationId;

#[derive(Debug, Clone)]
pub struct FilesystemRawBackupStore {
    pub(super) root: PathBuf,
    #[cfg(test)]
    pub(super) fail_cleanup_progress_save_after_delete: Arc<AtomicBool>,
}

impl PartialEq for FilesystemRawBackupStore {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
    }
}

impl Eq for FilesystemRawBackupStore {}

impl FilesystemRawBackupStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            #[cfg(test)]
            fail_cleanup_progress_save_after_delete: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Resolve this store to the filesystem identity that will be used by a
    /// cleanup authority. The root is created before resolution so a new
    /// destination is handled without falling back to an unresolved textual
    /// path. Relative roots are rejected rather than being interpreted
    /// against whichever process working directory happens to be active.
    pub(crate) fn canonical_root(&self) -> RawBackupResult<PathBuf> {
        self.validate_root()?;
        fs::create_dir_all(self.root.as_path())?;
        Ok(fs::canonicalize(self.root.as_path())?)
    }

    /// Resolve the filesystem identity without creating a destination root.
    /// Existing roots use the kernel-resolved path; absent roots retain their
    /// validated absolute spelling for overlap checks and read-only planning.
    pub(crate) fn canonical_root_read_only(&self) -> RawBackupResult<PathBuf> {
        self.validate_root()?;
        match fs::canonicalize(self.root.as_path()) {
            Ok(root) => Ok(root),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(self.root.clone()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn validate_root(&self) -> RawBackupResult<()> {
        if !self.root.is_absolute() {
            return Err(RawBackupError::RelativeStoreRoot);
        }
        Ok(())
    }

    /// Bind a filesystem store to its resolved root for a multi-step operation.
    /// The returned handle never performs I/O through a caller-supplied alias,
    /// while preserving the shared test failure controls used by cleanup tests.
    pub(crate) fn canonicalized(&self) -> RawBackupResult<Self> {
        Ok(Self {
            root: self.canonical_root()?,
            #[cfg(test)]
            fail_cleanup_progress_save_after_delete: Arc::clone(
                &self.fail_cleanup_progress_save_after_delete,
            ),
        })
    }

    #[cfg(test)]
    pub(crate) fn write_object(
        &self,
        request: &RawBackupWriteRequest,
    ) -> RawBackupResult<RawBackupObjectIndex> {
        Ok(self.write_object_report(request)?.index)
    }

    pub(crate) fn write_object_report(
        &self,
        request: &RawBackupWriteRequest,
    ) -> RawBackupResult<RawBackupWriteReport> {
        request.validate()?;
        self.canonical_root()?;
        fs::create_dir_all(self.root.as_path())?;
        let object_path = self.object_path(&request.object_id);
        let index_path = self.index_path(&request.object_id);
        let encoded = encode_records(request)?;
        let checksum = checksum_hex(encoded.bytes.as_slice());
        let index = RawBackupObjectIndex {
            object_id: request.object_id.clone(),
            operation_id: request.operation_id.clone(),
            source_identity: request.source_identity.clone(),
            object_path: object_path.to_string_lossy().into_owned(),
            index_path: index_path.to_string_lossy().into_owned(),
            layout: request.layout.clone(),
            record_count: request.records.len() as u64,
            byte_count: encoded.bytes.len() as u64,
            uncompressed_byte_count: encoded.uncompressed_len as u64,
            checksum,
            privacy_policy_version: request.privacy_policy_version.clone(),
            compacted_from_object_ids: request.compacted_from_object_ids.clone(),
        };
        let index_bytes = serde_json::to_vec_pretty(&index)?;
        let marker_bytes = serde_json::to_vec(&index)?;
        let marker_path = self.publication_marker_path(&request.object_id);
        create_or_verify_marker(marker_path.as_path(), marker_bytes.as_slice())?;
        write_or_verify_file(object_path.as_path(), encoded.bytes.as_slice())?;
        write_or_verify_file(index_path.as_path(), index_bytes.as_slice())?;
        remove_file_if_present(marker_path.as_path())?;
        Ok(RawBackupWriteReport {
            metrics: RawBackupMetrics {
                bytes_written: encoded.bytes.len() as u64,
                records_written: request.records.len() as u64,
                compression_ratio_per_mille: compression_ratio_per_mille(
                    encoded.bytes.len(),
                    encoded.uncompressed_len,
                ),
                policy_drops: request.policy_dropped_records,
                ..RawBackupMetrics::default()
            },
            index,
        })
    }

    /// Publish a fixture object from a test-support consumer.
    ///
    /// Raw-backup publication has no accepted external destination fence, so
    /// the writer is intentionally unavailable from the default public API.
    /// This feature-gated seam exists only for local fixture preparation.
    #[cfg(any(test, feature = "test-support"))]
    pub fn write_fixture_object(
        &self,
        request: &RawBackupWriteRequest,
    ) -> RawBackupResult<RawBackupObjectIndex> {
        Ok(self.write_object_report(request)?.index)
    }

    /// Publish a fixture object and return its publication report.
    #[cfg(any(test, feature = "test-support"))]
    pub fn write_fixture_object_report(
        &self,
        request: &RawBackupWriteRequest,
    ) -> RawBackupResult<RawBackupWriteReport> {
        self.write_object_report(request)
    }

    pub fn replay_object(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<Vec<RawBackupReplayRecord>> {
        Ok(self.replay_object_report(index)?.records)
    }

    pub fn replay_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupReplayReport> {
        Ok(self.replay_verified_object(index)?.1)
    }

    pub fn replay_verified_object(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<(RawBackupVerifyReport, RawBackupReplayReport)> {
        let (bytes, verification) = self.read_verified_object(index)?;
        if !verification.checksum_valid {
            return Err(RawBackupError::ChecksumMismatch {
                object_id: index.object_id.clone(),
                expected: verification.expected_checksum,
                actual: verification.actual_checksum,
            });
        }
        let replay = replay_verified_bytes(index, &bytes)?;
        Ok((verification, replay))
    }

    pub fn replay_object_report_with_privacy_policy(
        &self,
        index: &RawBackupObjectIndex,
        policy: &PrivacyPolicy,
    ) -> RawBackupResult<RawBackupReplayReport> {
        apply_raw_backup_replay_privacy_policy(self.replay_object_report(index)?, policy)
    }

    pub fn verify_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupVerifyReport> {
        Ok(self.read_verified_object(index)?.1)
    }

    pub fn read_index(
        &self,
        object_id: &RawBackupObjectId,
    ) -> RawBackupResult<RawBackupObjectIndex> {
        self.validate_root()?;
        let index: RawBackupObjectIndex =
            serde_json::from_slice(fs::read(self.index_path(object_id))?.as_slice())?;
        index.validate_requested_object_id(object_id)?;
        index.validate_schema_version()?;
        Ok(index)
    }

    pub(super) fn object_path(&self, object_id: &RawBackupObjectId) -> PathBuf {
        self.root.join(format!("{}.jsonl", object_id.as_str()))
    }

    pub(super) fn index_path(&self, object_id: &RawBackupObjectId) -> PathBuf {
        self.root.join(format!("{}.index.json", object_id.as_str()))
    }

    pub(super) fn publication_marker_path(&self, object_id: &RawBackupObjectId) -> PathBuf {
        self.root
            .join(format!(".{}.publish.json", object_id.as_str()))
    }

    pub(super) fn cleanup_progress_path(&self, operation_id: &OperationId) -> PathBuf {
        self.root.join(format!(
            ".privacy-cleanup-{}.json",
            hex_component(operation_id.as_str())
        ))
    }

    pub(super) fn validate_index_paths(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<(PathBuf, PathBuf)> {
        let object_path = self.object_path(&index.object_id);
        let index_path = self.index_path(&index.object_id);
        if !path_matches_store_path(Path::new(index.object_path.as_str()), object_path.as_path())
            || !path_matches_store_path(Path::new(index.index_path.as_str()), index_path.as_path())
        {
            return Err(RawBackupError::ObjectStore(format!(
                "raw backup index paths do not match object {}",
                index.object_id
            )));
        }
        Ok((object_path, index_path))
    }

    fn read_verified_object(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<(Vec<u8>, RawBackupVerifyReport)> {
        self.validate_root()?;
        index.validate_schema_version()?;
        let (object_path, _) = self.validate_index_paths(index)?;
        #[cfg(test)]
        super::test_support::record_object_read();
        let bytes = fs::read(object_path)?;
        let report = verify_object_bytes(index, bytes.as_slice())?;
        Ok((bytes, report))
    }
}

impl RawBackupObjectStore for FilesystemRawBackupStore {
    fn destination_kind(&self) -> RawBackupDestinationKind {
        RawBackupDestinationKind::Filesystem
    }

    fn replay_verified_object(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<(RawBackupVerifyReport, RawBackupReplayReport)> {
        FilesystemRawBackupStore::replay_verified_object(self, index)
    }

    fn verify_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupVerifyReport> {
        FilesystemRawBackupStore::verify_object_report(self, index)
    }

    fn read_index(&self, object_id: &RawBackupObjectId) -> RawBackupResult<RawBackupObjectIndex> {
        FilesystemRawBackupStore::read_index(self, object_id)
    }
}
