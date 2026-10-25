use std::fs;
#[cfg(test)]
use std::sync::atomic::Ordering;

use super::{
    FilesystemRawBackupStore, RawBackupCleanupAuthority, RawBackupCleanupMetadata,
    RawBackupCleanupProgress, RawBackupCleanupProgressEnvelope, RawBackupError, RawBackupObjectId,
    RawBackupObjectIndex, RawBackupResult, checksum_hex, create_or_verify_marker, path_exists,
    remove_file_if_present, same_raw_backup_fingerprint, verify_object_bytes, write_or_verify_file,
};
#[cfg(test)]
use super::{RawBackupRecord, RawBackupWriteReport, RawBackupWriteRequest};
use crate::operation_contract::OperationId;

impl FilesystemRawBackupStore {
    /// Delete an object/index pair while treating an already-completed cleanup
    /// as success. Privacy-fix retries use this idempotent boundary after a
    /// persisted cleanup journal.
    pub(crate) fn delete_object_if_present(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<()> {
        self.canonical_root()?;
        let (object_path, index_path) = self.validate_index_paths(index)?;
        remove_file_if_present(object_path.as_path())?;
        remove_file_if_present(index_path.as_path())?;
        remove_file_if_present(self.publication_marker_path(&index.object_id).as_path())?;
        Ok(())
    }

    pub(crate) fn delete_object_id_if_present(
        &self,
        object_id: &RawBackupObjectId,
    ) -> RawBackupResult<()> {
        self.canonical_root()?;
        remove_file_if_present(self.object_path(object_id).as_path())?;
        remove_file_if_present(self.index_path(object_id).as_path())?;
        remove_file_if_present(self.publication_marker_path(object_id).as_path())?;
        Ok(())
    }

    /// Return whether either durable source file remains for an object. A
    /// cleanup retry may treat a missing source as an already-completed delete
    /// only when both the object and its index are gone; a lone object must
    /// never be deleted without the committed index fingerprint.
    pub(crate) fn object_parts_present(
        &self,
        object_id: &RawBackupObjectId,
    ) -> RawBackupResult<(bool, bool)> {
        self.canonical_root()?;
        Ok((
            path_exists(self.object_path(object_id).as_path())?
                || path_exists(self.publication_marker_path(object_id).as_path())?,
            path_exists(self.index_path(object_id).as_path())?,
        ))
    }

    #[cfg(test)]
    pub(crate) fn begin_cleanup_progress(
        &self,
        operation_id: &OperationId,
        object_ids: &[RawBackupObjectId],
    ) -> RawBackupResult<RawBackupCleanupProgress> {
        self.begin_cleanup_progress_with_metadata(operation_id, object_ids, None)
    }

    #[cfg(test)]
    pub(crate) fn begin_cleanup_progress_with_metadata(
        &self,
        operation_id: &OperationId,
        object_ids: &[RawBackupObjectId],
        metadata: Option<RawBackupCleanupMetadata>,
    ) -> RawBackupResult<RawBackupCleanupProgress> {
        if let Some(progress) = self.load_cleanup_progress(operation_id)? {
            return Ok(progress);
        }
        let progress = RawBackupCleanupProgress {
            operation_id: operation_id.clone(),
            pending_object_ids: object_ids.to_vec(),
            completed_object_ids: Vec::new(),
            claimed_object_ids: Vec::new(),
            metadata,
            authority: None,
        };
        self.save_cleanup_progress(&progress)?;
        Ok(progress)
    }

    pub(crate) fn begin_cleanup_progress_with_metadata_and_authority(
        &self,
        operation_id: &OperationId,
        object_ids: &[RawBackupObjectId],
        metadata: Option<RawBackupCleanupMetadata>,
        authority: RawBackupCleanupAuthority,
    ) -> RawBackupResult<RawBackupCleanupProgress> {
        if authority.object_ids.as_slice() != object_ids
            || authority.expected_source_indexes.len() != object_ids.len()
            || authority
                .expected_source_indexes
                .iter()
                .zip(object_ids)
                .any(|(index, object_id)| &index.object_id != object_id)
        {
            return Err(RawBackupError::CleanupProgressIntegrity);
        }
        if let Some(progress) = self.load_cleanup_progress(operation_id)? {
            return Ok(progress);
        }
        let progress = RawBackupCleanupProgress {
            operation_id: operation_id.clone(),
            pending_object_ids: object_ids.to_vec(),
            completed_object_ids: Vec::new(),
            claimed_object_ids: Vec::new(),
            metadata,
            authority: Some(authority),
        };
        self.save_cleanup_progress(&progress)?;
        Ok(progress)
    }

    pub(crate) fn load_cleanup_progress(
        &self,
        operation_id: &OperationId,
    ) -> RawBackupResult<Option<RawBackupCleanupProgress>> {
        self.validate_root()?;
        let path = self.cleanup_progress_path(operation_id);
        match fs::read(path) {
            Ok(bytes) => {
                let envelope: RawBackupCleanupProgressEnvelope =
                    serde_json::from_slice(bytes.as_slice())?;
                let progress_bytes = serde_json::to_vec(&envelope.progress)?;
                if envelope.checksum != checksum_hex(progress_bytes.as_slice()) {
                    return Err(RawBackupError::CleanupProgressIntegrity);
                }
                Ok(Some(envelope.progress))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn save_cleanup_progress(
        &self,
        progress: &RawBackupCleanupProgress,
    ) -> RawBackupResult<()> {
        self.canonical_root()?;
        #[cfg(test)]
        if progress.claimed_object_ids.is_empty()
            && !progress.completed_object_ids.is_empty()
            && self
                .fail_cleanup_progress_save_after_delete
                .swap(false, Ordering::SeqCst)
        {
            return Err(RawBackupError::Io(std::io::Error::other(
                "injected cleanup progress save failure after delete",
            )));
        }
        fs::create_dir_all(self.root.as_path())?;
        let path = self.cleanup_progress_path(&progress.operation_id);
        let progress_bytes = serde_json::to_vec(progress)?;
        let envelope = RawBackupCleanupProgressEnvelope {
            progress: progress.clone(),
            checksum: checksum_hex(progress_bytes.as_slice()),
        };
        let bytes = serde_json::to_vec_pretty(&envelope)?;
        let temporary = path.with_extension("json.tmp");
        fs::write(temporary.as_path(), bytes)?;
        fs::rename(temporary.as_path(), path.as_path())?;
        Ok(())
    }

    pub(crate) fn clear_cleanup_progress(&self, operation_id: &OperationId) -> RawBackupResult<()> {
        self.canonical_root()?;
        remove_file_if_present(self.cleanup_progress_path(operation_id).as_path())
    }

    #[cfg(test)]
    pub(crate) fn fail_next_cleanup_progress_save_after_delete(&self) {
        self.fail_cleanup_progress_save_after_delete
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn quarantine_object(
        &self,
        index: &RawBackupObjectIndex,
        quarantine_store: &FilesystemRawBackupStore,
    ) -> RawBackupResult<RawBackupObjectIndex> {
        self.canonical_root()?;
        quarantine_store.canonical_root()?;
        let persisted_index = self.read_index(&index.object_id)?;
        if persisted_index != *index {
            return Err(RawBackupError::ObjectStore(format!(
                "raw backup source index differs for {}",
                index.object_id
            )));
        }
        let quarantine_object_id =
            RawBackupObjectId::try_new(format!("quarantine_{}", index.object_id.as_str()))?;
        let source_object_path = self.validate_index_paths(index)?.0;
        fs::create_dir_all(quarantine_store.root.as_path())?;
        let object_path = quarantine_store.object_path(&quarantine_object_id);
        let index_path = quarantine_store.index_path(&quarantine_object_id);
        if object_path.is_file() && index_path.is_file() {
            let existing = quarantine_store.read_index(&quarantine_object_id)?;
            if !same_raw_backup_fingerprint(&existing, index) {
                return Err(RawBackupError::ObjectStore(format!(
                    "quarantine publication differs for {}",
                    index.object_id
                )));
            }
            let verification = quarantine_store.verify_object_report(&existing)?;
            if !verification.checksum_valid {
                return Err(RawBackupError::ChecksumMismatch {
                    object_id: quarantine_object_id,
                    expected: verification.expected_checksum,
                    actual: verification.actual_checksum,
                });
            }
            remove_file_if_present(
                quarantine_store
                    .publication_marker_path(&existing.object_id)
                    .as_path(),
            )?;
            return Ok(existing);
        }
        let source_bytes = fs::read(source_object_path)?;
        let verification = verify_object_bytes(index, source_bytes.as_slice())?;
        if !verification.checksum_valid {
            return Err(RawBackupError::ChecksumMismatch {
                object_id: index.object_id.clone(),
                expected: verification.expected_checksum,
                actual: verification.actual_checksum,
            });
        }
        if verification.byte_count != index.byte_count {
            return Err(RawBackupError::ObjectStore(format!(
                "raw backup object byte count differs for {}",
                index.object_id
            )));
        }
        let mut quarantine_index = index.clone();
        quarantine_index.object_id = quarantine_object_id;
        quarantine_index.object_path = object_path.to_string_lossy().into_owned();
        quarantine_index.index_path = index_path.to_string_lossy().into_owned();
        let index_bytes = serde_json::to_vec_pretty(&quarantine_index)?;
        let marker_bytes = serde_json::to_vec(&quarantine_index)?;
        let marker_path = quarantine_store.publication_marker_path(&quarantine_index.object_id);
        create_or_verify_marker(marker_path.as_path(), marker_bytes.as_slice())?;
        write_or_verify_file(object_path.as_path(), source_bytes.as_slice())?;
        write_or_verify_file(index_path.as_path(), index_bytes.as_slice())?;
        remove_file_if_present(marker_path.as_path())?;
        Ok(quarantine_index)
    }

    #[cfg(test)]
    pub(crate) fn compact_objects(
        &self,
        operation_id: OperationId,
        object_id: RawBackupObjectId,
        source_identity: impl Into<String>,
        privacy_policy_version: Option<String>,
        indexes: &[RawBackupObjectIndex],
    ) -> RawBackupResult<RawBackupWriteReport> {
        let mut records = Vec::new();
        for index in indexes {
            records.extend(
                self.replay_object(index)?
                    .into_iter()
                    .map(|record| RawBackupRecord {
                        table_name: record.table_name,
                        record_key: String::from_utf8_lossy(record.record_key.as_slice())
                            .into_owned(),
                        source_position: record.source_position,
                        record: record.record,
                    }),
            );
        }
        let source_ids = indexes
            .iter()
            .map(|index| index.object_id.clone())
            .collect();
        let request = RawBackupWriteRequest::try_new(
            operation_id,
            object_id,
            source_identity,
            privacy_policy_version,
            records,
        )?
        .compacted_from(source_ids);
        self.write_object_report(&request)
    }
}
