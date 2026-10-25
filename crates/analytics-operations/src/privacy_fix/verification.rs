use super::{PrivacyFixEffectBoundary, PrivacyFixError, PrivacyFixResult, run_effect};
use crate::{
    FilesystemRawBackupStore, OperationChunkKey, RawBackupError, RawBackupObjectId,
    RawBackupObjectIndex,
};

pub(crate) fn verify_clean_output(
    clean_store: &FilesystemRawBackupStore,
    object_id: &RawBackupObjectId,
    policy: &analytics_contract::PrivacyPolicy,
) -> PrivacyFixResult<()> {
    let index = clean_store
        .read_index(object_id)
        .map_err(|_| PrivacyFixError::CleanReplayVerificationFailed(object_id.clone()))?;
    let verification = clean_store
        .verify_object_report(&index)
        .map_err(|_| PrivacyFixError::CleanReplayVerificationFailed(object_id.clone()))?;
    let replay = clean_store
        .replay_object_report_with_privacy_policy(&index, policy)
        .map_err(|_| PrivacyFixError::CleanReplayVerificationFailed(object_id.clone()))?;
    if !verification.checksum_valid
        || replay.metrics.policy_drops != 0
        || replay.records.len() as u64 != index.record_count
    {
        return Err(PrivacyFixError::CleanReplayVerificationFailed(
            object_id.clone(),
        ));
    }
    Ok(())
}

pub(crate) fn verify_source_object_for_cleanup(
    source_store: &FilesystemRawBackupStore,
    index: &RawBackupObjectIndex,
) -> PrivacyFixResult<()> {
    let verification = source_store.verify_object_report(index)?;
    if !verification.checksum_valid {
        return Err(PrivacyFixError::RawBackup(
            RawBackupError::ChecksumMismatch {
                object_id: index.object_id.clone(),
                expected: verification.expected_checksum,
                actual: verification.actual_checksum,
            },
        ));
    }
    if verification.byte_count != index.byte_count {
        return Err(PrivacyFixError::RawBackup(RawBackupError::ObjectStore(
            format!(
                "raw backup object byte count differs for {}",
                index.object_id
            ),
        )));
    }
    Ok(())
}

pub(crate) fn verify_source_index_for_cleanup(
    source_store: &FilesystemRawBackupStore,
    expected_index: &RawBackupObjectIndex,
) -> PrivacyFixResult<()> {
    let persisted_index = source_store.read_index(&expected_index.object_id)?;
    if persisted_index != *expected_index {
        return Err(PrivacyFixError::CleanupAuthorityMismatch);
    }
    verify_source_object_for_cleanup(source_store, expected_index)
}

pub(crate) fn verify_quarantine_output(
    quarantine_store: &FilesystemRawBackupStore,
    source_object_id: &RawBackupObjectId,
    expected_source_index: &RawBackupObjectIndex,
) -> PrivacyFixResult<()> {
    let quarantine_object_id =
        RawBackupObjectId::try_new(format!("quarantine_{}", source_object_id.as_str()))?;
    let index = quarantine_store
        .read_index(&quarantine_object_id)
        .map_err(PrivacyFixError::RawBackup)?;
    if index.object_id != quarantine_object_id
        || index.operation_id != expected_source_index.operation_id
        || index.source_identity != expected_source_index.source_identity
        || index.layout != expected_source_index.layout
        || index.record_count != expected_source_index.record_count
        || index.byte_count != expected_source_index.byte_count
        || index.uncompressed_byte_count != expected_source_index.uncompressed_byte_count
        || index.checksum != expected_source_index.checksum
        || index.privacy_policy_version != expected_source_index.privacy_policy_version
        || index.compacted_from_object_ids != expected_source_index.compacted_from_object_ids
    {
        return Err(PrivacyFixError::CleanupAuthorityMismatch);
    }
    let verification = quarantine_store
        .verify_object_report(&index)
        .map_err(PrivacyFixError::RawBackup)?;
    if !verification.checksum_valid {
        return Err(PrivacyFixError::RawBackup(
            RawBackupError::ChecksumMismatch {
                object_id: quarantine_object_id,
                expected: verification.expected_checksum,
                actual: verification.actual_checksum,
            },
        ));
    }
    Ok(())
}

pub(crate) fn save_cleanup_progress_with_effects(
    source_store: &FilesystemRawBackupStore,
    progress: &crate::RawBackupCleanupProgress,
    effects: &mut impl PrivacyFixEffectBoundary,
    key: OperationChunkKey,
) -> PrivacyFixResult<()> {
    // The effect key is persisted with the active execution generation and
    // owner by the maintenance facade. This keeps a takeover from replaying
    // or overwriting a journal transition through a stale attempt.
    run_effect(effects, key, || {
        source_store
            .save_cleanup_progress(progress)
            .map_err(PrivacyFixError::RawBackup)
    })
}

pub(crate) fn clear_cleanup_progress_with_effects(
    source_store: &FilesystemRawBackupStore,
    progress: &crate::RawBackupCleanupProgress,
    effects: &mut impl PrivacyFixEffectBoundary,
    key: OperationChunkKey,
) -> PrivacyFixResult<()> {
    run_effect(effects, key, || {
        source_store
            .clear_cleanup_progress(&progress.operation_id)
            .map_err(PrivacyFixError::RawBackup)
    })
}
