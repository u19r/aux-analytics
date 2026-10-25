use std::path::Path;

use super::{
    PrivacyFixDestructiveAction, PrivacyFixEffectBoundary, PrivacyFixError, PrivacyFixRequest,
    PrivacyFixResult, run_effect,
};
use crate::{
    FilesystemRawBackupStore, OperationChunkKey, OperationId, OperationRateLimiter, RateLimitClock,
    RateLimitUsage, RawBackupCleanupAuthority, RawBackupError, RawBackupObjectId,
    RawBackupObjectIndex, enforce_rate_limit_with_limiter,
};

pub(crate) fn destructive_action_name(action: PrivacyFixDestructiveAction) -> String {
    match action {
        PrivacyFixDestructiveAction::None => "none".to_string(),
        PrivacyFixDestructiveAction::QuarantineTainted => "quarantine_tainted".to_string(),
        PrivacyFixDestructiveAction::DeleteTainted => "delete_tainted".to_string(),
    }
}

pub(crate) struct PrivacyBackupRoots<'a> {
    pub(crate) source: &'a Path,
    pub(crate) clean: &'a Path,
    pub(crate) quarantine: &'a Path,
}
impl PrivacyBackupRoots<'_> {
    pub(crate) fn validate_disjoint(&self) -> PrivacyFixResult<()> {
        if roots_overlap(self.source, self.clean)
            || roots_overlap(self.source, self.quarantine)
            || roots_overlap(self.clean, self.quarantine)
        {
            return Err(PrivacyFixError::OverlappingBackupStores);
        }
        Ok(())
    }
}

pub(crate) fn cleanup_authority(
    request: &PrivacyFixRequest,
    roots: PrivacyBackupRoots<'_>,
    expected_source_indexes: &[RawBackupObjectIndex],
) -> PrivacyFixResult<RawBackupCleanupAuthority> {
    let source_store_root = roots
        .source
        .to_str()
        .ok_or(RawBackupError::NonUtf8StoreRoot)?
        .to_string();
    let clean_store_root = roots
        .clean
        .to_str()
        .ok_or(RawBackupError::NonUtf8StoreRoot)?
        .to_string();
    let quarantine_store_root = roots
        .quarantine
        .to_str()
        .ok_or(RawBackupError::NonUtf8StoreRoot)?
        .to_string();
    let object_ids = expected_source_indexes
        .iter()
        .map(|index| index.object_id.clone())
        .collect();
    Ok(RawBackupCleanupAuthority {
        source_backup_prefix: request.source_backup_prefix.clone(),
        clean_target_prefix: request.clean_target_prefix.clone(),
        quarantine_target_prefix: request.quarantine_target_prefix.clone(),
        source_store_root,
        clean_store_root,
        quarantine_store_root,
        requested_object_ids: request.raw_backup_object_ids.clone(),
        object_ids,
        expected_source_indexes: expected_source_indexes.to_vec(),
        tables: request.tables.clone(),
    })
}

pub(crate) fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}

pub(crate) fn roots_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

pub(crate) struct StagedPrivacyOutput<'a, Effects> {
    pub(crate) clean_store: &'a FilesystemRawBackupStore,
    pub(crate) staged: &'a [RawBackupObjectIndex],
    pub(crate) current_object_id: Option<&'a RawBackupObjectId>,
    pub(crate) effects: &'a mut Effects,
    pub(crate) operation_id: &'a OperationId,
}
impl<Effects: PrivacyFixEffectBoundary> StagedPrivacyOutput<'_, Effects> {
    pub(crate) fn reserve<Clock: RateLimitClock>(
        self,
        limiter: &OperationRateLimiter,
        usage: RateLimitUsage,
        clock: &Clock,
    ) -> PrivacyFixResult<()> {
        enforce_rate_limit_with_limiter(limiter, usage, clock)
            .map_err(|error| self.rollback(error.into()))
    }
    pub(crate) fn rollback(self, original: PrivacyFixError) -> PrivacyFixError {
        let Self {
            clean_store,
            staged,
            current_object_id,
            effects,
            operation_id,
        } = self;
        let cleanup = (|| {
            for (ordinal, index) in staged.iter().enumerate() {
                let key = OperationChunkKey::for_operation(
                    operation_id,
                    "privacy-cleanup-staged",
                    u64::try_from(ordinal).unwrap_or(u64::MAX),
                );
                run_effect(effects, key, || {
                    clean_store
                        .delete_object_if_present(index)
                        .map_err(Into::into)
                })?;
            }
            if let Some(object_id) = current_object_id {
                let key =
                    OperationChunkKey::for_operation(operation_id, "privacy-cleanup-current", 0);
                run_effect(effects, key, || {
                    clean_store
                        .delete_object_id_if_present(object_id)
                        .map_err(Into::into)
                })?;
            }
            Ok::<(), PrivacyFixError>(())
        })();
        match cleanup {
            Ok(()) => original,
            Err(error) => PrivacyFixError::CleanupFailed {
                original_error: original.to_string(),
                cleanup_error: error.to_string(),
            },
        }
    }
}

impl<Effects: PrivacyFixEffectBoundary> StagedPrivacyOutput<'_, Effects> {
    pub(crate) fn find_index<'a, Lookup: super::PrivacyRawBackupIndexLookup<'a>>(
        self,
        source: &FilesystemRawBackupStore,
        lookup: &Lookup,
        object_id: &RawBackupObjectId,
    ) -> PrivacyFixResult<super::ResolvedPrivacyRawBackupIndex<'a>> {
        super::ResolvedPrivacyRawBackupIndex::resolve(source, lookup, object_id)
            .map_err(|error| self.rollback(error))
    }
    pub(crate) fn replay(
        self,
        source: &FilesystemRawBackupStore,
        index: &RawBackupObjectIndex,
        policy: &analytics_contract::PrivacyPolicy,
    ) -> PrivacyFixResult<crate::RawBackupReplayReport> {
        source
            .replay_object_report_with_privacy_policy(index, policy)
            .map_err(|error| self.rollback(error.into()))
    }
    pub(crate) fn verify_table_scope(
        self,
        replay: &crate::RawBackupReplayReport,
        request: &PrivacyFixRequest,
    ) -> PrivacyFixResult<()> {
        request
            .validate_replay_scope(replay)
            .map_err(|error| self.rollback(error))
    }
    pub(crate) fn make_write_request(
        self,
        request: &PrivacyFixRequest,
        index: &RawBackupObjectIndex,
        replay: crate::RawBackupReplayReport,
    ) -> PrivacyFixResult<crate::RawBackupWriteRequest> {
        let records = replay
            .records
            .into_iter()
            .map(|record| {
                let record_key = String::from_utf8(record.record_key)
                    .map_err(|_| RawBackupError::PrivacyIdentityDenied)?;
                Ok(crate::RawBackupRecord {
                    table_name: record.table_name,
                    record_key,
                    source_position: record.source_position,
                    record: record.record,
                })
            })
            .collect::<Result<Vec<_>, RawBackupError>>();
        let result = records
            .and_then(|records| {
                crate::RawBackupWriteRequest::try_new(
                    request.operation_id.clone(),
                    index.object_id.clone(),
                    index.source_identity.clone(),
                    Some(request.policy.version.clone()),
                    records,
                )
            })
            .map(|request| request.with_layout(index.layout.clone()));
        result.map_err(|error| self.rollback(error.into()))
    }
    pub(crate) fn write_object(
        self,
        request: &crate::RawBackupWriteRequest,
    ) -> PrivacyFixResult<RawBackupObjectIndex> {
        let key = OperationChunkKey::for_operation(
            self.operation_id,
            "privacy-clean-object",
            u64::try_from(self.staged.len()).unwrap_or(u64::MAX),
        );
        let result = run_effect(self.effects, key, || {
            self.clean_store
                .write_object_report(request)
                .map(|report| report.index)
                .map_err(Into::into)
        });
        result.map_err(|error| self.rollback(error))
    }
    pub(crate) fn verify_object(
        self,
        index: &RawBackupObjectIndex,
    ) -> PrivacyFixResult<crate::RawBackupVerifyReport> {
        self.clean_store
            .verify_object_report(index)
            .map_err(|error| self.rollback(error.into()))
    }
}
