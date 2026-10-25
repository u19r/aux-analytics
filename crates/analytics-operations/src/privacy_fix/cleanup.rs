use std::path::Path;

use super::{
    PendingPrivacyCleanup, PrivacyAffectedTables, PrivacyFixDestructiveAction,
    PrivacyFixEffectBoundary, PrivacyFixError, PrivacyFixRequest, PrivacyFixResult,
    StagedPrivacyOutput, cleanup_authority, cleanup_ordinal, clear_cleanup_progress_with_effects,
    destructive_action_name, run_effect, save_cleanup_progress_with_effects,
    verify_source_index_for_cleanup,
};
use crate::{
    FilesystemRawBackupStore, OperationChunkKey, OperationRateLimiter, RateLimitClock,
    RateLimitUsage, RawBackupCleanupMetadata, RawBackupObjectIndex,
    enforce_rate_limit_with_limiter,
};

pub(crate) struct PendingCleanupExecution<'a, Clock, Tables, Effects> {
    pub(super) request: &'a PrivacyFixRequest,
    pub(super) source_store: &'a FilesystemRawBackupStore,
    pub(super) clean_store: &'a FilesystemRawBackupStore,
    pub(super) limiter: &'a OperationRateLimiter,
    pub(super) clock: &'a Clock,
    pub(super) effects: &'a mut Effects,
    pub(super) quarantine_store: &'a FilesystemRawBackupStore,
    pub(super) source_root: &'a Path,
    pub(super) clean_root: &'a Path,
    pub(super) quarantine_root: &'a Path,
    pub(super) staged_clean_indexes: &'a [RawBackupObjectIndex],
    pub(super) tainted_objects: u64,
    pub(super) rewritten_objects: u64,
    pub(super) affected_tables: &'a Tables,
    pub(super) privacy_dropped_fields: u64,
    pub(super) clean_replay_verified: bool,
}

impl<'a, Clock, Tables, Effects> PendingCleanupExecution<'a, Clock, Tables, Effects>
where
    Clock: RateLimitClock,
    Tables: PrivacyAffectedTables,
    Effects: PrivacyFixEffectBoundary,
{
    pub(crate) fn run(
        mut self,
        pending_cleanup: Vec<PendingPrivacyCleanup<'a>>,
    ) -> PrivacyFixResult<(u64, u64)> {
        let authority = cleanup_authority(
            self.request,
            crate::privacy_fix::PrivacyBackupRoots {
                source: self.source_root,
                clean: self.clean_root,
                quarantine: self.quarantine_root,
            },
            &pending_cleanup
                .iter()
                .map(|pending| pending.index.as_ref().clone())
                .collect::<Vec<_>>(),
        )?;
        if pending_cleanup.is_empty() {
            return Ok((0, 0));
        }
        let mut progress = self.begin_progress(&pending_cleanup, authority)?;
        let mut completed = 0_u64;
        for pending in pending_cleanup {
            self.claim_object(&mut progress, &pending.object_id)?;
            self.verify_source_index(pending.index.as_ref())?;
            self.reserve_cleanup()?;
            if self.request.destructive_action == PrivacyFixDestructiveAction::QuarantineTainted {
                self.quarantine_object(&progress, &pending)?;
                self.verify_source_index(pending.index.as_ref())?;
            }
            if let Err(error) = self.delete_object(&progress, &pending) {
                return Err(self.record_cleanup_failure(&progress, &pending.object_id, error));
            }
            self.complete_object(&mut progress, pending.object_id)?;
            completed += 1;
        }
        let key = OperationChunkKey::for_operation(
            &self.request.operation_id,
            "privacy-cleanup-journal-clear",
            0,
        );
        clear_cleanup_progress_with_effects(self.source_store, &progress, self.effects, key)?;
        Ok(match self.request.destructive_action {
            PrivacyFixDestructiveAction::None => (0, 0),
            PrivacyFixDestructiveAction::DeleteTainted => (completed, 0),
            PrivacyFixDestructiveAction::QuarantineTainted => (0, completed),
        })
    }

    fn begin_progress(
        &mut self,
        pending: &[PendingPrivacyCleanup<'a>],
        authority: crate::RawBackupCleanupAuthority,
    ) -> PrivacyFixResult<crate::RawBackupCleanupProgress> {
        let key = OperationChunkKey::for_operation(
            &self.request.operation_id,
            "privacy-cleanup-journal-create",
            0,
        );
        let ids = pending
            .iter()
            .map(|pending| pending.object_id.clone())
            .collect::<Vec<_>>();
        let metadata = self.cleanup_metadata();
        let result = run_effect(self.effects, key, || {
            self.source_store
                .begin_cleanup_progress_with_metadata_and_authority(
                    &self.request.operation_id,
                    &ids,
                    Some(metadata),
                    authority,
                )
                .map_err(PrivacyFixError::RawBackup)
        });
        result.map_err(|error| {
            StagedPrivacyOutput {
                clean_store: self.clean_store,
                staged: self.staged_clean_indexes,
                current_object_id: None,
                effects: self.effects,
                operation_id: &self.request.operation_id,
            }
            .rollback(error)
        })
    }

    fn cleanup_metadata(&self) -> RawBackupCleanupMetadata {
        RawBackupCleanupMetadata {
            scanned_objects: self.request.raw_backup_object_ids.len() as u64,
            tainted_objects: self.tainted_objects,
            rewritten_objects: self.rewritten_objects,
            affected_tables: self.affected_tables.snapshot(),
            policy_version: self.request.policy.version.clone(),
            destructive_action: destructive_action_name(self.request.destructive_action),
            privacy_dropped_fields: self.privacy_dropped_fields,
            clean_replay_verified: self.clean_replay_verified,
        }
    }

    fn claim_object(
        &mut self,
        progress: &mut crate::RawBackupCleanupProgress,
        object_id: &crate::RawBackupObjectId,
    ) -> PrivacyFixResult<()> {
        if progress.claimed_object_ids.contains(object_id) {
            return Ok(());
        }
        progress.claimed_object_ids.push(object_id.clone());
        let key = OperationChunkKey::for_operation(
            &self.request.operation_id,
            "privacy-cleanup-journal-claim",
            cleanup_ordinal(progress, object_id),
        );
        save_cleanup_progress_with_effects(self.source_store, progress, self.effects, key).map_err(
            |error| PrivacyFixError::CleanupFailed {
                original_error: "cleanup intent could not be persisted".to_string(),
                cleanup_error: error.to_string(),
            },
        )
    }

    fn verify_source_index(&mut self, index: &RawBackupObjectIndex) -> PrivacyFixResult<()> {
        verify_source_index_for_cleanup(self.source_store, index).map_err(|error| {
            StagedPrivacyOutput {
                clean_store: self.clean_store,
                staged: self.staged_clean_indexes,
                current_object_id: None,
                effects: self.effects,
                operation_id: &self.request.operation_id,
            }
            .rollback(error)
        })
    }

    fn reserve_cleanup(&self) -> PrivacyFixResult<()> {
        enforce_rate_limit_with_limiter(
            self.limiter,
            RateLimitUsage {
                source_requests: 1,
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
            self.clock,
        )?;
        Ok(())
    }

    fn quarantine_object(
        &mut self,
        progress: &crate::RawBackupCleanupProgress,
        pending: &PendingPrivacyCleanup<'a>,
    ) -> PrivacyFixResult<()> {
        let key = OperationChunkKey::for_operation(
            &self.request.operation_id,
            "privacy-quarantine-object",
            cleanup_ordinal(progress, &pending.object_id),
        );
        run_effect(self.effects, key, || {
            self.source_store
                .quarantine_object(pending.index.as_ref(), self.quarantine_store)
                .map(|_| ())
                .map_err(Into::into)
        })
    }

    fn delete_object(
        &mut self,
        progress: &crate::RawBackupCleanupProgress,
        pending: &PendingPrivacyCleanup<'a>,
    ) -> PrivacyFixResult<()> {
        if self.request.destructive_action == PrivacyFixDestructiveAction::None {
            return Ok(());
        }
        let key = OperationChunkKey::for_operation(
            &self.request.operation_id,
            "privacy-delete-object",
            cleanup_ordinal(progress, &pending.object_id),
        );
        run_effect(self.effects, key, || {
            self.source_store
                .delete_object_if_present(pending.index.as_ref())
                .map_err(Into::into)
        })
    }

    fn record_cleanup_failure(
        &mut self,
        progress: &crate::RawBackupCleanupProgress,
        object_id: &crate::RawBackupObjectId,
        error: PrivacyFixError,
    ) -> PrivacyFixError {
        let key = OperationChunkKey::for_operation(
            &self.request.operation_id,
            "privacy-cleanup-journal-failure",
            cleanup_ordinal(progress, object_id),
        );
        match save_cleanup_progress_with_effects(self.source_store, progress, self.effects, key) {
            Ok(()) => error,
            Err(progress_error) => PrivacyFixError::CleanupFailed {
                original_error: error.to_string(),
                cleanup_error: progress_error.to_string(),
            },
        }
    }

    fn complete_object(
        &mut self,
        progress: &mut crate::RawBackupCleanupProgress,
        object_id: crate::RawBackupObjectId,
    ) -> PrivacyFixResult<()> {
        progress
            .pending_object_ids
            .retain(|pending| pending != &object_id);
        progress.completed_object_ids.push(object_id.clone());
        progress
            .claimed_object_ids
            .retain(|claimed| claimed != &object_id);
        let key = OperationChunkKey::for_operation(
            &self.request.operation_id,
            "privacy-cleanup-journal-complete",
            cleanup_ordinal(progress, &object_id),
        );
        save_cleanup_progress_with_effects(self.source_store, progress, self.effects, key).map_err(
            |error| PrivacyFixError::CleanupFailed {
                original_error: "cleanup completed but progress persistence failed".to_string(),
                cleanup_error: error.to_string(),
            },
        )
    }
}
