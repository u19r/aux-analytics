use super::{
    PrivacyFixDestructiveAction, PrivacyFixEffectBoundary, PrivacyFixError, PrivacyFixReport,
    PrivacyFixResult, PrivacyRawBackupIndexLookup, PrivacyRewriteExecution,
    ResolvedPrivacyRawBackupIndex, cleanup_ordinal, clear_cleanup_progress_with_effects,
    destructive_action_name, run_effect, same_cleanup_scope, save_cleanup_progress_with_effects,
    validate_cleanup_partition, verify_clean_output, verify_quarantine_output,
    verify_source_index_for_cleanup, verify_source_object_for_cleanup,
};
use crate::{
    FilesystemRawBackupStore, OperationChunkKey, RateLimitClock, RateLimitUsage,
    RawBackupCleanupAuthority, RawBackupError, enforce_rate_limit_with_limiter,
};

impl<Clock: RateLimitClock, Effects: PrivacyFixEffectBoundary>
    PrivacyRewriteExecution<'_, Clock, Effects>
{
    pub(crate) fn resume<'a, Lookup>(
        self,
        index_lookup: &Lookup,
        progress: crate::RawBackupCleanupProgress,
        authority: &RawBackupCleanupAuthority,
        quarantine_store: &FilesystemRawBackupStore,
    ) -> PrivacyFixResult<PrivacyFixReport>
    where
        Lookup: PrivacyRawBackupIndexLookup<'a>,
    {
        PrivacyCleanupResume {
            execution: self,
            progress,
            authority,
            quarantine_store,
        }
        .resume_objects(index_lookup)
    }
}
struct PrivacyCleanupResume<'a, Clock, Effects> {
    execution: PrivacyRewriteExecution<'a, Clock, Effects>,
    progress: crate::RawBackupCleanupProgress,
    authority: &'a RawBackupCleanupAuthority,
    quarantine_store: &'a FilesystemRawBackupStore,
}
impl<Clock: RateLimitClock, Effects: PrivacyFixEffectBoundary>
    PrivacyCleanupResume<'_, Clock, Effects>
{
    fn resume_objects<'a, Lookup: PrivacyRawBackupIndexLookup<'a>>(
        mut self,
        lookup: &Lookup,
    ) -> PrivacyFixResult<PrivacyFixReport> {
        let metadata = self.validate_authority()?;
        self.verify_clean_outputs()?;
        for object_id in self.progress.pending_object_ids.clone() {
            let already_claimed = self.progress.claimed_object_ids.contains(&object_id);
            self.reserve_cleanup()?;
            let index = self.load_source_index(lookup, &object_id, already_claimed)?;
            self.verify_source_index(&object_id, index.as_ref())?;
            self.claim_object(&object_id, already_claimed)?;
            if let Err(error) = self.cleanup_object(&object_id, index.as_ref()) {
                return self.persist_failure(&object_id, error);
            }
            self.complete_object(&object_id)?;
        }
        let key = OperationChunkKey::for_operation(
            &self.execution.request.operation_id,
            "privacy-cleanup-journal-clear",
            0,
        );
        clear_cleanup_progress_with_effects(
            self.execution.source_store,
            &self.progress,
            &mut *self.execution.effects,
            key,
        )?;
        Ok(self.report(metadata))
    }
    fn validate_authority(&self) -> PrivacyFixResult<crate::RawBackupCleanupMetadata> {
        let request = self.execution.request;
        let authority = self.authority;
        let Some(journal_authority) = self.progress.authority.as_ref() else {
            return Err(PrivacyFixError::CleanupAuthorityMismatch);
        };
        if self.progress.operation_id != request.operation_id
            || !same_cleanup_scope(journal_authority, authority)
            || journal_authority
                .object_ids
                .iter()
                .any(|object_id| !request.raw_backup_object_ids.contains(object_id))
        {
            return Err(PrivacyFixError::CleanupAuthorityMismatch);
        }
        validate_cleanup_partition(&self.progress, journal_authority, request)?;
        let Some(metadata) = self.progress.metadata.clone() else {
            return Err(PrivacyFixError::CleanupFailed {
                original_error: "cleanup progress metadata is missing".to_string(),
                cleanup_error: "report reconstruction is unsafe".to_string(),
            });
        };
        if metadata
            .affected_tables
            .iter()
            .any(|table| !authority.tables.iter().any(|allowed| allowed == table))
        {
            return Err(PrivacyFixError::CleanupAuthorityMismatch);
        }
        if metadata.policy_version != request.policy.version
            || metadata.destructive_action != destructive_action_name(request.destructive_action)
            || !metadata.clean_replay_verified
        {
            return Err(PrivacyFixError::CleanupAuthorityMismatch);
        }
        Ok(metadata)
    }
    fn verify_clean_outputs(&self) -> PrivacyFixResult<()> {
        let request = self.execution.request;
        let clean_store = self.execution.clean_store;
        let limiter = self.execution.limiter;
        let clock = self.execution.clock;
        for object_id in &self.progress.pending_object_ids {
            // A resumed cleanup still has to pass the same source/destination
            // gate before verification reads.  Otherwise a denied or
            // out-of-window retry could touch the clean destination before
            // deciding that no cleanup call is allowed.
            enforce_rate_limit_with_limiter(
                limiter,
                RateLimitUsage {
                    source_requests: 1,
                    destination_writes: 1,
                    ..RateLimitUsage::default()
                },
                clock,
            )?;
            verify_clean_output(clean_store, object_id, &request.policy)?;
        }
        Ok(())
    }
    fn reserve_cleanup(&self) -> PrivacyFixResult<()> {
        enforce_rate_limit_with_limiter(
            self.execution.limiter,
            RateLimitUsage {
                source_requests: 1,
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
            self.execution.clock,
        )?;
        Ok(())
    }
    fn load_source_index<'a, Lookup: PrivacyRawBackupIndexLookup<'a>>(
        &self,
        index_lookup: &Lookup,
        object_id: &crate::RawBackupObjectId,
        already_claimed: bool,
    ) -> PrivacyFixResult<Option<ResolvedPrivacyRawBackupIndex<'a>>> {
        let source_store = self.execution.source_store;
        let supplied_index = index_lookup.find(object_id);
        let index = match source_store.read_index(object_id) {
            Ok(persisted_index) => {
                if supplied_index
                    .as_ref()
                    .is_some_and(|supplied| supplied.as_ref() != &persisted_index)
                {
                    return Err(PrivacyFixError::CleanupAuthorityMismatch);
                }
                Some(ResolvedPrivacyRawBackupIndex::Owned(Box::new(
                    persisted_index,
                )))
            }
            Err(RawBackupError::Io(error))
                if already_claimed && error.kind() == std::io::ErrorKind::NotFound =>
            {
                let (object_present, index_present) =
                    source_store.object_parts_present(object_id)?;
                if object_present || index_present {
                    return Err(PrivacyFixError::CleanupAuthorityMismatch);
                }
                None
            }
            Err(error) => return Err(error.into()),
        };
        Ok(index)
    }
    fn expected_index(
        &self,
        object_id: &crate::RawBackupObjectId,
    ) -> PrivacyFixResult<&crate::RawBackupObjectIndex> {
        self.progress
            .authority
            .as_ref()
            .ok_or(PrivacyFixError::CleanupAuthorityMismatch)?
            .expected_source_indexes
            .iter()
            .find(|expected| &expected.object_id == object_id)
            .ok_or(PrivacyFixError::CleanupAuthorityMismatch)
    }
    fn verify_source_index(
        &self,
        object_id: &crate::RawBackupObjectId,
        index: Option<&ResolvedPrivacyRawBackupIndex<'_>>,
    ) -> PrivacyFixResult<()> {
        let source_store = self.execution.source_store;
        let expected_index = self
            .progress
            .authority
            .as_ref()
            .ok_or(PrivacyFixError::CleanupAuthorityMismatch)?
            .expected_source_indexes
            .iter()
            .find(|expected| &expected.object_id == object_id)
            .ok_or(PrivacyFixError::CleanupAuthorityMismatch)?;
        if index
            .as_ref()
            .is_some_and(|actual| actual.as_ref() != expected_index)
        {
            return Err(PrivacyFixError::CleanupAuthorityMismatch);
        }
        if let Some(index) = index.as_ref() {
            verify_source_object_for_cleanup(source_store, index.as_ref())?;
        }
        Ok(())
    }
    fn claim_object(
        &mut self,
        object_id: &crate::RawBackupObjectId,
        already_claimed: bool,
    ) -> PrivacyFixResult<()> {
        let request = self.execution.request;
        let source_store = self.execution.source_store;
        let effects = &mut *self.execution.effects;
        if !already_claimed {
            self.progress.claimed_object_ids.push(object_id.clone());
            let progress_key = OperationChunkKey::for_operation(
                &request.operation_id,
                "privacy-cleanup-journal-claim",
                cleanup_ordinal(&self.progress, object_id),
            );
            if let Err(progress_error) = save_cleanup_progress_with_effects(
                source_store,
                &self.progress,
                effects,
                progress_key,
            ) {
                return Err(PrivacyFixError::CleanupFailed {
                    original_error: "cleanup intent could not be persisted".to_string(),
                    cleanup_error: progress_error.to_string(),
                });
            }
        }
        Ok(())
    }
    fn cleanup_object(
        &mut self,
        object_id: &crate::RawBackupObjectId,
        index: Option<&ResolvedPrivacyRawBackupIndex<'_>>,
    ) -> PrivacyFixResult<()> {
        let request = self.execution.request;
        let quarantine_store = self.quarantine_store;
        let expected_index = self.expected_index(object_id)?;
        match (request.destructive_action, index) {
            // A claimed delete can have no readable source index after a crash:
            // the idempotent object delete still makes the retry safe and does
            // not require reconstructing source truth. Quarantine requires a
            // verified copy before allowing source cleanup.
            (PrivacyFixDestructiveAction::DeleteTainted, None) => {
                self.delete_object(object_id, None)
            }
            (PrivacyFixDestructiveAction::QuarantineTainted, None) => {
                verify_quarantine_output(quarantine_store, object_id, expected_index)?;
                self.delete_object(object_id, None)
            }
            (PrivacyFixDestructiveAction::None, None) => Ok(()),
            (PrivacyFixDestructiveAction::QuarantineTainted, Some(index)) => {
                self.quarantine_object(object_id, index)?;
                self.delete_object(object_id, Some(index))
            }
            (PrivacyFixDestructiveAction::DeleteTainted, Some(index)) => {
                self.delete_object(object_id, Some(index))
            }
            (PrivacyFixDestructiveAction::None, Some(_)) => Ok(()),
        }
    }
    fn quarantine_object(
        &mut self,
        object_id: &crate::RawBackupObjectId,
        index: &ResolvedPrivacyRawBackupIndex<'_>,
    ) -> PrivacyFixResult<()> {
        let quarantine_store = self.quarantine_store;
        let request = self.execution.request;
        let source_store = self.execution.source_store;
        let effects = &mut *self.execution.effects;
        let ordinal = cleanup_ordinal(&self.progress, object_id);
        let quarantine_key = OperationChunkKey::for_operation(
            &request.operation_id,
            "privacy-quarantine-object",
            ordinal,
        );
        run_effect(effects, quarantine_key, || {
            source_store
                .quarantine_object(index.as_ref(), quarantine_store)
                .map(|_| ())
                .map_err(Into::into)
        })?;
        verify_source_index_for_cleanup(source_store, index.as_ref())?;
        Ok(())
    }
    fn delete_object(
        &mut self,
        object_id: &crate::RawBackupObjectId,
        index: Option<&ResolvedPrivacyRawBackupIndex<'_>>,
    ) -> PrivacyFixResult<()> {
        let key = OperationChunkKey::for_operation(
            &self.execution.request.operation_id,
            "privacy-delete-object",
            cleanup_ordinal(&self.progress, object_id),
        );
        let source_store = self.execution.source_store;
        run_effect(self.execution.effects, key, || match index {
            Some(index) => source_store
                .delete_object_if_present(index.as_ref())
                .map_err(Into::into),
            None => source_store
                .delete_object_id_if_present(object_id)
                .map_err(Into::into),
        })
    }
    fn persist_failure(
        &mut self,
        object_id: &crate::RawBackupObjectId,
        error: PrivacyFixError,
    ) -> PrivacyFixResult<PrivacyFixReport> {
        let request = self.execution.request;
        let source_store = self.execution.source_store;
        let effects = &mut *self.execution.effects;
        let progress_key = OperationChunkKey::for_operation(
            &request.operation_id,
            "privacy-cleanup-journal-failure",
            cleanup_ordinal(&self.progress, object_id),
        );
        match save_cleanup_progress_with_effects(
            source_store,
            &self.progress,
            effects,
            progress_key,
        ) {
            Ok(()) => Err(error),
            Err(progress_error) => Err(PrivacyFixError::CleanupFailed {
                original_error: error.to_string(),
                cleanup_error: progress_error.to_string(),
            }),
        }
    }
    fn complete_object(&mut self, object_id: &crate::RawBackupObjectId) -> PrivacyFixResult<()> {
        let request = self.execution.request;
        let source_store = self.execution.source_store;
        let effects = &mut *self.execution.effects;
        self.progress
            .pending_object_ids
            .retain(|pending| pending != object_id);
        self.progress.completed_object_ids.push(object_id.clone());
        self.progress
            .claimed_object_ids
            .retain(|claimed| claimed != object_id);
        let progress_key = OperationChunkKey::for_operation(
            &request.operation_id,
            "privacy-cleanup-journal-complete",
            cleanup_ordinal(&self.progress, object_id),
        );
        if let Err(progress_error) =
            save_cleanup_progress_with_effects(source_store, &self.progress, effects, progress_key)
        {
            return Err(PrivacyFixError::CleanupFailed {
                original_error: "cleanup completed but progress persistence failed".to_string(),
                cleanup_error: progress_error.to_string(),
            });
        }
        Ok(())
    }
    fn report(self, metadata: crate::RawBackupCleanupMetadata) -> PrivacyFixReport {
        let request = self.execution.request;
        let completed = self.progress.completed_object_ids.len() as u64;
        PrivacyFixReport {
            operation_id: request.operation_id.clone(),
            dry_run: request.dry_run,
            scanned_objects: metadata.scanned_objects,
            tainted_objects: metadata.tainted_objects,
            rewritten_objects: metadata.rewritten_objects,
            deleted_tainted_objects: if matches!(
                request.destructive_action,
                PrivacyFixDestructiveAction::DeleteTainted
            ) {
                completed
            } else {
                0
            },
            quarantined_tainted_objects: if matches!(
                request.destructive_action,
                PrivacyFixDestructiveAction::QuarantineTainted
            ) {
                completed
            } else {
                0
            },
            affected_tables: metadata.affected_tables,
            policy_version: metadata.policy_version,
            privacy_dropped_fields: metadata.privacy_dropped_fields,
            clean_replay_verified: metadata.clean_replay_verified,
            destructive_action: request.destructive_action,
        }
    }
}
