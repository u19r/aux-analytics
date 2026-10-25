use super::{
    PendingCleanupExecution, PendingPrivacyCleanup, PrivacyAffectedTables,
    PrivacyFixDestructiveAction, PrivacyFixEffectBoundary, PrivacyFixError, PrivacyFixReport,
    PrivacyFixResult, PrivacyRawBackupIndexLookup, PrivacyRewriteExecution, StagedPrivacyOutput,
    cleanup_authority,
};
use crate::{FilesystemRawBackupStore, RateLimitClock, RateLimitUsage};

impl<Clock: RateLimitClock, Effects: PrivacyFixEffectBoundary>
    PrivacyRewriteExecution<'_, Clock, Effects>
{
    pub(crate) fn rewrite<'a, Lookup, Tables>(
        self,
        index_lookup: &Lookup,
        affected_tables: Tables,
    ) -> PrivacyFixResult<PrivacyFixReport>
    where
        Lookup: PrivacyRawBackupIndexLookup<'a>,
        Tables: PrivacyAffectedTables,
    {
        if self.request.dry_run {
            return self.plan_rewrite(index_lookup, affected_tables);
        }
        let stores = CanonicalPrivacyRewriteStores::try_new(
            self.request,
            self.source_store,
            self.clean_store,
        )?;
        let mut execution = PrivacyRewriteExecution {
            source_store: &stores.source,
            clean_store: &stores.clean,
            ..self
        };
        let authority = cleanup_authority(execution.request, stores.roots(), &[])?;
        execution.reserve_entry()?;
        if let Some(progress) = execution
            .source_store
            .load_cleanup_progress(&execution.request.operation_id)?
        {
            return execution.resume(index_lookup, progress, &authority, &stores.quarantine);
        }
        let mut outputs = PrivacyRewriteOutputs::new(affected_tables);
        execution.rewrite_objects(index_lookup, &mut outputs)?;
        let (deleted, quarantined) = outputs.cleanup(&mut execution, &stores)?;
        Ok(outputs.report(execution.request, deleted, quarantined))
    }
    fn reserve_entry(&mut self) -> PrivacyFixResult<()> {
        self.reserve_output(
            &[],
            RateLimitUsage {
                rows: 1,
                source_requests: 1,
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
        )
    }
    fn rewrite_objects<
        'a,
        Lookup: PrivacyRawBackupIndexLookup<'a>,
        Tables: PrivacyAffectedTables,
    >(
        &mut self,
        lookup: &Lookup,
        outputs: &mut PrivacyRewriteOutputs<'a, Tables>,
    ) -> PrivacyFixResult<()> {
        let request = self.request;
        for object_id in &request.raw_backup_object_ids {
            self.rewrite_object(object_id, lookup, outputs)?;
        }
        Ok(())
    }
    fn reserve_output(
        &mut self,
        staged: &[crate::RawBackupObjectIndex],
        usage: RateLimitUsage,
    ) -> PrivacyFixResult<()> {
        let limiter = self.limiter;
        let clock = self.clock;
        self.staged_output(staged, None)
            .reserve(limiter, usage, clock)
    }
    fn staged_output<'a>(
        &'a mut self,
        staged: &'a [crate::RawBackupObjectIndex],
        current_object_id: Option<&'a crate::RawBackupObjectId>,
    ) -> StagedPrivacyOutput<'a, Effects> {
        StagedPrivacyOutput {
            clean_store: self.clean_store,
            staged,
            current_object_id,
            effects: &mut *self.effects,
            operation_id: &self.request.operation_id,
        }
    }
    fn rewrite_object<
        'a,
        Lookup: PrivacyRawBackupIndexLookup<'a>,
        Tables: PrivacyAffectedTables,
    >(
        &mut self,
        object_id: &crate::RawBackupObjectId,
        index_lookup: &Lookup,
        outputs: &mut PrivacyRewriteOutputs<'a, Tables>,
    ) -> PrivacyFixResult<()> {
        let (resolved_index, replay) =
            self.load_object(index_lookup, object_id, &outputs.staged_clean_indexes)?;
        let index = resolved_index.as_ref();
        outputs.privacy_dropped_fields += replay.metrics.policy_drops;
        if replay.metrics.policy_drops > 0 {
            outputs.tainted_objects += 1;
        }
        let object_is_tainted = replay.metrics.policy_drops > 0;
        for record in &replay.records {
            outputs.affected_tables.observe(record.table_name.as_str());
        }
        self.stage_object(outputs, index, replay)?;
        outputs.rewritten_objects += 1;
        if object_is_tainted
            && !matches!(
                self.request.destructive_action,
                PrivacyFixDestructiveAction::None
            )
        {
            outputs.pending_cleanup.push(PendingPrivacyCleanup {
                object_id: index.object_id.clone(),
                index: resolved_index,
            });
        }
        Ok(())
    }
    fn load_object<'a, Lookup: PrivacyRawBackupIndexLookup<'a>>(
        &mut self,
        index_lookup: &Lookup,
        object_id: &crate::RawBackupObjectId,
        staged: &[crate::RawBackupObjectIndex],
    ) -> PrivacyFixResult<(
        super::ResolvedPrivacyRawBackupIndex<'a>,
        crate::RawBackupReplayReport,
    )> {
        let request = self.request;
        let source_store = self.source_store;
        self.reserve_output(
            staged,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
        )?;
        let resolved_index =
            self.staged_output(staged, None)
                .find_index(source_store, index_lookup, object_id)?;
        let index = resolved_index.as_ref();
        self.reserve_output(
            staged,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
        )?;
        let replay =
            self.staged_output(staged, None)
                .replay(source_store, index, &request.policy)?;
        self.staged_output(staged, None)
            .verify_table_scope(&replay, request)?;
        Ok((resolved_index, replay))
    }
    fn stage_object<Tables: PrivacyAffectedTables>(
        &mut self,
        outputs: &mut PrivacyRewriteOutputs<'_, Tables>,
        index: &crate::RawBackupObjectIndex,
        replay: crate::RawBackupReplayReport,
    ) -> PrivacyFixResult<()> {
        let request = self.request;
        let write_request = self
            .staged_output(&outputs.staged_clean_indexes, None)
            .make_write_request(request, index, replay)?;
        self.reserve_output(
            &outputs.staged_clean_indexes,
            RateLimitUsage {
                rows: write_request.records.len() as u64,
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
        )?;
        let clean_index = self
            .staged_output(
                &outputs.staged_clean_indexes,
                Some(&write_request.object_id),
            )
            .write_object(&write_request)?;
        outputs.staged_clean_indexes.push(clean_index.clone());
        self.verify_staged_object(
            &outputs.staged_clean_indexes,
            index,
            &write_request,
            &clean_index,
        )
    }
    fn verify_staged_object(
        &mut self,
        staged: &[crate::RawBackupObjectIndex],
        source_index: &crate::RawBackupObjectIndex,
        write_request: &crate::RawBackupWriteRequest,
        clean_index: &crate::RawBackupObjectIndex,
    ) -> PrivacyFixResult<()> {
        let request = self.request;
        let clean_store = self.clean_store;
        self.reserve_output(
            staged,
            RateLimitUsage {
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
        )?;
        let verification = self
            .staged_output(staged, None)
            .verify_object(clean_index)?;
        self.reserve_output(
            staged,
            RateLimitUsage {
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
        )?;
        let clean_replay =
            self.staged_output(staged, None)
                .replay(clean_store, clean_index, &request.policy)?;
        let object_verified = verification.checksum_valid
            && clean_replay.metrics.policy_drops == 0
            && clean_replay.records.len() == write_request.records.len();
        if !object_verified {
            return Err(self.staged_output(staged, None).rollback(
                PrivacyFixError::CleanReplayVerificationFailed(source_index.object_id.clone()),
            ));
        }
        Ok(())
    }
}
struct PrivacyRewriteOutputs<'a, Tables> {
    tainted_objects: u64,
    rewritten_objects: u64,
    privacy_dropped_fields: u64,
    affected_tables: Tables,
    staged_clean_indexes: Vec<crate::RawBackupObjectIndex>,
    pending_cleanup: Vec<PendingPrivacyCleanup<'a>>,
}
impl<Tables: PrivacyAffectedTables> PrivacyRewriteOutputs<'_, Tables> {
    fn new(affected_tables: Tables) -> Self {
        Self {
            tainted_objects: 0,
            rewritten_objects: 0,
            privacy_dropped_fields: 0,
            affected_tables,
            staged_clean_indexes: Vec::new(),
            pending_cleanup: Vec::new(),
        }
    }
    fn cleanup<Clock: RateLimitClock, Effects: PrivacyFixEffectBoundary>(
        &mut self,
        execution: &mut PrivacyRewriteExecution<'_, Clock, Effects>,
        stores: &CanonicalPrivacyRewriteStores,
    ) -> PrivacyFixResult<(u64, u64)> {
        PendingCleanupExecution {
            request: execution.request,
            source_store: &stores.source,
            clean_store: &stores.clean,
            limiter: execution.limiter,
            clock: execution.clock,
            effects: &mut *execution.effects,
            quarantine_store: &stores.quarantine,
            source_root: &stores.source_root,
            clean_root: &stores.clean_root,
            quarantine_root: &stores.quarantine_root,
            staged_clean_indexes: &self.staged_clean_indexes,
            tainted_objects: self.tainted_objects,
            rewritten_objects: self.rewritten_objects,
            affected_tables: &self.affected_tables,
            privacy_dropped_fields: self.privacy_dropped_fields,
            clean_replay_verified: true,
        }
        .run(std::mem::take(&mut self.pending_cleanup))
    }
    fn report(
        self,
        request: &super::PrivacyFixRequest,
        deleted_tainted_objects: u64,
        quarantined_tainted_objects: u64,
    ) -> PrivacyFixReport {
        PrivacyFixReport {
            operation_id: request.operation_id.clone(),
            dry_run: request.dry_run,
            scanned_objects: request.raw_backup_object_ids.len() as u64,
            tainted_objects: self.tainted_objects,
            rewritten_objects: self.rewritten_objects,
            deleted_tainted_objects,
            quarantined_tainted_objects,
            affected_tables: self.affected_tables.finish(),
            policy_version: request.policy.version.clone(),
            privacy_dropped_fields: self.privacy_dropped_fields,
            clean_replay_verified: true,
            destructive_action: request.destructive_action,
        }
    }
}

struct CanonicalPrivacyRewriteStores {
    source: FilesystemRawBackupStore,
    clean: FilesystemRawBackupStore,
    quarantine: FilesystemRawBackupStore,
    source_root: std::path::PathBuf,
    clean_root: std::path::PathBuf,
    quarantine_root: std::path::PathBuf,
}
impl CanonicalPrivacyRewriteStores {
    fn try_new(
        request: &super::PrivacyFixRequest,
        source: &FilesystemRawBackupStore,
        clean: &FilesystemRawBackupStore,
    ) -> PrivacyFixResult<Self> {
        let source = source.canonicalized()?;
        let clean = clean.canonicalized()?;
        let quarantine =
            FilesystemRawBackupStore::new(&request.quarantine_target_prefix).canonicalized()?;
        let source_root = source.canonical_root()?;
        let clean_root = clean.canonical_root()?;
        let quarantine_root = quarantine.canonical_root()?;
        super::PrivacyBackupRoots {
            source: &source_root,
            clean: &clean_root,
            quarantine: &quarantine_root,
        }
        .validate_disjoint()?;
        Ok(Self {
            source,
            clean,
            quarantine,
            source_root,
            clean_root,
            quarantine_root,
        })
    }
    fn roots(&self) -> super::PrivacyBackupRoots<'_> {
        super::PrivacyBackupRoots {
            source: &self.source_root,
            clean: &self.clean_root,
            quarantine: &self.quarantine_root,
        }
    }
}
