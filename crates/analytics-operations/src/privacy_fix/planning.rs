use super::{
    PrivacyAffectedTables, PrivacyBackupRoots, PrivacyFixEffectBoundary, PrivacyFixReport,
    PrivacyFixResult, PrivacyRawBackupIndexLookup, PrivacyRewriteExecution,
    ResolvedPrivacyRawBackupIndex,
};
use crate::{
    FilesystemRawBackupStore, RateLimitClock, RateLimitUsage, enforce_rate_limit_with_limiter,
};

impl<Clock: RateLimitClock, Effects: PrivacyFixEffectBoundary>
    PrivacyRewriteExecution<'_, Clock, Effects>
{
    pub(crate) fn plan_rewrite<'a, Lookup, Tables>(
        self,
        index_lookup: &Lookup,
        mut affected_tables: Tables,
    ) -> PrivacyFixResult<PrivacyFixReport>
    where
        Lookup: PrivacyRawBackupIndexLookup<'a>,
        Tables: PrivacyAffectedTables,
    {
        self.validate_read_only_roots()?;
        self.reserve_source_read()?;
        let mut tainted_objects = 0;
        let mut privacy_dropped_fields = 0;
        for object_id in &self.request.raw_backup_object_ids {
            let replay = self.load_private_object(index_lookup, object_id)?;
            privacy_dropped_fields += replay.metrics.policy_drops;
            if replay.metrics.policy_drops > 0 {
                tainted_objects += 1;
            }
            for record in &replay.records {
                affected_tables.observe(record.table_name.as_str());
            }
        }
        Ok(PrivacyFixReport::from_plan(
            self.request,
            affected_tables.finish(),
            tainted_objects,
            privacy_dropped_fields,
        ))
    }

    fn validate_read_only_roots(&self) -> PrivacyFixResult<()> {
        let source = self.source_store.canonical_root_read_only()?;
        let clean = FilesystemRawBackupStore::new(&self.request.clean_target_prefix)
            .canonical_root_read_only()?;
        let quarantine = FilesystemRawBackupStore::new(&self.request.quarantine_target_prefix)
            .canonical_root_read_only()?;
        PrivacyBackupRoots {
            source: &source,
            clean: &clean,
            quarantine: &quarantine,
        }
        .validate_disjoint()
    }

    fn reserve_source_read(&self) -> PrivacyFixResult<()> {
        enforce_rate_limit_with_limiter(
            self.limiter,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
            self.clock,
        )?;
        Ok(())
    }

    fn load_private_object<'a>(
        &self,
        index_lookup: &impl PrivacyRawBackupIndexLookup<'a>,
        object_id: &crate::RawBackupObjectId,
    ) -> PrivacyFixResult<crate::RawBackupReplayReport> {
        self.reserve_source_read()?;
        let index =
            ResolvedPrivacyRawBackupIndex::resolve(self.source_store, index_lookup, object_id)?;
        self.reserve_source_read()?;
        let replay = self
            .source_store
            .replay_object_report_with_privacy_policy(index.as_ref(), &self.request.policy)?;
        self.request.validate_replay_scope(&replay)?;
        Ok(replay)
    }
}
