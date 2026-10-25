use super::{
    IndexedPrivacyRawBackupIndexes, LinearPrivacyAffectedTables, PrivacyFixEffectBoundary,
    PrivacyFixExecutor, PrivacyFixReport, PrivacyFixRequest, PrivacyFixResult,
};
#[cfg(test)]
use crate::RateLimitPolicy;
use crate::{FilesystemRawBackupStore, OperationRateLimiter, RateLimitClock, RawBackupObjectIndex};

pub(crate) struct PrivacyRewriteInput<'a> {
    pub(crate) request: &'a PrivacyFixRequest,
    pub(crate) source_store: &'a FilesystemRawBackupStore,
    pub(crate) clean_store: &'a FilesystemRawBackupStore,
    pub(crate) indexes: &'a [RawBackupObjectIndex],
}
impl<'a> PrivacyRewriteInput<'a> {
    pub(crate) fn try_new(
        request: &'a PrivacyFixRequest,
        source_store: &'a FilesystemRawBackupStore,
        clean_store: &'a FilesystemRawBackupStore,
        indexes: &'a [RawBackupObjectIndex],
    ) -> PrivacyFixResult<Self> {
        request.validate()?;
        Ok(Self {
            request,
            source_store,
            clean_store,
            indexes,
        })
    }
}

pub(crate) struct PrivacyRewriteExecution<'a, Clock, Effects> {
    pub(crate) request: &'a PrivacyFixRequest,
    pub(crate) source_store: &'a FilesystemRawBackupStore,
    pub(crate) clean_store: &'a FilesystemRawBackupStore,
    pub(crate) limiter: &'a OperationRateLimiter,
    pub(crate) clock: &'a Clock,
    pub(crate) effects: &'a mut Effects,
}
impl PrivacyFixExecutor {
    #[cfg(test)]
    pub(crate) fn execute_raw_backup_rewrite_with_effects_at<Clock: RateLimitClock>(
        &self,
        input: &PrivacyRewriteInput<'_>,
        rate_limit: &RateLimitPolicy,
        clock: &Clock,
        effects: &mut impl PrivacyFixEffectBoundary,
    ) -> PrivacyFixResult<PrivacyFixReport> {
        let limiter = OperationRateLimiter::new(rate_limit.clone());
        self.execute_raw_backup_rewrite_with_limiter_at(input, &limiter, clock, effects)
    }
    pub(crate) fn execute_raw_backup_rewrite_with_limiter_at<Clock: RateLimitClock>(
        &self,
        input: &PrivacyRewriteInput<'_>,
        limiter: &OperationRateLimiter,
        clock: &Clock,
        effects: &mut impl PrivacyFixEffectBoundary,
    ) -> PrivacyFixResult<PrivacyFixReport> {
        let index_lookup = IndexedPrivacyRawBackupIndexes::new(input.indexes);
        PrivacyRewriteExecution {
            request: input.request,
            source_store: input.source_store,
            clean_store: input.clean_store,
            limiter,
            clock,
            effects,
        }
        .rewrite(&index_lookup, LinearPrivacyAffectedTables::default())
    }
}
