use super::effects::GuardedPrivacyFixEffects;
use crate::{
    FilesystemRawBackupStore, MaintenanceResult, OperationActor, OperationRateLimiter,
    PrivacyFixError, PrivacyFixExecutor, PrivacyFixRequest, RateLimitPolicy,
    RawBackupDestinationKind, RawBackupError, RawBackupObjectIndex, SystemRateLimitClock,
    maintenance::{MaintenanceExecutionReport, MaintenanceOperations},
};

impl<'a> MaintenanceOperations<'a> {
    pub fn execute_privacy_fix_raw_backup(
        &self,
        request: &PrivacyFixRequest,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
        indexes: &[RawBackupObjectIndex],
        source_store: &FilesystemRawBackupStore,
        clean_store: &FilesystemRawBackupStore,
    ) -> MaintenanceResult<MaintenanceExecutionReport> {
        request.validate()?;
        if !request.dry_run {
            return Err(
                PrivacyFixError::RawBackup(RawBackupError::DestinationFenceUnavailable {
                    destination: RawBackupDestinationKind::Filesystem,
                })
                .into(),
            );
        }
        let submission =
            self.submit_privacy_fix_dry_run_with_rate_limit(request, actor, rate_limit.clone())?;
        let Some((expected, attempt)) = self.prepare_submission(&submission, actor)? else {
            return Ok(MaintenanceExecutionReport::PrivacyFix(
                self.store.privacy_fix_report(&submission.operation_id)?,
            ));
        };
        let (expected, effect_key) =
            self.begin_execution_effect(&expected, &attempt, actor, "privacy_fix")?;
        let limiter = OperationRateLimiter::new(rate_limit);
        let result = {
            let effects = self.execution_effects(&attempt);
            let mut privacy_effects = GuardedPrivacyFixEffects::new(&effects);
            crate::privacy_fix::PrivacyRewriteInput::try_new(
                request,
                source_store,
                clean_store,
                indexes,
            )
            .and_then(|input| {
                PrivacyFixExecutor.execute_raw_backup_rewrite_with_limiter_at(
                    &input,
                    &limiter,
                    &SystemRateLimitClock,
                    &mut privacy_effects,
                )
            })
        };
        let report = match result {
            Ok(report) => report,
            Err(error) => {
                return self.fail_execution_after_effect(&expected, &attempt, error.into());
            }
        };
        self.finish_execution_effect(
            &attempt,
            &effect_key,
            super::ExecutionReport {
                kind: crate::store::OperationReportKind::PrivacyFix,
                report: &report,
                counts: report.operation_counts(),
                message: "privacy fix completed",
            },
        )?;
        Ok(MaintenanceExecutionReport::PrivacyFix(report))
    }
}
