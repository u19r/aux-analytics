use super::effects::GuardedTrimDestination;
use crate::{
    MaintenanceResult, OperationActor, OperationRateLimiter, RateLimitPolicy, SystemRateLimitClock,
    TrimCandidateSource, TrimDestination, TrimRequest,
    maintenance::{MaintenanceExecutionReport, MaintenanceOperations, MaintenanceRequest},
    trim::TrimExecution,
};

impl<'a> MaintenanceOperations<'a> {
    pub fn execute_trim<Source, Destination>(
        &self,
        request: &TrimRequest,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
        source: &Source,
        destination: &mut Destination,
    ) -> MaintenanceResult<MaintenanceExecutionReport>
    where
        Source: TrimCandidateSource,
        Destination: TrimDestination,
    {
        let submission = self.submit_with_rate_limit(
            &MaintenanceRequest::trim(request.clone()),
            actor,
            rate_limit.clone(),
        )?;
        let Some((expected, attempt)) = self.prepare_submission(&submission, actor)? else {
            return Ok(MaintenanceExecutionReport::Trim(
                self.store.trim_report(&submission.operation_id)?,
            ));
        };
        let (expected, effect_key) =
            self.begin_execution_effect(&expected, &attempt, actor, "trim")?;
        let authority = attempt
            .maintenance_authority()
            .map_err(crate::OperationStoreError::Type)
            .map_err(crate::MaintenanceError::Store)?;
        let limiter = OperationRateLimiter::new(rate_limit);
        let result = {
            let effects = self.execution_effects(&attempt);
            let mut guarded_destination = GuardedTrimDestination {
                destination,
                effects: &effects,
                authority: authority.clone(),
            };
            TrimExecution::try_new(request, &authority, &limiter, &SystemRateLimitClock)
                .and_then(|execution| execution.execute(source, &mut guarded_destination))
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
                kind: crate::store::OperationReportKind::Trim,
                report: &report,
                counts: report.operation_counts(),
                message: "trim completed",
            },
        )?;
        Ok(MaintenanceExecutionReport::Trim(report))
    }
}
