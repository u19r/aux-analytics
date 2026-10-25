use super::*;

/// A validated, preflight-budgeted trim that can consume one source snapshot.
/// Private fields prevent deletion without passing request and rate checks.
pub(crate) struct TrimExecution<'a, Clock> {
    request: &'a TrimRequest,
    authority: &'a MaintenanceExecutionAuthority,
    limiter: &'a OperationRateLimiter,
    clock: &'a Clock,
}

impl<'a, Clock: RateLimitClock> TrimExecution<'a, Clock> {
    pub(crate) fn try_new(
        request: &'a TrimRequest,
        authority: &'a MaintenanceExecutionAuthority,
        limiter: &'a OperationRateLimiter,
        clock: &'a Clock,
    ) -> TrimResult<Self> {
        request.validate()?;
        preflight_rate_limit(request, limiter, clock)?;
        Ok(Self {
            request,
            authority,
            limiter,
            clock,
        })
    }

    pub(crate) fn execute(
        self,
        source: &impl TrimCandidateSource,
        destination: &mut impl TrimDestination,
    ) -> TrimResult<TrimReport> {
        let request = self.request;
        let candidate_snapshot =
            request.read_candidate_snapshot_at(source, self.limiter, self.clock)?;
        let candidate_rows = candidate_snapshot.candidate_rows();
        if request.dry_run {
            return Ok(TrimReport::from_dry_run(request, candidate_rows));
        }
        enforce_rate_limit_with_limiter(
            self.limiter,
            RateLimitUsage {
                rows: candidate_rows,
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
            self.clock,
        )?;
        let key = OperationChunkKey::for_operation(&request.operation_id, "trim", 0);
        let outcome = destination.delete_candidate_snapshot_with_key(
            self.authority,
            &key,
            request.table_name.as_str(),
            &request.target,
            &candidate_snapshot,
        )?;
        TrimReport::try_from_deletion(request, outcome)
    }
}

impl TrimRequest {
    fn read_candidate_snapshot_at<Clock: RateLimitClock>(
        &self,
        source: &impl TrimCandidateSource,
        limiter: &OperationRateLimiter,
        clock: &Clock,
    ) -> TrimResult<TrimCandidateSnapshot> {
        enforce_rate_limit_with_limiter(
            limiter,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
            clock,
        )?;
        source.candidate_snapshot(self.table_name.as_str(), &self.target)
    }
}
