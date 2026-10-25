use super::{
    contracts::{
        TableFixError, TableFixReport, TableFixRequest, TableFixResult, TableFixSelection,
        TableFixSource, VerifiedTableFixDestination, VersionedTableFixCurrentSource,
    },
    verification::{
        ValidatedTableFixSnapshot, check_report_mismatch_keys, current_source_identity,
        preflight_rate_limit, selected_row_keys,
    },
};
use crate::{
    CheckReport, MaintenanceExecutionAuthority, OperationRateLimiter, RateLimitClock,
    RateLimitUsage, enforce_rate_limit_with_limiter,
};

pub(crate) struct TableFixExecutor<'a, Store: ?Sized> {
    pub(super) raw_backup_store: &'a Store,
    pub(super) authority: MaintenanceExecutionAuthority,
}

impl<'a, Store: ?Sized> TableFixExecutor<'a, Store> {
    #[must_use]
    pub(crate) fn new(
        raw_backup_store: &'a Store,
        authority: MaintenanceExecutionAuthority,
    ) -> Self {
        Self {
            raw_backup_store,
            authority,
        }
    }

    /// Execute a check-report repair through the revision-aware destination
    /// contract.
    ///
    /// Check reports identify the selected keys, while the current source still
    /// supplies the authoritative revision and row values immediately
    /// before the repair. This keeps the check-report path subject to the
    /// same freshness and post-write verification guarantees as an explicit
    /// current-source selection.
    pub(crate) fn execute_check_report_mismatches_verified_with_limiter_at<
        Source,
        Destination,
        Clock: RateLimitClock,
    >(
        &self,
        request: &TableFixRequest,
        check_report: &CheckReport,
        source: &Source,
        destination: &mut Destination,
        limiter: &OperationRateLimiter,
        clock: &Clock,
    ) -> TableFixResult<TableFixReport>
    where
        Source: VersionedTableFixCurrentSource,
        Destination: VerifiedTableFixDestination,
    {
        request.validate()?;
        preflight_rate_limit(request, limiter, clock)?;
        let TableFixSelection::CheckReport {
            operation_id: selection_operation_id,
        } = &request.selection
        else {
            return Err(TableFixError::UnsupportedSelection);
        };
        let TableFixSource::CheckReportMismatchSet { operation_id } = &request.source else {
            return Err(TableFixError::UnsupportedSource);
        };
        if operation_id != &check_report.operation_id
            || selection_operation_id != &check_report.operation_id
        {
            return Err(TableFixError::UnsupportedSource);
        }
        let selected_keys = check_report_mismatch_keys(check_report, request.table_name.as_str());
        enforce_rate_limit_with_limiter(
            limiter,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
            clock,
        )?;
        let snapshot = source.read_truth_snapshot(
            "check_report_mismatch_set",
            request.table_name.as_str(),
            selected_keys.as_slice(),
        )?;
        ValidatedTableFixSnapshot::try_new(request, selected_keys.as_slice(), snapshot)?.execute(
            destination,
            &self.authority,
            limiter,
            clock,
        )
    }

    pub(crate) fn execute_current_source_verified_with_limiter_at<
        Source,
        Destination,
        Clock: RateLimitClock,
    >(
        &self,
        request: &TableFixRequest,
        source: &Source,
        destination: &mut Destination,
        limiter: &OperationRateLimiter,
        clock: &Clock,
    ) -> TableFixResult<TableFixReport>
    where
        Source: VersionedTableFixCurrentSource,
        Destination: VerifiedTableFixDestination,
    {
        request.validate()?;
        preflight_rate_limit(request, limiter, clock)?;
        let selected_keys = selected_row_keys(&request.selection)?;
        let source_identity = current_source_identity(&request.source)?;
        enforce_rate_limit_with_limiter(
            limiter,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
            clock,
        )?;
        let snapshot = source.read_truth_snapshot(
            source_identity,
            request.table_name.as_str(),
            selected_keys,
        )?;
        ValidatedTableFixSnapshot::try_new(request, selected_keys, snapshot)?.execute(
            destination,
            &self.authority,
            limiter,
            clock,
        )
    }
}
