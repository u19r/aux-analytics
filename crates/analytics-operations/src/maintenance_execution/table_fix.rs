use super::effects::GuardedTableFixDestination;
use crate::{
    CheckOutcome, CheckReport, FilesystemRawBackupStore, MaintenanceError, MaintenanceResult,
    OperationActor, OperationChunkKey, OperationKind, OperationRateLimiter, RateLimitPolicy,
    RateLimitUsage, RawBackupObjectIndex, RawBackupObjectStore, StoredOperation,
    SystemRateLimitClock, TableFixExecutor, TableFixRequest, TableFixSelection, TableFixSource,
    VerifiedTableFixDestination, VersionedTableFixCurrentSource, enforce_rate_limit_with_limiter,
    maintenance::{MaintenanceExecutionReport, MaintenanceOperations, MaintenanceRequest},
};

fn raw_backup_object_indexes(
    request: &TableFixRequest,
    store: &FilesystemRawBackupStore,
) -> MaintenanceResult<Vec<RawBackupObjectIndex>> {
    let TableFixSource::RawBackup { object_ids } = &request.source else {
        return Err(MaintenanceError::TableFix(
            crate::TableFixError::UnsupportedSource,
        ));
    };
    object_ids
        .iter()
        .map(|object_id| {
            store
                .read_index(object_id)
                .map_err(MaintenanceError::RawBackup)
        })
        .collect()
}

fn verified_check_report(
    store: &crate::OperationStore,
    request: &TableFixRequest,
    actor: &OperationActor,
) -> MaintenanceResult<CheckReport> {
    let TableFixSelection::CheckReport {
        operation_id: selection_operation_id,
    } = &request.selection
    else {
        return Err(MaintenanceError::TableFix(
            crate::TableFixError::UnsupportedSelection,
        ));
    };
    let TableFixSource::CheckReportMismatchSet {
        operation_id: source_operation_id,
    } = &request.source
    else {
        return Err(MaintenanceError::TableFix(
            crate::TableFixError::UnsupportedSource,
        ));
    };
    if selection_operation_id != source_operation_id {
        return Err(MaintenanceError::TableFix(
            crate::TableFixError::CheckReportProvenanceMismatch,
        ));
    }
    let report = store.check_report(selection_operation_id)?;
    let check_operation = store.show_operation(selection_operation_id)?;
    if report.operation_id != *selection_operation_id
        || check_operation.operation_id != *selection_operation_id
        || check_operation.kind != OperationKind::Check
        || check_operation.actor != *actor
        || check_operation.target_tables.len() != 1
        || check_operation.target_tables.first() != Some(&request.table_name)
        || report.outcome != CheckOutcome::Mismatched
    {
        return Err(MaintenanceError::TableFix(
            crate::TableFixError::CheckReportProvenanceMismatch,
        ));
    }
    Ok(report)
}

impl<'a> MaintenanceOperations<'a> {
    /// Canonical current-source table-fix path. The source supplies a revision
    /// and the destination supplies keyed, post-write verification, so
    /// retries cannot silently apply truth from a newer source snapshot or
    /// report an unverified repair as successful.
    pub fn execute_table_fix_current_source_verified<Source, Destination>(
        &self,
        request: &TableFixRequest,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
        source: &Source,
        destination: &mut Destination,
    ) -> MaintenanceResult<MaintenanceExecutionReport>
    where
        Source: VersionedTableFixCurrentSource,
        Destination: VerifiedTableFixDestination,
    {
        if !matches!(request.source, TableFixSource::CurrentSource { .. }) {
            return Err(MaintenanceError::UnsupportedExecutor {
                kind: OperationKind::TableFix,
            });
        }
        let submission = self.submit_with_rate_limit(
            &MaintenanceRequest::table_fix(request.clone()),
            actor,
            rate_limit.clone(),
        )?;
        let Some((expected, attempt)) = self.prepare_submission(&submission, actor)? else {
            return Ok(MaintenanceExecutionReport::TableFix(
                self.store.table_fix_report(&submission.operation_id)?,
            ));
        };
        let (expected, effect_key) =
            self.begin_execution_effect(&expected, &attempt, actor, "table_fix")?;
        let authority = attempt
            .maintenance_authority()
            .map_err(crate::OperationStoreError::Type)
            .map_err(MaintenanceError::Store)?;
        let limiter = OperationRateLimiter::new(rate_limit);
        let result = {
            let effects = self.execution_effects(&attempt);
            let mut guarded_destination = GuardedTableFixDestination {
                destination,
                effects: &effects,
                authority: authority.clone(),
            };
            TableFixExecutor::new(source, authority)
                .execute_current_source_verified_with_limiter_at(
                    request,
                    source,
                    &mut guarded_destination,
                    &limiter,
                    &SystemRateLimitClock,
                )
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
                kind: crate::store::OperationReportKind::TableFix,
                report: &report,
                counts: report.operation_counts(),
                message: "table fix completed",
            },
        )?;
        Ok(MaintenanceExecutionReport::TableFix(report))
    }

    /// Canonical raw-backup table-fix path with a revision derived from the
    /// replayed source indexes, keyed idempotent repairs, and destination
    /// post-write verification.
    pub fn execute_table_fix_raw_backup_verified<Store, Destination>(
        &self,
        request: &TableFixRequest,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
        indexes: &[RawBackupObjectIndex],
        raw_backup_store: &Store,
        destination: &mut Destination,
    ) -> MaintenanceResult<MaintenanceExecutionReport>
    where
        Store: RawBackupObjectStore,
        Destination: VerifiedTableFixDestination,
    {
        if !matches!(request.source, TableFixSource::RawBackup { .. }) {
            return Err(MaintenanceError::UnsupportedExecutor {
                kind: OperationKind::TableFix,
            });
        }
        let submission = self.submit_with_rate_limit(
            &MaintenanceRequest::table_fix(request.clone()),
            actor,
            rate_limit.clone(),
        )?;
        let Some((expected, attempt)) = self.prepare_submission(&submission, actor)? else {
            return Ok(MaintenanceExecutionReport::TableFix(
                self.store.table_fix_report(&submission.operation_id)?,
            ));
        };
        let (expected, effect_key) =
            self.begin_execution_effect(&expected, &attempt, actor, "table_fix")?;
        let limiter = OperationRateLimiter::new(rate_limit);
        self.execute_table_fix_raw_backup_prepared(
            RawBackupTableFixInput {
                request,
                limiter: &limiter,
                indexes,
                raw_backup_store,
            },
            destination,
            PreparedTableFixEffect {
                expected,
                effect_key,
                attempt,
            },
        )
    }

    /// Canonical raw-backup table-fix entry point for callers that have not
    /// read source indexes yet. Submission, authorization, and the effect
    /// fence happen before those source reads.
    pub fn execute_table_fix_raw_backup_verified_from_store<Destination, Factory>(
        &self,
        request: &TableFixRequest,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
        raw_backup_store: &FilesystemRawBackupStore,
        make_destination: Factory,
    ) -> MaintenanceResult<MaintenanceExecutionReport>
    where
        Destination: VerifiedTableFixDestination,
        Factory: FnOnce() -> MaintenanceResult<Destination>,
    {
        if !matches!(request.source, TableFixSource::RawBackup { .. }) {
            return Err(MaintenanceError::UnsupportedExecutor {
                kind: OperationKind::TableFix,
            });
        }
        let submission = self.submit_with_rate_limit(
            &MaintenanceRequest::table_fix(request.clone()),
            actor,
            rate_limit.clone(),
        )?;
        let Some((expected, attempt)) = self.prepare_submission(&submission, actor)? else {
            return Ok(MaintenanceExecutionReport::TableFix(
                self.store.table_fix_report(&submission.operation_id)?,
            ));
        };
        let (expected, effect_key) =
            self.begin_execution_effect(&expected, &attempt, actor, "table_fix")?;
        let limiter = OperationRateLimiter::new(rate_limit);
        if let Err(error) = enforce_rate_limit_with_limiter(
            &limiter,
            RateLimitUsage {
                rows: u64::from(!request.dry_run),
                source_requests: 1,
                destination_writes: u64::from(!request.dry_run),
                ..RateLimitUsage::default()
            },
            &SystemRateLimitClock,
        ) {
            return self.fail_execution_after_effect(&expected, &attempt, error.into());
        }
        let indexes = match raw_backup_object_indexes(request, raw_backup_store) {
            Ok(indexes) => indexes,
            Err(error) => {
                return self.fail_execution_after_effect(&expected, &attempt, error);
            }
        };
        // Destination construction can open a manifest and database. Keep it
        // behind the durable submit/prepare/fence and source-read gates so
        // invalid, cancelled, or denied requests cannot perform destination
        // I/O. A construction failure is retryable and leaves the operation
        // paused through the same recovery path as executor failures.
        let mut destination = match make_destination() {
            Ok(destination) => destination,
            Err(error) => {
                return self.fail_execution_after_effect(&expected, &attempt, error);
            }
        };
        self.execute_table_fix_raw_backup_prepared(
            RawBackupTableFixInput {
                request,
                limiter: &limiter,
                indexes: indexes.as_slice(),
                raw_backup_store,
            },
            &mut destination,
            PreparedTableFixEffect {
                expected,
                effect_key,
                attempt,
            },
        )
    }

    fn execute_table_fix_raw_backup_prepared<Store, Destination>(
        &self,
        input: RawBackupTableFixInput<'_, Store>,
        destination: &mut Destination,
        execution: PreparedTableFixEffect,
    ) -> MaintenanceResult<MaintenanceExecutionReport>
    where
        Store: RawBackupObjectStore,
        Destination: VerifiedTableFixDestination,
    {
        let RawBackupTableFixInput {
            request,
            limiter,
            indexes,
            raw_backup_store,
        } = input;
        let PreparedTableFixEffect {
            expected,
            effect_key,
            attempt,
        } = execution;
        let authority = attempt
            .maintenance_authority()
            .map_err(crate::OperationStoreError::Type)
            .map_err(MaintenanceError::Store)?;
        let result = {
            let effects = self.execution_effects(&attempt);
            let mut guarded_destination = GuardedTableFixDestination {
                destination,
                effects: &effects,
                authority: authority.clone(),
            };
            TableFixExecutor::new(raw_backup_store, authority)
                .execute_raw_backup_verified_with_limiter_at(
                    request,
                    indexes,
                    &mut guarded_destination,
                    limiter,
                    &SystemRateLimitClock,
                )
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
                kind: crate::store::OperationReportKind::TableFix,
                report: &report,
                counts: report.operation_counts(),
                message: "table fix completed",
            },
        )?;
        Ok(MaintenanceExecutionReport::TableFix(report))
    }

    /// Canonical check-report table-fix path. The report selects the keys, the
    /// versioned source supplies fresh truth, and the destination makes
    /// each repair idempotent and verifiable.
    pub fn execute_table_fix_check_report_verified<Source, Destination>(
        &self,
        request: &TableFixRequest,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
        source: &Source,
        destination: &mut Destination,
    ) -> MaintenanceResult<MaintenanceExecutionReport>
    where
        Source: VersionedTableFixCurrentSource,
        Destination: VerifiedTableFixDestination,
    {
        if !matches!(
            request.source,
            TableFixSource::CheckReportMismatchSet { .. }
        ) {
            return Err(MaintenanceError::UnsupportedExecutor {
                kind: OperationKind::TableFix,
            });
        }
        request.validate().map_err(MaintenanceError::TableFix)?;
        let check_report = verified_check_report(self.store, request, actor)?;
        let submission = self.submit_with_rate_limit(
            &MaintenanceRequest::table_fix(request.clone()),
            actor,
            rate_limit.clone(),
        )?;
        let Some((expected, attempt)) = self.prepare_submission(&submission, actor)? else {
            return Ok(MaintenanceExecutionReport::TableFix(
                self.store.table_fix_report(&submission.operation_id)?,
            ));
        };
        let (expected, effect_key) =
            self.begin_execution_effect(&expected, &attempt, actor, "table_fix")?;
        let authority = attempt
            .maintenance_authority()
            .map_err(crate::OperationStoreError::Type)
            .map_err(MaintenanceError::Store)?;
        let limiter = OperationRateLimiter::new(rate_limit);
        let result = {
            let effects = self.execution_effects(&attempt);
            let mut guarded_destination = GuardedTableFixDestination {
                destination,
                effects: &effects,
                authority: authority.clone(),
            };
            TableFixExecutor::new(source, authority)
                .execute_check_report_mismatches_verified_with_limiter_at(
                    request,
                    &check_report,
                    source,
                    &mut guarded_destination,
                    &limiter,
                    &SystemRateLimitClock,
                )
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
                kind: crate::store::OperationReportKind::TableFix,
                report: &report,
                counts: report.operation_counts(),
                message: "table fix completed",
            },
        )?;
        Ok(MaintenanceExecutionReport::TableFix(report))
    }
}

struct RawBackupTableFixInput<'a, Store> {
    request: &'a TableFixRequest,
    limiter: &'a OperationRateLimiter,
    indexes: &'a [RawBackupObjectIndex],
    raw_backup_store: &'a Store,
}
struct PreparedTableFixEffect {
    expected: StoredOperation,
    effect_key: OperationChunkKey,
    attempt: crate::ExecutionAttempt,
}
