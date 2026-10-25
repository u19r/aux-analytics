use super::*;

impl LocalCheckExecutor {
    pub fn execute<Source, Destination>(
        &self,
        store: &OperationStore,
        request: &CheckRequest,
        source: &Source,
        destination: &Destination,
    ) -> CheckResult<CheckReport>
    where
        Source: SourceTruthRows,
        Destination: DestinationRows,
    {
        self.execute_at(store, request, source, destination, &SystemRateLimitClock)
    }

    pub fn execute_at<Source, Destination, Clock>(
        &self,
        store: &OperationStore,
        request: &CheckRequest,
        source: &Source,
        destination: &Destination,
        clock: &Clock,
    ) -> CheckResult<CheckReport>
    where
        Source: SourceTruthRows,
        Destination: DestinationRows,
        Clock: RateLimitClock,
    {
        request.validate()?;
        create_or_resume_operation(store, request)?;
        // Keep the exact snapshot paired with its lease. Row reads may outlive
        // the lease, so every later state write must prove this same attempt
        // still owns it.
        let attempt = store.prepare_for_execution(&request.operation_id, &request.actor)?;
        let expected = store.show_operation(&request.operation_id)?;
        let limiter = OperationRateLimiter::new(request.rate_limit.clone());

        if let Err(error) = enforce_rate_limit_with_limiter(
            &limiter,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
            clock,
        ) {
            return Err(pause_for_rate_limit(store, &expected, &attempt, error));
        }
        let source_rows = source
            .source_row_stream(&request.target_tables)
            .map_err(|error| {
                fail_after_error(
                    store,
                    &expected,
                    &attempt,
                    error,
                    "check source read failed",
                )
            })?;
        if let Err(error) = enforce_rate_limit_with_limiter(
            &limiter,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
            clock,
        ) {
            return Err(pause_for_rate_limit(store, &expected, &attempt, error));
        }
        let destination_rows = destination
            .destination_row_stream(&request.target_tables)
            .map_err(|error| {
                fail_after_error(
                    store,
                    &expected,
                    &attempt,
                    error,
                    "check destination read failed",
                )
            })?;
        let report =
            compare_row_streams(request, source_rows, destination_rows).map_err(|error| {
                fail_after_error(store, &expected, &attempt, error, "check comparison failed")
            })?;
        store.save_check_report_and_finish_for_attempt(&expected, &attempt, &report)?;
        Ok(report)
    }
}

pub(crate) fn fail_after_error(
    store: &OperationStore,
    expected: &StoredOperation,
    attempt: &ExecutionAttempt,
    error: CheckError,
    message: &'static str,
) -> CheckError {
    match store.transition_with_counts_for_attempt(
        expected,
        attempt,
        crate::OperationTransition {
            phase: OperationPhase::Executing,
            status: OperationStatus::Failed,
            cursor: expected.cursor.clone(),
            event_kind: OperationEventKind::Failed,
            counts: serde_json::json!({}),
            message: Some(message),
        },
    ) {
        Ok(()) => error,
        Err(persistence_error) => CheckError::FailurePersistence {
            operation_error: error.to_string(),
            persistence_error: persistence_error.to_string(),
        },
    }
}

pub(crate) fn pause_for_rate_limit(
    store: &OperationStore,
    expected: &StoredOperation,
    attempt: &ExecutionAttempt,
    error: RateLimitDenied,
) -> CheckError {
    let operation_error = CheckError::RateLimit(error);
    let mut progress = expected.progress.clone();
    if let Some(fields) = progress.as_object_mut() {
        fields.insert("retryable".to_string(), serde_json::json!(true));
        fields.insert(
            "rate_limit_wait_ms".to_string(),
            serde_json::json!(error.wait_ms),
        );
    }
    match store.transition_with_counts_for_attempt(
        expected,
        attempt,
        crate::OperationTransition {
            phase: OperationPhase::Executing,
            status: OperationStatus::Paused,
            cursor: expected.cursor.clone(),
            event_kind: OperationEventKind::Paused,
            counts: progress,
            message: Some("check rate limit denied; operation paused for retry"),
        },
    ) {
        Ok(()) => operation_error,
        Err(persistence_error) => CheckError::FailurePersistence {
            operation_error: operation_error.to_string(),
            persistence_error: persistence_error.to_string(),
        },
    }
}

pub(crate) fn create_or_resume_operation(
    store: &OperationStore,
    request: &CheckRequest,
) -> CheckResult<()> {
    let operation_request = OperationRequest {
        operation_id: request.operation_id.clone(),
        kind: OperationKind::Check,
        actor: request.actor.clone(),
        target_tables: request.target_tables.clone(),
        dry_run: true,
        rate_limit: request.rate_limit.clone(),
        payload: serde_json::json!({
            "mode": "local_check_execution",
            "strategy": request.strategy,
            "sample_limit": request.sample_limit,
            "privacy_policy_version": request.privacy_policy_version,
            "output": request.output,
        }),
    };
    store
        .create_or_verify_operation(&operation_request)
        .map(|_| ())
        .map_err(Into::into)
}
