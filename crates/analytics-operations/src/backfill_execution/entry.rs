use super::*;

/// A validated operation request bound to its snapshot and catch-up sources.
pub struct BackfillExecutionInput<'a, Snapshots, Stream> {
    store: &'a OperationStore,
    request: &'a BackfillExecutionRequest,
    snapshot_source: &'a Snapshots,
    stream_source: &'a Stream,
}

impl<'a, Snapshots, Stream> BackfillExecutionInput<'a, Snapshots, Stream> {
    pub fn try_new(
        store: &'a OperationStore,
        request: &'a BackfillExecutionRequest,
        snapshot_source: &'a Snapshots,
        stream_source: &'a Stream,
    ) -> BackfillExecutionResult<Self> {
        request.validate()?;
        Ok(Self {
            store,
            request,
            snapshot_source,
            stream_source,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalBackfillExecutor;

impl LocalBackfillExecutor {
    pub fn execute_with_privacy_policy<Snapshots, Stream>(
        &self,
        input: &BackfillExecutionInput<'_, Snapshots, Stream>,
        policy: &PrivacyPolicy,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
    {
        let mut destination = CountingBackfillDestinationWriter;
        self.execute_with_privacy_policy_to_destination(input, &mut destination, policy)
    }

    pub fn execute_with_privacy_policy_to_destination<Snapshots, Stream, Destination>(
        &self,
        input: &BackfillExecutionInput<'_, Snapshots, Stream>,
        destination: &mut Destination,
        policy: &PrivacyPolicy,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
        Destination: BackfillDestinationWriter,
    {
        self.execute_with_privacy_policy_to_destination_at(
            input,
            destination,
            policy,
            &SystemRateLimitClock,
        )
    }

    pub fn execute_with_privacy_policy_to_destination_at<Snapshots, Stream, Destination, Clock>(
        &self,
        input: &BackfillExecutionInput<'_, Snapshots, Stream>,
        destination: &mut Destination,
        policy: &PrivacyPolicy,
        clock: &Clock,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
        Destination: BackfillDestinationWriter,
        Clock: RateLimitClock,
    {
        let BackfillExecutionInput {
            store,
            request,
            snapshot_source,
            stream_source,
        } = *input;
        policy.validate()?;
        create_or_resume_operation(store, request, Some(policy))?;
        let existing = store.show_operation(&request.operation_id)?;
        if existing.status == OperationStatus::Succeeded {
            return Ok(store.backfill_report(&request.operation_id)?);
        }
        let attempt = store.prepare_for_execution(&request.operation_id, &request.actor)?;
        let prepared = store.show_operation(&request.operation_id)?;
        let limiter = OperationRateLimiter::new(request.rate_limit.clone());
        let execution = execution_loop::BackfillExecution {
            store,
            request,
            destination,
            attempt,
            clock,
            limiter: &limiter,
        };
        let (chunks, chunk_drops) = execution.fetch_private_snapshot(snapshot_source, policy)?;
        let (stream_updates, stream_drops) =
            execution.fetch_private_stream(stream_source, policy)?;
        let privacy_metrics = PrivacyBackfillMetrics {
            dropped_fields: chunk_drops.dropped_fields + stream_drops.dropped_fields,
            dropped_records: chunk_drops.dropped_records + stream_drops.dropped_records,
        };
        execution.execute(execution_loop::PreparedBackfillRows {
            chunks: &chunks,
            stream_updates: &stream_updates,
            privacy_policy_version: Some(policy.version.clone()),
            privacy_metrics,
            initial_status: prepared.status,
        })
    }

    pub fn execute<Snapshots, Stream>(
        &self,
        store: &OperationStore,
        request: &BackfillExecutionRequest,
        snapshot_source: &Snapshots,
        stream_source: &Stream,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
    {
        let mut destination = CountingBackfillDestinationWriter;
        self.execute_to_destination_at(
            &BackfillExecutionInput::try_new(store, request, snapshot_source, stream_source)?,
            &mut destination,
            &SystemRateLimitClock,
        )
    }

    pub fn execute_to_destination<Snapshots, Stream, Destination>(
        &self,
        input: &BackfillExecutionInput<'_, Snapshots, Stream>,
        destination: &mut Destination,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
        Destination: BackfillDestinationWriter,
    {
        self.execute_to_destination_at(input, destination, &SystemRateLimitClock)
    }

    pub fn execute_to_destination_at<Snapshots, Stream, Destination, Clock>(
        &self,
        input: &BackfillExecutionInput<'_, Snapshots, Stream>,
        destination: &mut Destination,
        clock: &Clock,
    ) -> BackfillExecutionResult<BackfillExecutionReport>
    where
        Snapshots: SnapshotChunkSource,
        Stream: StreamCatchupSource,
        Destination: BackfillDestinationWriter,
        Clock: RateLimitClock,
    {
        let BackfillExecutionInput {
            store,
            request,
            snapshot_source,
            stream_source,
        } = *input;
        create_or_resume_operation(store, request, None)?;
        let existing = store.show_operation(&request.operation_id)?;
        if existing.status == OperationStatus::Succeeded {
            return Ok(store.backfill_report(&request.operation_id)?);
        }
        let attempt = store.prepare_for_execution(&request.operation_id, &request.actor)?;
        let prepared = store.show_operation(&request.operation_id)?;
        let limiter = OperationRateLimiter::new(request.rate_limit.clone());
        let execution = execution_loop::BackfillExecution {
            store,
            request,
            destination,
            attempt,
            clock,
            limiter: &limiter,
        };
        let chunks = execution.fetch_snapshot(
            snapshot_source,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
        )?;
        let stream_updates = execution.fetch_stream(
            stream_source,
            RateLimitUsage {
                source_requests: 1,
                ..RateLimitUsage::default()
            },
        )?;
        execution.execute(execution_loop::PreparedBackfillRows {
            chunks: &chunks,
            stream_updates: &stream_updates,
            privacy_policy_version: None,
            privacy_metrics: PrivacyBackfillMetrics::default(),
            initial_status: prepared.status,
        })
    }
}
