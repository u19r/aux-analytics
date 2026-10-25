use std::time::Instant;

use aws_sdk_dynamodbstreams::operation::get_records::{GetRecordsError, GetRecordsOutput};
use tokio::task::JoinSet;

use super::state::{
    AwsDiscoverySchedule, AwsStreamResponseBatchInput, AwsStreamTablePoller,
    apply_aws_iterator_checkpoint, aws_stream_response_batch, empty_poll_batch,
    iterator_checkpoint_shard_id,
};
use crate::{
    aws_stream::{AwsShardDiscoveryRequest, aws_stream_record_to_contract},
    error::{
        AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
        AnalyticsStorageResult,
    },
    source_batch::{PollBatch, SourceCheckpoint},
};

enum AwsGetRecordsOutcome {
    Response(GetRecordsOutput),
    Expired,
}

impl AwsGetRecordsOutcome {
    async fn fetch(
        client: &aws_sdk_dynamodbstreams::Client,
        iterator: String,
        limit: i32,
    ) -> AnalyticsStorageResult<Self> {
        match client
            .get_records()
            .shard_iterator(iterator)
            .limit(limit)
            .send()
            .await
        {
            Ok(response) => Ok(AwsGetRecordsOutcome::Response(response)),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(GetRecordsError::is_expired_iterator_exception) =>
            {
                Ok(AwsGetRecordsOutcome::Expired)
            }
            Err(error) => Err(AnalyticsStorageError::with_debug(
                AnalyticsStorageErrorKind::AwsSdk,
                AnalyticsStorageErrorDebug::AwsSdk(error.to_string()),
            )),
        }
    }
}

type AwsRecordTasks = JoinSet<(usize, String, AnalyticsStorageResult<AwsGetRecordsOutcome>)>;

#[derive(Default)]
struct AwsPollResponses {
    shard_batches: Vec<(usize, PollBatch)>,
    terminal_empty_shards: Vec<String>,
    terminal_record_shards: Vec<String>,
    expired_shards: Vec<String>,
    error: Option<AnalyticsStorageError>,
    failed_shard_id: Option<String>,
}

impl AwsPollResponses {
    fn record_batch(
        &mut self,
        selection: (usize, String),
        batch: PollBatch,
        terminal: bool,
        pending_terminal_shards: &std::collections::BTreeSet<String>,
    ) {
        let (selection_order, shard_id) = selection;
        let batch_has_records = batch.source_record_count > 0;
        self.shard_batches.push((selection_order, batch));
        if terminal {
            if !batch_has_records {
                if !pending_terminal_shards.contains(shard_id.as_str()) {
                    self.terminal_empty_shards.push(shard_id);
                }
                // A prior terminal page with records still needs its
                // durable checkpoint before discovery may move on.
            } else {
                self.terminal_record_shards.push(shard_id);
            }
        }
    }

    fn record_failure(&mut self, shard_id: Option<String>, error: AnalyticsStorageError) {
        if self.failed_shard_id.is_none() {
            self.failed_shard_id = shard_id;
        }
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

impl AwsStreamTablePoller {
    pub(super) async fn poll_once(
        &mut self,
        max_responses: usize,
        now: Instant,
    ) -> AnalyticsStorageResult<PollBatch> {
        if let Some(batch) = self.pending_batches.pop_front() {
            return Ok(batch);
        }
        self.fill_startup_iterators(now).await?;
        if self.shard_iterators.is_empty() {
            return Ok(empty_poll_batch());
        }
        let (next_shard_id, tasks) = self.schedule_record_tasks(max_responses);
        let mut responses = self.collect_responses(tasks).await;
        let topology_changed = self.apply_response_topology(&mut responses, now);
        let expired_retry_result = self.retry_expired_shards(now).await;
        self.finish_poll(
            responses,
            next_shard_id,
            topology_changed,
            expired_retry_result,
        )
    }

    fn schedule_record_tasks(&self, max_responses: usize) -> (Option<String>, AwsRecordTasks) {
        let mut get_records_tasks = JoinSet::new();
        let active_iterator_count = max_responses.min(self.shard_iterators.len());
        let selected_indices = self.selected_shard_indices(active_iterator_count);
        let next_shard_id = selected_indices.last().and_then(|last_index| {
            self.shard_iterators
                .get((*last_index + 1) % self.shard_iterators.len())
                .map(|state| state.shard_id.clone())
        });
        for (selection_order, state_index) in selected_indices.into_iter().enumerate() {
            let state = &self.shard_iterators[state_index];
            let shard_id = state.shard_id.clone();
            let iterator = state.iterator.clone();
            let client = self.client.clone_records_client();
            let limit = self.max_records_per_response as i32;
            get_records_tasks.spawn(async move {
                let outcome = AwsGetRecordsOutcome::fetch(&client, iterator, limit).await;
                (selection_order, shard_id, outcome)
            });
        }
        (next_shard_id, get_records_tasks)
    }

    async fn collect_responses(&mut self, mut tasks: AwsRecordTasks) -> AwsPollResponses {
        let mut responses = AwsPollResponses::default();
        while let Some(joined) = tasks.join_next().await {
            let (selection_order, shard_id, outcome) = match joined {
                Ok((selection_order, shard_id, Ok(outcome))) => {
                    (selection_order, shard_id, outcome)
                }
                Ok((_selection_order, shard_id, Err(error))) => {
                    responses.record_failure(Some(shard_id), error);
                    continue;
                }
                Err(error) => {
                    responses.record_failure(
                        None,
                        AnalyticsStorageError::with_debug(
                            AnalyticsStorageErrorKind::AwsSdk,
                            AnalyticsStorageErrorDebug::AwsSdk(error.to_string()),
                        ),
                    );
                    continue;
                }
            };
            let AwsGetRecordsOutcome::Response(response) = outcome else {
                responses.expired_shards.push(shard_id);
                continue;
            };
            let terminal = response.next_shard_iterator().is_none();
            let batch = match self.convert_response(&shard_id, response) {
                Ok(batch) => batch,
                Err(error) => {
                    responses.record_failure(Some(shard_id), error);
                    continue;
                }
            };
            responses.record_batch(
                (selection_order, shard_id),
                batch,
                terminal,
                &self.pending_terminal_shards,
            );
        }
        responses
    }

    pub(super) fn convert_response(
        &mut self,
        shard_id: &str,
        response: GetRecordsOutput,
    ) -> AnalyticsStorageResult<PollBatch> {
        let contract_records = response
            .records
            .unwrap_or_default()
            .into_iter()
            .map(aws_stream_record_to_contract)
            .collect::<AnalyticsStorageResult<Vec<_>>>()?;
        aws_stream_response_batch(
            AwsStreamResponseBatchInput {
                source_table_name: self.source_table_name.as_str(),
                analytics_table_names: &self.analytics_table_names,
                shard_id,
                source_predecessor: self
                    .durable_checkpoints
                    .iter()
                    .find(|checkpoint| checkpoint.shard_id == shard_id)
                    .cloned(),
                contract_records,
                next_iterator: response.next_shard_iterator,
            },
            &mut self.serialization_buffer,
        )
    }

    fn apply_response_topology(&mut self, responses: &mut AwsPollResponses, now: Instant) -> bool {
        let topology_changed =
            !responses.terminal_empty_shards.is_empty() || !responses.expired_shards.is_empty();
        for shard_id in responses.terminal_record_shards.drain(..) {
            self.pending_terminal_shards.insert(shard_id);
        }
        for shard_id in responses.terminal_empty_shards.drain(..) {
            if !self.pending_terminal_shards.contains(shard_id.as_str()) {
                self.shard_iterators
                    .retain(|state| state.shard_id != shard_id);
                self.discovery_schedule.schedule_immediate_retry(now);
            }
        }
        for shard_id in responses.expired_shards.drain(..) {
            self.suspend_expired_shard(shard_id);
        }
        topology_changed
    }

    fn finish_poll(
        &mut self,
        mut responses: AwsPollResponses,
        next_shard_id: Option<String>,
        topology_changed: bool,
        expired_retry_result: AnalyticsStorageResult<()>,
    ) -> AnalyticsStorageResult<PollBatch> {
        // A transient response/decoding error must leave the selected shard
        // at the head of the next rotation so it is retried before later
        // shards. Topology changes are the exception: advance past a shard
        // that was removed from active selection even when another selected
        // task failed in the same poll.
        if responses.error.is_none() || topology_changed || responses.failed_shard_id.is_some() {
            self.next_shard_id = responses
                .failed_shard_id
                .or(next_shard_id)
                .filter(|shard_id| {
                    self.shard_iterators
                        .iter()
                        .any(|state| state.shard_id == *shard_id)
                });
        }
        if let Some(error) = responses.error {
            return Err(error);
        }
        expired_retry_result?;
        responses
            .shard_batches
            .sort_unstable_by_key(|(selection_order, _)| *selection_order);
        self.pending_batches
            .extend(responses.shard_batches.into_iter().map(|(_, batch)| batch));
        Ok(self
            .pending_batches
            .pop_front()
            .unwrap_or_else(empty_poll_batch))
    }

    fn selected_shard_indices(&self, count: usize) -> Vec<usize> {
        if count == 0 || self.shard_iterators.is_empty() {
            return Vec::new();
        }
        let start = self
            .next_shard_id
            .as_deref()
            .and_then(|shard_id| {
                self.shard_iterators
                    .iter()
                    .position(|state| state.shard_id == shard_id)
            })
            .unwrap_or_default();
        (0..count)
            .map(|offset| (start + offset) % self.shard_iterators.len())
            .collect()
    }

    pub(super) async fn reload_checkpoints(
        &mut self,
        checkpoints: &[SourceCheckpoint],
    ) -> AnalyticsStorageResult<()> {
        let durable_checkpoints = checkpoints
            .iter()
            .filter(|checkpoint| {
                checkpoint.source_table_name == self.source_table_name && checkpoint.is_record()
            })
            .cloned()
            .collect::<Vec<_>>();
        let discovery = self
            .client
            .initial_shard_iterators(AwsShardDiscoveryRequest {
                stream_arn: self.stream_arn.as_str(),
                max_shards: self.max_shards,
                checkpoints: &durable_checkpoints,
                source_table_name: self.source_table_name.as_str(),
                include_closed_shards: true,
            })
            .await?;
        self.durable_checkpoints = durable_checkpoints;
        self.shard_iterators = discovery.shard_iterators;
        self.pending_shard_ids = discovery.pending_shard_ids;
        self.pending_expired_shard_ids.clear();
        self.pending_batches.clear();
        self.next_exclusive_start_shard_id = discovery.next_exclusive_start_shard_id;
        self.startup_exhausted = discovery.startup_exhausted;
        self.pending_terminal_shards.clear();
        self.next_shard_id = None;
        self.discovery_schedule = AwsDiscoverySchedule::Initial;
        Ok(())
    }

    pub(super) fn commit(&mut self, checkpoint: &SourceCheckpoint) {
        let applied_iterator = apply_aws_iterator_checkpoint(
            self.source_table_name.as_str(),
            &mut self.shard_iterators,
            checkpoint,
        );
        if applied_iterator {
            if let Some(shard_id) = iterator_checkpoint_shard_id(checkpoint) {
                self.pending_terminal_shards.remove(shard_id);
            }
            return;
        }
        if checkpoint.source_table_name == self.source_table_name && !checkpoint.is_iterator() {
            if let Some(existing) = self
                .durable_checkpoints
                .iter_mut()
                .rev()
                .find(|existing| existing.shard_id == checkpoint.shard_id)
            {
                existing.position.clone_from(&checkpoint.position);
            } else {
                self.durable_checkpoints.push(checkpoint.clone());
            }
            if self
                .pending_terminal_shards
                .remove(checkpoint.shard_id.as_str())
            {
                self.shard_iterators
                    .retain(|state| state.shard_id != checkpoint.shard_id);
                self.discovery_schedule
                    .schedule_immediate_retry(Instant::now());
            }
        }
    }
}
