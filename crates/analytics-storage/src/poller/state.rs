use std::{
    collections::{BTreeSet, VecDeque},
    time::{Duration, Instant},
};

use analytics_contract::StorageStreamRecord;

use crate::{
    aux_storage_client::AuxStorageStreamClient,
    aws_stream::{AwsShardIteratorState, AwsStreams},
    constants::{DISCOVERY_RETRY_INITIAL_DELAY, DISCOVERY_RETRY_MAX_DELAY},
    error::AnalyticsStorageResult,
    poller_config::PollerConfig,
    source_batch::{PollBatch, PolledRecord, SourceCheckpoint},
    source_routes::CompiledSourceRoutes,
};

#[derive(Debug)]
pub struct SourcePoller {
    pub(super) config: PollerConfig,
    pub(super) tables: Vec<SourceTablePoller>,
    pub(super) next_table_start_index: usize,
}

#[derive(Debug)]
pub(super) enum SourceTablePoller {
    AuxStorage(AuxStorageGlobalPoller),
    AwsStream(AwsStreamTablePoller),
}

#[derive(Debug)]
pub(crate) struct AuxStorageGlobalPoller {
    pub(crate) client: AuxStorageStreamClient,
    pub(crate) routes: CompiledSourceRoutes,
    pub(crate) source_scope: storage_types::SystemStreamScope,
    pub(crate) checkpoint: Option<storage_types::SystemStreamCheckpointToken>,
    // Unrouted pages advance scanning without creating a durable job effect.
    pub(crate) scan_checkpoint: Option<storage_types::SystemStreamCheckpointToken>,
    pub(crate) max_records_per_response: u32,
    pub(crate) serialization_buffer: Vec<u8>,
}

#[derive(Debug)]
pub(super) struct AwsStreamTablePoller {
    pub(super) client: AwsStreams,
    pub(super) stream_arn: String,
    pub(super) source_table_name: String,
    pub(super) analytics_table_names: Vec<String>,
    pub(super) max_shards: usize,
    pub(super) max_records_per_response: u32,
    pub(super) durable_checkpoints: Vec<SourceCheckpoint>,
    pub(super) shard_iterators: Vec<AwsShardIteratorState>,
    /// Responses fetched together are retained as individual scope batches.
    /// The queue is bounded by the response budget and drained before another
    /// remote poll starts.
    pub(super) pending_batches: VecDeque<PollBatch>,
    pub(super) pending_shard_ids: VecDeque<String>,
    pub(super) pending_expired_shard_ids: VecDeque<String>,
    pub(super) next_exclusive_start_shard_id: Option<String>,
    pub(super) startup_exhausted: bool,
    pub(super) pending_terminal_shards: BTreeSet<String>,
    pub(super) next_shard_id: Option<String>,
    pub(super) discovery_schedule: AwsDiscoverySchedule,
    pub(super) serialization_buffer: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum AwsDiscoverySchedule {
    Initial,
    Quiet,
    Retry {
        next_attempt_at: Instant,
        delay: Duration,
    },
}

#[derive(Debug, Clone, Copy)]
pub(super) enum AwsDiscoveryAttempt {
    Initial,
    Retry { delay: Duration },
}

impl AwsDiscoverySchedule {
    pub(super) fn begin(
        &mut self,
        active_shards: usize,
        now: Instant,
    ) -> Option<AwsDiscoveryAttempt> {
        match *self {
            Self::Initial if active_shards > 0 => {
                *self = Self::Quiet;
                None
            }
            Self::Initial => Some(AwsDiscoveryAttempt::Initial),
            Self::Quiet => None,
            Self::Retry {
                next_attempt_at,
                delay,
            } if now >= next_attempt_at => Some(AwsDiscoveryAttempt::Retry { delay }),
            Self::Retry { .. } => None,
        }
    }

    pub(super) fn begin_pending(&mut self, now: Instant) -> Option<AwsDiscoveryAttempt> {
        match *self {
            Self::Initial | Self::Quiet => Some(AwsDiscoveryAttempt::Initial),
            Self::Retry {
                next_attempt_at,
                delay,
            } if now >= next_attempt_at => Some(AwsDiscoveryAttempt::Retry { delay }),
            Self::Retry { .. } => None,
        }
    }

    pub(super) fn complete(
        &mut self,
        attempt: AwsDiscoveryAttempt,
        active_before: usize,
        active_after: usize,
        max_shards: usize,
        has_pending_work: bool,
        now: Instant,
    ) {
        if active_after >= max_shards {
            *self = Self::Quiet;
        } else if has_pending_work && active_after > active_before {
            *self = Self::Retry {
                next_attempt_at: now,
                delay: DISCOVERY_RETRY_INITIAL_DELAY,
            };
        } else {
            self.schedule_failure_retry(attempt.retry_delay(), now);
        }
    }

    pub(super) fn fail(&mut self, attempt: AwsDiscoveryAttempt, now: Instant) {
        self.schedule_failure_retry(attempt.retry_delay(), now);
    }

    pub(super) fn fail_current(&mut self, now: Instant) {
        let delay = match *self {
            Self::Retry {
                next_attempt_at, ..
            } if next_attempt_at > now => return,
            Self::Retry { delay, .. } => delay,
            Self::Initial | Self::Quiet => DISCOVERY_RETRY_INITIAL_DELAY,
        };
        self.schedule_failure_retry(delay, now);
    }

    fn schedule_failure_retry(&mut self, delay: Duration, now: Instant) {
        *self = Self::Retry {
            next_attempt_at: now + delay,
            delay: next_retry_delay(delay),
        };
    }

    pub(super) fn schedule_immediate_retry(&mut self, now: Instant) {
        *self = Self::Retry {
            next_attempt_at: now,
            delay: DISCOVERY_RETRY_INITIAL_DELAY,
        };
    }

    pub(super) fn allows_pending_work(&self, now: Instant) -> bool {
        !matches!(self, Self::Retry { next_attempt_at, .. } if now < *next_attempt_at)
    }
}

impl AwsDiscoveryAttempt {
    fn retry_delay(self) -> Duration {
        match self {
            Self::Initial => DISCOVERY_RETRY_INITIAL_DELAY,
            Self::Retry { delay } => delay,
        }
    }
}

pub(super) fn next_retry_delay(delay: Duration) -> Duration {
    delay
        .checked_mul(2)
        .unwrap_or(DISCOVERY_RETRY_MAX_DELAY)
        .min(DISCOVERY_RETRY_MAX_DELAY)
}

pub(crate) fn apply_aws_iterator_checkpoint(
    source_table_name: &str,
    shard_iterators: &mut [AwsShardIteratorState],
    checkpoint: &SourceCheckpoint,
) -> bool {
    let Some(shard_id) = iterator_checkpoint_shard_id(checkpoint) else {
        return false;
    };
    if checkpoint.source_table_name != source_table_name {
        return false;
    }
    let Some(state) = shard_iterators
        .iter_mut()
        .find(|state| state.shard_id == shard_id)
    else {
        return false;
    };
    state.iterator.clone_from(&checkpoint.position);
    true
}

pub(crate) fn iterator_checkpoint_shard_id(checkpoint: &SourceCheckpoint) -> Option<&str> {
    checkpoint.shard_id.strip_prefix("__iterator:")
}

pub(super) fn empty_poll_batch() -> PollBatch {
    PollBatch {
        source_response_count: 0,
        source_nonempty_response_count: 0,
        source_record_count: 0,
        source_encoded_bytes: 0,
        records: Vec::new(),
        checkpoints: Vec::new(),
        source_predecessor: None,
        system_stream: None,
    }
}

pub(crate) struct AwsStreamResponseBatchInput<'a> {
    pub(crate) source_table_name: &'a str,
    pub(crate) analytics_table_names: &'a [String],
    pub(crate) shard_id: &'a str,
    pub(crate) source_predecessor: Option<SourceCheckpoint>,
    pub(crate) contract_records: Vec<StorageStreamRecord>,
    pub(crate) next_iterator: Option<String>,
}

pub(crate) fn aws_stream_response_batch(
    input: AwsStreamResponseBatchInput<'_>,
    serialization_buffer: &mut Vec<u8>,
) -> AnalyticsStorageResult<PollBatch> {
    serialization_buffer.clear();
    serde_json::to_writer(
        &mut *serialization_buffer,
        input.contract_records.as_slice(),
    )?;
    let source_encoded_bytes = serialization_buffer.len();
    let source_record_count = input.contract_records.len();
    let checkpoints = collect_aws_response_checkpoints(
        input.source_table_name,
        input.shard_id,
        input.contract_records.as_slice(),
        input.next_iterator,
    );
    let records = route_records(
        input.source_table_name,
        input.analytics_table_names,
        input.shard_id,
        input.contract_records,
    );

    Ok(PollBatch {
        source_response_count: 1,
        source_nonempty_response_count: usize::from(source_record_count > 0),
        source_record_count,
        source_encoded_bytes,
        records,
        checkpoints,
        source_predecessor: input.source_predecessor,
        system_stream: None,
    })
}

fn collect_aws_response_checkpoints(
    source_table_name: &str,
    shard_id: &str,
    contract_records: &[StorageStreamRecord],
    next_iterator: Option<String>,
) -> Vec<SourceCheckpoint> {
    // A response emits at most one record and one iterator checkpoint.
    let checkpoint_capacity =
        usize::from(!contract_records.is_empty()) + usize::from(next_iterator.is_some());
    let mut checkpoints = Vec::with_capacity(checkpoint_capacity);
    if let Some(last_record) = contract_records.last() {
        checkpoints.push(SourceCheckpoint {
            source_table_name: source_table_name.to_string(),
            shard_id: shard_id.to_string(),
            position: last_record.sequence_number.clone(),
        });
    }
    if let Some(next_iterator) = next_iterator {
        checkpoints.push(SourceCheckpoint {
            source_table_name: source_table_name.to_string(),
            shard_id: format!("__iterator:{shard_id}"),
            position: next_iterator,
        });
    }
    checkpoints
}

pub(crate) fn route_records(
    source_table_name: &str,
    analytics_table_names: &[String],
    shard_id: &str,
    records: impl IntoIterator<Item = StorageStreamRecord>,
) -> Vec<PolledRecord> {
    if analytics_table_names.is_empty() {
        return Vec::new();
    }
    records
        .into_iter()
        .map(|record| PolledRecord {
            source_table_name: source_table_name.to_string(),
            shard_id: shard_id.to_string(),
            analytics_table_names: analytics_table_names.to_vec(),
            record_key: record.sequence_number.clone(),
            record,
            system_position: None,
        })
        .collect()
}

pub(crate) fn logical_record_key(position: &storage_types::SystemStreamPosition) -> String {
    let shard_id = position.shard_id().as_str();
    let lineage_id = position.lineage_id().as_str();
    let offset = position.offset().as_str();
    let mut key = String::with_capacity(shard_id.len() + lineage_id.len() + offset.len() + 2);
    key.push_str(shard_id);
    key.push('\u{1f}');
    key.push_str(lineage_id);
    key.push('\u{1f}');
    key.push_str(offset);
    key
}

#[cfg(test)]
pub(crate) fn checkpoint_position(
    checkpoints: &[SourceCheckpoint],
    source_table_name: &str,
    shard_id: &str,
) -> Option<String> {
    checkpoints
        .iter()
        .find(|checkpoint| {
            checkpoint.source_table_name == source_table_name && checkpoint.shard_id == shard_id
        })
        .map(|checkpoint| checkpoint.position.clone())
}
