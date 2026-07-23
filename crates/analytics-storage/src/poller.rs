use std::time::Duration;

use analytics_contract::{AnalyticsManifest, StorageStreamRecord};
use aws_sdk_dynamodbstreams::Client as StreamsClient;
use config::{AnalyticsSourceConfig, AnalyticsStreamType};
use tokio::task::JoinSet;

use crate::{
    aux_storage_client::AuxStorageStreamClient,
    aws_stream::{
        AwsShardIteratorState, aws_stream_record_to_contract, aws_streams_client,
        initial_shard_iterators,
    },
    error::{
        AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
        AnalyticsStorageResult,
    },
    execution::StreamCatchupSource,
    facade::storage_stream_record_from_facade,
    planning::{SourceTablePlan, table_plans},
    poller_config::PollerConfig,
    source_routes::CompiledSourceRoutes,
    types::{PollBatch, PolledRecord, SourceCheckpoint},
};

pub(crate) const AUX_STORAGE_SHARD_ID: &str = "aux-storage";
pub(crate) const GLOBAL_SOURCE_NAME: &str = "__global_system_stream";

#[derive(Debug)]
pub struct SourcePoller {
    config: PollerConfig,
    tables: Vec<SourceTablePoller>,
    next_table_start_index: usize,
}

#[derive(Debug)]
enum SourceTablePoller {
    AuxStorage(AuxStorageGlobalPoller),
    AwsStream(AwsStreamTablePoller),
}

#[derive(Debug)]
pub(crate) struct AuxStorageGlobalPoller {
    pub(crate) client: AuxStorageStreamClient,
    pub(crate) routes: CompiledSourceRoutes,
    pub(crate) last_evaluated_key: Option<String>,
    pub(crate) max_records_per_response: u32,
}

#[derive(Debug)]
struct AwsStreamTablePoller {
    client: StreamsClient,
    stream_arn: String,
    source_table_name: String,
    analytics_table_names: Vec<String>,
    shard_iterators: Vec<AwsShardIteratorState>,
}

impl SourcePoller {
    /// Builds a source poller for standalone continuous ETL.
    ///
    /// AWS Lambda does not use this runtime because the Lambda event source
    /// mapping performs stream polling and invokes the handler with batches.
    pub async fn from_config(
        source: &AnalyticsSourceConfig,
        manifest: &AnalyticsManifest,
        checkpoints: &[SourceCheckpoint],
    ) -> AnalyticsStorageResult<Self> {
        let plans = table_plans(source, manifest)?;
        Self::from_plans(source, plans, checkpoints).await
    }

    pub async fn from_plans(
        source: &AnalyticsSourceConfig,
        plans: Vec<SourceTablePlan>,
        checkpoints: &[SourceCheckpoint],
    ) -> AnalyticsStorageResult<Self> {
        let config = PollerConfig::from_source_config(source);
        let mut tables = Vec::with_capacity(plans.len());
        let aux_storage_plans = plans
            .iter()
            .filter(|plan| plan.stream_type == AnalyticsStreamType::AuxStorage)
            .cloned()
            .collect::<Vec<_>>();
        if !aux_storage_plans.is_empty() {
            let endpoint_url = source.endpoint_url.as_deref().ok_or_else(|| {
                AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingAuxStorageEndpoint)
            })?;
            tables.push(SourceTablePoller::AuxStorage(AuxStorageGlobalPoller {
                client: AuxStorageStreamClient::new(endpoint_url, config.request_timeout)?,
                routes: CompiledSourceRoutes::from_plans(&aux_storage_plans)?,
                last_evaluated_key: checkpoint_position(
                    checkpoints,
                    GLOBAL_SOURCE_NAME,
                    AUX_STORAGE_SHARD_ID,
                ),
                max_records_per_response: config.max_records_per_response,
            }));
        }
        for plan in plans
            .into_iter()
            .filter(|plan| plan.stream_type == AnalyticsStreamType::StorageStream)
        {
            tracing::info!(
                source_table_name = plan.source_table_name,
                analytics_table_names = ?plan.analytics_table_names,
                stream_type = ?plan.stream_type,
                "analytics source poller table plan registered"
            );
            match plan.stream_type {
                AnalyticsStreamType::AuxStorage => {
                    unreachable!("aux-storage plans are consolidated")
                }
                AnalyticsStreamType::StorageStream => {
                    let stream_arn = plan.stream_identifier.ok_or_else(|| {
                        AnalyticsStorageError::with_debug(
                            AnalyticsStorageErrorKind::MissingStreamIdentifier,
                            AnalyticsStorageErrorDebug::SourceTableName(
                                plan.source_table_name.clone(),
                            ),
                        )
                    })?;
                    let client = aws_streams_client(source).await;
                    let shard_iterators = initial_shard_iterators(
                        &client,
                        stream_arn.as_str(),
                        config.max_shards,
                        checkpoints,
                        plan.source_table_name.as_str(),
                    )
                    .await?;
                    tables.push(SourceTablePoller::AwsStream(AwsStreamTablePoller {
                        client,
                        stream_arn,
                        source_table_name: plan.source_table_name,
                        analytics_table_names: plan.analytics_table_names,
                        shard_iterators,
                    }));
                }
            }
        }
        Ok(Self {
            config,
            tables,
            next_table_start_index: 0,
        })
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub async fn poll_once(&mut self) -> AnalyticsStorageResult<PollBatch> {
        if self.tables.is_empty() {
            return Ok(PollBatch {
                records: Vec::new(),
                checkpoints: Vec::new(),
            });
        }
        let table_index = self.next_table_start_index % self.tables.len();
        self.next_table_start_index = (table_index + 1) % self.tables.len();
        self.tables[table_index]
            .poll_once(self.config.max_responses_per_interval)
            .await
    }

    pub async fn poll_aux_storage_table(
        &mut self,
        source_table_name: &str,
    ) -> AnalyticsStorageResult<PollBatch> {
        let Some(table) = self
            .tables
            .iter_mut()
            .find(|table| table.is_aux_storage_source_table(source_table_name))
        else {
            return Ok(PollBatch {
                records: Vec::new(),
                checkpoints: Vec::new(),
            });
        };
        table
            .poll_once(self.config.max_responses_per_interval)
            .await
    }

    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        self.config.poll_interval
    }

    pub fn commit(&mut self, checkpoints: &[SourceCheckpoint]) {
        for checkpoint in checkpoints {
            for table in &mut self.tables {
                table.commit(checkpoint);
            }
        }
    }
}

impl StreamCatchupSource for SourcePoller {
    async fn poll_stream_catchup(&mut self) -> AnalyticsStorageResult<PollBatch> {
        self.poll_once().await
    }

    fn commit_stream_catchup(&mut self, checkpoints: &[SourceCheckpoint]) {
        self.commit(checkpoints);
    }
}

impl SourceTablePoller {
    fn is_aux_storage_source_table(&self, source_table_name: &str) -> bool {
        match self {
            Self::AuxStorage(poller) => poller.routes.contains_source(source_table_name),
            Self::AwsStream(_) => false,
        }
    }

    async fn poll_once(&mut self, max_responses: usize) -> AnalyticsStorageResult<PollBatch> {
        match self {
            Self::AuxStorage(poller) => poller.poll_once(max_responses).await,
            Self::AwsStream(poller) => poller.poll_once(max_responses).await,
        }
    }

    pub(crate) fn commit(&mut self, checkpoint: &SourceCheckpoint) {
        match self {
            Self::AuxStorage(poller) => poller.commit(checkpoint),
            Self::AwsStream(poller) => poller.commit(checkpoint),
        }
    }
}

impl AuxStorageGlobalPoller {
    pub(crate) async fn poll_once(
        &mut self,
        max_responses: usize,
    ) -> AnalyticsStorageResult<PollBatch> {
        let mut records = Vec::new();
        let mut last_checkpoint = None;
        let mut cursor = self.last_evaluated_key.clone();
        for _ in 0..max_responses {
            let response = self
                .client
                .get_stream_records(cursor.clone(), self.max_records_per_response)
                .await?;
            tracing::info!(
                request_cursor = ?cursor,
                response_records = response.records.len(),
                response_has_next = response.last_evaluated_key.is_some(),
                "analytics aux-storage source poll response received"
            );
            let latest_record_key = response
                .records
                .last()
                .map(|record| record.sequence_number.clone());
            cursor = next_aux_storage_cursor(response.last_evaluated_key, latest_record_key);
            records.extend(route_global_records(&self.routes, response.records)?);
            if let Some(position) = cursor.clone() {
                last_checkpoint = Some(SourceCheckpoint {
                    source_table_name: GLOBAL_SOURCE_NAME.to_string(),
                    shard_id: AUX_STORAGE_SHARD_ID.to_string(),
                    position,
                });
            }
            if cursor.is_none() {
                break;
            }
        }
        Ok(PollBatch {
            records,
            checkpoints: last_checkpoint.into_iter().collect(),
        })
    }

    pub(crate) fn commit(&mut self, checkpoint: &SourceCheckpoint) {
        if checkpoint.source_table_name == GLOBAL_SOURCE_NAME
            && checkpoint.shard_id == AUX_STORAGE_SHARD_ID
        {
            self.last_evaluated_key = Some(checkpoint.position.clone());
        }
    }
}

impl AwsStreamTablePoller {
    async fn poll_once(&mut self, max_responses: usize) -> AnalyticsStorageResult<PollBatch> {
        if self.shard_iterators.is_empty() {
            self.shard_iterators = initial_shard_iterators(
                &self.client,
                self.stream_arn.as_str(),
                max_responses,
                &[],
                self.source_table_name.as_str(),
            )
            .await?;
        }
        let mut records = Vec::new();
        let mut checkpoints = Vec::new();
        let mut get_records_tasks = JoinSet::new();
        let active_iterator_count = max_responses.min(self.shard_iterators.len());
        for state in self
            .shard_iterators
            .iter()
            .take(active_iterator_count)
            .cloned()
        {
            let client = self.client.clone();
            get_records_tasks.spawn(async move {
                let response = client
                    .get_records()
                    .shard_iterator(state.iterator)
                    .limit(1000)
                    .send()
                    .await
                    .map_err(|error| {
                        AnalyticsStorageError::with_debug(
                            AnalyticsStorageErrorKind::AwsSdk,
                            AnalyticsStorageErrorDebug::AwsSdk(error.to_string()),
                        )
                    })?;
                Ok::<_, AnalyticsStorageError>((state.shard_id, response))
            });
        }
        while let Some(joined) = get_records_tasks.join_next().await {
            let (shard_id, response) = joined.map_err(|error| {
                AnalyticsStorageError::with_debug(
                    AnalyticsStorageErrorKind::AwsSdk,
                    AnalyticsStorageErrorDebug::AwsSdk(error.to_string()),
                )
            })??;
            let contract_records = response
                .records()
                .iter()
                .map(aws_stream_record_to_contract)
                .collect::<AnalyticsStorageResult<Vec<_>>>()?;
            let batch = aws_stream_response_batch(
                self.source_table_name.as_str(),
                &self.analytics_table_names,
                shard_id.as_str(),
                contract_records,
                response.next_shard_iterator().map(ToOwned::to_owned),
            );
            records.extend(batch.records);
            checkpoints.extend(batch.checkpoints);
        }
        Ok(PollBatch {
            records,
            checkpoints,
        })
    }

    fn commit(&mut self, checkpoint: &SourceCheckpoint) {
        apply_aws_iterator_checkpoint(
            self.source_table_name.as_str(),
            &mut self.shard_iterators,
            checkpoint,
        );
    }
}

#[cfg(test)]
pub(crate) fn table_request_names_from(table_count: usize, start_index: usize) -> Vec<String> {
    if table_count == 0 {
        return Vec::new();
    }
    let start_index = start_index % table_count;
    (0..table_count)
        .map(|offset| ((start_index + offset) % table_count).to_string())
        .collect()
}

#[cfg(test)]
pub(crate) fn table_index_from_request_shard(shard_id: &str) -> Option<usize> {
    shard_id.parse::<usize>().ok()
}

pub(crate) fn next_aux_storage_cursor(
    last_evaluated_key: Option<String>,
    latest_record_key: Option<String>,
) -> Option<String> {
    last_evaluated_key.or(latest_record_key)
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

pub(crate) fn aws_stream_response_batch(
    source_table_name: &str,
    analytics_table_names: &[String],
    shard_id: &str,
    contract_records: Vec<StorageStreamRecord>,
    next_iterator: Option<String>,
) -> PollBatch {
    let mut checkpoints = Vec::new();
    if let Some(last_record) = contract_records.last() {
        checkpoints.push(SourceCheckpoint {
            source_table_name: source_table_name.to_string(),
            shard_id: shard_id.to_string(),
            position: last_record.sequence_number.clone(),
        });
    }
    let records = expand_records(source_table_name, analytics_table_names, contract_records);
    if let Some(next_iterator) = next_iterator {
        checkpoints.push(SourceCheckpoint {
            source_table_name: source_table_name.to_string(),
            shard_id: format!("__iterator:{shard_id}"),
            position: next_iterator,
        });
    }
    PollBatch {
        records,
        checkpoints,
    }
}

pub(crate) fn expand_records(
    source_table_name: &str,
    analytics_table_names: &[String],
    records: impl IntoIterator<Item = StorageStreamRecord>,
) -> Vec<PolledRecord> {
    let mut expanded = Vec::new();
    for record in records {
        for analytics_table_name in analytics_table_names {
            expanded.push(PolledRecord {
                source_table_name: source_table_name.to_string(),
                analytics_table_name: analytics_table_name.clone(),
                record_key: record.sequence_number.clone(),
                record: record.clone(),
            });
        }
    }
    expanded
}

fn route_global_records(
    routes: &CompiledSourceRoutes,
    records: Vec<storage_types::StreamRecord>,
) -> AnalyticsStorageResult<Vec<PolledRecord>> {
    let mut routed = Vec::new();
    for record in records {
        let source_table_name = record.source_table_name.as_ref().ok_or_else(|| {
            AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingSourceTableIdentity)
        })?;
        let contract_record = storage_stream_record_from_facade(record.clone());
        for analytics_table_name in
            routes.analytics_tables(source_table_name.as_ref(), &contract_record)
        {
            routed.push(PolledRecord {
                source_table_name: source_table_name.to_string(),
                analytics_table_name: analytics_table_name.to_string(),
                record_key: contract_record.sequence_number.clone(),
                record: contract_record.clone(),
            });
        }
    }
    Ok(routed)
}

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

#[cfg(test)]
pub(crate) fn aux_storage_initial_cursor(
    checkpoints: &[SourceCheckpoint],
    _source_table_name: &str,
    _analytics_table_names: &[String],
) -> Option<String> {
    checkpoint_position(checkpoints, GLOBAL_SOURCE_NAME, AUX_STORAGE_SHARD_ID)
}
