use std::{
    collections::{BTreeSet, VecDeque},
    time::{Duration, Instant},
};

use analytics_contract::AnalyticsManifest;
use config::{AnalyticsSourceConfig, AnalyticsStreamType};

use super::state::{
    AuxStorageGlobalPoller, AwsDiscoverySchedule, AwsStreamTablePoller, SourcePoller,
    SourceTablePoller,
};
use crate::{
    aws_stream::{AwsShardDiscoveryRequest, AwsStreams},
    constants::AWS_GET_RECORDS_MAX_RECORDS,
    error::{
        AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
        AnalyticsStorageResult,
    },
    execution::StreamCatchupSource,
    planning::SourceTablePlan,
    poller_config::PollerConfig,
    source_batch::{PollBatch, SourceCheckpoint, SystemStreamBatch, SystemStreamResume},
};

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
        let plans = SourceTablePlan::from_source(source, manifest)?;
        Self::from_plans(source, plans, checkpoints).await
    }

    pub async fn from_plans(
        source: &AnalyticsSourceConfig,
        plans: Vec<SourceTablePlan>,
        checkpoints: &[SourceCheckpoint],
    ) -> AnalyticsStorageResult<Self> {
        Self::from_plans_with_system_resume(source, plans, checkpoints, None).await
    }

    pub async fn from_plans_with_system_resume(
        source: &AnalyticsSourceConfig,
        plans: Vec<SourceTablePlan>,
        checkpoints: &[SourceCheckpoint],
        system_resume: Option<SystemStreamResume>,
    ) -> AnalyticsStorageResult<Self> {
        let config = PollerConfig::from_source_config(source);
        let mut tables = Vec::with_capacity(plans.len());
        let aux_storage_plans = plans
            .iter()
            .filter(|plan| plan.stream_type == AnalyticsStreamType::AuxStorage)
            .cloned()
            .collect::<Vec<_>>();
        if !aux_storage_plans.is_empty() {
            tables.push(SourceTablePoller::AuxStorage(
                AuxStorageGlobalPoller::try_from_source(
                    source,
                    &aux_storage_plans,
                    &config,
                    system_resume,
                )
                .await?,
            ));
        }
        for plan in plans
            .into_iter()
            .filter(|plan| plan.stream_type == AnalyticsStreamType::StorageStream)
        {
            tracing::info!(source_table_name = plan.source_table_name,
                analytics_table_names = ?plan.analytics_table_names, stream_type = ?plan.stream_type,
                "analytics source poller table plan registered");
            tables.push(SourceTablePoller::AwsStream(
                AwsStreamRegistration::try_new(source, plan, checkpoints)
                    .await?
                    .open_poller(&config)
                    .await?,
            ));
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

    /// Returns the provider-described SystemStream scope currently bound to
    /// this poller. The scope is read from the live provider during
    /// construction and is never synthesized from a job payload.
    #[must_use]
    pub fn system_stream_scope(&self) -> Option<storage_types::SystemStreamScope> {
        self.tables
            .iter()
            .find_map(SourceTablePoller::system_stream_scope)
    }

    pub async fn poll_once(&mut self) -> AnalyticsStorageResult<PollBatch> {
        self.poll_once_with_now(Instant::now()).await
    }

    #[cfg(test)]
    pub(crate) async fn poll_once_at(&mut self, now: Instant) -> AnalyticsStorageResult<PollBatch> {
        self.poll_once_with_now(now).await
    }

    async fn poll_once_with_now(&mut self, now: Instant) -> AnalyticsStorageResult<PollBatch> {
        if self.tables.is_empty() {
            return Ok(PollBatch {
                source_response_count: 0,
                source_nonempty_response_count: 0,
                source_record_count: 0,
                source_encoded_bytes: 0,
                records: Vec::new(),
                checkpoints: Vec::new(),
                source_predecessor: None,
                system_stream: None,
            });
        }
        let table_index = self
            .pending_table_index()
            .unwrap_or_else(|| self.next_table_start_index % self.tables.len());
        self.next_table_start_index = (table_index + 1) % self.tables.len();
        self.tables[table_index]
            .poll_once(self.config.max_responses_per_interval, now)
            .await
    }

    fn pending_table_index(&self) -> Option<usize> {
        (0..self.tables.len())
            .map(|offset| (self.next_table_start_index + offset) % self.tables.len())
            .find(|index| self.tables[*index].has_pending_batch())
    }

    /// Retry an oversized uncommitted page with fewer source records. Transient
    /// progress is rewound to the durable frontier so no source records are
    /// lost.
    pub async fn reduce_page_limit(&mut self) -> AnalyticsStorageResult<bool> {
        if self.config.max_records_per_response <= 1 {
            return Ok(false);
        }
        let limit = self.config.max_records_per_response / 2;
        for table in &mut self.tables {
            match table {
                SourceTablePoller::AuxStorage(poller) => {
                    poller.max_records_per_response = limit;
                    poller.scan_checkpoint = poller.checkpoint.clone();
                }
                SourceTablePoller::AwsStream(poller) => {
                    let checkpoints = poller.durable_checkpoints.clone();
                    poller.reload_checkpoints(&checkpoints).await?;
                    poller.max_records_per_response = limit.min(AWS_GET_RECORDS_MAX_RECORDS);
                }
            }
        }
        self.config.max_records_per_response = limit;
        Ok(true)
    }

    /// Rebuild transient stream cursors from the latest durable checkpoints
    /// after leadership is reacquired.
    pub async fn reload_checkpoints(
        &mut self,
        checkpoints: &[SourceCheckpoint],
    ) -> AnalyticsStorageResult<()> {
        for table in &mut self.tables {
            table.reload_checkpoints(checkpoints).await?;
        }
        Ok(())
    }

    /// Restores the destination's durable SystemStream frontier after a job
    /// commits without acknowledging local progress. Scope mismatches fail
    /// before changing either cursor.
    pub fn reload_system_resume(
        &mut self,
        resume: Option<SystemStreamResume>,
    ) -> AnalyticsStorageResult<()> {
        let Some(poller) = self.tables.iter_mut().find_map(|table| match table {
            SourceTablePoller::AuxStorage(poller) => Some(poller),
            SourceTablePoller::AwsStream(_) => None,
        }) else {
            return Ok(());
        };
        let checkpoint = match resume {
            Some(resume) => {
                let (scope, checkpoint) = resume.into_parts();
                if scope != poller.source_scope {
                    return Err(AnalyticsStorageError::new(
                        AnalyticsStorageErrorKind::InvalidSourceBatchScope,
                    ));
                }
                Some(checkpoint)
            }
            None => None,
        };
        poller.checkpoint = checkpoint.clone();
        poller.scan_checkpoint = checkpoint;
        Ok(())
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
                source_response_count: 0,
                source_nonempty_response_count: 0,
                source_record_count: 0,
                source_encoded_bytes: 0,
                records: Vec::new(),
                checkpoints: Vec::new(),
                source_predecessor: None,
                system_stream: None,
            });
        };
        table
            .poll_once(self.config.max_responses_per_interval, Instant::now())
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

    pub fn commit_batch(&mut self, batch: &PollBatch) {
        if batch.system_stream.is_none() && !self.batch_predecessor_is_current(batch) {
            return;
        }
        for checkpoint in &batch.checkpoints {
            self.commit(std::slice::from_ref(checkpoint));
        }
        for table in &mut self.tables {
            table.commit_system_stream(batch.system_stream.as_ref());
        }
    }

    fn batch_predecessor_is_current(&self, batch: &PollBatch) -> bool {
        let Ok(Some(scope)) = batch.source_scope() else {
            return batch.records.is_empty() && batch.checkpoints.is_empty();
        };
        let current = self.tables.iter().find_map(|table| {
            table.current_record_checkpoint(scope.source_table_name(), scope.shard_id())
        });
        match (batch.source_predecessor.as_ref(), current.as_ref()) {
            (None, None) => true,
            (Some(expected), Some(actual)) => expected == actual,
            _ => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn aws_shard_iterator_count(&self) -> usize {
        self.tables
            .iter()
            .filter_map(|table| match table {
                SourceTablePoller::AuxStorage(_) => None,
                SourceTablePoller::AwsStream(poller) => Some(poller.shard_iterators.len()),
            })
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn aws_pending_terminal_count(&self) -> usize {
        self.tables
            .iter()
            .filter_map(|table| match table {
                SourceTablePoller::AuxStorage(_) => None,
                SourceTablePoller::AwsStream(poller) => Some(poller.pending_terminal_shards.len()),
            })
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn aws_pending_shard_count(&self) -> usize {
        self.tables
            .iter()
            .filter_map(|table| match table {
                SourceTablePoller::AuxStorage(_) => None,
                SourceTablePoller::AwsStream(poller) => Some(poller.pending_shard_ids.len()),
            })
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn aws_pending_expired_shard_ids(&self) -> Vec<String> {
        self.tables
            .iter()
            .filter_map(|table| match table {
                SourceTablePoller::AuxStorage(_) => None,
                SourceTablePoller::AwsStream(poller) => {
                    Some(poller.pending_expired_shard_ids.iter().cloned())
                }
            })
            .flatten()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn aws_record_checkpoint(
        &self,
        source_table_name: &str,
        shard_id: &str,
    ) -> Option<SourceCheckpoint> {
        self.tables
            .iter()
            .find_map(|table| table.current_record_checkpoint(source_table_name, shard_id))
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
    fn has_pending_batch(&self) -> bool {
        match self {
            Self::AuxStorage(_) => false,
            Self::AwsStream(poller) => !poller.pending_batches.is_empty(),
        }
    }

    fn is_aux_storage_source_table(&self, source_table_name: &str) -> bool {
        match self {
            Self::AuxStorage(poller) => poller.routes.contains_source(source_table_name),
            Self::AwsStream(_) => false,
        }
    }

    fn system_stream_scope(&self) -> Option<storage_types::SystemStreamScope> {
        match self {
            Self::AuxStorage(poller) => Some(poller.source_scope.clone()),
            Self::AwsStream(_) => None,
        }
    }

    async fn poll_once(
        &mut self,
        max_responses: usize,
        now: Instant,
    ) -> AnalyticsStorageResult<PollBatch> {
        match self {
            Self::AuxStorage(poller) => poller.poll_once(max_responses).await,
            Self::AwsStream(poller) => poller.poll_once(max_responses, now).await,
        }
    }

    async fn reload_checkpoints(
        &mut self,
        checkpoints: &[SourceCheckpoint],
    ) -> AnalyticsStorageResult<()> {
        match self {
            Self::AuxStorage(poller) => {
                poller.scan_checkpoint = poller.checkpoint.clone();
                Ok(())
            }
            Self::AwsStream(poller) => poller.reload_checkpoints(checkpoints).await,
        }
    }

    pub(crate) fn commit(&mut self, checkpoint: &SourceCheckpoint) {
        match self {
            Self::AuxStorage(_) => {}
            Self::AwsStream(poller) => poller.commit(checkpoint),
        }
    }

    fn current_record_checkpoint(
        &self,
        source_table_name: &str,
        shard_id: &str,
    ) -> Option<SourceCheckpoint> {
        match self {
            Self::AwsStream(poller) if poller.source_table_name == source_table_name => poller
                .durable_checkpoints
                .iter()
                .find(|checkpoint| checkpoint.shard_id == shard_id)
                .cloned(),
            Self::AuxStorage(_) | Self::AwsStream(_) => None,
        }
    }

    fn commit_system_stream(&mut self, batch: Option<&SystemStreamBatch>) {
        if let (Self::AuxStorage(poller), Some(batch)) = (self, batch)
            && poller.source_scope == batch.source_scope
            && poller.checkpoint == batch.previous_checkpoint
        {
            poller.checkpoint = Some(batch.checkpoint.clone());
            poller.scan_checkpoint = poller.checkpoint.clone();
        }
    }
}

struct AwsStreamRegistration {
    client: AwsStreams,
    stream_arn: String,
    plan: SourceTablePlan,
    durable_checkpoints: Vec<SourceCheckpoint>,
}

impl AwsStreamRegistration {
    async fn try_new(
        source: &AnalyticsSourceConfig,
        mut plan: SourceTablePlan,
        checkpoints: &[SourceCheckpoint],
    ) -> AnalyticsStorageResult<Self> {
        let stream_arn = plan.stream_identifier.take().ok_or_else(|| {
            AnalyticsStorageError::with_debug(
                AnalyticsStorageErrorKind::MissingStreamIdentifier,
                AnalyticsStorageErrorDebug::SourceTableName(plan.source_table_name.clone()),
            )
        })?;
        let client = AwsStreams::from_source(source).await;
        let durable_checkpoints = checkpoints
            .iter()
            .filter(|checkpoint| {
                checkpoint.source_table_name == plan.source_table_name && checkpoint.is_record()
            })
            .cloned()
            .collect::<Vec<_>>();
        Ok(Self {
            client,
            stream_arn,
            plan,
            durable_checkpoints,
        })
    }

    async fn open_poller(
        self,
        config: &PollerConfig,
    ) -> AnalyticsStorageResult<AwsStreamTablePoller> {
        let Self {
            client,
            stream_arn,
            plan,
            durable_checkpoints,
        } = self;
        let discovery = client
            .initial_shard_iterators(AwsShardDiscoveryRequest {
                stream_arn: stream_arn.as_str(),
                max_shards: config.max_shards,
                checkpoints: &durable_checkpoints,
                source_table_name: plan.source_table_name.as_str(),
                include_closed_shards: true,
            })
            .await?;
        Ok(AwsStreamTablePoller {
            client,
            stream_arn,
            source_table_name: plan.source_table_name,
            analytics_table_names: plan.analytics_table_names,
            max_shards: config.max_shards,
            max_records_per_response: config
                .max_records_per_response
                .min(AWS_GET_RECORDS_MAX_RECORDS),
            durable_checkpoints,
            shard_iterators: discovery.shard_iterators,
            pending_batches: VecDeque::new(),
            pending_shard_ids: discovery.pending_shard_ids,
            pending_expired_shard_ids: VecDeque::new(),
            next_exclusive_start_shard_id: discovery.next_exclusive_start_shard_id,
            startup_exhausted: discovery.startup_exhausted,
            pending_terminal_shards: BTreeSet::new(),
            next_shard_id: None,
            discovery_schedule: AwsDiscoverySchedule::Initial,
            serialization_buffer: Vec::new(),
        })
    }
}
