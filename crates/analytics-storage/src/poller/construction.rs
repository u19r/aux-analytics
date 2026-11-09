use std::collections::{BTreeSet, VecDeque};

use config::AnalyticsSourceConfig;

use super::state::{AuxStorageGlobalPoller, AwsDiscoverySchedule, AwsStreamTablePoller};
use crate::{
    aux_storage_client::AuxStorageStreamClient,
    aws_stream::{AwsShardDiscoveryRequest, AwsStreams},
    constants::AWS_GET_RECORDS_MAX_RECORDS,
    error::{
        AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
        AnalyticsStorageResult,
    },
    planning::SourceTablePlan,
    poller_config::PollerConfig,
    source_batch::{SourceCheckpoint, SystemStreamResume},
    source_routes::CompiledSourceRoutes,
};

impl AuxStorageGlobalPoller {
    pub(super) async fn try_from_source(
        source: &AnalyticsSourceConfig,
        plans: &[SourceTablePlan],
        config: &PollerConfig,
        system_resume: Option<SystemStreamResume>,
    ) -> AnalyticsStorageResult<Self> {
        let endpoint_url = source.endpoint_url.as_deref().ok_or_else(|| {
            AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingAuxStorageEndpoint)
        })?;
        let mut client = AuxStorageStreamClient::try_new(endpoint_url, config.request_timeout)?;
        if let Some(path) = source.internal_service_token_file.as_deref() {
            client = client.with_service_token(&load_service_token(path)?)?;
        }
        let routes = CompiledSourceRoutes::from_plans(plans)?;
        if routes.max_fanout() > crate::constants::MAX_SOURCE_BATCH_RECORDS {
            return Err(AnalyticsStorageError::new(
                AnalyticsStorageErrorKind::SourceBatchExceedsBudget,
            ));
        }
        let source_scope = client
            .describe_system_stream()
            .await?
            .source_scope()
            .clone();
        let checkpoint = if let Some(resume) = system_resume {
            let (resume_scope, checkpoint) = resume.into_parts();
            if resume_scope != source_scope {
                return Err(AnalyticsStorageError::new(
                    AnalyticsStorageErrorKind::InvalidSourceBatchScope,
                ));
            }
            Some(checkpoint)
        } else {
            None
        };
        Ok(Self {
            client,
            routes,
            source_scope,
            scan_checkpoint: checkpoint.clone(),
            checkpoint,
            pending_effect: None,
            max_records_per_response: config.max_records_per_response,
            max_records_per_batch: crate::constants::MAX_SOURCE_BATCH_RECORDS,
            max_batch_bytes: config.max_batch_bytes,
            serialization_buffer: Vec::new(),
        })
    }
}

pub(super) struct AwsStreamRegistration {
    client: AwsStreams,
    stream_arn: String,
    plan: SourceTablePlan,
    durable_checkpoints: Vec<SourceCheckpoint>,
}

impl AwsStreamRegistration {
    pub(super) async fn try_new(
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

    pub(super) async fn open_poller(
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

fn load_service_token(path: &std::path::Path) -> AnalyticsStorageResult<String> {
    let token = std::fs::read_to_string(path).map_err(|source| {
        AnalyticsStorageError::with_source(
            AnalyticsStorageErrorKind::InvalidServiceTokenFile,
            source,
        )
    })?;
    let token = token.trim();
    if token.is_empty() {
        return Err(AnalyticsStorageError::new(
            AnalyticsStorageErrorKind::InvalidServiceTokenFile,
        ));
    }
    Ok(token.to_owned())
}
