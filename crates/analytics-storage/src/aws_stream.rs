use std::collections::{HashMap, VecDeque};

use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use aws_sdk_dynamodbstreams::{
    Client as StreamsClient,
    types::{AttributeValue as AwsStreamAttributeValue, ShardIteratorType},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use config::AnalyticsSourceConfig;

use crate::{
    aws_client::resolve_aws_config,
    constants::AWS_DESCRIBE_STREAM_MAX_SHARDS,
    error::{
        AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
        AnalyticsStorageResult,
    },
    source_batch::SourceCheckpoint,
};

#[derive(Debug, Clone)]
pub(crate) struct AwsShardIteratorState {
    pub(crate) shard_id: String,
    pub(crate) iterator: String,
}

#[derive(Debug)]
pub(crate) struct AwsShardDiscovery {
    pub(crate) shard_iterators: Vec<AwsShardIteratorState>,
    pub(crate) pending_shard_ids: VecDeque<String>,
    pub(crate) next_exclusive_start_shard_id: Option<String>,
    pub(crate) startup_exhausted: bool,
}

pub(crate) struct AwsShardDiscoveryRequest<'a> {
    pub(crate) stream_arn: &'a str,
    pub(crate) max_shards: usize,
    pub(crate) checkpoints: &'a [SourceCheckpoint],
    pub(crate) source_table_name: &'a str,
    pub(crate) include_closed_shards: bool,
}

#[derive(Debug)]
pub(crate) struct AwsStreams {
    client: StreamsClient,
}

impl AwsStreams {
    pub(crate) async fn from_source(source: &AnalyticsSourceConfig) -> Self {
        let config = resolve_aws_config(
            source.region.as_deref(),
            source.endpoint_url.as_deref(),
            source.credentials.as_ref(),
        )
        .await;
        Self {
            client: StreamsClient::new(&config),
        }
    }

    pub(crate) fn clone_records_client(&self) -> StreamsClient {
        self.client.clone()
    }

    pub(crate) async fn initial_shard_iterators(
        &self,
        request: AwsShardDiscoveryRequest<'_>,
    ) -> AnalyticsStorageResult<AwsShardDiscovery> {
        let mut discovery = AwsShardDiscovery {
            shard_iterators: Vec::new(),
            pending_shard_ids: VecDeque::new(),
            next_exclusive_start_shard_id: None,
            startup_exhausted: false,
        };
        while discovery.shard_iterators.len() < request.max_shards {
            if discovery.pending_shard_ids.is_empty() {
                let capacity = request
                    .max_shards
                    .saturating_sub(discovery.shard_iterators.len());
                self.refill_shard_ids(
                    request.stream_arn,
                    capacity,
                    request.include_closed_shards,
                    &mut discovery,
                )
                .await?;
            }
            let Some(shard_id) = discovery.pending_shard_ids.pop_front() else {
                break;
            };
            if let Some(state) = self
                .shard_iterator(
                    request.stream_arn,
                    request.checkpoints,
                    request.source_table_name,
                    shard_id.as_str(),
                )
                .await?
            {
                discovery.shard_iterators.push(state);
            }
        }
        if !request.include_closed_shards {
            // Runtime discovery only needs the current open-shard window. Do
            // not retain undispatched IDs from this snapshot: closed IDs are
            // filtered out on the next discovery and open IDs are rediscovered.
            discovery.pending_shard_ids.clear();
        }
        Ok(discovery)
    }

    pub(crate) async fn refill_shard_ids(
        &self,
        stream_arn: &str,
        capacity: usize,
        include_closed_shards: bool,
        discovery: &mut AwsShardDiscovery,
    ) -> AnalyticsStorageResult<()> {
        while discovery.pending_shard_ids.len() < capacity && !discovery.startup_exhausted {
            let page_capacity = capacity.saturating_sub(discovery.pending_shard_ids.len());
            let description = self
                .fetch_shard_description(
                    stream_arn,
                    page_capacity,
                    discovery.next_exclusive_start_shard_id.as_deref(),
                )
                .await?;
            let Some(description) = description else {
                discovery.startup_exhausted = true;
                break;
            };
            let unconsumed =
                discovery.collect_page_shards(&description, capacity, include_closed_shards);
            discovery.advance_page(&description, unconsumed);
        }
        Ok(())
    }

    async fn fetch_shard_description(
        &self,
        stream_arn: &str,
        capacity: usize,
        page_start: Option<&str>,
    ) -> AnalyticsStorageResult<Option<aws_sdk_dynamodbstreams::types::StreamDescription>> {
        let mut request = self
            .client
            .describe_stream()
            .stream_arn(stream_arn)
            .limit(capacity.min(AWS_DESCRIBE_STREAM_MAX_SHARDS) as i32);
        if let Some(shard_id) = page_start {
            request = request.exclusive_start_shard_id(shard_id);
        }
        let response = request.send().await.map_err(|error| {
            AnalyticsStorageError::with_debug(
                AnalyticsStorageErrorKind::AwsSdk,
                AnalyticsStorageErrorDebug::AwsSdk(error.to_string()),
            )
        })?;
        Ok(response.stream_description)
    }

    pub(crate) async fn shard_iterator(
        &self,
        stream_arn: &str,
        checkpoints: &[SourceCheckpoint],
        source_table_name: &str,
        shard_id: &str,
    ) -> AnalyticsStorageResult<Option<AwsShardIteratorState>> {
        let mut request = self
            .client
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id);
        if let Some(position) = find_checkpoint_position(checkpoints, source_table_name, shard_id) {
            request = request
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .sequence_number(position);
        } else {
            request = request.shard_iterator_type(ShardIteratorType::TrimHorizon);
        }
        let response = request.send().await.map_err(|error| {
            AnalyticsStorageError::with_debug(
                AnalyticsStorageErrorKind::AwsSdk,
                AnalyticsStorageErrorDebug::AwsSdk(error.to_string()),
            )
        })?;
        Ok(response
            .shard_iterator()
            .map(|iterator| AwsShardIteratorState {
                shard_id: shard_id.to_string(),
                iterator: iterator.to_string(),
            }))
    }
}

fn find_checkpoint_position(
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

pub(crate) fn aws_stream_record_to_contract(
    record: aws_sdk_dynamodbstreams::types::Record,
) -> AnalyticsStorageResult<StorageStreamRecord> {
    let storage_record = record.dynamodb.ok_or_else(|| {
        AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingSequenceNumber)
    })?;
    let sequence_number = storage_record.sequence_number.ok_or_else(|| {
        AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingSequenceNumber)
    })?;
    let keys = storage_record
        .keys
        .ok_or_else(|| AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingKeys))?;
    Ok(StorageStreamRecord {
        keys: aws_stream_item_to_contract(keys)?,
        sequence_number,
        old_image: optional_aws_stream_item_to_contract(storage_record.old_image)?,
        new_image: optional_aws_stream_item_to_contract(storage_record.new_image)?,
    })
}

fn optional_aws_stream_item_to_contract(
    item: Option<HashMap<String, AwsStreamAttributeValue>>,
) -> AnalyticsStorageResult<Option<StorageItem>> {
    let Some(item) = item else {
        return Ok(None);
    };
    let item = aws_stream_item_to_contract(item)?;
    Ok((!item.is_empty()).then_some(item))
}

fn aws_stream_item_to_contract(
    item: HashMap<String, AwsStreamAttributeValue>,
) -> AnalyticsStorageResult<StorageItem> {
    item.into_iter()
        .map(|(name, value)| Ok((name, aws_stream_value_to_contract(value)?)))
        .collect()
}

fn aws_stream_value_to_contract(
    value: AwsStreamAttributeValue,
) -> AnalyticsStorageResult<StorageValue> {
    Ok(match value {
        AwsStreamAttributeValue::S(value) => StorageValue::S(value),
        AwsStreamAttributeValue::N(value) => StorageValue::N(value),
        AwsStreamAttributeValue::B(value) => StorageValue::B(BASE64.encode(value.as_ref())),
        AwsStreamAttributeValue::Ss(value) => StorageValue::SS(value),
        AwsStreamAttributeValue::Ns(value) => StorageValue::NS(value),
        AwsStreamAttributeValue::Bs(value) => StorageValue::BS(
            value
                .iter()
                .map(|blob| BASE64.encode(blob.as_ref()))
                .collect(),
        ),
        AwsStreamAttributeValue::Bool(value) => StorageValue::BOOL(value),
        AwsStreamAttributeValue::Null(value) => StorageValue::NULL(value),
        AwsStreamAttributeValue::L(values) => StorageValue::L(
            values
                .into_iter()
                .map(aws_stream_value_to_contract)
                .collect::<AnalyticsStorageResult<Vec<_>>>()?,
        ),
        AwsStreamAttributeValue::M(values) => StorageValue::M(
            values
                .into_iter()
                .map(|(name, value)| Ok((name, aws_stream_value_to_contract(value)?)))
                .collect::<AnalyticsStorageResult<StorageItem>>()?,
        ),
        _ => {
            return Err(AnalyticsStorageError::new(
                AnalyticsStorageErrorKind::UnsupportedAwsAttributeValue,
            ));
        }
    })
}

impl AwsShardDiscovery {
    fn collect_page_shards(
        &mut self,
        stream_description: &aws_sdk_dynamodbstreams::types::StreamDescription,
        capacity: usize,
        include_closed_shards: bool,
    ) -> bool {
        let mut page_has_unconsumed_shard = false;
        for shard in stream_description.shards() {
            let Some(shard_id) = shard.shard_id().filter(|shard_id| !shard_id.is_empty()) else {
                continue;
            };
            // DescribeStream returns shards in provider order, and
            // ExclusiveStartShardId is exclusive. That cursor is the bounded
            // deduplication boundary across pages.
            if self.pending_shard_ids.len() < capacity
                && (include_closed_shards
                    || shard
                        .sequence_number_range()
                        .is_none_or(|range| range.ending_sequence_number().is_none()))
            {
                self.pending_shard_ids.push_back(shard_id.to_string());
            } else if self.pending_shard_ids.len() >= capacity {
                page_has_unconsumed_shard = true;
            }
        }
        page_has_unconsumed_shard
    }

    fn advance_page(
        &mut self,
        stream_description: &aws_sdk_dynamodbstreams::types::StreamDescription,
        page_has_unconsumed_shard: bool,
    ) {
        let last_shard_id = stream_description
            .shards()
            .iter()
            .filter_map(|shard| shard.shard_id())
            .rfind(|shard_id| !shard_id.is_empty())
            .map(ToOwned::to_owned);
        let provider_next_shard_id = stream_description
            .last_evaluated_shard_id()
            .filter(|shard_id| !shard_id.is_empty())
            .map(ToOwned::to_owned);
        let next_shard_id = if page_has_unconsumed_shard {
            self.pending_shard_ids.back().cloned()
        } else {
            provider_next_shard_id.clone().or(last_shard_id)
        };
        let Some(next_shard_id) = next_shard_id else {
            self.startup_exhausted = true;
            return;
        };
        if self.next_exclusive_start_shard_id.as_deref() == Some(next_shard_id.as_str()) {
            self.startup_exhausted = true;
            return;
        }
        self.next_exclusive_start_shard_id = Some(next_shard_id);
        self.startup_exhausted = provider_next_shard_id.is_none();
    }
}
