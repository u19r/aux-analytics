use std::collections::HashMap;

use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_dynamodbstreams::{
    Client as StreamsClient,
    config::Region,
    types::{AttributeValue as AwsStreamAttributeValue, ShardIteratorType},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use config::AnalyticsSourceConfig;

use crate::{
    error::{
        AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
        AnalyticsStorageResult,
    },
    types::SourceCheckpoint,
};

#[derive(Debug, Clone)]
pub(crate) struct AwsShardIteratorState {
    pub(crate) shard_id: String,
    pub(crate) iterator: String,
}

pub(crate) async fn aws_streams_client(source: &AnalyticsSourceConfig) -> StreamsClient {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = source.region.as_deref() {
        loader = loader.region(Region::new(region.to_string()));
    }
    if let Some(endpoint_url) = source.endpoint_url.as_deref() {
        loader = loader.endpoint_url(endpoint_url);
    }
    if let Some(credentials) = source.credentials.as_ref()
        && let Some(static_credentials) = credentials.r#static.as_ref()
    {
        loader = loader.credentials_provider(Credentials::new(
            static_credentials.access_key.clone(),
            static_credentials.secret_key.clone(),
            static_credentials.session_token.clone(),
            None,
            "aux-analytics",
        ));
    }
    StreamsClient::new(&loader.load().await)
}

pub(crate) async fn initial_shard_iterators(
    client: &StreamsClient,
    stream_arn: &str,
    max_shards: usize,
    checkpoints: &[SourceCheckpoint],
    source_table_name: &str,
) -> AnalyticsStorageResult<Vec<AwsShardIteratorState>> {
    let description = client
        .describe_stream()
        .stream_arn(stream_arn)
        .send()
        .await
        .map_err(|error| {
            AnalyticsStorageError::with_debug(
                AnalyticsStorageErrorKind::AwsSdk,
                AnalyticsStorageErrorDebug::AwsSdk(error.to_string()),
            )
        })?;
    let Some(stream_description) = description.stream_description() else {
        return Ok(Vec::new());
    };
    let mut iterators = Vec::new();
    for shard in stream_description.shards().iter().take(max_shards) {
        let Some(shard_id) = shard.shard_id() else {
            continue;
        };
        let mut request = client
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id);
        if let Some(position) = checkpoint_position(checkpoints, source_table_name, shard_id) {
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
        if let Some(iterator) = response.shard_iterator() {
            iterators.push(AwsShardIteratorState {
                shard_id: shard_id.to_string(),
                iterator: iterator.to_string(),
            });
        }
    }
    Ok(iterators)
}

fn checkpoint_position(
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
    record: &aws_sdk_dynamodbstreams::types::Record,
) -> AnalyticsStorageResult<StorageStreamRecord> {
    let Some(storage_record) = record.dynamodb() else {
        return Err(AnalyticsStorageError::new(
            AnalyticsStorageErrorKind::MissingSequenceNumber,
        ));
    };
    let sequence_number = storage_record
        .sequence_number()
        .ok_or_else(|| {
            AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingSequenceNumber)
        })?
        .to_string();
    Ok(StorageStreamRecord {
        keys: aws_stream_item_to_contract(
            storage_record.keys().ok_or_else(|| {
                AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingKeys)
            })?,
        )?,
        sequence_number,
        old_image: optional_aws_stream_item_to_contract(storage_record.old_image())?,
        new_image: optional_aws_stream_item_to_contract(storage_record.new_image())?,
    })
}

fn optional_aws_stream_item_to_contract(
    item: Option<&HashMap<String, AwsStreamAttributeValue>>,
) -> AnalyticsStorageResult<Option<StorageItem>> {
    let Some(item) = item else {
        return Ok(None);
    };
    let item = aws_stream_item_to_contract(item)?;
    Ok((!item.is_empty()).then_some(item))
}

fn aws_stream_item_to_contract(
    item: &HashMap<String, AwsStreamAttributeValue>,
) -> AnalyticsStorageResult<StorageItem> {
    item.iter()
        .map(|(name, value)| Ok((name.clone(), aws_stream_value_to_contract(value)?)))
        .collect()
}

fn aws_stream_value_to_contract(
    value: &AwsStreamAttributeValue,
) -> AnalyticsStorageResult<StorageValue> {
    Ok(match value {
        AwsStreamAttributeValue::S(value) => StorageValue::S(value.clone()),
        AwsStreamAttributeValue::N(value) => StorageValue::N(value.clone()),
        AwsStreamAttributeValue::B(value) => StorageValue::B(BASE64.encode(value.as_ref())),
        AwsStreamAttributeValue::Ss(value) => StorageValue::SS(value.clone()),
        AwsStreamAttributeValue::Ns(value) => StorageValue::NS(value.clone()),
        AwsStreamAttributeValue::Bs(value) => StorageValue::BS(
            value
                .iter()
                .map(|blob| BASE64.encode(blob.as_ref()))
                .collect(),
        ),
        AwsStreamAttributeValue::Bool(value) => StorageValue::BOOL(*value),
        AwsStreamAttributeValue::Null(value) => StorageValue::NULL(*value),
        AwsStreamAttributeValue::L(values) => StorageValue::L(
            values
                .iter()
                .map(aws_stream_value_to_contract)
                .collect::<AnalyticsStorageResult<Vec<_>>>()?,
        ),
        AwsStreamAttributeValue::M(values) => StorageValue::M(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), aws_stream_value_to_contract(value)?)))
                .collect::<AnalyticsStorageResult<StorageItem>>()?,
        ),
        _ => {
            return Err(AnalyticsStorageError::new(
                AnalyticsStorageErrorKind::UnsupportedAwsAttributeValue,
            ));
        }
    })
}
