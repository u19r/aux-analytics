use std::cmp::Ordering;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de::Error as DeError};
use storage_types::{SystemStreamPosition, SystemStreamScope};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SourcePositionError {
    #[error("row identity cannot be empty")]
    EmptyRowIdentity,
    #[error("source position token cannot be empty")]
    EmptyPositionToken,
    #[error("SystemStream offset cannot be ordered")]
    InvalidSystemStreamOffset,
    #[error("source positions are incomparable: {left:?} and {right:?}")]
    Incomparable {
        left: Box<SourcePosition>,
        right: Box<SourcePosition>,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema, ToSchema,
)]
#[serde(try_from = "String", into = "String")]
pub struct RowIdentityValue(String);

impl RowIdentityValue {
    pub fn try_new(value: impl Into<String>) -> Result<Self, SourcePositionError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(SourcePositionError::EmptyRowIdentity);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<String> for RowIdentityValue {
    type Error = SourcePositionError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<RowIdentityValue> for String {
    fn from(value: RowIdentityValue) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub enum SourcePosition {
    DynamoDbStreamSequence {
        #[serde(deserialize_with = "deserialize_position_token")]
        stream_arn: String,
        #[serde(deserialize_with = "deserialize_position_token")]
        shard_id: String,
        #[serde(deserialize_with = "deserialize_position_token")]
        sequence_number: String,
    },
    AuxStorageStreamCursor {
        #[serde(deserialize_with = "deserialize_position_token")]
        stream_name: String,
        #[serde(deserialize_with = "deserialize_position_token")]
        cursor: String,
    },
    SystemStream {
        /// Versioned logical source authority. The serialized fields are
        /// PascalCase (`Version`, `ScopeId`, and `Generation`) because the
        /// storage contract owns this wire type.
        #[schemars(with = "SystemStreamScopeSchema")]
        source_scope: SystemStreamScope,
        /// Logical shard position. The serialized fields are PascalCase
        /// (`ShardId`, `LineageId`, and `Offset`) because the storage
        /// contract owns this wire type.
        #[schemars(with = "SystemStreamPositionSchema")]
        #[serde(deserialize_with = "deserialize_system_position")]
        position: SystemStreamPosition,
    },
    DynamoDbExportSnapshot {
        #[serde(deserialize_with = "deserialize_position_token")]
        export_arn: String,
        exported_at_ms: u64,
    },
    DynamoDbScanChunk {
        #[serde(deserialize_with = "deserialize_position_token")]
        table_name: String,
        scan_generation: u64,
        segment: u32,
        item_index: u64,
    },
    RawBackupObjectOffset {
        #[serde(deserialize_with = "deserialize_position_token")]
        object_id: String,
        offset: u64,
    },
}

fn deserialize_position_token<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
    let token = String::deserialize(deserializer)?;
    validate_token(&token).map_err(D::Error::custom)?;
    Ok(token)
}

fn deserialize_system_position<'de, D>(deserializer: D) -> Result<SystemStreamPosition, D::Error>
where D: Deserializer<'de> {
    let position = SystemStreamPosition::deserialize(deserializer)?;
    validate_system_position(&position).map_err(D::Error::custom)?;
    Ok(position)
}

fn validate_system_position(position: &SystemStreamPosition) -> Result<(), SourcePositionError> {
    position
        .offset()
        .compare_order(position.offset())
        .map(|_| ())
        .map_err(|_| SourcePositionError::InvalidSystemStreamOffset)
}

impl SourcePosition {
    pub fn validate(&self) -> Result<(), SourcePositionError> {
        match self {
            Self::DynamoDbStreamSequence {
                stream_arn,
                shard_id,
                sequence_number,
            } => {
                validate_token(stream_arn)?;
                validate_token(shard_id)?;
                validate_token(sequence_number)
            }
            Self::AuxStorageStreamCursor {
                stream_name,
                cursor,
            } => {
                validate_token(stream_name)?;
                validate_token(cursor)
            }
            Self::SystemStream { position, .. } => validate_system_position(position),
            Self::DynamoDbExportSnapshot { export_arn, .. } => validate_token(export_arn),
            Self::DynamoDbScanChunk { table_name, .. } => validate_token(table_name),
            Self::RawBackupObjectOffset { object_id, .. } => validate_token(object_id),
        }
    }

    #[must_use]
    pub fn partial_cmp_position(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (
                Self::DynamoDbStreamSequence {
                    stream_arn: left_stream,
                    shard_id: left_shard,
                    sequence_number: left_sequence,
                },
                Self::DynamoDbStreamSequence {
                    stream_arn: right_stream,
                    shard_id: right_shard,
                    sequence_number: right_sequence,
                },
            ) if left_stream == right_stream && left_shard == right_shard => {
                compare_decimal_sequences(left_sequence, right_sequence)
            }
            (
                Self::AuxStorageStreamCursor {
                    stream_name: left_stream,
                    cursor: left_cursor,
                },
                Self::AuxStorageStreamCursor {
                    stream_name: right_stream,
                    cursor: right_cursor,
                },
            ) if left_stream == right_stream => Some(left_cursor.cmp(right_cursor)),
            (
                Self::SystemStream {
                    source_scope: left_scope,
                    position: left_position,
                },
                Self::SystemStream {
                    source_scope: right_scope,
                    position: right_position,
                },
            ) if left_scope == right_scope
                && left_position.shard_id() == right_position.shard_id()
                && left_position.lineage_id() == right_position.lineage_id() =>
            {
                left_position
                    .offset()
                    .compare_order(right_position.offset())
                    .ok()
            }
            (
                Self::DynamoDbExportSnapshot {
                    export_arn: left_export,
                    exported_at_ms: left_time,
                },
                Self::DynamoDbExportSnapshot {
                    export_arn: right_export,
                    exported_at_ms: right_time,
                },
            ) if left_export == right_export => Some(left_time.cmp(right_time)),
            (
                Self::DynamoDbScanChunk {
                    table_name: left_table,
                    scan_generation: left_generation,
                    segment: left_segment,
                    item_index: left_index,
                },
                Self::DynamoDbScanChunk {
                    table_name: right_table,
                    scan_generation: right_generation,
                    segment: right_segment,
                    item_index: right_index,
                },
            ) if left_table == right_table
                && left_generation == right_generation
                && left_segment == right_segment =>
            {
                Some(left_index.cmp(right_index))
            }
            (
                Self::RawBackupObjectOffset {
                    object_id: left_object,
                    offset: left_offset,
                },
                Self::RawBackupObjectOffset {
                    object_id: right_object,
                    offset: right_offset,
                },
            ) if left_object == right_object => Some(left_offset.cmp(right_offset)),
            _ => None,
        }
    }
}

/// DynamoDB sequences are arbitrary-width decimals, not machine integers or
/// lexically ordered strings. Opaque non-decimal tokens establish no order.
fn compare_decimal_sequences(left: &str, right: &str) -> Option<Ordering> {
    if !left.bytes().all(|byte| byte.is_ascii_digit())
        || !right.bytes().all(|byte| byte.is_ascii_digit())
        || left.is_empty()
        || right.is_empty()
    {
        return None;
    }
    let left = left.trim_start_matches('0');
    let right = right.trim_start_matches('0');
    Some(left.len().cmp(&right.len()).then_with(|| left.cmp(right)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceMutation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MergeDecision {
    Insert,
    Replace,
    Delete,
    IgnoreDuplicate,
    IgnoreStale,
    Append,
    RequiresValidation,
}

pub fn merge_decision(
    existing_position: Option<&SourcePosition>,
    incoming_position: &SourcePosition,
    mutation: SourceMutation,
    append_only: bool,
) -> Result<MergeDecision, SourcePositionError> {
    incoming_position.validate()?;
    let Some(existing_position) = existing_position else {
        return Ok(if append_only {
            MergeDecision::Append
        } else {
            match mutation {
                SourceMutation::Insert | SourceMutation::Update => MergeDecision::Insert,
                SourceMutation::Delete => MergeDecision::Delete,
            }
        });
    };
    existing_position.validate()?;

    let Some(ordering) = incoming_position.partial_cmp_position(existing_position) else {
        return Ok(MergeDecision::RequiresValidation);
    };
    match ordering {
        Ordering::Less => Ok(MergeDecision::IgnoreStale),
        Ordering::Equal => Ok(MergeDecision::IgnoreDuplicate),
        Ordering::Greater if append_only => Ok(MergeDecision::Append),
        Ordering::Greater => Ok(match mutation {
            SourceMutation::Insert | SourceMutation::Update => MergeDecision::Replace,
            SourceMutation::Delete => MergeDecision::Delete,
        }),
    }
}

fn validate_token(value: &str) -> Result<(), SourcePositionError> {
    if value.trim().is_empty() {
        return Err(SourcePositionError::EmptyPositionToken);
    }
    Ok(())
}

/// Schemars cannot derive a schema for the storage contract's custom
/// validated string wrappers. Keep this schema description local to this
/// public contract while serde continues to serialize the real storage types.
#[derive(Serialize, JsonSchema)]
#[schemars(rename_all = "PascalCase", deny_unknown_fields)]
struct SystemStreamScopeSchema {
    #[schemars(description = "Version of the logical SystemStream source scope")]
    version: u8,
    #[schemars(description = "Logical SystemStream source scope identifier")]
    scope_id: String,
    #[schemars(description = "Generation fencing recreation of the source scope")]
    generation: u64,
}

#[derive(Serialize, JsonSchema)]
#[schemars(rename_all = "PascalCase", deny_unknown_fields)]
struct SystemStreamPositionSchema {
    #[schemars(description = "Logical SystemStream shard identifier")]
    shard_id: String,
    #[schemars(description = "Logical SystemStream shard lineage identifier")]
    lineage_id: String,
    #[schemars(description = "Provider opaque offset ordered within one shard lineage")]
    offset: String,
}
