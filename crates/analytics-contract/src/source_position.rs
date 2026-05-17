use std::cmp::Ordering;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SourcePositionError {
    #[error("row identity cannot be empty")]
    EmptyRowIdentity,
    #[error("source position token cannot be empty")]
    EmptyPositionToken,
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
    pub fn new(value: impl Into<String>) -> Result<Self, SourcePositionError> {
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
        Self::new(value)
    }
}

impl From<RowIdentityValue> for String {
    fn from(value: RowIdentityValue) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourcePosition {
    DynamoDbStreamSequence {
        stream_arn: String,
        shard_id: String,
        sequence_number: String,
    },
    AuxStorageStreamCursor {
        stream_name: String,
        cursor: String,
    },
    DynamoDbExportSnapshot {
        export_arn: String,
        exported_at_ms: u64,
    },
    DynamoDbScanChunk {
        table_name: String,
        scan_generation: u64,
        segment: u32,
        item_index: u64,
    },
    RawBackupObjectOffset {
        object_id: String,
        offset: u64,
    },
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
                Some(left_sequence.cmp(right_sequence))
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
