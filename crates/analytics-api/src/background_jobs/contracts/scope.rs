use super::*;
use crate::source_polling::source_effect_scope;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SystemStreamCheckpointPredecessor {
    Initial,
    Token {
        checkpoint: SystemStreamCheckpointToken,
    },
}

/// The durable source authority represented by a source job. A SystemStream
/// scope carries only the DB-owned versioned authority and the opaque issued
/// checkpoint; no physical table or synthetic shard is introduced.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "scope_kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SourceJobScope {
    Table {
        source_table_name: String,
        shard_id: String,
        previous_position: Option<String>,
    },
    System {
        source_scope: SystemStreamScope,
        previous_checkpoint: SystemStreamCheckpointPredecessor,
        checkpoint: SystemStreamCheckpointToken,
    },
}

impl SourceJobScope {
    pub(super) fn from_poll_batch(batch: &PollBatch) -> Result<Self, BackgroundJobError> {
        if let Some(system_stream) = batch.system_stream.as_ref() {
            return Ok(Self::system(system_stream));
        }
        let scope = source_effect_scope(batch)
            .map_err(|_| BackgroundJobError::InvalidPayload("source job scope is invalid"))?
            .ok_or(BackgroundJobError::MissingScope)?;
        let (source_table_name, shard_id) = scope
            .table_parts()
            .ok_or(BackgroundJobError::MissingScope)?;
        Ok(Self::table_with_predecessor(
            source_table_name,
            shard_id,
            scope.table_previous_position().map(ToOwned::to_owned),
        ))
    }

    pub(crate) fn table_with_predecessor(
        source_table_name: &str,
        shard_id: &str,
        previous_position: Option<String>,
    ) -> Self {
        Self::Table {
            source_table_name: source_table_name.to_string(),
            shard_id: shard_id.to_string(),
            previous_position,
        }
    }

    pub(crate) fn system(batch: &SystemStreamBatch) -> Self {
        Self::System {
            source_scope: batch.source_scope.clone(),
            previous_checkpoint: match &batch.previous_checkpoint {
                None => SystemStreamCheckpointPredecessor::Initial,
                Some(checkpoint) => SystemStreamCheckpointPredecessor::Token {
                    checkpoint: checkpoint.clone(),
                },
            },
            checkpoint: batch.checkpoint.clone(),
        }
    }
}
