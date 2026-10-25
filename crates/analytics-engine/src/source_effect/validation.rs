use std::num::NonZeroU64;

use analytics_contract::AnalyticsManifest;

use super::contracts::SourceEffectScope;
use crate::{
    constants::{CONTENT_DIGEST_BYTES, EXECUTION_ID_BYTES, MAX_SCOPE_COMPONENT_BYTES},
    engine::{AnalyticsEngineError, AnalyticsEngineResult, RecordCheckpoint, SourceBatchRecord},
};

pub(crate) fn validate_record_registrations(
    manifest: &AnalyticsManifest,
    records: &[SourceBatchRecord],
) -> AnalyticsEngineResult<()> {
    for record in records {
        let Some(table) = manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == record.item.analytics_table_name)
        else {
            return Err(AnalyticsEngineError::TableNotRegistered(
                record.item.analytics_table_name.clone(),
            ));
        };
        let source_matches = table.source_table_name_prefix.as_deref().map_or_else(
            || table.source_table_name == record.item.source_table_name,
            |prefix| record.item.source_table_name.starts_with(prefix),
        );
        if !source_matches {
            return Err(AnalyticsEngineError::RecordDoesNotMatchRegistration(
                table.analytics_table_name.clone(),
            ));
        }
    }
    Ok(())
}

/// Bounded outcome data retained in a destination effect receipt.
pub(crate) fn validate_scope_contents(
    scope: &SourceEffectScope,
    records: &[SourceBatchRecord],
    checkpoints: &[RecordCheckpoint],
) -> AnalyticsEngineResult<()> {
    match scope {
        SourceEffectScope::Table {
            source_table_name,
            shard_id,
            ..
        } => {
            for record in records {
                if record.item.source_table_name != *source_table_name
                    || record.item.shard_id.as_deref() != Some(shard_id.as_str())
                    || record.item.system_position.is_some()
                {
                    return Err(AnalyticsEngineError::MixedSourceEffectScope);
                }
            }
            if checkpoints.iter().any(|checkpoint| {
                checkpoint.source_table_name() != source_table_name
                    || checkpoint.shard_id() != shard_id
            }) {
                return Err(AnalyticsEngineError::MixedSourceEffectScope);
            }
        }
        SourceEffectScope::System { .. } => {
            if !checkpoints.is_empty()
                || records
                    .iter()
                    .any(|record| record.item.system_position.is_none())
            {
                return Err(AnalyticsEngineError::MixedSourceEffectScope);
            }
        }
    }
    if checkpoints.len() > 1 {
        return Err(AnalyticsEngineError::DuplicateSourceEffectCheckpoint);
    }
    Ok(())
}

pub(crate) fn valid_scope_component(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_SCOPE_COMPONENT_BYTES
        && !value.chars().any(char::is_control)
}

pub(crate) fn decode_execution_id(bytes: &[u8]) -> AnalyticsEngineResult<[u8; EXECUTION_ID_BYTES]> {
    bytes
        .try_into()
        .map_err(|_| AnalyticsEngineError::InvalidSourceEffectExecutionId {
            actual: bytes.len(),
        })
}

pub(crate) fn decode_content_digest(
    bytes: Option<&[u8]>,
) -> AnalyticsEngineResult<Option<[u8; CONTENT_DIGEST_BYTES]>> {
    bytes
        .map(|bytes| {
            bytes
                .try_into()
                .map_err(|_| AnalyticsEngineError::InvalidSourceEffectReceipt)
        })
        .transpose()
}

pub(crate) fn decode_fence(value: &str) -> AnalyticsEngineResult<NonZeroU64> {
    let fence = value
        .parse::<u64>()
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(AnalyticsEngineError::InvalidSourceEffectFence)?;
    if fence.get().to_string() != value {
        return Err(AnalyticsEngineError::InvalidSourceEffectFence);
    }
    Ok(fence)
}

pub(crate) fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub(crate) fn decode_count(value: i64) -> AnalyticsEngineResult<u64> {
    u64::try_from(value).map_err(|_| AnalyticsEngineError::InvalidSourceEffectReceipt)
}
