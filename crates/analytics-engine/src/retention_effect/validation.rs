use std::num::NonZeroU64;

use super::contracts::*;
use crate::{
    AnalyticsEngineError, AnalyticsEngineResult,
    constants::{EXECUTION_ID_BYTES, MAX_SCOPE_COMPONENT_BYTES},
};
pub(super) fn valid_scope_component(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_SCOPE_COMPONENT_BYTES
        && !value.chars().any(char::is_control)
}

pub(super) fn valid_cursor(value: &str) -> bool {
    valid_scope_component(value)
}

pub(super) fn decode_execution_id(bytes: &[u8]) -> AnalyticsEngineResult<[u8; EXECUTION_ID_BYTES]> {
    bytes.try_into().map_err(
        |_| AnalyticsEngineError::InvalidRetentionEffectExecutionId {
            actual: bytes.len(),
        },
    )
}

pub(super) fn decode_version(value: i64) -> AnalyticsEngineResult<()> {
    if value == 1 {
        Ok(())
    } else {
        Err(AnalyticsEngineError::InvalidRetentionEffectReceipt)
    }
}

pub(super) fn decode_fence(value: &str) -> AnalyticsEngineResult<NonZeroU64> {
    let fence = value
        .parse::<u64>()
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(AnalyticsEngineError::InvalidRetentionEffectFence)?;
    if fence.get().to_string() != value {
        return Err(AnalyticsEngineError::InvalidRetentionEffectFence);
    }
    Ok(fence)
}

pub(super) fn decode_cursor(value: Option<&str>) -> AnalyticsEngineResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !valid_cursor(value) {
        return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
    }
    Ok(Some(value.to_string()))
}

pub(super) fn decode_batch_size(value: i64) -> AnalyticsEngineResult<u64> {
    let value =
        u64::try_from(value).map_err(|_| AnalyticsEngineError::InvalidRetentionEffectReceipt)?;
    if value == 0 || value > MAX_RETENTION_BATCH_SIZE {
        return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
    }
    Ok(value)
}

pub(super) fn decode_count(value: i64) -> AnalyticsEngineResult<u64> {
    u64::try_from(value).map_err(|_| AnalyticsEngineError::InvalidRetentionEffectReceipt)
}

pub(super) fn encode_server_time(value: u64) -> AnalyticsEngineResult<i64> {
    i64::try_from(value).map_err(|_| {
        AnalyticsEngineError::InvalidRetentionEffectRequest(
            "server time is outside the supported time range",
        )
    })
}

pub(super) fn decode_server_time(value: i64) -> AnalyticsEngineResult<u64> {
    u64::try_from(value).map_err(|_| AnalyticsEngineError::InvalidRetentionEffectReceipt)
}

pub(super) fn decode_occurrence(
    version: i64,
    cutoff_ms: i64,
    schedule_at_ms: i64,
    server_time_ms: i64,
    cursor: Option<&str>,
    complete: bool,
) -> AnalyticsEngineResult<RetentionScheduleOccurrence> {
    if version != 1 || cutoff_ms < 0 || schedule_at_ms < 0 || cutoff_ms != schedule_at_ms {
        return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
    }
    let server_time_ms = decode_server_time(server_time_ms)?;
    if server_time_ms < u64::try_from(schedule_at_ms).unwrap_or(u64::MAX) {
        return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
    }
    let cursor = decode_cursor(cursor)?;
    if complete && cursor.is_some() {
        return Err(AnalyticsEngineError::InvalidRetentionEffectReceipt);
    }
    Ok(RetentionScheduleOccurrence {
        cutoff_ms,
        schedule_at_ms,
        server_time_ms,
        cursor,
        complete,
    })
}

pub(super) fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
