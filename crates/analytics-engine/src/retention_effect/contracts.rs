use std::num::NonZeroU64;

pub use config::MAX_RETENTION_DELETE_BATCH_SIZE as MAX_RETENTION_BATCH_SIZE;

use super::validation::{valid_cursor, valid_scope_component};
pub use crate::constants::EXECUTION_ID_BYTES;
use crate::{AnalyticsEngineResult, engine::AnalyticsEngineError};
/// Destination authority for one job execution. Its fence increases when
/// that job is reclaimed; distinct batch jobs can have equal fences.
/// Authorized occurrence progress and its exact predecessor cursor order
/// separate batches. The destination receipt makes an execution replay safe.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RetentionEffectAuthority {
    execution_id: [u8; EXECUTION_ID_BYTES],
    fence: NonZeroU64,
}
impl RetentionEffectAuthority {
    pub fn try_new(execution_id: impl AsRef<[u8]>, fence: u64) -> AnalyticsEngineResult<Self> {
        let bytes = execution_id.as_ref();
        let execution_id = bytes.try_into().map_err(|_| {
            AnalyticsEngineError::InvalidRetentionEffectExecutionId {
                actual: bytes.len(),
            }
        })?;
        let fence =
            NonZeroU64::new(fence).ok_or(AnalyticsEngineError::InvalidRetentionEffectFence)?;
        Ok(Self {
            execution_id,
            fence,
        })
    }
    #[must_use]
    pub const fn from_parts(execution_id: [u8; EXECUTION_ID_BYTES], fence: NonZeroU64) -> Self {
        Self {
            execution_id,
            fence,
        }
    }
    #[must_use]
    pub const fn execution_id(&self) -> &[u8; EXECUTION_ID_BYTES] {
        &self.execution_id
    }
    #[must_use]
    pub const fn fence(&self) -> NonZeroU64 {
        self.fence
    }
}
/// Exact analytics table covered by a retention effect.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RetentionEffectScope {
    table_name: String,
}

impl RetentionEffectScope {
    pub fn try_new(table_name: impl Into<String>) -> AnalyticsEngineResult<Self> {
        let table_name = table_name.into();
        if !valid_scope_component(&table_name) {
            return Err(AnalyticsEngineError::InvalidRetentionEffectScope);
        }
        Ok(Self { table_name })
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        self.table_name.as_str()
    }
}

/// Inputs for one validated bounded retention batch.
pub struct RetentionEffectRequestInput {
    pub authority: RetentionEffectAuthority,
    pub scope: RetentionEffectScope,
    pub cutoff_ms: i64,
    pub schedule_at_ms: i64,
    pub server_time_ms: u64,
    pub batch_size: u64,
    pub cursor: Option<String>,
}

/// Validated retention effect request owned by one destination scope.
pub struct RetentionEffectRequest {
    pub(crate) authority: RetentionEffectAuthority,
    pub(crate) scope: RetentionEffectScope,
    pub(crate) cutoff_ms: i64,
    pub(crate) schedule_at_ms: i64,
    pub(crate) server_time_ms: u64,
    pub(crate) batch_size: u64,
    pub(crate) cursor: Option<String>,
}

impl RetentionEffectRequest {
    pub fn try_new(input: RetentionEffectRequestInput) -> AnalyticsEngineResult<Self> {
        if input.cutoff_ms < 0 {
            return Err(AnalyticsEngineError::InvalidRetentionEffectRequest(
                "cutoff must be non-negative",
            ));
        }
        Self::validate_schedule(input.cutoff_ms, input.schedule_at_ms, input.server_time_ms)?;
        if input.batch_size == 0 || input.batch_size > MAX_RETENTION_BATCH_SIZE {
            return Err(AnalyticsEngineError::InvalidRetentionEffectRequest(
                "batch size is outside its supported bound",
            ));
        }
        if input
            .cursor
            .as_deref()
            .is_some_and(|cursor| !valid_cursor(cursor))
        {
            return Err(AnalyticsEngineError::InvalidRetentionEffectRequest(
                "cursor is empty, oversized, or contains control characters",
            ));
        }
        Ok(Self {
            authority: input.authority,
            scope: input.scope,
            cutoff_ms: input.cutoff_ms,
            schedule_at_ms: input.schedule_at_ms,
            server_time_ms: input.server_time_ms,
            batch_size: input.batch_size,
            cursor: input.cursor,
        })
    }

    pub(crate) fn validate_schedule(
        cutoff_ms: i64,
        schedule_at_ms: i64,
        server_time_ms: u64,
    ) -> AnalyticsEngineResult<()> {
        if schedule_at_ms < 0 || schedule_at_ms != cutoff_ms {
            return Err(AnalyticsEngineError::InvalidRetentionEffectRequest(
                "cutoff must equal the non-negative schedule occurrence",
            ));
        }
        let schedule_at_ms = u64::try_from(schedule_at_ms).map_err(|_| {
            AnalyticsEngineError::InvalidRetentionEffectRequest(
                "schedule occurrence is outside the supported time range",
            )
        })?;
        if server_time_ms == 0 || server_time_ms < schedule_at_ms {
            return Err(AnalyticsEngineError::InvalidRetentionEffectRequest(
                "server time must be at or after the schedule occurrence",
            ));
        }
        i64::try_from(server_time_ms).map_err(|_| {
            AnalyticsEngineError::InvalidRetentionEffectRequest(
                "server time is outside the supported time range",
            )
        })?;
        Ok(())
    }
}

/// Bounded result retained by a destination receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionEffectSummary {
    rows_deleted: u64,
    cursor: Option<String>,
    complete: bool,
}

impl RetentionEffectSummary {
    pub(crate) fn from_parts(rows_deleted: u64, cursor: Option<String>, complete: bool) -> Self {
        Self {
            rows_deleted,
            cursor,
            complete,
        }
    }

    #[must_use]
    pub const fn rows_deleted(&self) -> u64 {
        self.rows_deleted
    }

    #[must_use]
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }
}

/// Result of the atomic destination transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetentionEffectDisposition {
    Applied { summary: RetentionEffectSummary },
    AlreadyApplied { summary: RetentionEffectSummary },
    StaleFence,
}

#[derive(Clone, Debug)]
pub(crate) struct StoredAuthority {
    pub(crate) execution_id: [u8; EXECUTION_ID_BYTES],
    pub(crate) fence: NonZeroU64,
    pub(crate) cursor: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct StoredReceipt {
    pub(crate) table_name: String,
    pub(crate) fence: NonZeroU64,
    pub(crate) cutoff_ms: i64,
    pub(crate) schedule_at_ms: i64,
    pub(crate) server_time_ms: u64,
    pub(crate) batch_size: u64,
    pub(crate) cursor_before: Option<String>,
    pub(crate) summary: RetentionEffectSummary,
}

#[derive(Clone, Debug)]
/// Destination-owned progress for one authorized retention sweep.
pub struct RetentionScheduleOccurrence {
    pub cutoff_ms: i64,
    pub schedule_at_ms: i64,
    pub server_time_ms: u64,
    pub cursor: Option<String>,
    pub complete: bool,
}
