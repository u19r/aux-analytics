use std::num::NonZeroU64;

use analytics_contract::{
    AnalyticsManifest, StorageStreamRecord, TenantRangePurgeRequest, TenantRangePurgeResponse,
};

use crate::{
    AnalyticsEngineError, AnalyticsEngineResult, constants::MAX_MAINTENANCE_EXECUTION_ID_BYTES,
};

/// Destination authority supplied by the maintenance execution owner.
///
/// `execution_id` is the stable logical operation identity. `fence` is the
/// monotonic execution generation and is compared in the same transaction as
/// the destination effect and receipt.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MaintenanceEffectAuthority {
    execution_id: String,
    fence: NonZeroU64,
}

impl MaintenanceEffectAuthority {
    pub fn try_new(execution_id: impl Into<String>, fence: u64) -> AnalyticsEngineResult<Self> {
        let execution_id = execution_id.into();
        validate_execution_id(&execution_id)?;
        let fence =
            NonZeroU64::new(fence).ok_or(AnalyticsEngineError::InvalidMaintenanceEffectFence)?;
        Ok(Self {
            execution_id,
            fence,
        })
    }

    #[must_use]
    pub fn execution_id(&self) -> &str {
        self.execution_id.as_str()
    }

    #[must_use]
    pub const fn fence(&self) -> NonZeroU64 {
        self.fence
    }
}

/// Inputs for applying one source record as a fenced maintenance effect.
///
/// The record is owned because applying it transfers ownership into the
/// transaction-aware ingestion path. The remaining references keep the
/// request lightweight without cloning manifests or authority state.
pub struct MaintenanceEffectApplyRequest<'a> {
    pub manifest: &'a AnalyticsManifest,
    pub analytics_table_name: &'a str,
    pub record_key: &'a [u8],
    pub record: StorageStreamRecord,
    pub repair_key: &'a str,
    pub source_revision: &'a str,
    pub authority: &'a MaintenanceEffectAuthority,
}

/// Inputs for verifying one previously applied fenced maintenance effect.
///
/// Verification borrows the requested record because it never needs to
/// transfer it into ingestion.
pub struct MaintenanceEffectVerifyRequest<'a> {
    pub manifest: &'a AnalyticsManifest,
    pub analytics_table_name: &'a str,
    pub record_key: &'a [u8],
    pub expected: &'a StorageStreamRecord,
    pub repair_key: &'a str,
    pub source_revision: &'a str,
    pub authority: &'a MaintenanceEffectAuthority,
}

/// Result of one atomically fenced maintenance effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceEffectDisposition {
    Applied,
    AlreadyApplied,
}

/// Inputs for one atomically fenced tenant-range purge chunk.
#[derive(Clone, Copy)]
pub struct TenantRangePurgeEffectRequest<'a> {
    pub request: &'a TenantRangePurgeRequest,
    pub authority: &'a bg_jobs::RemoteJobClaim,
    pub request_id: &'a str,
}

pub(crate) fn validate_execution_id(execution_id: &str) -> AnalyticsEngineResult<()> {
    if execution_id.trim().is_empty()
        || execution_id.len() > MAX_MAINTENANCE_EXECUTION_ID_BYTES
        || execution_id.chars().any(char::is_control)
    {
        return Err(AnalyticsEngineError::InvalidMaintenanceEffectExecutionId);
    }
    Ok(())
}

/// Result retained by the destination receipt for a tenant-range purge chunk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TenantRangePurgeEffectDisposition {
    Applied(TenantRangePurgeResponse),
    AlreadyApplied(TenantRangePurgeResponse),
}
