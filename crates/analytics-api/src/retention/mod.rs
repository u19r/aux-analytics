use self::cache::RetentionCacheEntry;
mod cache;
pub(crate) mod schedule;
use std::{collections::HashMap, time::Instant};

use analytics_contract::{AnalyticsManifest, RetentionTimestamp, StorageStreamRecord};
use analytics_engine::{IngestRetention, TenantResolver};
use analytics_storage::RetentionPolicyLookup;
use config::{AnalyticsRetentionConfig, RetentionTimestampConfig};
#[cfg(feature = "bdd-test-support")]
pub(crate) use schedule::retention_schedule_due_for_bdd;
use tokio::sync::RwLock;

use crate::constants::{
    RETENTION_LOOKUP_FAILURES_TOTAL_METRIC, RETENTION_LOOKUP_LATENCY_METRIC,
    RETENTION_LOOKUPS_TOTAL_METRIC,
};

mod runtime;
mod table;
pub use runtime::RetentionRuntime;
pub(crate) use schedule::RetentionScheduleState;
use table::RetentionTableRuntime;
