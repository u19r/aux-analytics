mod driver;
mod feeder;
pub(crate) mod limits;
#[cfg(test)]
mod limits_tests;

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use analytics_engine::StorageBackend;
use analytics_storage::{SourceCheckpoint, SourcePoller, SourceTablePlan, SystemStreamResume};
#[cfg(feature = "bdd-test-support")]
use bg_jobs::runtime::JobSystem;
use bg_jobs::{JobHandle, JobStore, Namespace, OwnerId};
use bg_jobs_tokio::JobRuntime;
#[cfg(any(test, feature = "bdd-test-support"))]
use bg_jobs_tokio::test_support::JobClock;
use config::{AnalyticsJobStoreConfig, AnalyticsSourceConfig};
use storage::{
    RemoteScheduledJobStoreConfig, ScheduledJobStore, connect_remote_scheduled_job_store,
};

use self::limits::{bound_source_page, driver_budget, job_store_settings, source_job_shared};
#[cfg(any(test, feature = "bdd-test-support"))]
use crate::background_jobs::test_support;
use crate::{
    AppState, SourceHealth, SourcePollingPhase,
    background_jobs::{
        JobCatalogBundle,
        contracts::{SourceJobKind, SourceJobPayload},
        error::api_config_error,
        observation::{BackgroundJobObserver, SourceJobShared, set_source_phase},
    },
    constants::{BG_JOBS_NAMESPACE, MAX_SOURCE_JOB_TIMEOUT},
    error::{ApiError, ApiErrorKind, ApiResult},
};

mod composition;
mod polling;
pub(crate) use composition::BackgroundJobRuntime;
#[cfg(any(test, feature = "bdd-test-support"))]
pub(crate) use composition::TestRuntimeInput;
pub(crate) use limits::JobStoreSettings;
