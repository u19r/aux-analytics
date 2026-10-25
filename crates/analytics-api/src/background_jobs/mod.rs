use std::{collections::HashMap, sync::Arc, time::Duration};

use bg_jobs::{JobCatalog, JobHandle};
use tokio::sync::RwLock;

mod authority;
#[cfg(feature = "bdd-test-support")]
mod bdd;
mod contracts;
mod error;
mod observation;
mod retention;
mod retention_contracts;
pub(crate) mod runtime;
mod source;
mod source_record_destinations;
#[cfg(test)]
mod source_record_destinations_tests;
#[cfg(any(test, feature = "bdd-test-support"))]
pub(crate) mod test_support;

#[cfg(test)]
pub(crate) use authority::SourceJobAuthority;
#[cfg(feature = "bdd-test-support")]
pub(crate) use bdd::{
    cancel_and_repair_source, retention_cancel_and_repair, retention_schedule, source_stale_fence,
    start_source_polling, system_stream_cutover,
};
pub(crate) use contracts::{SourceJobKind, SourceJobPayload};
#[cfg(test)]
pub(crate) use error::BackgroundJobError;
#[cfg(test)]
pub(crate) use observation::{BackgroundJobObserver, SourceJobShared};
pub(crate) use retention_contracts::RetentionJobPayload;
pub(crate) use runtime::BackgroundJobRuntime;
#[cfg(test)]
pub(crate) use runtime::limits::source_job_deduplication;
#[cfg(feature = "bdd-test-support")]
pub(crate) use runtime::limits::source_job_shared_for_bdd;
#[cfg(any(test, feature = "bdd-test-support"))]
pub(crate) use runtime::limits::{bounded_timeout, source_deadline_at_ms};
#[cfg(test)]
pub(crate) use source::settle_source_effect;

mod catalog;
pub(crate) use catalog::JobCatalogBundle;
#[cfg(any(test, feature = "bdd-test-support"))]
pub(crate) use catalog::job_catalog;

#[cfg(test)]
mod background_jobs_tests;
#[cfg(test)]
mod retention_tests;
