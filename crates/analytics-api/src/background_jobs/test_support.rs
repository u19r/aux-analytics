#[cfg(feature = "bdd-test-support")]
use std::time::{SystemTime, UNIX_EPOCH};
use std::{sync::Arc, time::Duration};
#[cfg(any(test, feature = "bdd-test-support"))]
use std::{sync::OnceLock, time::Instant};

#[cfg(feature = "bdd-test-support")]
use bg_jobs::Timestamp;
#[cfg(any(test, feature = "bdd-test-support"))]
use bg_jobs::runtime::MonotonicDeadline;
#[cfg(feature = "bdd-test-support")]
use bg_jobs::store::StoreContractError;
use bg_jobs::{
    DriveBudget, JobCatalog, JobStore, Namespace, OwnerId, ValidationError,
    observer::JobObserver,
    runtime::{JobSystem, JobSystemBuilder, WakeScheduler},
    test_support::{FakeStore, TestProfile, raw::DriveRequest},
};
#[cfg(any(test, feature = "bdd-test-support"))]
const DRIVER_CLAIMS: u32 = 8;
#[cfg(any(test, feature = "bdd-test-support"))]
const DRIVER_CONCURRENCY: u16 = 4;
#[cfg(any(test, feature = "bdd-test-support"))]
const DRIVER_STORE_OPERATIONS: u32 = 64;
#[cfg(any(test, feature = "bdd-test-support"))]
const MONOTONIC_DRIVE_WINDOW: Duration = Duration::from_millis(250);

#[cfg(feature = "bdd-test-support")]
pub(crate) fn wall_now() -> Timestamp {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    Timestamp::from_millis(i64::try_from(milliseconds).unwrap_or(i64::MAX))
}

#[cfg(any(test, feature = "bdd-test-support"))]
pub(crate) fn monotonic_now() -> MonotonicDeadline {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    let elapsed = ORIGIN.get_or_init(Instant::now).elapsed();
    MonotonicDeadline::from_nanos(elapsed.as_nanos().try_into().unwrap_or(u64::MAX))
}

#[cfg(any(test, feature = "bdd-test-support"))]
pub(crate) fn drive_request() -> Result<DriveRequest, ValidationError> {
    let budget = DriveBudget::try_from(bg_jobs::DriveBudgetInput {
        queues: bg_jobs::QueueSelection::all(),
        max_claims: DRIVER_CLAIMS,
        max_concurrency: DRIVER_CONCURRENCY,
        max_store_operations: DRIVER_STORE_OPERATIONS,
        stop_claiming_after: MONOTONIC_DRIVE_WINDOW,
        finish_after: MONOTONIC_DRIVE_WINDOW.saturating_mul(2),
    })?;
    budget.request_at(monotonic_now())
}

pub(crate) fn build_system(
    store: FakeStore,
    catalog: JobCatalog,
    namespace: &[u8],
    owner: u8,
    observer: Option<Arc<dyn JobObserver>>,
) -> JobSystem<FakeStore> {
    build_system_with_scheduler(
        store,
        catalog,
        namespace,
        OwnerId::from([owner; 16]),
        test_scheduler(),
        observer,
    )
    .expect("test job system is valid")
}

pub(crate) fn build_system_with_scheduler<S: JobStore>(
    store: S,
    catalog: JobCatalog,
    namespace: &[u8],
    owner: OwnerId,
    scheduler: Arc<dyn WakeScheduler>,
    observer: Option<Arc<dyn JobObserver>>,
) -> Result<JobSystem<S>, ValidationError> {
    let mut builder = JobSystemBuilder::new(store)
        .catalog(catalog)
        .namespace(Namespace::try_new(namespace).expect("test namespace is valid"))
        .owner(owner)
        .scheduler(scheduler);
    if let Some(observer) = observer {
        builder = builder.with_observer(observer);
    }
    builder.build()
}

#[cfg(feature = "bdd-test-support")]
pub(crate) fn build_distributed_system<S: JobStore>(
    store: S,
    catalog: JobCatalog,
    namespace: &[u8],
    owner: u8,
    observer: Option<Arc<dyn JobObserver>>,
) -> Result<JobSystem<S>, StoreContractError> {
    let mut builder = JobSystemBuilder::new(store)
        .catalog(catalog)
        .namespace(Namespace::try_new(namespace).expect("test namespace is valid"))
        .owner(OwnerId::from([owner; 16]))
        .scheduler(test_scheduler());
    if let Some(observer) = observer {
        builder = builder.with_observer(observer);
    }
    builder.build_distributed()
}

pub(crate) fn test_scheduler() -> Arc<dyn WakeScheduler> {
    Arc::new(TestProfile::new())
}
