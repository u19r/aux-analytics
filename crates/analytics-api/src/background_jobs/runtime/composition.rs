use super::*;

pub(crate) struct BackgroundJobRuntime<S: JobStore = ScheduledJobStore> {
    // The raw system is intentionally retained only for BDD lifecycle probes;
    // the production driver uses `runtime` and its typed facade.
    #[cfg(feature = "bdd-test-support")]
    pub(crate) system: Arc<JobSystem<S>>,
    pub(crate) runtime: Arc<JobRuntime<S>>,
    pub(super) source_handles: HashMap<SourceJobKind, JobHandle<SourceJobPayload>>,
    pub(crate) retention_handle: Option<JobHandle<crate::background_jobs::RetentionJobPayload>>,
    pub(super) schedule_store: S,
    pub(super) shared: Arc<SourceJobShared>,
    pub(crate) source_job_timeout: Duration,
    pub(super) driver_started: AtomicBool,
    pub(super) feeder_shutdown_requested: AtomicBool,
    pub(super) feeder_shutdown: tokio::sync::Notify,
    pub(super) feeder_handles: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub(crate) retention_schedule: Option<Arc<crate::retention::RetentionScheduleState>>,
    pub(crate) retention_health: Option<Arc<tokio::sync::RwLock<crate::RetentionHealth>>>,
}

#[cfg(any(test, feature = "bdd-test-support"))]
pub(crate) struct TestRuntimeInput<S: JobStore> {
    pub(crate) store: S,
    pub(crate) catalog: crate::background_jobs::JobCatalogBundle,
    pub(crate) owner: OwnerId,
    pub(crate) shared: Arc<SourceJobShared>,
    pub(crate) retention: Option<Arc<crate::retention::RetentionRuntime>>,
    pub(crate) health: Option<Arc<tokio::sync::RwLock<crate::RetentionHealth>>>,
    pub(crate) source_job_timeout: Duration,
    pub(crate) clock: Arc<dyn JobClock>,
}

impl BackgroundJobRuntime<ScheduledJobStore> {
    pub(crate) async fn compose(
        job_store: &AnalyticsJobStoreConfig,
        backend: &StorageBackend,
        app_state: &AppState,
    ) -> ApiResult<Option<Arc<Self>>> {
        let Some(settings) = job_store_settings(job_store, backend)? else {
            return Ok(None);
        };
        app_state
            .engine
            .with_write(analytics_engine::AnalyticsEngine::verify_destination_fence)
            .await
            .map_err(|error| ApiError::with_source(ApiErrorKind::Config, error))?;

        let source_job_timeout = MAX_SOURCE_JOB_TIMEOUT;
        let store = connect_job_store(settings).await?;
        let shared = source_job_shared(app_state);
        let retention = app_state
            .retention
            .clone()
            .map(|retention| (retention, Arc::clone(&app_state.retention_health)));
        let catalog =
            JobCatalogBundle::try_new(Arc::clone(&shared), retention, source_job_timeout)?;
        let runtime = register_job_runtime(
            store.clone(),
            catalog.catalog,
            source_job_timeout,
            configure_observer(&shared, app_state),
        )?;
        let runtime = Arc::new(runtime);
        #[cfg(feature = "bdd-test-support")]
        let system = runtime.test_system();
        Ok(Some(Arc::new(Self {
            #[cfg(feature = "bdd-test-support")]
            system,
            runtime,
            source_handles: catalog.source_handles,
            retention_handle: catalog.retention_handle,
            schedule_store: store,
            shared,
            source_job_timeout,
            driver_started: AtomicBool::new(false),
            feeder_shutdown_requested: AtomicBool::new(false),
            feeder_shutdown: tokio::sync::Notify::new(),
            feeder_handles: tokio::sync::Mutex::new(Vec::new()),
            retention_schedule: app_state
                .retention
                .clone()
                .map(crate::retention::RetentionScheduleState::new),
            retention_health: app_state
                .retention
                .clone()
                .map(|_| Arc::clone(&app_state.retention_health)),
        })))
    }
}

fn configure_observer(
    shared: &SourceJobShared,
    app_state: &AppState,
) -> Arc<BackgroundJobObserver> {
    let observer =
        BackgroundJobObserver::new(Arc::clone(&shared.source_health), shared.worker_id.clone());
    let observer = if app_state.retention.is_some() {
        observer.with_retention_health(Arc::clone(&app_state.retention_health))
    } else {
        observer
    };
    Arc::new(observer)
}

impl<S: JobStore + Clone> BackgroundJobRuntime<S> {
    pub(crate) fn schedule_store(&self) -> &S {
        &self.schedule_store
    }

    pub(crate) fn engine_access(&self) -> crate::AnalyticsEngineAccess {
        self.shared.engine.clone()
    }

    #[cfg(any(test, feature = "bdd-test-support"))]
    pub(crate) fn for_test(input: TestRuntimeInput<S>) -> Arc<Self> {
        let TestRuntimeInput {
            store,
            catalog,
            owner,
            shared,
            retention,
            health,
            source_job_timeout,
            clock,
        } = input;
        let scheduler = test_support::test_scheduler();
        let budget = driver_budget(source_job_timeout).expect("test job budget is valid");
        let runtime =
            bg_jobs_tokio::test_support::runtime_builder(store.clone(), catalog.catalog, budget)
                .namespace(Namespace::try_new(BG_JOBS_NAMESPACE).expect("test namespace is valid"))
                .owner(owner)
                .scheduler(Arc::clone(&scheduler))
                .clock(clock)
                .build()
                .expect("test job runtime configuration is valid");
        let runtime = Arc::new(runtime);
        #[cfg(feature = "bdd-test-support")]
        let system = runtime.test_system();
        Arc::new(Self {
            #[cfg(feature = "bdd-test-support")]
            system,
            runtime,
            source_handles: catalog.source_handles,
            retention_handle: catalog.retention_handle,
            schedule_store: store,
            shared,
            source_job_timeout,
            driver_started: AtomicBool::new(false),
            feeder_shutdown_requested: AtomicBool::new(false),
            feeder_shutdown: tokio::sync::Notify::new(),
            feeder_handles: tokio::sync::Mutex::new(Vec::new()),
            retention_schedule: retention.map(crate::retention::RetentionScheduleState::new),
            retention_health: health,
        })
    }
}

async fn connect_job_store(settings: JobStoreSettings<'_>) -> ApiResult<ScheduledJobStore> {
    let store_config = RemoteScheduledJobStoreConfig::new([settings.endpoint], settings.timeout)
        .map_err(|error| ApiError::with_source(ApiErrorKind::Config, error))?;
    connect_remote_scheduled_job_store(store_config)
        .await
        .map_err(|error| ApiError::with_source(ApiErrorKind::Config, error))
}

fn register_job_runtime<S: JobStore + Clone>(
    store: S,
    catalog: bg_jobs::JobCatalog,
    timeout: Duration,
    observer: Arc<BackgroundJobObserver>,
) -> ApiResult<JobRuntime<S>> {
    let namespace = Namespace::try_new(BG_JOBS_NAMESPACE).map_err(api_config_error)?;
    let owner = OwnerId::generate().map_err(api_config_error)?;
    let budget = driver_budget(timeout).map_err(api_config_error)?;
    JobRuntime::distributed_with_observer(store, catalog, budget, namespace, owner, observer)
        .map_err(api_config_error)
}
