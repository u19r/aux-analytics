use super::*;

pub struct AnalyticsLambdaHandler {
    pub(super) manifest: Arc<AnalyticsManifest>,
    pub(super) engine: AnalyticsEngineAccess,
    pub(super) privacy_policy: Option<Arc<PrivacyPolicy>>,
    pub(super) response_config: LambdaResponseConfig,
    pub(super) ingest_enabled: bool,
    pub(super) operation_store_path: Option<Arc<PathBuf>>,
}

impl AnalyticsLambdaHandler {
    pub async fn from_env() -> Result<Self, AnalyticsLambdaError> {
        let resolved = resolve_config(ConfigRequest::lambda(LambdaEnvironment::from_process()))?;
        let backend = resolved.require_storage_backend()?.clone();
        let ingest_enabled = resolved.routes.ingest_enabled;
        let root = resolved.root;
        let manifest = resolved.manifest.value;
        let privacy_policy = load_privacy_policy(&root)?.map(Arc::new);
        let mut handler = Self::try_new(
            manifest,
            &backend,
            LambdaHandlerOptions {
                privacy_policy,
                max_read_connections: root.analytics.query.max_read_connections,
                response_config: LambdaResponseConfig::from(&root.analytics.query),
                ingest_enabled,
            },
        )?;
        handler.operation_store_path = root
            .analytics
            .operations
            .operation_store_path
            .map(PathBuf::from)
            .map(Arc::new);
        Ok(handler)
    }

    #[must_use]
    pub fn with_operation_store_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.operation_store_path = Some(Arc::new(path.into()));
        self
    }

    pub fn try_new(
        manifest: AnalyticsManifest,
        backend: &StorageBackend,
        options: LambdaHandlerOptions,
    ) -> Result<Self, AnalyticsLambdaError> {
        let LambdaHandlerOptions {
            privacy_policy,
            max_read_connections,
            response_config,
            ingest_enabled,
        } = options;
        let engine = AnalyticsEngine::connect(backend)?;
        if !response_config.query_only {
            engine.ensure_manifest(&manifest)?;
        }
        let engine = if response_config.query_only {
            AnalyticsEngineAccess::shared(engine)
        } else {
            AnalyticsEngineAccess::backend_aware_with_max_read_connections(
                engine,
                backend.clone(),
                max_read_connections,
            )
        };
        Ok(Self {
            manifest: Arc::new(manifest),
            engine,
            privacy_policy,
            response_config,
            ingest_enabled,
            operation_store_path: None,
        })
    }
}
