use std::{path::PathBuf, sync::Arc};

use analytics_contract::{AnalyticsManifest, PrivacyPolicy};
use tokio::sync::RwLock;

use super::{RetentionHealth, SourceHealth};
use crate::AnalyticsEngineAccess;

#[derive(Clone)]
pub struct AppState {
    pub engine: AnalyticsEngineAccess,
    pub manifest: Arc<RwLock<Arc<AnalyticsManifest>>>,
    pub source_health: Arc<RwLock<SourceHealth>>,
    pub(crate) background_jobs: Option<Arc<crate::background_jobs::BackgroundJobRuntime>>,
    pub retention: Option<Arc<crate::retention::RetentionRuntime>>,
    pub privacy_policy: Option<Arc<PrivacyPolicy>>,
    pub retention_health: Arc<RwLock<RetentionHealth>>,
    pub operation_store_path: Option<Arc<PathBuf>>,
}
impl AppState {
    #[must_use]
    pub fn new(engine: AnalyticsEngineAccess, manifest: AnalyticsManifest) -> Self {
        Self {
            engine,
            manifest: Arc::new(RwLock::new(Arc::new(manifest))),
            source_health: Arc::new(RwLock::new(SourceHealth::disabled())),
            background_jobs: None,
            retention: None,
            privacy_policy: None,
            retention_health: Arc::new(RwLock::new(RetentionHealth::disabled())),
            operation_store_path: None,
        }
    }

    #[must_use]
    pub fn with_retention(
        mut self,
        retention: Option<Arc<crate::retention::RetentionRuntime>>,
    ) -> Self {
        self.retention = retention;
        self
    }

    #[must_use]
    pub fn with_privacy_policy(mut self, policy: Option<Arc<PrivacyPolicy>>) -> Self {
        self.privacy_policy = policy;
        self
    }

    #[must_use]
    pub(crate) fn with_background_jobs(
        mut self,
        runtime: Arc<crate::background_jobs::BackgroundJobRuntime>,
    ) -> Self {
        self.background_jobs = Some(runtime);
        self
    }

    #[must_use]
    pub fn with_operation_store_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.operation_store_path = Some(Arc::new(path.into()));
        self
    }
}
