use super::*;

pub(crate) struct JobCatalogBundle {
    pub(crate) catalog: JobCatalog,
    pub(crate) source_handles: HashMap<SourceJobKind, JobHandle<SourceJobPayload>>,
    pub(crate) retention_handle: Option<JobHandle<RetentionJobPayload>>,
}

impl JobCatalogBundle {
    pub(crate) fn try_new(
        shared: Arc<observation::SourceJobShared>,
        retention: Option<(
            Arc<crate::retention::RetentionRuntime>,
            Arc<RwLock<crate::RetentionHealth>>,
        )>,
        timeout: Duration,
    ) -> crate::error::ApiResult<JobCatalogBundle> {
        let (mut catalog, source_handles) = source::handler_catalog(Arc::clone(&shared), timeout)?;
        let mut retention_handle = None;
        if let Some((retention, health)) = retention {
            retention_handle = Some(retention::register_catalog(
                &mut catalog,
                shared,
                retention,
                health,
                timeout,
            )?);
        }
        Ok(JobCatalogBundle {
            catalog,
            source_handles,
            retention_handle,
        })
    }
}

#[cfg(any(test, feature = "bdd-test-support"))]
pub(crate) fn job_catalog(
    shared: Arc<observation::SourceJobShared>,
    retention: Option<(
        Arc<crate::retention::RetentionRuntime>,
        Arc<RwLock<crate::RetentionHealth>>,
    )>,
    timeout: Duration,
) -> crate::error::ApiResult<JobCatalog> {
    Ok(JobCatalogBundle::try_new(shared, retention, timeout)?.catalog)
}
