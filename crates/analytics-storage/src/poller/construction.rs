use config::AnalyticsSourceConfig;

use super::state::AuxStorageGlobalPoller;
use crate::{
    aux_storage_client::AuxStorageStreamClient,
    error::{AnalyticsStorageError, AnalyticsStorageErrorKind, AnalyticsStorageResult},
    planning::SourceTablePlan,
    poller_config::PollerConfig,
    source_batch::SystemStreamResume,
    source_routes::CompiledSourceRoutes,
};

impl AuxStorageGlobalPoller {
    pub(super) async fn try_from_source(
        source: &AnalyticsSourceConfig,
        plans: &[SourceTablePlan],
        config: &PollerConfig,
        system_resume: Option<SystemStreamResume>,
    ) -> AnalyticsStorageResult<Self> {
        let endpoint_url = source.endpoint_url.as_deref().ok_or_else(|| {
            AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingAuxStorageEndpoint)
        })?;
        let client = AuxStorageStreamClient::try_new(endpoint_url, config.request_timeout)?;
        let routes = CompiledSourceRoutes::from_plans(plans)?;
        let source_scope = client
            .describe_system_stream()
            .await?
            .source_scope()
            .clone();
        let checkpoint = if let Some(resume) = system_resume {
            let (resume_scope, checkpoint) = resume.into_parts();
            if resume_scope != source_scope {
                return Err(AnalyticsStorageError::new(
                    AnalyticsStorageErrorKind::InvalidSourceBatchScope,
                ));
            }
            Some(checkpoint)
        } else {
            None
        };
        Ok(Self {
            client,
            routes,
            source_scope,
            scan_checkpoint: checkpoint.clone(),
            checkpoint,
            max_records_per_response: config.max_records_per_response,
            serialization_buffer: Vec::new(),
        })
    }
}
