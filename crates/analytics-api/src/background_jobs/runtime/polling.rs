use super::*;

impl<S: JobStore + Clone> BackgroundJobRuntime<S> {
    pub(crate) async fn start_source_polling(
        self: &Arc<Self>,
        source: AnalyticsSourceConfig,
        plans: Vec<SourceTablePlan>,
        checkpoints: &[SourceCheckpoint],
        system_resume: Option<SystemStreamResume>,
    ) -> ApiResult<()> {
        let registered_kinds = self
            .register_source_pollers(&source, &plans, checkpoints, system_resume)
            .await?;
        self.shared
            .initialize_source_authority(&plans)
            .map_err(api_config_error)?;
        if registered_kinds.is_empty() {
            *self.shared.source_health.write().await = SourceHealth::disabled();
            return Ok(());
        }
        set_source_phase(&self.shared, SourcePollingPhase::Polling).await;
        let mut feeder_handles = self.feeder_handles.lock().await;
        if self.feeder_shutdown_requested.load(Ordering::Acquire) {
            return Ok(());
        }
        self.start_driver();
        for kind in registered_kinds {
            let runtime = Arc::clone(self);
            let poll_interval = Duration::from_millis(source.poll_interval_ms.max(1));
            let timeout = self.source_job_timeout;
            feeder_handles.push(tokio::spawn(async move {
                runtime.run_feeder(poll_interval, kind, timeout).await;
            }));
        }
        Ok(())
    }

    async fn register_source_pollers(
        &self,
        source: &AnalyticsSourceConfig,
        plans: &[SourceTablePlan],
        checkpoints: &[SourceCheckpoint],
        system_resume: Option<SystemStreamResume>,
    ) -> ApiResult<Vec<SourceJobKind>> {
        let mut registered_kinds = Vec::new();
        for kind in SourceJobKind::ALL {
            let kind_plans = plans
                .iter()
                .filter(|plan| plan.view().stream_type == kind.stream_type())
                .cloned()
                .collect::<Vec<_>>();
            if kind_plans.is_empty() {
                continue;
            }
            let bounded_source =
                bound_source_page(source, &kind_plans).map_err(api_config_error)?;
            let poller = SourcePoller::from_plans_with_system_resume(
                &bounded_source,
                kind_plans,
                checkpoints,
                if kind == SourceJobKind::AuxStorage {
                    system_resume.clone()
                } else {
                    None
                },
            )
            .await?;
            if poller.is_empty() {
                continue;
            }
            self.shared
                .pollers
                .lock()
                .await
                .insert(kind, Arc::new(tokio::sync::Mutex::new(poller)));
            registered_kinds.push(kind);
        }
        Ok(registered_kinds)
    }

    pub(crate) fn start_driver(self: &Arc<Self>) {
        if self
            .driver_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let runtime = Arc::clone(self);
            tokio::spawn(async move { runtime.run_driver().await });
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.feeder_shutdown_requested
            .store(true, Ordering::Release);
        self.feeder_shutdown.notify_waiters();
        let feeder_handles = {
            let mut handles = self.feeder_handles.lock().await;
            std::mem::take(&mut *handles)
        };
        for handle in feeder_handles {
            if let Err(error) = handle.await {
                tracing::warn!(error = %error, "background source feeder stopped unexpectedly");
            }
        }
        self.runtime.request_shutdown();
    }

    pub(crate) fn feeder_shutdown_requested(&self) -> bool {
        self.feeder_shutdown_requested.load(Ordering::Acquire) || self.runtime.shutdown_requested()
    }
}
