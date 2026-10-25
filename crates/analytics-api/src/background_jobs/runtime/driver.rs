use std::sync::{Arc, atomic::Ordering};

use bg_jobs::{DriveReport, DriveStopReason, JobStore, WakeHint};
use bg_jobs_tokio::WaitOutcome;

use super::BackgroundJobRuntime;
use crate::{background_jobs::observation::source_poll_failure, constants::DRIVER_ERROR_WAKE};

impl<S: JobStore + Clone> BackgroundJobRuntime<S> {
    pub(super) async fn run_driver(self: Arc<Self>) {
        loop {
            if self.runtime.shutdown_requested() {
                self.driver_started.store(false, Ordering::Release);
                return;
            }
            let retention_wake = self.materialize_retention_jobs().await;
            if self.runtime.shutdown_requested() {
                self.driver_started.store(false, Ordering::Release);
                return;
            }
            let report = match self.runtime.drive_once().await {
                Ok(report) => report,
                Err(error) => {
                    tracing::warn!(error = %error, "background source job drive failed");
                    match self.runtime.wait_after_error(DRIVER_ERROR_WAKE).await {
                        Ok(WaitOutcome::Woken) => {}
                        Ok(WaitOutcome::Shutdown) | Err(_) => {
                            self.driver_started.store(false, Ordering::Release);
                            return;
                        }
                    }
                    continue;
                }
            };
            if report.stop_reason == DriveStopReason::Shutdown {
                self.driver_started.store(false, Ordering::Release);
                return;
            }
            if !self
                .should_continue_after_wake(&report, retention_wake)
                .await
            {
                self.driver_started.store(false, Ordering::Release);
                return;
            }
        }
    }

    async fn should_continue_after_wake(
        &self,
        report: &DriveReport,
        retention_wake: Option<std::time::Duration>,
    ) -> bool {
        let retention_hint = retention_wake.and_then(|delay| {
            self.runtime
                .wall_now()
                .checked_add(delay)
                .ok()
                .map(WakeHint::At)
        });
        match self
            .runtime
            .wait_for(report, retention_hint.unwrap_or(WakeHint::None))
            .await
        {
            Ok(WaitOutcome::Woken) => true,
            Ok(WaitOutcome::Shutdown) => false,
            Err(error) => {
                source_poll_failure(&self.shared, "background job wake scheduling failed").await;
                tracing::error!(error = %error, "background job wake scheduling failed");
                false
            }
        }
    }
}
