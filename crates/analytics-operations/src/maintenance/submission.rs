use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::MaintenanceRequest;
use crate::{
    OperationActor, OperationId, OperationRequest, OperationStore, OperationStoreError,
    PrivacyFixError, PrivacyFixRequest, RateLimitPolicy, RawBackupDestinationKind, RawBackupError,
    error::{MaintenanceError, MaintenanceResult},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceSubmissionOutcome {
    Created,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct MaintenanceSubmission {
    pub operation_id: OperationId,
    pub outcome: MaintenanceSubmissionOutcome,
}

impl MaintenanceSubmission {
    #[must_use]
    pub fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }
}

pub struct MaintenanceOperations<'a> {
    pub(crate) store: &'a OperationStore,
}

impl<'a> MaintenanceOperations<'a> {
    #[must_use]
    pub const fn new(store: &'a OperationStore) -> Self {
        Self { store }
    }

    pub fn submit(
        &self,
        request: &MaintenanceRequest,
        actor: &OperationActor,
    ) -> MaintenanceResult<MaintenanceSubmission> {
        self.submit_with_rate_limit(request, actor, RateLimitPolicy::default())
    }

    pub(crate) fn submit_with_rate_limit(
        &self,
        request: &MaintenanceRequest,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
    ) -> MaintenanceResult<MaintenanceSubmission> {
        request.validate()?;
        request.reject_unsupported_submission()?;
        self.persist_submission(request.build_operation_request(actor, rate_limit)?)
    }

    pub(crate) fn submit_privacy_fix_dry_run_with_rate_limit(
        &self,
        request: &PrivacyFixRequest,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
    ) -> MaintenanceResult<MaintenanceSubmission> {
        if !request.dry_run {
            return Err(MaintenanceError::PrivacyFix(PrivacyFixError::RawBackup(
                RawBackupError::DestinationFenceUnavailable {
                    destination: RawBackupDestinationKind::Generic,
                },
            )));
        }
        self.persist_submission(request.build_operation_request(actor, rate_limit)?)
    }

    fn persist_submission(
        &self,
        operation_request: OperationRequest,
    ) -> MaintenanceResult<MaintenanceSubmission> {
        let created = self
            .store
            .create_or_verify_operation(&operation_request)
            .map_err(|error| match error {
                OperationStoreError::ConflictingOperation(operation_id) => {
                    MaintenanceError::ConflictingOperation { operation_id }
                }
                error => error.into(),
            })?;
        Ok(MaintenanceSubmission {
            operation_id: operation_request.operation_id,
            outcome: if created {
                MaintenanceSubmissionOutcome::Created
            } else {
                MaintenanceSubmissionOutcome::Existing
            },
        })
    }

    /// Authorize an actor and move a submitted/planned/paused operation into
    /// `Running`.
    pub fn prepare(
        &self,
        operation_id: &OperationId,
        actor: &OperationActor,
    ) -> MaintenanceResult<()> {
        self.store
            .prepare_for_execution(operation_id, actor)
            .map(|_| ())
            .map_err(Into::into)
    }

    pub fn request_cancellation(
        &self,
        operation_id: &OperationId,
        actor: &OperationActor,
    ) -> MaintenanceResult<()> {
        self.store
            .request_cancellation_as(operation_id, actor)
            .map_err(Into::into)
    }

    pub fn accept_cancellation(
        &self,
        operation_id: &OperationId,
        actor: &OperationActor,
    ) -> MaintenanceResult<()> {
        self.store
            .accept_cancellation_as(operation_id, actor)
            .map_err(Into::into)
    }
}
