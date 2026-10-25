use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    OperationActor, OperationId, OperationKind, OperationRequest, PrivacyFixError,
    PrivacyFixRequest, RateLimitPolicy, RawBackupDestinationKind, RawBackupError,
    RawBackupWriteRequest, TableFixRequest, TrimRequest,
    error::{MaintenanceError, MaintenanceResult},
};

/// The single typed request contract used by maintenance callers.
///
/// Authorization and rate policy are supplied by the caller boundary and are
/// never accepted as request payload fields. The executor-specific request is
/// retained as the variant payload so CLI, HTTP, and Lambda all deserialize
/// the same contract before reaching the durable operation facade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(
    tag = "kind",
    content = "request",
    rename_all = "snake_case",
    deny_unknown_fields
)]
#[schemars(deny_unknown_fields)]
pub enum MaintenanceRequest {
    TableFix(TableFixRequest),
    PrivacyFix(PrivacyFixRequest),
    Trim(TrimRequest),
    RawBackup(RawBackupWriteRequest),
}

impl MaintenanceRequest {
    #[must_use]
    pub const fn table_fix(request: TableFixRequest) -> Self {
        Self::TableFix(request)
    }

    #[must_use]
    pub const fn privacy_fix(request: PrivacyFixRequest) -> Self {
        Self::PrivacyFix(request)
    }

    #[must_use]
    pub const fn trim(request: TrimRequest) -> Self {
        Self::Trim(request)
    }

    #[must_use]
    pub const fn raw_backup(request: RawBackupWriteRequest) -> Self {
        Self::RawBackup(request)
    }

    #[must_use]
    pub fn operation_id(&self) -> &OperationId {
        match self {
            Self::TableFix(request) => &request.operation_id,
            Self::PrivacyFix(request) => &request.operation_id,
            Self::Trim(request) => &request.operation_id,
            Self::RawBackup(request) => &request.operation_id,
        }
    }

    #[must_use]
    pub fn kind(&self) -> OperationKind {
        match self {
            Self::TableFix(_) => OperationKind::TableFix,
            Self::PrivacyFix(_) => OperationKind::PrivacyFix,
            Self::Trim(_) => OperationKind::Trim,
            Self::RawBackup(_) => OperationKind::RawBackup,
        }
    }

    pub fn validate(&self) -> MaintenanceResult<()> {
        match self {
            Self::TableFix(request) => request.validate().map_err(Into::into),
            Self::PrivacyFix(request) => request.validate().map_err(Into::into),
            Self::Trim(request) => request.validate().map_err(Into::into),
            Self::RawBackup(request) => request.validate().map_err(Into::into),
        }
    }

    pub(super) fn reject_unsupported_submission(&self) -> MaintenanceResult<()> {
        match self {
            Self::RawBackup(_) => Err(MaintenanceError::RawBackup(
                RawBackupError::DestinationFenceUnavailable {
                    destination: RawBackupDestinationKind::Generic,
                },
            )),
            Self::PrivacyFix(_) => Err(MaintenanceError::PrivacyFix(PrivacyFixError::RawBackup(
                RawBackupError::DestinationFenceUnavailable {
                    destination: RawBackupDestinationKind::Generic,
                },
            ))),
            Self::TableFix(_) | Self::Trim(_) => Ok(()),
        }
    }

    pub(super) fn build_operation_request(
        &self,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
    ) -> MaintenanceResult<OperationRequest> {
        let target_tables = match self {
            Self::TableFix(request) => vec![request.table_name.clone()],
            Self::Trim(request) => vec![request.table_name.clone()],
            Self::PrivacyFix(request) => return request.build_operation_request(actor, rate_limit),
            Self::RawBackup(request) => request
                .records
                .iter()
                .map(|record| record.table_name.as_str())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(str::to_owned)
                .collect(),
        };
        let payload = self.build_operation_payload()?;
        Ok(OperationRequest {
            operation_id: self.operation_id().clone(),
            kind: self.kind(),
            actor: actor.clone(),
            target_tables,
            dry_run: self.dry_run(),
            rate_limit,
            payload,
        })
    }

    fn dry_run(&self) -> bool {
        match self {
            Self::TableFix(request) => request.dry_run,
            Self::PrivacyFix(request) => request.dry_run,
            Self::Trim(request) => request.dry_run,
            Self::RawBackup(_) => false,
        }
    }
}

impl PrivacyFixRequest {
    pub(super) fn build_operation_request(
        &self,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
    ) -> MaintenanceResult<OperationRequest> {
        Ok(OperationRequest {
            operation_id: self.operation_id.clone(),
            kind: OperationKind::PrivacyFix,
            actor: actor.clone(),
            target_tables: self
                .tables
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            dry_run: self.dry_run,
            rate_limit,
            payload: self.build_operation_payload()?,
        })
    }
}
