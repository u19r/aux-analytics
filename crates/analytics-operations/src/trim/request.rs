use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{TrimError, TrimPredicate, TrimResult};
use crate::OperationId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct TrimRequest {
    pub operation_id: OperationId,
    pub table_name: String,
    pub dry_run: bool,
    pub target: TrimTarget,
    pub confirmation_token: Option<String>,
}

impl TrimRequest {
    pub fn validate(&self) -> TrimResult<()> {
        if self.table_name.trim().is_empty() {
            return Err(TrimError::EmptyTableName);
        }
        self.target.validate()?;
        if !self.dry_run
            && self
                .confirmation_token
                .as_deref()
                .is_none_or(|token| token.trim().is_empty())
        {
            return Err(TrimError::MissingConfirmationToken);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub enum TrimTarget {
    RowKeys { keys: Vec<Vec<u8>> },
    RowPredicate { predicate: TrimPredicate },
    FullTableDrop,
    HorizonTrim { older_than_ms: u64 },
    DuckLakeSnapshotExpiry { older_than_ms: u64 },
    RawBackupExpiration { older_than_ms: u64 },
}

impl TrimTarget {
    pub(super) fn validate(&self) -> TrimResult<()> {
        match self {
            Self::RowKeys { keys } if keys.is_empty() => Err(TrimError::EmptyRowSelection),
            Self::RowKeys { keys } if keys.iter().any(Vec::is_empty) => Err(TrimError::EmptyRowKey),
            Self::RowPredicate { predicate } => predicate.validate(),
            Self::HorizonTrim { older_than_ms }
            | Self::DuckLakeSnapshotExpiry { older_than_ms }
            | Self::RawBackupExpiration { older_than_ms }
                if *older_than_ms == 0 =>
            {
                Err(TrimError::EmptyHorizon)
            }
            Self::RowKeys { .. }
            | Self::FullTableDrop
            | Self::HorizonTrim { .. }
            | Self::DuckLakeSnapshotExpiry { .. }
            | Self::RawBackupExpiration { .. } => Ok(()),
        }
    }
}
