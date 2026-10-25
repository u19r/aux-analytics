use analytics_contract::StorageStreamRecord;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

#[cfg(test)]
use super::verification::{
    projected_rows_revision, selected_key_set, truth_record_from_projected_row,
};
use crate::{
    CheckError, MaintenanceExecutionAuthority, OperationId, OperationStoreError,
    OperationTypeError, RawBackupError, RawBackupObjectId,
};
#[cfg(test)]
use crate::{CheckRowProjection, CurrentRowReader};

#[derive(Debug, Error)]
pub enum TableFixError {
    #[error(transparent)]
    OperationType(#[from] OperationTypeError),
    #[error(transparent)]
    RawBackup(#[from] RawBackupError),
    #[error(transparent)]
    Check(#[from] CheckError),
    #[error("table fix table name cannot be empty")]
    EmptyTableName,
    #[error("table fix source identity cannot be empty")]
    EmptySourceIdentity,
    #[error("table fix row key cannot be empty")]
    EmptyRowKey,
    #[error("table fix request must include at least one selected row")]
    EmptyRowSelection,
    #[error("table fix check report id cannot be empty")]
    EmptyCheckReportId,
    #[error("table fix check report provenance does not match the persisted check operation")]
    CheckReportProvenanceMismatch,
    #[error("table fix apply requires a non-empty typed confirmation token")]
    MissingConfirmationToken,
    #[error("table fix raw backup source must include at least one object id")]
    EmptyRawBackupObjects,
    #[error("table fix source type is not supported by this executor")]
    UnsupportedSource,
    #[error("table fix selection type is not supported by this executor")]
    UnsupportedSelection,
    #[error("raw backup index {0} was not supplied to table fix execution")]
    MissingRawBackupIndex(RawBackupObjectId),
    #[error("current source returned stale truth for selected table fix rows")]
    StaleSourceTruth,
    #[error("current source returned a table-fix row outside the requested selection")]
    TruthRecordOutsideSelection,
    #[error("current source returned duplicate truth for a selected table-fix row")]
    DuplicateTruthRecord,
    #[error("table fix source revision evidence is missing")]
    MissingSourceRevision,
    #[error("table fix destination verification failed for key {0}")]
    DestinationVerificationFailed(String),
    #[error(transparent)]
    RateLimit(#[from] crate::RateLimitDenied),
    #[error("table fix selected {selected_rows} rows but max_rows is {max_rows}")]
    RepairSetTooLarge { selected_rows: u64, max_rows: u64 },
    #[error(
        "table fix post-fix validation failed because {missing_rows} selected rows were missing"
    )]
    PostFixValidationFailed { missing_rows: u64 },
    #[error("table fix destination failed: {0}")]
    Destination(String),
    #[error(transparent)]
    OperationStore(#[from] OperationStoreError),
}

pub type TableFixResult<T> = Result<T, TableFixError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFixApplyOutcome {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct TableFixRequest {
    pub operation_id: OperationId,
    pub table_name: String,
    pub dry_run: bool,
    pub selection: TableFixSelection,
    pub source: TableFixSource,
    pub validation: TableFixValidation,
}

impl TableFixRequest {
    pub fn validate(&self) -> TableFixResult<()> {
        if self.table_name.trim().is_empty() {
            return Err(TableFixError::EmptyTableName);
        }
        self.selection.validate()?;
        self.source.validate()?;
        if !self.dry_run && self.validation.confirmation.is_none() {
            return Err(TableFixError::MissingConfirmationToken);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub enum TableFixSelection {
    RowKeys { keys: Vec<Vec<u8>> },
    CheckReport { operation_id: OperationId },
}

impl TableFixSelection {
    fn validate(&self) -> TableFixResult<()> {
        match self {
            Self::RowKeys { keys } => {
                if keys.is_empty() {
                    return Err(TableFixError::EmptyRowSelection);
                }
                if keys.iter().any(Vec::is_empty) {
                    return Err(TableFixError::EmptyRowKey);
                }
                Ok(())
            }
            Self::CheckReport { operation_id } => {
                if operation_id.as_str().trim().is_empty() {
                    return Err(TableFixError::EmptyCheckReportId);
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub enum TableFixSource {
    RawBackup { object_ids: Vec<RawBackupObjectId> },
    CurrentSource { source_identity: String },
    CheckReportMismatchSet { operation_id: OperationId },
}

impl TableFixSource {
    fn validate(&self) -> TableFixResult<()> {
        match self {
            Self::RawBackup { object_ids } if object_ids.is_empty() => {
                Err(TableFixError::EmptyRawBackupObjects)
            }
            Self::CurrentSource { source_identity } if source_identity.trim().is_empty() => {
                Err(TableFixError::EmptySourceIdentity)
            }
            Self::CheckReportMismatchSet { operation_id } if operation_id.as_str().is_empty() => {
                Err(TableFixError::EmptyCheckReportId)
            }
            Self::RawBackup { .. }
            | Self::CurrentSource { .. }
            | Self::CheckReportMismatchSet { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct TableFixValidation {
    pub require_post_fix_check: bool,
    pub max_rows: u64,
    #[serde(default)]
    #[schema(nullable = true, default = json!(null))]
    pub confirmation: Option<TableFixConfirmation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(try_from = "String", into = "String")]
pub struct TableFixConfirmation(String);

impl TableFixConfirmation {
    pub fn try_new(value: impl Into<String>) -> TableFixResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(TableFixError::MissingConfirmationToken);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<String> for TableFixConfirmation {
    type Error = TableFixError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<TableFixConfirmation> for String {
    fn from(value: TableFixConfirmation) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFixReport {
    pub operation_id: OperationId,
    pub table_name: String,
    pub dry_run: bool,
    pub selected_rows: u64,
    pub repaired_rows: u64,
    pub missing_rows: u64,
    pub destination_mutated: bool,
    pub sample_keys: Vec<String>,
}

impl TableFixReport {
    #[must_use]
    pub fn operation_counts(&self) -> serde_json::Value {
        serde_json::json!({
            "selected_rows": self.selected_rows,
            "repaired_rows": self.repaired_rows,
            "missing_rows": self.missing_rows,
            "destination_mutated": self.destination_mutated,
            "sample_keys": self.sample_keys,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFixTruthRecord {
    pub table_name: String,
    pub record_key: Vec<u8>,
    pub record: StorageStreamRecord,
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFixSourceSnapshot {
    pub source_revision: String,
    pub records: Vec<TableFixTruthRecord>,
}

pub trait VersionedTableFixCurrentSource {
    fn read_truth_snapshot(
        &self,
        source_identity: &str,
        table_name: &str,
        row_keys: &[Vec<u8>],
    ) -> TableFixResult<TableFixSourceSnapshot>;
}

/// Destination contract for an apply path that can safely retry each repair
/// key.
pub trait VerifiedTableFixDestination {
    fn apply_repair_with_key(
        &mut self,
        authority: &MaintenanceExecutionAuthority,
        key: &crate::OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        expected_source_revision: &str,
    ) -> TableFixResult<TableFixApplyOutcome>;

    fn verify_repair(
        &self,
        authority: &MaintenanceExecutionAuthority,
        key: &crate::OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        expected: &StorageStreamRecord,
        expected_source_revision: &str,
    ) -> TableFixResult<bool>;
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionBackedTableFixCurrentSource<Reader> {
    projection: CheckRowProjection,
    reader: Reader,
}

#[cfg(test)]
impl<Reader> ProjectionBackedTableFixCurrentSource<Reader> {
    #[must_use]
    pub(crate) fn new(projection: CheckRowProjection, reader: Reader) -> Self {
        Self { projection, reader }
    }
}

#[cfg(test)]
impl<Reader> VersionedTableFixCurrentSource for ProjectionBackedTableFixCurrentSource<Reader>
where Reader: CurrentRowReader
{
    fn read_truth_snapshot(
        &self,
        _source_identity: &str,
        table_name: &str,
        row_keys: &[Vec<u8>],
    ) -> TableFixResult<TableFixSourceSnapshot> {
        if table_name != self.projection.table_name {
            return Ok(TableFixSourceSnapshot {
                source_revision: format!("projection:{}:empty", self.projection.table_name),
                records: Vec::new(),
            });
        }
        let selected_keys = selected_key_set(row_keys);
        let current_rows = self.reader.current_rows(&self.projection)?;
        let source_revision = projected_rows_revision(&current_rows);
        let records = current_rows
            .into_iter()
            .filter(|row| selected_keys.contains(row.key.as_bytes()))
            .map(|row| truth_record_from_projected_row(table_name, &row))
            .collect();
        Ok(TableFixSourceSnapshot {
            source_revision,
            records,
        })
    }
}

impl Default for TableFixValidation {
    fn default() -> Self {
        Self {
            require_post_fix_check: true,
            max_rows: 1_000,
            confirmation: None,
        }
    }
}
