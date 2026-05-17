use std::collections::{HashMap, HashSet};

use analytics_contract::{StorageStreamRecord, StorageValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    CheckError, CheckReport, CheckRowProjection, CurrentRowReader, OperationId, OperationTypeError,
    ProjectedCurrentRow, RawBackupError, RawBackupObjectId, RawBackupObjectIndex,
    RawBackupObjectStore,
};

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
    #[error("table fix selected {selected_rows} rows but max_rows is {max_rows}")]
    RepairSetTooLarge { selected_rows: u64, max_rows: u64 },
    #[error(
        "table fix post-fix validation failed because {missing_rows} selected rows were missing"
    )]
    PostFixValidationFailed { missing_rows: u64 },
    #[error("table fix destination failed: {0}")]
    Destination(String),
}

pub type TableFixResult<T> = Result<T, TableFixError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFixRequest {
    pub operation_id: OperationId,
    pub table_name: String,
    pub dry_run: bool,
    pub selection: TableFixSelection,
    pub source: TableFixSource,
    pub validation: TableFixValidation,
}

impl TableFixRequest {
    pub fn new(
        operation_id: OperationId,
        table_name: impl Into<String>,
        dry_run: bool,
        selection: TableFixSelection,
        source: TableFixSource,
    ) -> TableFixResult<Self> {
        let request = Self {
            operation_id,
            table_name: table_name.into(),
            dry_run,
            selection,
            source,
            validation: TableFixValidation::default(),
        };
        request.validate()?;
        Ok(request)
    }

    #[must_use]
    pub fn with_validation(mut self, validation: TableFixValidation) -> Self {
        self.validation = validation;
        self
    }

    pub fn validate(&self) -> TableFixResult<()> {
        if self.table_name.trim().is_empty() {
            return Err(TableFixError::EmptyTableName);
        }
        self.selection.validate()?;
        self.source.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFixValidation {
    pub require_post_fix_check: bool,
    pub max_rows: u64,
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

pub trait TableFixDestination {
    fn apply_repair(
        &mut self,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
    ) -> TableFixResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFixTruthRecord {
    pub table_name: String,
    pub record_key: Vec<u8>,
    pub record: StorageStreamRecord,
    pub stale: bool,
}

pub trait TableFixCurrentSource {
    fn read_truth(
        &self,
        source_identity: &str,
        table_name: &str,
        row_keys: &[Vec<u8>],
    ) -> TableFixResult<Vec<TableFixTruthRecord>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionBackedTableFixCurrentSource<Reader> {
    projection: CheckRowProjection,
    reader: Reader,
}

impl<Reader> ProjectionBackedTableFixCurrentSource<Reader> {
    #[must_use]
    pub fn new(projection: CheckRowProjection, reader: Reader) -> Self {
        Self { projection, reader }
    }
}

impl<Reader> TableFixCurrentSource for ProjectionBackedTableFixCurrentSource<Reader>
where Reader: CurrentRowReader
{
    fn read_truth(
        &self,
        _source_identity: &str,
        table_name: &str,
        row_keys: &[Vec<u8>],
    ) -> TableFixResult<Vec<TableFixTruthRecord>> {
        if table_name != self.projection.table_name {
            return Ok(Vec::new());
        }
        let selected_keys = selected_key_set(row_keys);
        Ok(self
            .reader
            .current_rows(&self.projection)?
            .into_iter()
            .filter(|row| selected_keys.contains(row.key.as_bytes()))
            .map(|row| truth_record_from_projected_row(table_name, &row))
            .collect())
    }
}

pub struct TableFixExecutor<'a, Store: ?Sized> {
    raw_backup_store: &'a Store,
}

impl<'a, Store> TableFixExecutor<'a, Store>
where Store: RawBackupObjectStore + ?Sized
{
    #[must_use]
    pub fn new(raw_backup_store: &'a Store) -> Self {
        Self { raw_backup_store }
    }

    pub fn execute_raw_backup(
        &self,
        request: &TableFixRequest,
        indexes: &[RawBackupObjectIndex],
        destination: &mut impl TableFixDestination,
    ) -> TableFixResult<TableFixReport> {
        request.validate()?;
        let selected_keys = selected_row_keys(&request.selection)?;
        let selected_key_set = selected_key_set(selected_keys);
        let object_ids = raw_backup_object_ids(&request.source)?;
        let mut repaired_rows = 0_u64;
        let mut sample_keys = Vec::new();

        for object_id in object_ids {
            let index = indexes
                .iter()
                .find(|index| &index.object_id == object_id)
                .ok_or_else(|| TableFixError::MissingRawBackupIndex(object_id.clone()))?;
            let replay = self.raw_backup_store.replay_object_report(index)?;
            for record in replay.records {
                if record.table_name == request.table_name
                    && selected_key_set.contains(record.record_key.as_slice())
                {
                    if !request.dry_run {
                        destination.apply_repair(
                            record.table_name.as_str(),
                            record.record_key.as_slice(),
                            record.record,
                        )?;
                    }
                    repaired_rows += 1;
                    push_sample_key(&mut sample_keys, record.record_key.as_slice());
                }
            }
        }

        let selected_rows = u64::try_from(selected_keys.len()).unwrap_or(u64::MAX);
        validate_selected_row_limit(&request.validation, selected_rows)?;
        let report = TableFixReport {
            operation_id: request.operation_id.clone(),
            table_name: request.table_name.clone(),
            dry_run: request.dry_run,
            selected_rows,
            repaired_rows,
            missing_rows: selected_rows.saturating_sub(repaired_rows),
            destination_mutated: !request.dry_run && repaired_rows > 0,
            sample_keys,
        };
        validate_post_fix_report(request, &report)?;
        Ok(report)
    }

    pub fn execute_current_source(
        &self,
        request: &TableFixRequest,
        source: &impl TableFixCurrentSource,
        destination: &mut impl TableFixDestination,
    ) -> TableFixResult<TableFixReport> {
        request.validate()?;
        let selected_keys = selected_row_keys(&request.selection)?;
        let source_identity = current_source_identity(&request.source)?;
        execute_truth_records(
            request,
            selected_keys,
            source.read_truth(source_identity, request.table_name.as_str(), selected_keys)?,
            destination,
        )
    }

    pub fn execute_check_report_mismatches(
        &self,
        request: &TableFixRequest,
        check_report: &CheckReport,
        source: &impl TableFixCurrentSource,
        destination: &mut impl TableFixDestination,
    ) -> TableFixResult<TableFixReport> {
        request.validate()?;
        let TableFixSource::CheckReportMismatchSet { operation_id } = &request.source else {
            return Err(TableFixError::UnsupportedSource);
        };
        if operation_id != &check_report.operation_id {
            return Err(TableFixError::UnsupportedSource);
        }
        let selected_keys = check_report_mismatch_keys(check_report, request.table_name.as_str());
        execute_truth_records(
            request,
            selected_keys.as_slice(),
            source.read_truth(
                "check_report_mismatch_set",
                request.table_name.as_str(),
                selected_keys.as_slice(),
            )?,
            destination,
        )
    }
}

fn selected_row_keys(selection: &TableFixSelection) -> TableFixResult<&[Vec<u8>]> {
    match selection {
        TableFixSelection::RowKeys { keys } => Ok(keys.as_slice()),
        TableFixSelection::CheckReport { .. } => Err(TableFixError::UnsupportedSelection),
    }
}

fn selected_key_set(row_keys: &[Vec<u8>]) -> HashSet<&[u8]> {
    row_keys.iter().map(Vec::as_slice).collect()
}

fn raw_backup_object_ids(source: &TableFixSource) -> TableFixResult<&[RawBackupObjectId]> {
    match source {
        TableFixSource::RawBackup { object_ids } => Ok(object_ids.as_slice()),
        TableFixSource::CurrentSource { .. } | TableFixSource::CheckReportMismatchSet { .. } => {
            Err(TableFixError::UnsupportedSource)
        }
    }
}

fn current_source_identity(source: &TableFixSource) -> TableFixResult<&str> {
    match source {
        TableFixSource::CurrentSource { source_identity } => Ok(source_identity.as_str()),
        TableFixSource::RawBackup { .. } | TableFixSource::CheckReportMismatchSet { .. } => {
            Err(TableFixError::UnsupportedSource)
        }
    }
}

fn check_report_mismatch_keys(check_report: &CheckReport, table_name: &str) -> Vec<Vec<u8>> {
    let prefix = format!("{table_name}:");
    check_report
        .samples
        .mismatched_keys
        .iter()
        .filter_map(|sample| sample.strip_prefix(prefix.as_str()))
        .map(|key| key.as_bytes().to_vec())
        .collect()
}

fn execute_truth_records(
    request: &TableFixRequest,
    selected_keys: &[Vec<u8>],
    truth: Vec<TableFixTruthRecord>,
    destination: &mut impl TableFixDestination,
) -> TableFixResult<TableFixReport> {
    if truth.iter().any(|record| record.stale) {
        return Err(TableFixError::StaleSourceTruth);
    }
    let mut repaired_rows = 0_u64;
    let mut sample_keys = Vec::new();
    for record in truth {
        if !request.dry_run {
            destination.apply_repair(
                record.table_name.as_str(),
                record.record_key.as_slice(),
                record.record,
            )?;
        }
        repaired_rows += 1;
        push_sample_key(&mut sample_keys, record.record_key.as_slice());
    }
    let selected_rows = u64::try_from(selected_keys.len()).unwrap_or(u64::MAX);
    validate_selected_row_limit(&request.validation, selected_rows)?;
    let report = TableFixReport {
        operation_id: request.operation_id.clone(),
        table_name: request.table_name.clone(),
        dry_run: request.dry_run,
        selected_rows,
        repaired_rows,
        missing_rows: selected_rows.saturating_sub(repaired_rows),
        destination_mutated: !request.dry_run && repaired_rows > 0,
        sample_keys,
    };
    validate_post_fix_report(request, &report)?;
    Ok(report)
}

fn validate_selected_row_limit(
    validation: &TableFixValidation,
    selected_rows: u64,
) -> TableFixResult<()> {
    if selected_rows > validation.max_rows {
        return Err(TableFixError::RepairSetTooLarge {
            selected_rows,
            max_rows: validation.max_rows,
        });
    }
    Ok(())
}

fn validate_post_fix_report(
    request: &TableFixRequest,
    report: &TableFixReport,
) -> TableFixResult<()> {
    if request.validation.require_post_fix_check && report.missing_rows > 0 {
        return Err(TableFixError::PostFixValidationFailed {
            missing_rows: report.missing_rows,
        });
    }
    Ok(())
}

fn push_sample_key(sample_keys: &mut Vec<String>, record_key: &[u8]) {
    const SAMPLE_KEY_LIMIT: usize = 10;
    if sample_keys.len() < SAMPLE_KEY_LIMIT {
        sample_keys.push(String::from_utf8_lossy(record_key).into_owned());
    }
}

fn truth_record_from_projected_row(
    table_name: &str,
    row: &ProjectedCurrentRow,
) -> TableFixTruthRecord {
    let item = projected_values_to_storage_item(row);
    TableFixTruthRecord {
        table_name: table_name.to_string(),
        record_key: row.key.as_bytes().to_vec(),
        record: StorageStreamRecord {
            keys: storage_key_item(&row.key),
            sequence_number: row.source_position.to_string(),
            old_image: None,
            new_image: Some(item),
        },
        stale: false,
    }
}

fn storage_key_item(key: &str) -> HashMap<String, StorageValue> {
    HashMap::from([("pk".to_string(), StorageValue::S(key.to_string()))])
}

fn projected_values_to_storage_item(row: &ProjectedCurrentRow) -> HashMap<String, StorageValue> {
    let mut item = storage_key_item(&row.key);
    for (column, value) in &row.values {
        if let Some(value) = value {
            item.insert(column.clone(), StorageValue::S(value.clone()));
        }
    }
    item
}

impl Default for TableFixValidation {
    fn default() -> Self {
        Self {
            require_post_fix_check: true,
            max_rows: 1_000,
        }
    }
}
