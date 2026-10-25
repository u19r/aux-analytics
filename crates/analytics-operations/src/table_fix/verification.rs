use std::collections::HashSet;

#[cfg(test)]
use analytics_contract::StorageStreamRecord;
#[cfg(test)]
use sha2::{Digest, Sha256};

#[cfg(test)]
use super::contracts::TableFixTruthRecord;
use super::contracts::{
    TableFixError, TableFixReport, TableFixRequest, TableFixResult, TableFixSelection,
    TableFixSource, TableFixSourceSnapshot, TableFixValidation, VerifiedTableFixDestination,
};
#[cfg(test)]
use crate::ProjectedCurrentRow;
use crate::{
    CheckReport, MaintenanceExecutionAuthority, OperationRateLimiter, RateLimitClock,
    RateLimitUsage, RawBackupObjectId, constants::SAMPLE_KEY_LIMIT,
    enforce_rate_limit_with_limiter,
};

pub(super) fn selected_row_keys(selection: &TableFixSelection) -> TableFixResult<&[Vec<u8>]> {
    match selection {
        TableFixSelection::RowKeys { keys } => Ok(keys.as_slice()),
        TableFixSelection::CheckReport { .. } => Err(TableFixError::UnsupportedSelection),
    }
}

pub(super) fn preflight_rate_limit<Clock: RateLimitClock>(
    request: &TableFixRequest,
    limiter: &OperationRateLimiter,
    clock: &Clock,
) -> TableFixResult<()> {
    enforce_rate_limit_with_limiter(
        limiter,
        RateLimitUsage {
            rows: u64::from(!request.dry_run),
            source_requests: 1,
            destination_writes: u64::from(!request.dry_run),
            ..RateLimitUsage::default()
        },
        clock,
    )?;
    Ok(())
}

pub(super) fn selected_key_set(row_keys: &[Vec<u8>]) -> HashSet<&[u8]> {
    row_keys.iter().map(Vec::as_slice).collect()
}

pub(super) fn raw_backup_object_ids(
    source: &TableFixSource,
) -> TableFixResult<&[RawBackupObjectId]> {
    match source {
        TableFixSource::RawBackup { object_ids } => Ok(object_ids.as_slice()),
        TableFixSource::CurrentSource { .. } | TableFixSource::CheckReportMismatchSet { .. } => {
            Err(TableFixError::UnsupportedSource)
        }
    }
}

pub(super) fn current_source_identity(source: &TableFixSource) -> TableFixResult<&str> {
    match source {
        TableFixSource::CurrentSource { source_identity } => Ok(source_identity.as_str()),
        TableFixSource::RawBackup { .. } | TableFixSource::CheckReportMismatchSet { .. } => {
            Err(TableFixError::UnsupportedSource)
        }
    }
}

pub(super) fn check_report_mismatch_keys(
    check_report: &CheckReport,
    table_name: &str,
) -> Vec<Vec<u8>> {
    let prefix = format!("{table_name}:");
    check_report
        .samples
        .mismatched_keys
        .iter()
        .filter_map(|sample| sample.strip_prefix(prefix.as_str()))
        .map(|key| key.as_bytes().to_vec())
        .collect()
}

pub(super) struct ValidatedTableFixSnapshot {
    snapshot: TableFixSourceSnapshot,
    report: TableFixReport,
}

impl ValidatedTableFixSnapshot {
    pub(super) fn try_new(
        request: &TableFixRequest,
        selected_keys: &[Vec<u8>],
        snapshot: TableFixSourceSnapshot,
    ) -> TableFixResult<Self> {
        if snapshot.source_revision.trim().is_empty() {
            return Err(TableFixError::MissingSourceRevision);
        }
        let selected_keys = selected_key_set(selected_keys);
        let selected_rows = u64::try_from(selected_keys.len()).unwrap_or(u64::MAX);
        validate_selected_row_limit(&request.validation, selected_rows)?;
        snapshot.validate_truth_records(request, &selected_keys)?;
        let matched_rows = u64::try_from(snapshot.records.len()).unwrap_or(u64::MAX);
        let missing_rows = selected_rows.saturating_sub(matched_rows);
        if !request.dry_run && request.validation.require_post_fix_check && missing_rows > 0 {
            return Err(TableFixError::PostFixValidationFailed { missing_rows });
        }
        let report = TableFixReport::from_snapshot(request, selected_rows, &snapshot);
        Ok(Self { snapshot, report })
    }

    pub(super) fn execute<Clock: RateLimitClock>(
        self,
        destination: &mut impl VerifiedTableFixDestination,
        authority: &MaintenanceExecutionAuthority,
        limiter: &OperationRateLimiter,
        clock: &Clock,
    ) -> TableFixResult<TableFixReport> {
        if !self.report.dry_run {
            self.apply_verified_records(destination, authority, limiter, clock)?;
        }
        Ok(self.report)
    }

    fn apply_verified_records<Clock: RateLimitClock>(
        &self,
        destination: &mut impl VerifiedTableFixDestination,
        authority: &MaintenanceExecutionAuthority,
        limiter: &OperationRateLimiter,
        clock: &Clock,
    ) -> TableFixResult<()> {
        for record in &self.snapshot.records {
            let key = crate::OperationChunkKey::for_repair(
                &self.report.operation_id,
                self.report.table_name.as_str(),
                record.record_key.as_slice(),
            );
            reserve_repair_write_at(limiter, clock, 1)?;
            let _apply_outcome = destination.apply_repair_with_key(
                authority,
                &key,
                record.table_name.as_str(),
                record.record_key.as_slice(),
                record.record.clone(),
                self.snapshot.source_revision.as_str(),
            )?;
            reserve_repair_write_at(limiter, clock, 0)?;
            if !destination.verify_repair(
                authority,
                &key,
                record.table_name.as_str(),
                record.record_key.as_slice(),
                &record.record,
                self.snapshot.source_revision.as_str(),
            )? {
                return Err(TableFixError::DestinationVerificationFailed(
                    String::from_utf8_lossy(record.record_key.as_slice()).into_owned(),
                ));
            }
        }
        Ok(())
    }
}

impl TableFixSourceSnapshot {
    fn validate_truth_records(
        &self,
        request: &TableFixRequest,
        selected_keys: &HashSet<&[u8]>,
    ) -> TableFixResult<()> {
        if self.records.iter().any(|record| record.stale) {
            return Err(TableFixError::StaleSourceTruth);
        }
        let mut seen_keys = HashSet::new();
        for record in &self.records {
            if record.table_name != request.table_name
                || !selected_keys.contains(record.record_key.as_slice())
            {
                return Err(TableFixError::TruthRecordOutsideSelection);
            }
            if !seen_keys.insert(record.record_key.as_slice()) {
                return Err(TableFixError::DuplicateTruthRecord);
            }
        }
        Ok(())
    }
}

fn reserve_repair_write_at<Clock: RateLimitClock>(
    limiter: &OperationRateLimiter,
    clock: &Clock,
    rows: u64,
) -> TableFixResult<()> {
    enforce_rate_limit_with_limiter(
        limiter,
        RateLimitUsage {
            rows,
            destination_writes: 1,
            ..RateLimitUsage::default()
        },
        clock,
    )?;
    Ok(())
}

pub(super) fn validate_selected_row_limit(
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

impl TableFixReport {
    fn from_snapshot(
        request: &TableFixRequest,
        selected_rows: u64,
        snapshot: &TableFixSourceSnapshot,
    ) -> Self {
        let matched_rows = u64::try_from(snapshot.records.len()).unwrap_or(u64::MAX);
        let sample_keys = snapshot
            .records
            .iter()
            .take(SAMPLE_KEY_LIMIT)
            .map(|record| String::from_utf8_lossy(&record.record_key).into_owned())
            .collect();
        Self {
            operation_id: request.operation_id.clone(),
            table_name: request.table_name.clone(),
            dry_run: request.dry_run,
            selected_rows,
            repaired_rows: if request.dry_run { 0 } else { matched_rows },
            missing_rows: selected_rows.saturating_sub(matched_rows),
            destination_mutated: !request.dry_run && matched_rows > 0,
            sample_keys,
        }
    }
}

#[cfg(test)]
pub(super) fn projected_rows_revision(rows: &[ProjectedCurrentRow]) -> String {
    let bytes = serde_json::to_vec(rows).expect("projected rows are serializable");
    let digest = Sha256::digest(bytes);
    let mut revision = String::with_capacity(7 + digest.len() * 2);
    revision.push_str("sha256:");
    for byte in digest {
        revision.push_str(format!("{byte:02x}").as_str());
    }
    revision
}

#[cfg(test)]
pub(super) fn truth_record_from_projected_row(
    table_name: &str,
    row: &ProjectedCurrentRow,
) -> TableFixTruthRecord {
    let item = projected_values_to_storage_item(row);
    TableFixTruthRecord {
        table_name: table_name.to_string(),
        record_key: row.key.as_bytes().to_vec(),
        record: StorageStreamRecord {
            keys: storage_types::VecMap::from_iter([(
                "pk".to_string(),
                analytics_contract::StorageValue::S(row.key.clone()),
            )]),
            sequence_number: row.source_position.to_string(),
            old_image: None,
            new_image: Some(item),
        },
        stale: false,
    }
}

#[cfg(test)]
pub(super) fn projected_values_to_storage_item(
    row: &ProjectedCurrentRow,
) -> storage_types::VecMap<String, analytics_contract::StorageValue> {
    let mut item = storage_types::VecMap::from_iter([(
        "pk".to_string(),
        analytics_contract::StorageValue::S(row.key.clone()),
    )]);
    for (column, value) in &row.values {
        if let Some(value) = value {
            item.insert(
                column.clone(),
                analytics_contract::StorageValue::S(value.clone()),
            );
        }
    }
    item
}
