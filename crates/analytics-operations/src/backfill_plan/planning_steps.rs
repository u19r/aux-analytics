use super::*;

pub(super) fn validate_request(request: &BackfillRequest) -> BackfillPlanResult<()> {
    if request.target_tables.is_empty() {
        return Err(BackfillPlanError::MissingTargetTables);
    }
    if !request.all_tables_have_row_identity {
        return Err(BackfillPlanError::MissingRowIdentity {
            table: request.target_tables[0].clone(),
        });
    }
    Ok(())
}

pub(super) fn choose_snapshot_method(
    request: &BackfillRequest,
) -> BackfillPlanResult<SnapshotMethod> {
    if let Some(method) = request.requested_snapshot_method {
        validate_snapshot_method(request, method)?;
        return Ok(method);
    }
    if request.source.source_kind == BackfillSourceKind::DynamoDb
        && request.source.supports_pitr_export
    {
        return Ok(SnapshotMethod::DynamoDbPitrExport);
    }
    if request.source.source_kind == BackfillSourceKind::AuxStorage
        && request.source.supports_full_enumeration
    {
        return Ok(SnapshotMethod::AuxStorageEnumeration);
    }
    if request.source.source_kind == BackfillSourceKind::LocalFixture
        && request.source.supports_full_enumeration
    {
        return Ok(SnapshotMethod::LocalFixture);
    }
    if request.source.source_kind == BackfillSourceKind::DynamoDb
        && request.source.supports_parallel_scan
    {
        return Ok(SnapshotMethod::DynamoDbParallelScan);
    }
    if request.source.source_kind == BackfillSourceKind::RawBackup
        && request.source.supports_raw_replay
    {
        return Ok(SnapshotMethod::RawBackupReplay);
    }
    if request.source.source_kind == BackfillSourceKind::DynamoDb {
        Err(BackfillPlanError::UnsafeStreamOnlyBackfill {
            table: request.target_tables[0].clone(),
        })
    } else {
        Err(BackfillPlanError::HistoricalEnumerationUnsupported)
    }
}

fn validate_snapshot_method(
    request: &BackfillRequest,
    method: SnapshotMethod,
) -> BackfillPlanResult<()> {
    match method {
        SnapshotMethod::DynamoDbPitrExport
            if request.source.source_kind == BackfillSourceKind::DynamoDb
                && request.source.supports_pitr_export =>
        {
            Ok(())
        }
        SnapshotMethod::DynamoDbParallelScan
            if request.source.source_kind == BackfillSourceKind::DynamoDb
                && request.source.supports_parallel_scan =>
        {
            Ok(())
        }
        SnapshotMethod::AuxStorageEnumeration
            if request.source.source_kind == BackfillSourceKind::AuxStorage
                && request.source.supports_full_enumeration =>
        {
            Ok(())
        }
        SnapshotMethod::RawBackupReplay
            if request.source.source_kind == BackfillSourceKind::RawBackup
                && request.source.supports_raw_replay =>
        {
            Ok(())
        }
        SnapshotMethod::LocalFixture
            if request.source.source_kind == BackfillSourceKind::LocalFixture
                && request.source.supports_full_enumeration =>
        {
            Ok(())
        }
        _ if request.source.source_kind == BackfillSourceKind::DynamoDb => {
            Err(BackfillPlanError::UnsafeStreamOnlyBackfill {
                table: request.target_tables[0].clone(),
            })
        }
        _ => Err(BackfillPlanError::HistoricalEnumerationUnsupported),
    }
}

pub(super) fn estimate_chunks(estimated_rows: u64, chunk_target_rows: u64) -> u64 {
    let chunk_target_rows = chunk_target_rows.max(1);
    estimated_rows.div_ceil(chunk_target_rows).max(1)
}

pub(super) fn source_impact(method: SnapshotMethod) -> &'static str {
    match method {
        SnapshotMethod::DynamoDbPitrExport => "low table read impact; requires PITR export access",
        SnapshotMethod::DynamoDbParallelScan => "bounded table read capacity impact",
        SnapshotMethod::AuxStorageEnumeration => "bounded aux-storage enumeration reads",
        SnapshotMethod::RawBackupReplay => "object-store reads only",
        SnapshotMethod::LocalFixture => "local fixture reads only",
    }
}

pub(super) fn validation_steps(mode: ValidationMode) -> Vec<String> {
    match mode {
        ValidationMode::CountsAndHashes => vec![
            "compare_table_counts".to_string(),
            "compare_key_set_hashes".to_string(),
            "compare_projected_column_hashes".to_string(),
        ],
        ValidationMode::FullRows => vec![
            "compare_table_counts".to_string(),
            "compare_key_sets".to_string(),
            "compare_full_rows".to_string(),
        ],
    }
}

pub(super) fn required_permissions(
    method: SnapshotMethod,
    destination_backend: DestinationBackend,
) -> Vec<String> {
    let mut permissions = match method {
        SnapshotMethod::DynamoDbPitrExport => vec![
            "dynamodb:DescribeTable".to_string(),
            "dynamodb:DescribeContinuousBackups".to_string(),
            "dynamodb:ExportTableToPointInTime".to_string(),
            "s3 read access to the export location".to_string(),
        ],
        SnapshotMethod::DynamoDbParallelScan => vec![
            "dynamodb:DescribeTable".to_string(),
            "dynamodb:Scan".to_string(),
        ],
        SnapshotMethod::AuxStorageEnumeration => vec![
            "aux-storage metadata read access".to_string(),
            "aux-storage stream/table enumeration read access".to_string(),
        ],
        SnapshotMethod::RawBackupReplay => vec![
            "object-store list access to the backup prefix".to_string(),
            "object-store read access to backup objects".to_string(),
        ],
        SnapshotMethod::LocalFixture => {
            vec!["local filesystem read access to fixture data".to_string()]
        }
    };
    permissions.push(match destination_backend {
        DestinationBackend::DuckDb => {
            "DuckDB read/write access to destination database".to_string()
        }
        DestinationBackend::DuckLake => {
            "DuckLake catalog and object-store read/write access".to_string()
        }
    });
    permissions
}

pub(super) fn warnings(request: &BackfillRequest, method: SnapshotMethod) -> Vec<String> {
    let mut warnings = Vec::new();
    if !request.source.can_prove_deletes {
        warnings.push(
            "source cannot prove historical deletes; follow-up check may report extras".to_string(),
        );
    }
    if method == SnapshotMethod::DynamoDbParallelScan {
        warnings.push(
            "parallel scan consumes live table read capacity; keep rate limits conservative"
                .to_string(),
        );
    }
    warnings
}

pub(super) fn step(order: u8, name: &str) -> BackfillPlanStep {
    BackfillPlanStep {
        order,
        name: name.to_string(),
    }
}
