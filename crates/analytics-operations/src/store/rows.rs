use super::*;

pub(super) fn row_to_operation(row: &duckdb::Row<'_>) -> duckdb::Result<StoredOperation> {
    let operation_id: String = row.get(0)?;
    let kind: String = row.get(1)?;
    let actor: String = row.get(2)?;
    let target_tables_json: String = row.get(3)?;
    let rate_limit_json: String = row.get(5)?;
    let payload_json: String = row.get(6)?;
    let phase: String = row.get(7)?;
    let status: String = row.get(8)?;
    let cancellation_state: String = row.get(9)?;
    let cursor_json: String = row.get(10)?;
    Ok(StoredOperation {
        operation_id: parse_json_error(OperationId::try_new(operation_id))?,
        kind: parse_json_error(OperationKind::from_str(kind.as_str()))?,
        actor: parse_json_error(OperationActor::try_new(actor))?,
        target_tables: parse_json_error(serde_json::from_str(target_tables_json.as_str()))?,
        dry_run: row.get(4)?,
        rate_limit: parse_json_error(serde_json::from_str(rate_limit_json.as_str()))?,
        payload: parse_json_error(serde_json::from_str(payload_json.as_str()))?,
        phase: parse_json_error(OperationPhase::from_str(phase.as_str()))?,
        status: parse_json_error(OperationStatus::from_str(status.as_str()))?,
        cancellation_state: parse_json_error(CancellationState::from_str(
            cancellation_state.as_str(),
        ))?,
        cursor: parse_json_error(serde_json::from_str(cursor_json.as_str()))?,
        created_at_ms: row.get(11)?,
        updated_at_ms: row.get(12)?,
        revision: parse_json_error(u64::try_from(row.get::<_, i64>(13)?))?,
        progress: parse_json_error(serde_json::from_str(row.get::<_, String>(14)?.as_str()))?,
        execution_generation: parse_json_error(u64::try_from(row.get::<_, i64>(15)?))?,
        execution_owner: {
            let owner: String = row.get(16)?;
            (!owner.is_empty()).then_some(owner)
        },
        execution_lease_until_ms: row.get(17)?,
    })
}

pub(super) fn row_to_event(row: &duckdb::Row<'_>) -> duckdb::Result<OperationEvent> {
    let operation_id: String = row.get(0)?;
    let kind: String = row.get(3)?;
    let phase: String = row.get(4)?;
    let status: String = row.get(5)?;
    let cursor_json: String = row.get(6)?;
    let counts_json: String = row.get(7)?;
    Ok(OperationEvent {
        operation_id: parse_json_error(OperationId::try_new(operation_id))?,
        event_id: row.get(1)?,
        occurred_at_ms: row.get(2)?,
        kind: parse_json_error(OperationEventKind::from_str(kind.as_str()))?,
        phase: parse_json_error(OperationPhase::from_str(phase.as_str()))?,
        status: parse_json_error(OperationStatus::from_str(status.as_str()))?,
        cursor: parse_json_error(serde_json::from_str(cursor_json.as_str()))?,
        counts: parse_json_error(serde_json::from_str(counts_json.as_str()))?,
        message: row.get(8)?,
    })
}

fn parse_json_error<T, E: std::error::Error + Send + Sync + 'static>(
    result: Result<T, E>,
) -> duckdb::Result<T> {
    result.map_err(|error| duckdb::Error::ToSqlConversionFailure(Box::new(error)))
}
