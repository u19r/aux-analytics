use super::*;

pub(super) fn validate_kind_scope(
    source_kind: SourceJobKind,
    scope: &SourceJobScope,
) -> Result<(), BackgroundJobError> {
    let valid = matches!(
        (source_kind, scope),
        (SourceJobKind::AuxStorage, SourceJobScope::System { .. })
            | (SourceJobKind::StorageStream, SourceJobScope::Table { .. })
    );
    if valid {
        Ok(())
    } else {
        Err(BackgroundJobError::InvalidPayload(
            "source job kind does not match its scope kind",
        ))
    }
}

impl SourceJobCheckpoint {
    pub(super) fn from_storage_checkpoint(checkpoint: SourceCheckpoint) -> Self {
        Self {
            iterator: checkpoint.is_iterator(),
            position: checkpoint.position,
        }
    }

    pub(super) fn into_storage_checkpoint(
        self,
        table_scope: Option<(&str, &str)>,
    ) -> SourceCheckpoint {
        let (source_table_name, shard_id) =
            table_scope.expect("validated source payload table checkpoints have a table scope");
        SourceCheckpoint {
            source_table_name: source_table_name.to_string(),
            shard_id: if self.iterator {
                format!("__iterator:{shard_id}")
            } else {
                shard_id.to_string()
            },
            position: self.position,
        }
    }
}

pub(super) fn validate_scope(scope: &SourceJobScope) -> Result<(), BackgroundJobError> {
    match scope {
        SourceJobScope::Table {
            source_table_name,
            shard_id,
            previous_position,
        } => {
            validate_scope_text(source_table_name)?;
            validate_scope_text(shard_id)?;
            if let Some(previous_position) = previous_position {
                validate_text(previous_position)?;
            }
        }
        SourceJobScope::System { source_scope, .. } => {
            validate_scope_text(source_scope.scope_id().as_str())?;
        }
    }
    Ok(())
}

pub(crate) fn validate_source_table_name(value: &str) -> Result<(), BackgroundJobError> {
    validate_scope_text(value)
}

fn validate_scope_text(value: &str) -> Result<(), BackgroundJobError> {
    if value.len() > MAX_SOURCE_SCOPE_BYTES {
        return Err(BackgroundJobError::InvalidPayload(
            "source job scope is oversized",
        ));
    }
    validate_text(value)
}

fn validate_text(value: &str) -> Result<(), BackgroundJobError> {
    if value.trim().is_empty()
        || value.len() > MAX_SOURCE_FIELD_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(BackgroundJobError::InvalidPayload(
            "source job text field is invalid",
        ));
    }
    Ok(())
}

pub(super) fn validate_batch_id(batch_id: &str) -> Result<(), BackgroundJobError> {
    validate_text(batch_id)
}

pub(super) fn validate_deadline(deadline_at_ms: u64) -> Result<(), BackgroundJobError> {
    if deadline_at_ms == 0 || deadline_at_ms > MAX_SOURCE_DEADLINE_AT_MS {
        return Err(BackgroundJobError::InvalidPayload(
            "source job deadline is invalid",
        ));
    }
    Ok(())
}

pub(super) fn validate_payload_bounds(
    scope: &SourceJobScope,
    records: &[SourceJobRecord],
    checkpoints: &[SourceJobCheckpoint],
) -> Result<(), BackgroundJobError> {
    if records
        .iter()
        .try_fold(0usize, |count, record| {
            count.checked_add(record.analytics_table_names.len())
        })
        .is_none_or(|count| count > MAX_SOURCE_RECORDS)
    {
        return Err(BackgroundJobError::InvalidPayload(
            "source job record bound exceeded",
        ));
    }
    if matches!(scope, SourceJobScope::Table { .. }) && checkpoints.is_empty() {
        return Err(BackgroundJobError::InvalidPayload(
            "source job requires a durable checkpoint",
        ));
    }
    if matches!(scope, SourceJobScope::System { .. }) && !checkpoints.is_empty() {
        return Err(BackgroundJobError::InvalidPayload(
            "system source job cannot carry table checkpoints",
        ));
    }
    if checkpoints.len() > MAX_SOURCE_CHECKPOINTS {
        return Err(BackgroundJobError::InvalidPayload(
            "source job checkpoint bound exceeded",
        ));
    }
    if checkpoints
        .iter()
        .filter(|checkpoint| !checkpoint.iterator)
        .count()
        > 1
    {
        return Err(BackgroundJobError::InvalidPayload(
            "source job has more than one durable checkpoint",
        ));
    }
    Ok(())
}

pub(super) fn validate_records(
    scope: &SourceJobScope,
    records: &[SourceJobRecord],
) -> Result<(), BackgroundJobError> {
    for record in records {
        validate_scope_text(&record.source_table_name)?;
        if record.analytics_table_names.is_empty() {
            return Err(BackgroundJobError::InvalidPayload(
                "source record has no destination",
            ));
        }
        for (index, name) in record.analytics_table_names.iter().enumerate() {
            validate_text(name)?;
            if record.analytics_table_names[..index].contains(name) {
                return Err(BackgroundJobError::InvalidPayload(
                    "source record has duplicate destinations",
                ));
            }
        }
        if record.record_key.len() > MAX_SOURCE_FIELD_BYTES {
            return Err(BackgroundJobError::InvalidPayload(
                "source record key bound exceeded",
            ));
        }
        if std::str::from_utf8(&record.record_key).is_err() {
            return Err(BackgroundJobError::InvalidPayload(
                "source record key is not UTF-8",
            ));
        }
        validate_text(&record.record.sequence_number)?;
        match scope {
            SourceJobScope::Table {
                source_table_name, ..
            } if record.source_table_name != *source_table_name => {
                return Err(BackgroundJobError::InvalidPayload(
                    "source job record table does not match its scope",
                ));
            }
            SourceJobScope::Table { .. } if record.system_position.is_some() => {
                return Err(BackgroundJobError::InvalidPayload(
                    "table source job carries a SystemStream position",
                ));
            }
            SourceJobScope::System { .. } if record.system_position.is_none() => {
                return Err(BackgroundJobError::InvalidPayload(
                    "system source job record is missing its logical position",
                ));
            }
            SourceJobScope::System { .. } => {}
            SourceJobScope::Table { .. } => {}
        }
    }
    Ok(())
}

pub(super) fn validate_checkpoints(
    checkpoints: &[SourceJobCheckpoint],
) -> Result<(), BackgroundJobError> {
    for checkpoint in checkpoints {
        validate_text(&checkpoint.position)?;
    }
    Ok(())
}

pub(super) fn validate_batch_identity(
    input: BatchIdentityInput<'_>,
    batch_id: &str,
) -> Result<(), BackgroundJobError> {
    let expected_batch_id = batch_identity(input);
    if batch_id != expected_batch_id {
        return Err(BackgroundJobError::InvalidPayload(
            "source job batch identity does not match its contents",
        ));
    }
    Ok(())
}
