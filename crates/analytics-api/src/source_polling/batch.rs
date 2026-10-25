use analytics_storage::SourceCheckpoint as StorageSourceCheckpoint;

/// Return only checkpoints that represent durable source positions.
pub(crate) fn persistable_checkpoints(
    checkpoints: &[StorageSourceCheckpoint],
) -> impl Iterator<Item = &StorageSourceCheckpoint> {
    checkpoints
        .iter()
        .filter(|checkpoint| !is_iterator_checkpoint(checkpoint))
}

/// Convert storage checkpoints to the engine's validated durable position type.
pub(crate) fn record_checkpoints(
    checkpoints: &[StorageSourceCheckpoint],
) -> Vec<analytics_engine::RecordCheckpoint> {
    let mut record_checkpoints = Vec::with_capacity(checkpoints.len());
    for checkpoint in persistable_checkpoints(checkpoints) {
        if let Ok(checkpoint) = analytics_engine::RecordCheckpoint::try_new(
            checkpoint.source_table_name.clone(),
            checkpoint.shard_id.clone(),
            checkpoint.position.clone(),
        ) {
            record_checkpoints.push(checkpoint);
        }
    }
    record_checkpoints
}

pub(crate) fn is_iterator_checkpoint(checkpoint: &StorageSourceCheckpoint) -> bool {
    checkpoint.is_iterator()
}
