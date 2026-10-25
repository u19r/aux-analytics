use super::*;
use crate::constants::HEX;

pub(super) fn batch_identity(input: BatchIdentityInput<'_>) -> String {
    let BatchIdentityInput {
        source_kind,
        manifest_digest,
        scope,
        records,
        checkpoints,
    } = input;
    let mut hasher = Sha256::new();
    append_identity_field(&mut hasher, b"analytics-api:source-batch:v1");
    append_identity_field(&mut hasher, source_kind.job_kind().as_bytes());
    append_identity_field(&mut hasher, manifest_digest);
    append_identity_scope(&mut hasher, scope);
    append_identity_records(&mut hasher, records);
    append_identity_checkpoints(&mut hasher, checkpoints);
    format_identity(hasher.finalize())
}

fn append_identity_scope(hasher: &mut Sha256, scope: &SourceJobScope) {
    match scope {
        SourceJobScope::Table {
            source_table_name,
            shard_id,
            previous_position,
        } => {
            append_identity_field(hasher, b"table");
            append_identity_field(hasher, source_table_name.as_bytes());
            append_identity_field(hasher, shard_id.as_bytes());
            match previous_position {
                Some(position) => append_identity_field(hasher, position.as_bytes()),
                None => hasher.update([0]),
            }
        }
        SourceJobScope::System {
            source_scope,
            previous_checkpoint,
            checkpoint,
        } => {
            append_identity_field(hasher, b"system");
            hasher.update([source_scope.version()]);
            append_identity_field(hasher, source_scope.scope_id().as_str().as_bytes());
            append_identity_field(hasher, source_scope.generation().to_be_bytes().as_slice());
            match previous_checkpoint {
                SystemStreamCheckpointPredecessor::Initial => hasher.update([0]),
                SystemStreamCheckpointPredecessor::Token { checkpoint } => {
                    hasher.update([2]);
                    append_identity_field(hasher, checkpoint.as_str().as_bytes());
                }
            }
            append_identity_field(hasher, checkpoint.as_str().as_bytes());
        }
    }
}

fn append_identity_records(hasher: &mut Sha256, records: &[SourceJobRecord]) {
    append_identity_count(hasher, records.len());
    let mut record_bytes = Vec::new();
    for record in records {
        append_identity_field(hasher, record.source_table_name.as_bytes());
        append_identity_count(hasher, record.analytics_table_names.len());
        for name in &record.analytics_table_names {
            append_identity_field(hasher, name.as_bytes());
        }
        append_identity_field(hasher, &record.record_key);
        append_identity_field(hasher, record.record.sequence_number.as_bytes());
        record_bytes.clear();
        record.record.write_canonical_json(&mut record_bytes);
        append_identity_field(hasher, &record_bytes);
        if let Some(position) = record.system_position.as_ref() {
            append_identity_field(hasher, position.shard_id().as_str().as_bytes());
            append_identity_field(hasher, position.lineage_id().as_str().as_bytes());
            append_identity_field(hasher, position.offset().as_str().as_bytes());
        }
    }
}

fn append_identity_checkpoints(hasher: &mut Sha256, checkpoints: &[SourceJobCheckpoint]) {
    append_identity_count(hasher, checkpoints.len());
    for checkpoint in checkpoints {
        hasher.update([u8::from(checkpoint.iterator)]);
        append_identity_field(hasher, checkpoint.position.as_bytes());
    }
}

fn format_identity(digest: sha2::digest::Output<Sha256>) -> String {
    let mut identity = String::with_capacity(3 + digest.len() * 2);
    identity.push_str("v1-");
    for byte in digest {
        identity.push(HEX[usize::from(byte >> 4)] as char);
        identity.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    identity
}

fn append_identity_count(hasher: &mut Sha256, count: usize) {
    hasher.update(u64::try_from(count).unwrap_or(u64::MAX).to_be_bytes());
}

fn append_identity_field(hasher: &mut Sha256, field: &[u8]) {
    append_identity_count(hasher, field.len());
    hasher.update(field);
}
