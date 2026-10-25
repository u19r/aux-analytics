use super::*;

/// Versioned neutral payload crossing the durable job boundary.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "version", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub(crate) enum SourceJobPayload {
    V1 {
        source_kind: SourceJobKind,
        manifest_digest: [u8; analytics_engine::MANIFEST_DIGEST_BYTES],
        scope: SourceJobScope,
        batch_id: String,
        deadline_at_ms: u64,
        records: Vec<SourceJobRecord>,
        checkpoints: Vec<SourceJobCheckpoint>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceJobRecord {
    pub(super) source_table_name: String,
    pub(super) analytics_table_names: Vec<String>,
    pub(super) record_key: Vec<u8>,
    pub(super) record: analytics_contract::StorageStreamRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) system_position: Option<SystemStreamPosition>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceJobCheckpoint {
    pub(super) iterator: bool,
    pub(super) position: String,
}

pub(super) struct BatchIdentityInput<'a> {
    pub(super) source_kind: SourceJobKind,
    pub(super) manifest_digest: &'a [u8; analytics_engine::MANIFEST_DIGEST_BYTES],
    pub(super) scope: &'a SourceJobScope,
    pub(super) records: &'a [SourceJobRecord],
    pub(super) checkpoints: &'a [SourceJobCheckpoint],
}

impl SourceJobPayload {
    pub(crate) fn from_batch(
        source_kind: SourceJobKind,
        batch: PollBatch,
        deadline_at_ms: u64,
        manifest_digest: [u8; analytics_engine::MANIFEST_DIGEST_BYTES],
    ) -> Result<Self, BackgroundJobError> {
        let scope = SourceJobScope::from_poll_batch(&batch)?;
        let records = records::coalesce_records(batch.records);
        let checkpoints = batch
            .checkpoints
            .into_iter()
            .map(SourceJobCheckpoint::from_storage_checkpoint)
            .collect::<Vec<_>>();
        let mut payload = Self::V1 {
            source_kind,
            manifest_digest,
            scope,
            batch_id: String::new(),
            deadline_at_ms,
            records,
            checkpoints,
        };
        payload.validate_structure()?;
        payload.assign_batch_identity();
        Ok(payload)
    }

    pub(crate) fn validate(&self) -> Result<(), BackgroundJobError> {
        self.validate_structure()?;
        let Self::V1 {
            source_kind,
            manifest_digest,
            scope,
            batch_id,
            records,
            checkpoints,
            ..
        } = self;
        validate_batch_id(batch_id)?;
        validate_batch_identity(
            BatchIdentityInput {
                source_kind: *source_kind,
                manifest_digest,
                scope,
                records,
                checkpoints,
            },
            batch_id,
        )?;
        Ok(())
    }

    fn validate_structure(&self) -> Result<(), BackgroundJobError> {
        let Self::V1 {
            source_kind,
            scope,
            deadline_at_ms,
            records,
            checkpoints,
            ..
        } = self;
        validate_kind_scope(*source_kind, scope)?;
        validate_scope(scope)?;
        validate_deadline(*deadline_at_ms)?;
        validate_payload_bounds(scope, records, checkpoints)?;
        validate_records(scope, records)?;
        validate_checkpoints(checkpoints)?;
        Ok(())
    }

    fn assign_batch_identity(&mut self) {
        let Self::V1 {
            source_kind,
            manifest_digest,
            scope,
            batch_id,
            records,
            checkpoints,
            ..
        } = self;
        *batch_id = batch_identity(BatchIdentityInput {
            source_kind: *source_kind,
            manifest_digest,
            scope,
            records,
            checkpoints,
        });
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, BackgroundJobError> {
        self.validate()?;
        payload::encode_payload(self)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, BackgroundJobError> {
        let decoded = payload::decode_payload(encoded)?;
        decoded.validate()?;
        Ok(decoded)
    }

    pub(crate) fn batch_id(&self) -> &str {
        let Self::V1 { batch_id, .. } = self;
        batch_id
    }

    pub(crate) fn source_kind(&self) -> SourceJobKind {
        let Self::V1 { source_kind, .. } = self;
        *source_kind
    }

    pub(crate) fn source_scope(&self) -> SourceJobScope {
        let Self::V1 { scope, .. } = self;
        scope.clone()
    }

    pub(crate) fn manifest_digest(&self) -> &[u8; analytics_engine::MANIFEST_DIGEST_BYTES] {
        let Self::V1 {
            manifest_digest, ..
        } = self;
        manifest_digest
    }

    pub(crate) fn deadline_at_ms(&self) -> u64 {
        let Self::V1 { deadline_at_ms, .. } = self;
        *deadline_at_ms
    }

    pub(crate) fn record_source_tables(&self) -> impl Iterator<Item = &str> {
        let Self::V1 { records, .. } = self;
        records
            .iter()
            .map(|record| record.source_table_name.as_str())
    }

    pub(crate) fn into_batch(self) -> PollBatch {
        match self {
            Self::V1 {
                scope,
                records,
                checkpoints,
                ..
            } => {
                let (table_scope, system_stream) = match &scope {
                    SourceJobScope::Table {
                        source_table_name,
                        shard_id,
                        ..
                    } => (Some((source_table_name.as_str(), shard_id.as_str())), None),
                    SourceJobScope::System {
                        source_scope,
                        previous_checkpoint,
                        checkpoint,
                    } => (
                        None,
                        Some(SystemStreamBatch {
                            source_scope: source_scope.clone(),
                            previous_checkpoint: match previous_checkpoint {
                                SystemStreamCheckpointPredecessor::Initial => None,
                                SystemStreamCheckpointPredecessor::Token { checkpoint } => {
                                    Some(checkpoint.clone())
                                }
                            },
                            checkpoint: checkpoint.clone(),
                        }),
                    ),
                };
                PollBatch {
                    source_response_count: 1,
                    source_nonempty_response_count: usize::from(!records.is_empty()),
                    source_record_count: records.len(),
                    source_encoded_bytes: 0,
                    records: records
                        .into_iter()
                        .map(|record| record.into_polled_record(table_scope))
                        .collect(),
                    checkpoints: checkpoints
                        .into_iter()
                        .map(|checkpoint| checkpoint.into_storage_checkpoint(table_scope))
                        .collect(),
                    source_predecessor: match &scope {
                        SourceJobScope::Table {
                            source_table_name,
                            shard_id,
                            previous_position,
                        } => previous_position.as_ref().map(|position| SourceCheckpoint {
                            source_table_name: source_table_name.clone(),
                            shard_id: shard_id.clone(),
                            position: position.clone(),
                        }),
                        SourceJobScope::System { .. } => None,
                    },
                    system_stream,
                }
            }
        }
    }
}
