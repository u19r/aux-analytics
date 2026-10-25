use std::{collections::HashMap, error::Error, future::Future, num::NonZeroU16, sync::Arc};

use analytics_contract::{AnalyticsManifest, PrivacyPolicy};
use analytics_engine::{
    RecordCheckpoint, SourceBatchRecord, SourceEffectAuthority, SourceEffectDisposition,
    SourceEffectRequest, SourceEffectRequestInput, SourceEffectScope,
};
use analytics_storage::{PollBatch, SystemStreamResume};
use bg_jobs::{
    EffectMode, ExecutionContext, FairnessPolicy, HandlerFuture, JobCatalog, JobHandle, JobHandler,
    JobKind, JobOutcome, JobSpec, OutcomeCode, QueueName, RetryPreset, RetrySelection,
    ValidationError,
};
use bytes::Bytes;

use crate::{
    background_jobs::{
        contracts::{SourceJobKind, SourceJobPayload, SourceJobScope},
        error::{BackgroundJobError, api_config_error},
        observation::{SourceJobShared, update_source_success},
    },
    constants::SOURCE_SCHEMA_VERSION,
    source_polling::{
        SourceMemoryObservation, SourceWriteObservation, persistable_checkpoints,
        record_checkpoints, source_effect_scope,
    },
};

pub(super) fn handler_catalog(
    shared: Arc<SourceJobShared>,
    timeout: std::time::Duration,
) -> crate::error::ApiResult<(
    JobCatalog,
    HashMap<SourceJobKind, JobHandle<SourceJobPayload>>,
)> {
    let mut catalog = JobCatalog::new();
    let mut handles = HashMap::new();
    for kind in SourceJobKind::ALL {
        let handle = catalog
            .register(job_spec(&shared, timeout, kind)?)
            .map_err(api_config_error)?;
        handles.insert(kind, handle);
    }
    Ok((catalog, handles))
}

fn job_spec(
    shared: &Arc<SourceJobShared>,
    timeout: std::time::Duration,
    kind: SourceJobKind,
) -> crate::error::ApiResult<JobSpec<SourceJobPayload, SourceJobHandler>> {
    let source_kind = kind;
    let job_kind = JobKind::try_new(source_kind.job_kind()).map_err(api_config_error)?;
    let schema = NonZeroU16::new(SOURCE_SCHEMA_VERSION).ok_or_else(|| {
        api_config_error(BackgroundJobError::InvalidConfiguration(
            "source schema version is zero",
        ))
    })?;
    let queue = QueueName::try_new(source_kind.queue_name()).map_err(api_config_error)?;
    Ok(JobSpec::new(
        job_kind,
        schema,
        queue,
        SourceJobHandler {
            shared: Arc::clone(shared),
            expected_kind: source_kind,
        },
        encode_source_payload as fn(&SourceJobPayload) -> Result<Bytes, ValidationError>,
    )
    .policy(
        EffectMode::Fenced,
        FairnessPolicy::low_volume(),
        RetrySelection::Preset(RetryPreset::Network),
        timeout,
    ))
}

fn encode_source_payload(payload: &SourceJobPayload) -> Result<Bytes, ValidationError> {
    payload
        .encode()
        .map(Bytes::from)
        .map_err(|error| match error {
            BackgroundJobError::PayloadTooLarge { actual, maximum } => ValidationError::TooLong {
                field: "source job payload",
                maximum,
                actual,
            },
            _ => ValidationError::InvalidValue {
                field: "source job payload",
            },
        })
}

struct SourceJobHandler {
    shared: Arc<SourceJobShared>,
    expected_kind: SourceJobKind,
}

struct PreparedSourceEffect {
    manifest: Arc<AnalyticsManifest>,
    manifest_digest: [u8; analytics_engine::MANIFEST_DIGEST_BYTES],
    authority: SourceEffectAuthority,
    scope: SourceEffectScope,
    records: Vec<SourceBatchRecord>,
    checkpoints: Vec<RecordCheckpoint>,
    privacy_policy: Option<Arc<PrivacyPolicy>>,
}

impl JobHandler for SourceJobHandler {
    fn execute<'a>(
        &'a self,
        context: ExecutionContext<'a>,
        payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        let shared = Arc::clone(&self.shared);
        let expected_kind = self.expected_kind;
        Box::pin(async move { execute_source_job(shared, expected_kind, context, payload).await })
    }
}

async fn execute_source_job(
    shared: Arc<SourceJobShared>,
    expected_kind: SourceJobKind,
    context: ExecutionContext<'_>,
    encoded: &[u8],
) -> Result<JobOutcome, Box<dyn Error + Send + Sync>> {
    let _memory = SourceMemoryObservation::start();
    if context.is_cancelled() {
        return Ok(JobOutcome::Cancelled);
    }
    let payload = decode_source_job(&shared, expected_kind, encoded).await?;
    let manifest_digest = *payload.manifest_digest();
    let mut batch = payload.into_batch();
    let record_count = batch
        .records
        .iter()
        .map(|record| record.analytics_table_names.len() as u64)
        .sum();
    let Some(effect) =
        prepare_source_effect(&shared, &context, &mut batch, manifest_digest).await?
    else {
        return Ok(JobOutcome::Cancelled);
    };
    let destination = apply_source_effect(&shared, effect).await;
    if let Ok(disposition) = &destination {
        let applied = match disposition {
            SourceEffectDisposition::Applied { .. } => Some(&batch),
            SourceEffectDisposition::AlreadyApplied { .. }
            | SourceEffectDisposition::StaleFence => None,
        };
        synchronize_source_progress(&shared, expected_kind, applied).await?;
    }
    settle_source_effect(destination, || async {
        update_source_success(&shared, &batch, record_count).await;
        #[cfg(any(test, feature = "bdd-test-support"))]
        shared
            .progress_commits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    })
    .await
}

async fn decode_source_job(
    shared: &SourceJobShared,
    expected_kind: SourceJobKind,
    encoded: &[u8],
) -> Result<SourceJobPayload, Box<dyn Error + Send + Sync>> {
    let payload = SourceJobPayload::decode(encoded)?;
    if payload.source_kind() != expected_kind {
        return Err(Box::new(BackgroundJobError::InvalidPayload(
            "source job kind does not match its registered queue",
        )));
    }
    let scope = payload.source_scope();
    let authorized_scope = match (&scope, expected_kind) {
        (SourceJobScope::System { source_scope, .. }, SourceJobKind::AuxStorage) => {
            let live_scope = shared.pollers.lock().await.get(&expected_kind).cloned();
            let live_scope = if let Some(poller) = live_scope {
                poller.lock().await.system_stream_scope()
            } else {
                None
            };
            live_scope.as_ref() == Some(source_scope)
                && shared.system_stream_is_authorized(expected_kind)
        }
        (SourceJobScope::System { .. }, SourceJobKind::StorageStream) => false,
        (
            SourceJobScope::Table {
                source_table_name, ..
            },
            _,
        ) => shared.source_table_is_authorized(expected_kind, source_table_name),
    };
    let authorized_records = payload
        .record_source_tables()
        .all(|source_table| shared.source_table_is_authorized(expected_kind, source_table));
    if !authorized_scope || !authorized_records {
        return Err(Box::new(BackgroundJobError::InvalidPayload(
            "source job source identity is not configured for its source kind",
        )));
    }
    let manifest = shared.manifest.read().await;
    if payload.manifest_digest() != &analytics_engine::manifest_digest(&manifest) {
        return Err(Box::new(BackgroundJobError::InvalidPayload(
            "source job manifest revision is no longer current",
        )));
    }
    Ok(payload)
}

async fn prepare_source_effect(
    shared: &SourceJobShared,
    context: &ExecutionContext<'_>,
    batch: &mut PollBatch,
    manifest_digest: [u8; analytics_engine::MANIFEST_DIGEST_BYTES],
) -> Result<Option<PreparedSourceEffect>, Box<dyn Error + Send + Sync>> {
    let scope = source_effect_scope(batch)
        .map_err(|_| BackgroundJobError::InvalidPayload("source job scope is invalid"))?
        .ok_or(BackgroundJobError::MissingScope)?;
    let checkpoints = durable_checkpoints(batch)?;
    if context.is_cancelled() {
        return Ok(None);
    }
    let manifest = shared.manifest.read().await.clone();
    let records = std::mem::take(&mut batch.records);
    let Some(records) = shared
        .prepare_source_batch_records(context, &manifest, records)
        .await
    else {
        return Ok(None);
    };
    if context.is_cancelled() {
        return Ok(None);
    }
    Ok(Some(PreparedSourceEffect {
        manifest,
        manifest_digest,
        authority: SourceEffectAuthority::from_parts(
            context.execution_id().into_bytes(),
            context.fence(),
        ),
        scope,
        records,
        checkpoints,
        privacy_policy: shared.privacy_policy.clone(),
    }))
}

fn durable_checkpoints(batch: &PollBatch) -> Result<Vec<RecordCheckpoint>, BackgroundJobError> {
    let checkpoints = record_checkpoints(&batch.checkpoints);
    if checkpoints.len() != persistable_checkpoints(&batch.checkpoints).count() {
        return Err(BackgroundJobError::InvalidPayload(
            "source checkpoint does not satisfy the durable position contract",
        ));
    }
    Ok(checkpoints)
}

impl SourceJobShared {
    async fn prepare_source_batch_records(
        &self,
        context: &ExecutionContext<'_>,
        manifest: &AnalyticsManifest,
        source_records: Vec<analytics_storage::PolledRecord>,
    ) -> Option<Vec<SourceBatchRecord>> {
        let capacity = source_records
            .iter()
            .map(|record| record.analytics_table_names.len())
            .sum();
        let mut records = Vec::with_capacity(capacity);
        for record in source_records
            .into_iter()
            .flat_map(SourceJobShared::into_destination_records)
        {
            if context.is_cancelled() {
                return None;
            }
            let retention = if let Some(runtime) = self.retention.as_ref() {
                runtime
                    .retention_for_record(
                        manifest,
                        &record.analytics_table_name,
                        &record.source_table_name,
                        &record.record,
                    )
                    .await
            } else {
                None
            };
            records.push(SourceBatchRecord {
                item: record,
                retention,
            });
        }
        Some(records)
    }
}

async fn apply_source_effect(
    shared: &SourceJobShared,
    effect: PreparedSourceEffect,
) -> Result<SourceEffectDisposition, BackgroundJobError> {
    let PreparedSourceEffect {
        manifest,
        manifest_digest,
        authority,
        scope,
        records,
        checkpoints,
        privacy_policy,
    } = effect;
    let observation = SourceWriteObservation::start();
    let result = shared
        .engine
        .with_write(move |engine| {
            let request = SourceEffectRequest::try_new(SourceEffectRequestInput {
                manifest: &manifest,
                manifest_digest,
                authority,
                scope,
                records,
                checkpoints,
                privacy_policy: privacy_policy.as_deref(),
            })?;
            engine.ingest_fenced_source_batch(request)
        })
        .await
        .map_err(classify_destination_error);
    observation.record_result(&result);
    result
}

pub(crate) async fn settle_source_effect<F, Fut>(
    destination: Result<SourceEffectDisposition, BackgroundJobError>,
    commit: F,
) -> Result<JobOutcome, Box<dyn std::error::Error + Send + Sync>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), BackgroundJobError>>,
{
    let (outcome_code, commit_progress) = match destination? {
        SourceEffectDisposition::Applied { .. } => ("source_applied", true),
        SourceEffectDisposition::AlreadyApplied { .. } => ("source_already_applied", false),
        SourceEffectDisposition::StaleFence => ("source_stale_fence", false),
    };
    if commit_progress {
        commit().await?;
    }
    Ok(JobOutcome::Complete(OutcomeCode::try_new(outcome_code)?))
}

fn classify_destination_error(error: analytics_engine::AnalyticsEngineError) -> BackgroundJobError {
    match error {
        analytics_engine::AnalyticsEngineError::SourceEffectFenceUnavailable => {
            BackgroundJobError::DestinationFenceUnavailable
        }
        analytics_engine::AnalyticsEngineError::DuckDb(_) => BackgroundJobError::DestinationBackend,
        analytics_engine::AnalyticsEngineError::Io(_) => BackgroundJobError::DestinationBackend,
        _ => BackgroundJobError::Destination,
    }
}

async fn synchronize_source_progress(
    shared: &SourceJobShared,
    kind: SourceJobKind,
    applied: Option<&PollBatch>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let poller = shared
        .pollers
        .lock()
        .await
        .get(&kind)
        .cloned()
        .ok_or(BackgroundJobError::PollerNotRegistered)?;
    // Hold the cursor lock while reading durable progress so an applied job
    // cannot advance the local cursor and then have it overwritten by an
    // older read. The destination fence remains the authority for progress.
    let mut poller = poller.lock().await;
    match (kind, applied) {
        (SourceJobKind::AuxStorage, _) => {
            let resume = shared
                .engine
                .with_read(|engine| engine.load_system_stream_checkpoint())
                .await??;
            poller.reload_system_resume(
                resume.map(|(scope, checkpoint)| SystemStreamResume::new(scope, checkpoint)),
            )?;
        }
        (SourceJobKind::StorageStream, Some(batch)) => poller.commit_batch(batch),
        (SourceJobKind::StorageStream, None) => {
            let checkpoints = shared
                .engine
                .with_read(|engine| engine.load_source_checkpoints())
                .await??;
            let checkpoints = crate::source_polling::storage_checkpoints_from_engine(checkpoints);
            poller.reload_checkpoints(&checkpoints).await?;
        }
    }
    Ok(())
}
