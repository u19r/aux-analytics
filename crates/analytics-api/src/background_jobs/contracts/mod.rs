use analytics_storage::{PollBatch, PolledRecord, SourceCheckpoint, SystemStreamBatch};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage_types::{SystemStreamCheckpointToken, SystemStreamPosition, SystemStreamScope};

use crate::{
    background_jobs::error::BackgroundJobError,
    constants::{
        MAX_SOURCE_CHECKPOINTS, MAX_SOURCE_DEADLINE_AT_MS, MAX_SOURCE_FIELD_BYTES,
        MAX_SOURCE_RECORDS, MAX_SOURCE_SCOPE_BYTES, SOURCE_AUX_JOB_KIND, SOURCE_AUX_QUEUE,
        SOURCE_STREAM_JOB_KIND, SOURCE_STREAM_QUEUE,
    },
};

mod identity;
#[cfg(test)]
mod identity_tests;
mod kind;
mod payload;
mod records;
mod scope;
mod validation;

use identity::batch_identity;
pub(crate) use kind::SourceJobKind;
pub(crate) use scope::{SourceJobScope, SystemStreamCheckpointPredecessor};
pub(crate) use validation::validate_source_table_name;
use validation::{
    validate_batch_id, validate_batch_identity, validate_checkpoints, validate_deadline,
    validate_kind_scope, validate_payload_bounds, validate_records, validate_scope,
};

#[cfg(test)]
mod source_job_performance_tests;
mod source_payload;
use source_payload::BatchIdentityInput;
pub(crate) use source_payload::{SourceJobCheckpoint, SourceJobPayload, SourceJobRecord};
