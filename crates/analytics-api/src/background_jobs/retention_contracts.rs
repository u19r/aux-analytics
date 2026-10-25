use bg_jobs::MAX_INLINE_PAYLOAD_BYTES;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    background_jobs::error::BackgroundJobError,
    constants::{
        HEX, MAX_RETENTION_BATCH_SIZE, MAX_RETENTION_DEADLINE_AT_MS, MAX_RETENTION_SCOPE_BYTES,
    },
};

/// Versioned, neutral retention work. Lifecycle supplies execution identity and
/// fence.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "version", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub(crate) enum RetentionJobPayload {
    V1 {
        analytics_table_name: String,
        cutoff_ms: i64,
        batch_size: u64,
        deadline_at_ms: u64,
        schedule_at_ms: i64,
        server_time_ms: u64,
        cursor: Option<String>,
        batch_id: String,
    },
}

impl RetentionJobPayload {
    pub(crate) fn from_schedule(
        analytics_table_name: &str,
        cutoff_ms: i64,
        batch_size: u64,
        deadline_at_ms: u64,
        cursor: Option<String>,
        schedule_at_ms: i64,
        server_time_ms: u64,
    ) -> Result<Self, BackgroundJobError> {
        let batch_id = retention_batch_identity(
            analytics_table_name,
            cutoff_ms,
            batch_size,
            schedule_at_ms,
            server_time_ms,
            cursor.as_deref(),
        );
        let payload = Self::V1 {
            analytics_table_name: analytics_table_name.to_string(),
            cutoff_ms,
            batch_size,
            deadline_at_ms,
            schedule_at_ms,
            server_time_ms,
            cursor,
            batch_id,
        };
        payload.validate()?;
        Ok(payload)
    }

    pub(crate) fn validate(&self) -> Result<(), BackgroundJobError> {
        validate_retention_text(self.analytics_table_name(), MAX_RETENTION_SCOPE_BYTES)?;
        validate_retention_bounds(
            self.cutoff_ms(),
            self.schedule_at_ms(),
            self.server_time_ms(),
            self.batch_size(),
            self.deadline_at_ms(),
        )?;
        validate_retention_cursor(self.cursor())?;
        validate_retention_identity(self)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, BackgroundJobError> {
        self.validate()?;
        let encoded = serde_json::to_vec(self).map_err(|_| {
            BackgroundJobError::InvalidPayload("retention job payload cannot be encoded")
        })?;
        if encoded.len() > MAX_INLINE_PAYLOAD_BYTES {
            return Err(BackgroundJobError::PayloadTooLarge {
                actual: encoded.len(),
                maximum: MAX_INLINE_PAYLOAD_BYTES,
            });
        }
        Ok(encoded)
    }

    pub(crate) fn decode(payload: &[u8]) -> Result<Self, BackgroundJobError> {
        if payload.len() > MAX_INLINE_PAYLOAD_BYTES {
            return Err(BackgroundJobError::PayloadTooLarge {
                actual: payload.len(),
                maximum: MAX_INLINE_PAYLOAD_BYTES,
            });
        }
        let decoded: Self = serde_json::from_slice(payload).map_err(|_| {
            BackgroundJobError::InvalidPayload("retention job payload is malformed")
        })?;
        decoded.validate()?;
        Ok(decoded)
    }

    pub(crate) fn analytics_table_name(&self) -> &str {
        let Self::V1 {
            analytics_table_name,
            ..
        } = self;
        analytics_table_name
    }

    pub(crate) fn cutoff_ms(&self) -> i64 {
        let Self::V1 { cutoff_ms, .. } = self;
        *cutoff_ms
    }

    pub(crate) fn batch_size(&self) -> u64 {
        let Self::V1 { batch_size, .. } = self;
        *batch_size
    }

    pub(crate) fn deadline_at_ms(&self) -> u64 {
        let Self::V1 { deadline_at_ms, .. } = self;
        *deadline_at_ms
    }

    pub(crate) fn schedule_at_ms(&self) -> i64 {
        let Self::V1 { schedule_at_ms, .. } = self;
        *schedule_at_ms
    }

    pub(crate) fn server_time_ms(&self) -> u64 {
        let Self::V1 { server_time_ms, .. } = self;
        *server_time_ms
    }

    pub(crate) fn cursor(&self) -> Option<&str> {
        let Self::V1 { cursor, .. } = self;
        cursor.as_deref()
    }

    pub(crate) fn batch_id(&self) -> &str {
        let Self::V1 { batch_id, .. } = self;
        batch_id
    }
}

fn validate_retention_bounds(
    cutoff_ms: i64,
    schedule_at_ms: i64,
    server_time_ms: u64,
    batch_size: u64,
    deadline_at_ms: u64,
) -> Result<(), BackgroundJobError> {
    validate_retention_condition(cutoff_ms >= 0, "retention cutoff is invalid")?;
    validate_retention_condition(
        schedule_at_ms >= 0,
        "retention schedule timestamp is invalid",
    )?;
    let schedule_at_ms = u64::try_from(schedule_at_ms).unwrap_or(0);
    validate_retention_condition(
        cutoff_ms == i64::try_from(schedule_at_ms).unwrap_or(-1),
        "retention cutoff must equal its schedule occurrence",
    )?;
    validate_retention_condition(
        server_time_ms > 0 && server_time_ms >= schedule_at_ms && server_time_ms <= deadline_at_ms,
        "retention server time is invalid",
    )?;
    validate_retention_condition(
        batch_size > 0 && batch_size <= MAX_RETENTION_BATCH_SIZE,
        "retention batch bound exceeded",
    )?;
    validate_retention_condition(
        deadline_at_ms > 0 && deadline_at_ms <= MAX_RETENTION_DEADLINE_AT_MS,
        "retention deadline is invalid",
    )
}

fn validate_retention_cursor(cursor: Option<&str>) -> Result<(), BackgroundJobError> {
    if cursor
        .is_some_and(|cursor| validate_retention_text(cursor, MAX_RETENTION_SCOPE_BYTES).is_err())
    {
        return Err(BackgroundJobError::InvalidPayload(
            "retention cursor is invalid",
        ));
    }
    Ok(())
}

fn validate_retention_identity(payload: &RetentionJobPayload) -> Result<(), BackgroundJobError> {
    validate_retention_text(payload.batch_id(), MAX_RETENTION_SCOPE_BYTES)?;
    let expected = retention_batch_identity(
        payload.analytics_table_name(),
        payload.cutoff_ms(),
        payload.batch_size(),
        payload.schedule_at_ms(),
        payload.server_time_ms(),
        payload.cursor(),
    );
    validate_retention_condition(
        payload.batch_id() == expected.as_str(),
        "retention batch identity does not match its contents",
    )
}

fn validate_retention_condition(
    is_valid: bool,
    message: &'static str,
) -> Result<(), BackgroundJobError> {
    if is_valid {
        Ok(())
    } else {
        Err(BackgroundJobError::InvalidPayload(message))
    }
}

fn validate_retention_text(value: &str, maximum: usize) -> Result<(), BackgroundJobError> {
    if value.trim().is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(BackgroundJobError::InvalidPayload(
            "retention payload text field is invalid",
        ));
    }
    Ok(())
}

fn retention_batch_identity(
    analytics_table_name: &str,
    cutoff_ms: i64,
    batch_size: u64,
    schedule_at_ms: i64,
    server_time_ms: u64,
    cursor: Option<&str>,
) -> String {
    let mut input = Vec::new();
    append_identity_field(&mut input, b"analytics-api:retention-batch:v1");
    append_identity_field(&mut input, analytics_table_name.as_bytes());
    input.extend_from_slice(&cutoff_ms.to_be_bytes());
    input.extend_from_slice(&batch_size.to_be_bytes());
    input.extend_from_slice(&schedule_at_ms.to_be_bytes());
    input.extend_from_slice(&server_time_ms.to_be_bytes());
    append_identity_field(&mut input, cursor.unwrap_or_default().as_bytes());
    let digest = Sha256::digest(input);
    let mut identity = String::with_capacity(3 + digest.len() * 2);
    identity.push_str("v1-");
    for byte in digest {
        identity.push(HEX[usize::from(byte >> 4)] as char);
        identity.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    identity
}

fn append_identity_field(input: &mut Vec<u8>, field: &[u8]) {
    input.extend_from_slice(&u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
    input.extend_from_slice(field);
}
